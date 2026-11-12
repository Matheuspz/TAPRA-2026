import csv
import io
import logging
import os
from datetime import datetime, timedelta, timezone

import azure.functions as func
import pandas as pd
import pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

logger = logging.getLogger(__name__)

blob_url = os.getenv("BLOBURL")
storage_conn = os.getenv("STORAGECONN")
container_name = "bronze"

dst_user = os.getenv("DST_USER")
dst_pass = os.getenv("DST_PASSWORD")
dst_server = os.getenv("DST_HOST")
dst_database = os.getenv("DST_DATABASE")

sql_user = os.getenv("USER")
sql_pass = os.getenv("PASSWORD")
sql_server = os.getenv("HOST")
sql_database = os.getenv("DATABASE")

dst_conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server=tcp:{dst_server};"
    f"Database={dst_database};"
    f"Uid={dst_user};"
    f"Pwd={dst_pass};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

src_conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={sql_server};"
    f"DATABASE={sql_database};"
    f"UID={sql_user};"
    f"PWD={sql_pass};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

def upload_to_bronze(colunas, rows, tabela):
    account_url = f"https://{blob_url}.blob.core.windows.net"
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=credential
    )

    utc_menos_3 = timezone(timedelta(hours=-3))
    now = datetime.now(utc_menos_3)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    blob_name = f"itsm/{tabela}/{year}/{month}/{day}/{tabela}_{year}{month}{day}.csv"

    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )

    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=",", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(colunas)
    for row in rows:
        writer.writerow(["" if v is None else v for v in row])

    blob_client.upload_blob(buffer.getvalue().encode("utf-8-sig"), overwrite=True)
    logger.info(f"Upload concluído: {container_name}/{blob_name}")


def extrair_tabela(tabela: str):
    try:
        with pyodbc.connect(src_conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(f"select * from itsm.{tabela}")
            colunas = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            upload_to_bronze(colunas, rows, tabela)
    except Exception as e:
        logger.error(f"Erro ao ler itsm.{tabela}: {e!s}")
        raise

def get_blob_service_client():
    account_url = f"https://{blob_url}.blob.core.windows.net"
    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url=account_url, credential=credential)


def get_latest_bronze_blob(tabela: str) -> str | None:
    """Retorna o nome do blob mais recente da tabela no container bronze."""
    client = get_blob_service_client()
    container_client = client.get_container_client(container_name)

    prefix = f"itsm/{tabela}/"
    blobs = list(container_client.list_blobs(name_starts_with=prefix))

    if not blobs:
        logger.warning(f"Nenhum arquivo encontrado no bronze para a tabela: {tabela}")
        return None

    # Pega o mais recente pelo nome do blob
    latest = max(blobs, key=lambda b: b.name)
    return latest.name


def download_csv_from_bronze(blob_name: str) -> pd.DataFrame:
    """Baixa o CSV do bronze e retorna um DataFrame."""
    client = get_blob_service_client()
    blob_client = client.get_blob_client(container=container_name, blob=blob_name)

    data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data), dtype=str)  # tudo como string evita problemas de tipo
    df = df.where(pd.notnull(df), None)            # converte NaN para None
    return df


def load_to_staging(df: pd.DataFrame, tabela: str):
    """Carrega o DataFrame na tabela de staging (stg_<tabela>)."""
    staging_table = f"stg_{tabela}"

    with pyodbc.connect(dst_conn_str) as conn:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        # Limpa a staging
        cursor.execute(f"TRUNCATE TABLE dbo.{staging_table}")

        if df.empty:
            logger.warning(f"DataFrame vazio para {tabela}. Staging limpa.")
            conn.commit()
            return

        # Monta o INSERT dinâmico
        cols = ", ".join([f"[{col}]" for col in df.columns])
        placeholders = ", ".join(["?" for _ in df.columns])
        insert_sql = f"INSERT INTO dbo.{staging_table} ({cols}) VALUES ({placeholders})"

        # Prepara os dados
        data = [
            tuple(None if pd.isna(v) else v for v in row)
            for row in df.itertuples(index=False, name=None)
        ]

        cursor.executemany(insert_sql, data)
        conn.commit()
        logger.info(f"Staging {staging_table}: {len(df)} linhas carregadas com sucesso.")


def execute_merge(tabela: str):
    """Executa a stored procedure de MERGE correspondente."""
    sp_name = f"usp_merge_{tabela}"

    with pyodbc.connect(dst_conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(f"EXEC dbo.{sp_name}")
        conn.commit()
        logger.info(f"MERGE executado com sucesso: {sp_name}")


def carregar_tabela(tabela: str):
    """Fluxo completo: Bronze → Staging → MERGE"""
    try:
        logger.info(f"Iniciando carga da tabela: {tabela}")

        blob_name = get_latest_bronze_blob(tabela)
        if not blob_name:
            return

        logger.info(f"Arquivo encontrado: {blob_name}")

        df = download_csv_from_bronze(blob_name)
        logger.info(f"CSV baixado. Total de linhas: {len(df)}")

        load_to_staging(df, tabela)
        execute_merge(tabela)

        logger.info(f"Carga + MERGE concluídos com sucesso para a tabela: {tabela}")

    except Exception as e:
        logger.error(f"Erro ao processar a tabela {tabela}: {e!s}", exc_info=True)
        raise

@app.timer_trigger(
    schedule="0 30 1 * * *",   # Todo dia às 01:30
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def load_analista(myTimer: func.TimerRequest) -> None:
    carregar_tabela("analista")


@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def load_categoria(myTimer: func.TimerRequest) -> None:
    carregar_tabela("categoria")


@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def load_chamado(myTimer: func.TimerRequest) -> None:
    carregar_tabela("chamado")


@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def load_chamado_sla(myTimer: func.TimerRequest) -> None:
    carregar_tabela("chamado_sla")


@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def load_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    carregar_tabela("chamado_status_historico")


@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def load_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    carregar_tabela("cliente_organizacao")


@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def load_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    carregar_tabela("csat_avaliacao")


@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def load_fila(myTimer: func.TimerRequest) -> None:
    carregar_tabela("fila")


@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def load_sla(myTimer: func.TimerRequest) -> None:
    carregar_tabela("sla")


@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def load_solicitante(myTimer: func.TimerRequest) -> None:
    carregar_tabela("solicitante")

@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_analista(myTimer: func.TimerRequest) -> None:
    extrair_tabela("analista")


@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    extrair_tabela("categoria")


@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    extrair_tabela("chamado")


@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    extrair_tabela("chamado_sla")


@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    extrair_tabela("chamado_status_historico")


@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    extrair_tabela("cliente_organizacao")


@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    extrair_tabela("csat_avaliacao")


@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_fila(myTimer: func.TimerRequest) -> None:
    extrair_tabela("fila")


@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_sla(myTimer: func.TimerRequest) -> None:
    extrair_tabela("sla")


@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    extrair_tabela("solicitante")