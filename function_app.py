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

########## EXTRAçÃO de Dados ##########

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

########## INSERÇÃO de Dados ##########

TABELAS = {
    "analista": {
        "colunas": [
            "id_analista",
            "cd_analista",
            "nm_analista",
            "ds_email",
            "ds_nivel",
            "id_fila_atual",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    },
    "categoria": {
        "colunas": [
            "id_categoria",
            "cd_categoria",
            "nm_categoria",
            "ds_descricao",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    },
    "chamado": {
        "colunas": [
            "id_chamado",
            "nr_chamado",
            "ds_tipo_chamado",
            "ds_status_chamado",
            "ds_prioridade",
            "dt_criacao",
            "dt_resolucao",
            "dt_ultima_atualizacao",
            "id_analista_atual",
            "id_reporter",
            "id_categoria",
            "id_cliente_organizacao",
            "id_fila_atual",
            "ds_titulo",
            "ds_descricao",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    },
    "chamado_sla": {
        "colunas": [
            "id_chamado_sla",
            "id_chamado",
            "id_sla",
            "fl_breach",
            "qt_tempo_restante_minutos",
            "qt_tempo_decorrido_minutos",
            "qt_meta_minutos",
            "dt_referencia",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    },
    "chamado_status_historico": {
        "colunas": [
            "id_chamado_status_historico",
            "id_chamado",
            "ds_status_chamado",
            "dt_inicio_status",
            "dt_fim_status",
            "qt_tempo_status_minutos",
            "id_analista_responsavel",
            "id_fila",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    },
    "cliente_organizacao": {
        "colunas": [
            "id_cliente_organizacao",
            "cd_cliente_organizacao",
            "nm_cliente_organizacao",
            "nr_cnpj",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
        "dtype": {"nr_cnpj": str},  # preserva zeros à esquerda
    },
    "csat_avaliacao": {
        "colunas": [
            "id_csat_avaliacao",
            "id_chamado",
            "id_analista",
            "nr_score",
            "ds_comentario",
            "dt_avaliacao",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    },
    "fila": {
        "colunas": [
            "id_fila",
            "cd_fila",
            "nm_fila",
            "ds_descricao",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    },
    "sla": {
        "colunas": [
            "id_sla",
            "cd_sla",
            "nm_sla",
            "qt_meta_minutos",
            "ds_descricao",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
    },
    "solicitante": {
        "colunas": [
            "id_solicitante",
            "cd_solicitante",
            "id_cliente_organizacao",
            "nm_solicitante",
            "ds_email",
            "ds_telefone",
            "fl_ativo",
            "dt_inclusao",
            "dt_atualizacao",
            "nm_sistema_origem",
            "cd_registro_origem",
        ],
        "dtype": {"ds_telefone": str},
    },
}

def inserir_tabela(tabela, colunas, sql_temp, sql_merge, myblob, dados):
    try:
        with pyodbc.connect(dst_conn_str) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True

            cursor.execute(sql_temp)
            cursor.executemany(
                f"""INSERT INTO {tabela} ({", ".join(colunas)})
                    VALUES ({", ".join("?" * len(colunas))})""",
                dados,
            )
            cursor.execute(sql_merge)
            conn.commit()

        logger.info(f"{len(dados)} linhas processadas de {myblob.name}")
    except Exception as e:
        logger.error(f"Erro ao carregar {myblob.name}: {e!s}")
        raise

def montar_sql(tabela: str, colunas: list[str]):
    """Gera o #staging e o MERGE a partir da lista de colunas (1ª = chave)."""
    pk, resto = colunas[0], colunas[1:]
    temp = f"#Staging_{tabela}"
 
    # "+ 0" remove a propriedade IDENTITY na temporária, permitindo inserir o id
    sql_temp = f"""
        SELECT TOP 0
            {pk} + 0 AS {pk},
            {", ".join(resto)}
        INTO {temp}
        FROM itsm.{tabela}
    """
 
    sets = ",\n                ".join(f"destino.{c} = origem.{c}" for c in resto)
    sql_merge = f"""
        MERGE INTO itsm.{tabela} AS destino
        USING {temp} AS origem
            ON destino.{pk} = origem.{pk}
 
        WHEN MATCHED THEN
            UPDATE SET
                {sets}
 
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({", ".join(colunas)})
            VALUES ({", ".join(f"origem.{c}" for c in colunas)});
    """
    return temp, sql_temp, sql_merge

def carregar_csv(myblob: func.InputStream, tabela: str):
    cfg = TABELAS[tabela]
    colunas = cfg["colunas"]
    logger.info(f"Processando {myblob.name}")
 
    df = pd.read_csv(
        io.BytesIO(myblob.read()),
        encoding="utf-8-sig",
        dtype=cfg.get("dtype"),
    )
 
    faltando = set(colunas) - set(df.columns)
    if faltando:
        raise ValueError(f"Colunas ausentes no CSV: {faltando}")
 
    df = df[colunas]
    df = df.astype(object).where(df.notna(), None)
    dados = [tuple(r) for r in df.itertuples(index=False, name=None)]
 
    temp, sql_temp, sql_merge = montar_sql(tabela, colunas)
    inserir_tabela(temp, colunas, sql_temp, sql_merge, myblob, dados)
 

@app.blob_trigger(
    arg_name="myblob",
    path="bronze/itsm/analista/{ano}/{mes}/{dia}/{nome_arquivo}",
    connection="adlstapra2026matheus_STORAGE",
)
def insert_analista(myblob: func.InputStream):
    carregar_csv(myblob, "analista")


@app.blob_trigger(
    arg_name="myblob",
    path="bronze/itsm/categoria/{ano}/{mes}/{dia}/{nome_arquivo}",
    connection="adlstapra2026matheus_STORAGE",
)
def insert_categoria(myblob: func.InputStream):
    carregar_csv(myblob, "categoria")


@app.blob_trigger(
    arg_name="myblob",
    path="bronze/itsm/chamado/{ano}/{mes}/{dia}/{nome_arquivo}",
    connection="adlstapra2026matheus_STORAGE",
)
def insert_chamado(myblob: func.InputStream):
    carregar_csv(myblob, "chamado")


@app.blob_trigger(
    arg_name="myblob",
    path="bronze/itsm/chamado_sla/{ano}/{mes}/{dia}/{nome_arquivo}",
    connection="adlstapra2026matheus_STORAGE",
)
def insert_chamado_sla(myblob: func.InputStream):
    carregar_csv(myblob, "chamado_sla")


@app.blob_trigger(
    arg_name="myblob",
    path="bronze/itsm/chamado_status_historico/{ano}/{mes}/{dia}/{nome_arquivo}",
    connection="adlstapra2026matheus_STORAGE",
)
def insert_chamado_status_historico(myblob: func.InputStream):
    carregar_csv(myblob, "chamado_status_historico")


@app.blob_trigger(
    arg_name="myblob",
    path="bronze/itsm/cliente_organizacao/{ano}/{mes}/{dia}/{nome_arquivo}",
    connection="adlstapra2026matheus_STORAGE",
)
def insert_cliente_organizacao(myblob: func.InputStream):
    carregar_csv(myblob, "cliente_organizacao")

@app.blob_trigger(
    arg_name="myblob",
    path="bronze/itsm/csat_avaliacao/{ano}/{mes}/{dia}/{nome_arquivo}",
    connection="adlstapra2026matheus_STORAGE",
)
def insert_csat_avaliacao(myblob: func.InputStream):
    carregar_csv(myblob, "csat_avaliacao")


@app.blob_trigger(
    arg_name="myblob",
    path="bronze/itsm/fila/{ano}/{mes}/{dia}/{nome_arquivo}",
    connection="adlstapra2026matheus_STORAGE",
)
def insert_fila(myblob: func.InputStream):
    carregar_csv(myblob, "fila")


@app.blob_trigger(
    arg_name="myblob",
    path="bronze/itsm/sla/{ano}/{mes}/{dia}/{nome_arquivo}",
    connection="adlstapra2026matheus_STORAGE",
)
def insert_sla(myblob: func.InputStream):
    carregar_csv(myblob, "sla")


@app.blob_trigger(
    arg_name="myblob",
    path="bronze/itsm/solicitante/{ano}/{mes}/{dia}/{nome_arquivo}",
    connection="adlstapra2026matheus_STORAGE",
)
def insert_solicitante(myblob: func.InputStream):
    carregar_csv(myblob, "solicitante")