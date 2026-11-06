import csv
import io
import logging
import os
from datetime import datetime, timedelta, timezone

import azure.functions as func
import pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

logger = logging.getLogger(__name__)

blob_url = os.getenv("BLOBURL")
storage_conn = os.getenv("STORAGECONN")
container_name = "bronze"


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
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(f"select * from itsm.{tabela}")
            colunas = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            upload_to_bronze(colunas, rows, tabela)
    except Exception as e:
        logger.error(f"Erro ao ler itsm.{tabela}: {e!s}")
        raise


########## ------------------- ANALISTA ------------------- ##########
@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_analista(myTimer: func.TimerRequest) -> None:
    extrair_tabela("analista")


########## ------------------- CATEGORIA ------------------- ##########
@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    extrair_tabela("categoria")


########## ------------------- CHAMADO ------------------- ##########
@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    extrair_tabela("chamado")


########## ------------------- CHAMADO_SLA ------------------- ##########
@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    extrair_tabela("chamado_sla")


########## ------------------- CHAMADO_STATUS_HISTÓRICO ------------------- ##########
@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    extrair_tabela("chamado_status_historico")


########## ------------------- CLIENTE_ORGANIZACAO ------------------- ##########
@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    extrair_tabela("cliente_organizacao")


########## ------------------- CSAT_AVALIACAO ------------------- ##########
@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_csta_avaliacao(myTimer: func.TimerRequest) -> None:
    extrair_tabela("csta_avaliacao")


########## ------------------- FILA ------------------- ##########
@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_fila(myTimer: func.TimerRequest) -> None:
    extrair_tabela("fila")


########## ------------------- SLA ------------------- ##########
@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_sla(myTimer: func.TimerRequest) -> None:
    extrair_tabela("sla")


########## ------------------- SOLICITANTE ------------------- ##########
@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    extrair_tabela("solicitante")
