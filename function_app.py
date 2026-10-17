import json
import logging
import os
from datetime import datetime, timedelta, timezone

import azure.functions as func
import pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

tabela = "analista"

logger = logging.getLogger(__name__)

blob_url = os.getenv("BLOBURL")
storage_conn = os.getenv("STORAGECONN")
container_name = "bronze"


def upload_to_bronze(dados):
    account_url = f"https://{blob_url}.blob.core.windows.net"

    credential = DefaultAzureCredential()

    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=credential
    )

    utc_menos_3 = timezone(timedelta(hours=-3))

    now = datetime.now(utc_menos_3)
    year = now.strftime("%Y")
    day = now.strftime("%d")
    mounth = now.strftime("%m")

    blob_name = f"itsm/{tabela}/{year}/{mounth}/{day}/{tabela}_{year}{mounth}{day}.json"

    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )

    conteudo = json.dumps(dados, ensure_ascii=False, default=str)

    blob_client.upload_blob(conteudo.encode("utf-8"), overwrite=True)
    logger.info(f"Upload concluído: {container_name}/{blob_name}")


@app.timer_trigger(
    schedule="* 1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_analista(myTimer: func.TimerRequest) -> None:
    logger.info(f"tabela {tabela}")

    sql_user = os.getenv("USER", None)
    sql_pass = os.getenv("PASSWORD", None)
    sql_server = os.getenv("HOST", None)
    sql_database = os.getenv("DATABASE", None)

    logger.info(
        f"servidor={sql_server}, banco de dados={sql_database}, usuario={sql_user}, senha={sql_pass} "
    )

    # Configura a string de conexão para o banco de dados SQL Server
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
        # Estabelece a conexão com o banco de dados usando pyodbc
        with pyodbc.connect(conn_str) as conn:
            # Cria um cursor para executar a consulta
            cursor = conn.cursor()

            query = f"select * from itsm.{tabela}"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()

            # ENVIA PARA O BLOB STORAGE bronze
            upload_to_bronze(rows)
            logger.info("Enviado para o blob")

    except Exception as e:
        logger.error(f"Erro ao ler itsm.{tabela}: {e!s}")
        raise
