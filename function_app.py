import logging
import azure.functions as func
import os
import pyodbc

app = func.FunctionApp()

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def extract_chamado(myTimer: func.TimerRequest) -> None:
    logging.info('tabela chamado_status_historico')
    
    sql_user = os.getenv("USER", None)
    sql_pass = os.getenv("PASSWORD", None)
    sql_server = os.getenv("HOST", None)
    sql_database = os.getenv("DATABASE", None)

    logging.info(f'servidor={sql_server}, banco de dados={sql_database}, usuario={sql_user}, senha={sql_pass} ')

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
            
            query = "select * from itsm.chamado_status_historico"

            # Executa a consulta SQL
            cursor.execute(query)

            # Busca todos os resultados da consulta
            rows = cursor.fetchall()

            logging.info(rows)

    except Exception as e:
        logging.error(f"Erro ao ler itsm.chamado_status_historico: {str(e)}")
        raise