import logging
import azure.functions as func
import os
import pyodbc as db

app = func.FunctionApp()

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def database_trigger(myTimer: func.TimerRequest) -> None:
    
    user = os.getenv("USER", None)
    password = os.getenv("PASSWORD", None)
    host = os.getenv("HOST", None)
    database = os.getenv("DATABASE", None)

    cnxn = db.connect("DRIVER={Driver 17 for SQL Server};"
                      f"SERVER={host},1433;"
                      f"DATABASE={database};"
                      f"UID={user};"
                      f"PWD={password};"
                      "Encrypt=yes;"
                      "TrustServerCertificate=no;"
                      "Connection Timeout=30;"
            )
    cursor = cnxn.cursor()

    cursor.execute("SELECT * FROM itsm.analista")
    row = cursor.fetchone()
    if row:
        logging.info(row)