import csv
import io
import logging
import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import azure.functions as func
import pandas as pd
import pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

app = func.FunctionApp()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
CONTAINER = "bronze"
STORAGE_CONNECTION = "adlstapra2026matheus_STORAGE"

# NCRONTAB: {segundo} {minuto} {hora} {dia} {mês} {dia-da-semana}
# ATENÇÃO: o original era "* 1 * * * *" = TODO segundo do minuto 1 de cada hora
# (60 execuções por tabela). Ajuste conforme a frequência desejada, ex.:
#   "0 */5 * * * *" = a cada 5 minutos | "0 0 * * * *" = de hora em hora
SCHEDULE = "0 0 * * * *"

blob_url = os.getenv("BLOBURL")
# TODO: origem e destino hoje usam as MESMAS variáveis (mesmo banco).
# Se o destino for outro banco, separe em SRC_* e DST_*.
src_conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server=tcp:{os.getenv('HOST')};"
    f"Database={os.getenv('DATABASE')};"
    f"Uid={os.getenv('USER')};"
    f"Pwd={os.getenv('PASSWORD')};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)
dst_conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server=tcp:{os.getenv('DST_HOST')};"
    f"Database={os.getenv('DST_DATABASE')};"
    f"Uid={os.getenv('DST_USER')};"
    f"Pwd={os.getenv('DST_PASSWORD')};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

AUDITORIA = ["dt_inclusao", "dt_atualizacao", "nm_sistema_origem", "cd_registro_origem"]

TABELAS = {
    "fila": {
        "colunas": ["id_fila", "cd_fila", "nm_fila", "ds_descricao", "fl_ativo"],
        "frequencia": "horaria",
        "incremental": True,
    },
    "categoria": {
        "colunas": ["id_categoria", "cd_categoria", "nm_categoria", "ds_descricao", "fl_ativo"],
        "frequencia": "horaria",
        "incremental": True,
    },
    "cliente_organizacao": {
        "colunas": [
            "id_cliente_organizacao", "cd_cliente_organizacao",
            "nm_cliente_organizacao", "nr_cnpj", "fl_ativo",
        ],
        "frequencia": "horaria",
        "incremental": True,
        "texto": ["nr_cnpj"],
    },
    "solicitante": {
        "colunas": [
            "id_solicitante", "cd_solicitante", "id_cliente_organizacao",
            "nm_solicitante", "ds_email", "ds_telefone", "fl_ativo",
        ],
        "frequencia": "horaria",
        "incremental": True,
        "texto": ["ds_telefone"],
    },
    "analista": {
        "colunas": [
            "id_analista", "cd_analista", "nm_analista", "ds_email",
            "ds_nivel", "id_fila_atual", "fl_ativo",
        ],
        "frequencia": "horaria",
        "incremental": True,
    },
    "sla": {
        "colunas": ["id_sla", "cd_sla", "nm_sla", "qt_meta_minutos", "ds_descricao", "fl_ativo"],
        "frequencia": "horaria",
        "incremental": True,
    },
    "chamado": {
        "colunas": [
            "id_chamado", "nr_chamado", "ds_tipo_chamado", "ds_status_chamado",
            "ds_prioridade", "dt_criacao", "dt_resolucao", "dt_ultima_atualizacao",
            "id_analista_atual", "id_reporter", "id_categoria",
            "id_cliente_organizacao", "id_fila_atual", "ds_titulo", "ds_descricao",
        ],
        "frequencia": "horaria",
        "incremental": True,
    },
    "chamado_sla": {
        "colunas": [
            "id_chamado_sla", "id_chamado", "id_sla", "fl_breach",
            "qt_tempo_restante_minutos", "qt_tempo_decorrido_minutos",
            "qt_meta_minutos", "dt_referencia",
        ],
        "frequencia": "horaria",
        "incremental": True,
    },
    "chamado_status_historico": {
        "colunas": [
            "id_chamado_status_historico", "id_chamado", "ds_status_chamado",
            "dt_inicio_status", "dt_fim_status", "qt_tempo_status_minutos",
            "id_analista_responsavel", "id_fila",
        ],
        "frequencia": "horaria",
        "incremental": True,
    },
    "csat_avaliacao": {
        "colunas": [
            "id_csat_avaliacao", "id_chamado", "id_analista",
            "nr_score", "ds_comentario", "dt_avaliacao",
        ],
        "frequencia": "horaria",
        "incremental": True,
    },
}
for _cfg in TABELAS.values():
    _cfg["colunas"] = _cfg["colunas"] + AUDITORIA


# ---------------------------------------------------------------------------
# Extração: SQL Server -> bronze (CSV)
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _blob_service() -> BlobServiceClient:
    return BlobServiceClient(
        account_url=f"https://{blob_url}.blob.core.windows.net",
        credential=DefaultAzureCredential(),
    )


def upload_to_bronze(colunas, rows, tabela):
    now = datetime.now(timezone(timedelta(hours=-3)))
    data = now.strftime("%Y/%m/%d")
    blob_name = f"itsm/{tabela}/{data}/{tabela}_{now.strftime('%Y%m%d')}.csv"

    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=",", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(colunas)
    for row in rows:
        writer.writerow(["" if v is None else v for v in row])

    blob_client = _blob_service().get_blob_client(container=CONTAINER, blob=blob_name)
    blob_client.upload_blob(buffer.getvalue().encode("utf-8-sig"), overwrite=True)
    logger.info(f"Upload concluído: {CONTAINER}/{blob_name}")

def _ler_watermark(tabela: str) -> str | None:
    blob = _blob_service().get_blob_client(CONTAINER, f"_controle/{tabela}.txt")
    try:
        return blob.download_blob().readall().decode()
    except ResourceNotFoundError:
        return None  # primeira execução = carga completa

def _gravar_watermark(tabela: str, valor: str):
    blob = _blob_service().get_blob_client(CONTAINER, f"_controle/{tabela}.txt")
    blob.upload_blob(valor.encode(), overwrite=True)

def extrair_tabela(tabela: str):
    cfg = TABELAS[tabela]
    sql, params = f"SELECT * FROM itsm.{tabela}", []

    if cfg.get("incremental"):
        wm = _ler_watermark(tabela)
        if wm:
            sql += " WHERE COALESCE(dt_atualizacao, dt_inclusao) > ?"
            params.append(wm)

    with pyodbc.connect(src_conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        colunas = [c[0] for c in cursor.description]
        rows = cursor.fetchall()

    if not rows:
        logger.info(f"itsm.{tabela}: nada novo")
        return  # sem arquivo, sem trigger, sem MERGE

    upload_to_bronze(colunas, rows, tabela, incremental=cfg.get("incremental", False))

    if cfg.get("incremental"):
        novo = max((r.dt_atualizacao or r.dt_inclusao) for r in rows)
        _gravar_watermark(tabela, novo.isoformat(sep=" "))  # só após o upload dar certo


@app.timer_trigger(
    schedule=SCHEDULE, arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_itsm(myTimer: func.TimerRequest) -> None:
    falhas = []
    for tabela in TABELAS:  # ordem de dependência
        try:
            extrair_tabela(tabela)
        except Exception:
            falhas.append(tabela)  # segue para as demais; erro já foi logado
    if falhas:
        raise RuntimeError(f"Falha na extração das tabelas: {falhas}")

def extrair_grupo(frequencia: str):
    falhas = []
    for tabela, cfg in TABELAS.items():
        if cfg.get("frequencia") == frequencia:
            try:
                extrair_tabela(tabela)
            except Exception:
                falhas.append(tabela)
    if falhas:
        raise RuntimeError(f"Falha na extração: {falhas}")


@app.timer_trigger(schedule="0 0 * * * *", arg_name="t", run_on_startup=False, use_monitor=False)
def extract_horaria(t: func.TimerRequest):
    extrair_grupo("horaria")        # toda hora cheia

@app.timer_trigger(schedule="0 0 3 1 * *", arg_name="t", run_on_startup=False, use_monitor=False)
def extract_mensal(t: func.TimerRequest):
    extrair_grupo("mensal")  

# ---------------------------------------------------------------------------
# Carga: bronze (CSV) -> SQL Server via #staging + MERGE
# ---------------------------------------------------------------------------
def preparar_dados(conteudo: bytes, tabela: str) -> list[tuple]:
    cfg = TABELAS[tabela]
    colunas = cfg["colunas"]

    df = pd.read_csv(
        io.BytesIO(conteudo),
        encoding="utf-8-sig",  # remove o BOM
        dtype={c: str for c in cfg.get("texto", [])},
        dtype_backend="numpy_nullable",  # inteiros com vazios ficam Int64, não float
    )

    faltando = set(colunas) - set(df.columns)
    if faltando:
        raise ValueError(f"Colunas ausentes no CSV de {tabela}: {faltando}")

    df = df[colunas]
    df = df.astype(object).where(df.notna(), None)  # NA/NaN -> None (NULL)
    return [tuple(r) for r in df.itertuples(index=False, name=None)]


def montar_sql(tabela: str) -> tuple[str, str]:
    """Gera o SQL da staging temporária e do MERGE. Os nomes vêm de TABELAS
    (fixo no código), nunca de entrada externa."""
    colunas = TABELAS[tabela]["colunas"]
    pk = colunas[0]
    staging = f"#Staging_{tabela}"

    select_cols = ",\n            ".join(
        f"{c} + 0 AS {c}" if c == pk else c for c in colunas  # "+ 0": não herda IDENTITY
    )
    sets = ",\n                ".join(f"destino.{c} = origem.{c}" for c in colunas[1:])
    lista = ", ".join(colunas)
    valores = ", ".join(f"origem.{c}" for c in colunas)

    sql_temp = f"""
        SELECT TOP 0
            {select_cols}
        INTO {staging}
        FROM itsm.{tabela}
    """
    sql_merge = f"""
        MERGE INTO itsm.{tabela} AS destino
        USING {staging} AS origem
            ON destino.{pk} = origem.{pk}
        WHEN MATCHED THEN
            UPDATE SET
                {sets}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({lista})
            VALUES ({valores});
    """
    return sql_temp, sql_merge


def carregar_tabela(tabela: str, myblob: func.InputStream):
    logger.info(f"Processando {myblob.name}")
    dados = preparar_dados(myblob.read(), tabela)
    colunas = TABELAS[tabela]["colunas"]
    sql_temp, sql_merge = montar_sql(tabela)

    try:
        with pyodbc.connect(src_conn_str) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True

            cursor.execute(sql_temp)
            cursor.executemany(
                f"INSERT INTO #Staging_{tabela} ({', '.join(colunas)}) "
                f"VALUES ({', '.join('?' * len(colunas))})",
                dados,
            )
            cursor.execute(sql_merge)
            conn.commit()

        logger.info(f"{len(dados)} linhas processadas de {myblob.name}")
    except Exception as e:
        logger.error(f"Erro ao carregar {myblob.name}: {e!s}")
        raise


def _path(tabela: str) -> str:
    return f"{CONTAINER}/itsm/{tabela}/{{ano}}/{{mes}}/{{dia}}/{{nome_arquivo}}"


@app.blob_trigger(arg_name="myblob", path=_path("fila"), connection=STORAGE_CONNECTION)
def insert_fila(myblob: func.InputStream):
    carregar_tabela("fila", myblob)


@app.blob_trigger(arg_name="myblob", path=_path("categoria"), connection=STORAGE_CONNECTION)
def insert_categoria(myblob: func.InputStream):
    carregar_tabela("categoria", myblob)


@app.blob_trigger(arg_name="myblob", path=_path("cliente_organizacao"), connection=STORAGE_CONNECTION)
def insert_cliente_organizacao(myblob: func.InputStream):
    carregar_tabela("cliente_organizacao", myblob)


@app.blob_trigger(arg_name="myblob", path=_path("solicitante"), connection=STORAGE_CONNECTION)
def insert_solicitante(myblob: func.InputStream):
    carregar_tabela("solicitante", myblob)


@app.blob_trigger(arg_name="myblob", path=_path("analista"), connection=STORAGE_CONNECTION)
def insert_analista(myblob: func.InputStream):
    carregar_tabela("analista", myblob)


@app.blob_trigger(arg_name="myblob", path=_path("sla"), connection=STORAGE_CONNECTION)
def insert_sla(myblob: func.InputStream):
    carregar_tabela("sla", myblob)


@app.blob_trigger(arg_name="myblob", path=_path("chamado"), connection=STORAGE_CONNECTION)
def insert_chamado(myblob: func.InputStream):
    carregar_tabela("chamado", myblob)


@app.blob_trigger(arg_name="myblob", path=_path("chamado_sla"), connection=STORAGE_CONNECTION)
def insert_chamado_sla(myblob: func.InputStream):
    carregar_tabela("chamado_sla", myblob)


@app.blob_trigger(arg_name="myblob", path=_path("chamado_status_historico"), connection=STORAGE_CONNECTION)
def insert_chamado_status_historico(myblob: func.InputStream):
    carregar_tabela("chamado_status_historico", myblob)


@app.blob_trigger(arg_name="myblob", path=_path("csat_avaliacao"), connection=STORAGE_CONNECTION)
def insert_csat_avaliacao(myblob: func.InputStream):
    carregar_tabela("csat_avaliacao", myblob)
