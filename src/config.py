from pathlib import Path
from datetime import datetime


# ==========================
# Rutas del proyecto
# ==========================


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "storage" / "musicflow.duckdb"


# ==========================
# Configuración del pipeline
# ==========================

PIPELINE_NAME = "fact_streams_hourly"

INITIAL_CHECKPOINT = datetime(2026, 1, 1, 0, 0, 0)

# ==========================
# Configuración de extracción
# ==========================

BATCH_SIZE = 50_000

WEEKLY_STREAMS = 50_000
# 50,000 streams / 168 horas en una semana = ~298 streams por hora
STREAMS_PER_HOUR = int(WEEKLY_STREAMS / 168) 

# ==========================
# Validaciones
# ==========================

MAX_ROWS_ALLOWED = 1_000_000


#******Conexion a Duckdb************

import duckdb

def get_duckdb_conn():
    """Devuelve una conexión segura a DuckDB. 
    Recuerda llamar a conn.close() al terminar."""
    return duckdb.connect(str(DB_PATH))