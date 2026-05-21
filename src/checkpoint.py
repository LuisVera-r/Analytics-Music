import duckdb
import logging 
from src.config import DB_PATH

logger = logging.getLogger(__name__)

# Funcion que revisa el checkpoint

def check_checkpoint(**context):
  
  conn = duckdb.connect(str(DB_PATH))

  conn.execute("""
      CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
        pipeline_name VARCHAR PRIMARY KEY,
        last_run_at   TIMESTAMPTZ,
        rows_loaded   INT
      )
  """)
  result = conn.execute("""
      SELECT last_run_at FROM pipeline_checkpoints
      WHERE pipeline_name = 'fact_streams_hourly'
      """).fetchone()

  last_run_at = result[0] if result else "2026-01-01 00:00:00"

  conn.close()

  context["ti"].xcom_push(key="last_run", value=str(last_run_at))
  logger.info(f"checkpoint leido: {last_run_at}")

# Funcion que actualiza el checkpoint 

def update_checkpoint(**context):
    
    conn = duckdb.connect(str(DB_PATH))
    rows = context["ti"].xcom_pull(
        task_ids = "extract_streams",
        key = "rows_extracted"
    )

    conn.execute("""
        INSERT OR REPLACE INTO pipeline_checkpoints VALUES
        ('fact_streams_hourly', NOW(), ?)
    """, [rows])

    conn.close()

    logger.info(f"Checkpoint actualizado - {rows:,} filas procesadas")
