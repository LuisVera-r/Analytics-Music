import logging 
import duckdb 
from src.config import DB_PATH

logger = logging.getLogger(__name__)

def load_to_staging(**context):
    rows_expected = context["ti"].xcom_pull(task_ids="extract_streams", key="rows_extracted")
    
    with duckdb.connect(str(DB_PATH)) as conn:
        # Validación real: contar cuántas filas hay en la tabla de staging
       
        result = conn.execute("SELECT COUNT(*) FROM stg_streams").fetchone()
        rows_in_staging = result[0] if result is not None else 0
        if rows_in_staging != rows_expected:
            raise ValueError(f"Error de staging: Se esperaban {rows_expected} filas pero hay {rows_in_staging}")
            
    logger.info(f"Staging OK: {rows_in_staging:,} filas validadas en tabla stg_streams")


def promote_to_fact(**context):
    with duckdb.connect(str(DB_PATH)) as conn:
        # Movimiento de datos Staging -> Producción
        # Usamos LEFT JOIN para evitar duplicados (Idempotencia) por si el DAG se re-ejecuta
        conn.execute("""
            INSERT INTO fact_streams
            SELECT s.* 
            FROM stg_streams s
            LEFT JOIN fact_streams f ON s.stream_sk = f.stream_sk
            WHERE f.stream_sk IS NULL;
        """)
        
        # Limpiamos la tabla de staging para la próxima hora
        conn.execute("DELETE FROM stg_streams")
        
    logger.info("stg_streams → fact_streams completado y staging limpiado")