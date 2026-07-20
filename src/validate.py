import logging
import duckdb

from src.config import DB_PATH

logger = logging.getLogger(__name__)

def validate_data(**context):
    with duckdb.connect(str(DB_PATH)) as conn:
        # Validamos directamente sobre la tabla de staging
        result = conn.execute("SELECT COUNT(*) FROM stg_streams").fetchone()
        rows = result[0] if result is not None else 0

        if rows == 0:
            raise ValueError("Validación fallida: stg_streams está vacía — posible falla en la fuente")
        elif rows > 10_000: # Límite razonable para 1 hora
            raise ValueError(f"Validación fallida: Volumen anómalo en staging ({rows:,} filas)")
        
        # 2. Validar integridad referencial de forma segura
        result_dates = conn.execute("""
            SELECT COUNT(*) FROM stg_streams s
            LEFT JOIN dim_date d ON s.date_sk = d.date_sk
            WHERE d.date_sk IS NULL
        """).fetchone()
        invalid_dates = result_dates[0] if result_dates is not None else 0
        
        if invalid_dates > 0:
            raise ValueError(f"Validación fallida: {invalid_dates} streams tienen date_sk inválidos")

    logger.info(f"Validación OK: {rows:,} filas en staging son íntegras")