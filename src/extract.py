import logging
from datetime import datetime
import pandas as pd 
import duckdb

from src.config import DB_PATH,  STREAMS_PER_HOUR
from data.generators.fact_generator import generate_incremental_streams

logger = logging.getLogger(__name__)

def extract_streams(**context):
    start_date = context["data_interval_start"] 
    end_date = context["data_interval_end"]

    with duckdb.connect(str(DB_PATH)) as conn:
        users = conn.execute("SELECT * FROM dim_user WHERE is_current = TRUE").fetchall()
        tracks = conn.execute("SELECT * FROM dim_track").fetchall()
        
        # Lectura segura del último ID de fact_streams
        result = conn.execute("SELECT MAX(stream_sk) FROM fact_streams").fetchone()
        last_stream_sk = result[0] if result and result[0] is not None else 0

        rows_to_generate = STREAMS_PER_HOUR
        
        logger.info(f"Generando {rows_to_generate} streams desde {start_date} hasta {end_date}")
        
        rows_data = generate_incremental_streams(
            users, tracks, start_date, end_date, last_stream_sk, rows_to_generate
        )

        # 1. Limpiamos staging de ejecuciones anteriores fallidas (Idempotencia)
        conn.execute("DELETE FROM stg_streams")

        # 2. Insertamos los nuevos datos en la tabla que YA EXISTE gracias a tu SQL
        if rows_data:
            df = pd.DataFrame(rows_data)
            conn.execute("INSERT INTO stg_streams SELECT * FROM df")

    rows_extracted = len(rows_data)
    
    context["ti"].xcom_push(key="rows_extracted", value=rows_extracted)

    logger.info(f"Extraídos {rows_extracted:,} streams y cargados en stg_streams")
  