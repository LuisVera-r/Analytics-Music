import logging
import pandas as pd
import duckdb

from src.config import (
    DB_PATH,
    STREAMS_PER_HOUR,
)

from data.generators.fact_generator import generate_incremental_streams


logger = logging.getLogger(__name__)


def extract_streams(**context):

    start_date = context["data_interval_start"]
    end_date = context["data_interval_end"]

    # Las ejecuciones manuales pueden tener intervalo 0
    if end_date <= start_date:
        end_date = start_date.add(hours=1)

    with duckdb.connect(str(DB_PATH)) as conn:

        users = conn.execute(
            """
            SELECT *
            FROM dim_user
            WHERE is_current = TRUE
            """
        ).fetchall()

        tracks = conn.execute(
            """
            SELECT *
            FROM dim_track
            """
        ).fetchall()

        result = conn.execute(
            """
            SELECT COALESCE(MAX(stream_sk), 0)
            FROM fact_streams
            """
        ).fetchone()

        last_stream_sk = result[0]

        rows_to_generate = STREAMS_PER_HOUR

        logger.info(
            f"""
================ MUSICFLOW =================

Ventana de extracción:
{start_date} --> {end_date}

Streams a generar:
{rows_to_generate}

Último stream_sk:
{last_stream_sk}

=============================================
"""
        )

        rows_data = generate_incremental_streams(
            users=users,
            tracks=tracks,
            start_date=start_date,
            end_date=end_date,
            last_stream_sk=last_stream_sk,
            num_streams=rows_to_generate,
        )

        # Limpiar staging físico
        conn.execute("DELETE FROM airflow_staging")

        if rows_data:

            df = pd.DataFrame(rows_data)

            conn.execute(
                """
                INSERT INTO airflow_staging
                SELECT *
                FROM df
                """
            )

    rows_extracted = len(rows_data)

    context["ti"].xcom_push(
        key="rows_extracted",
        value=rows_extracted,
    )

    logger.info(
        f"Extraídos {rows_extracted:,} streams "
        f"y cargados en airflow_staging"
    )