import logging 
import duckdb 
from src.config import DB_PATH

logger = logging.getLogger(__name__)

def load_to_staging(**context):

    rows_expected = context["ti"].xcom_pull(
        task_ids="extract_streams",
        key="rows_extracted"
    )

    with duckdb.connect(str(DB_PATH)) as conn:

        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM airflow_staging
            """
        ).fetchone()

        rows_in_staging = (
            result[0]
            if result is not None
            else 0
        )

        if rows_in_staging != rows_expected:
            raise ValueError(
                f"Error de staging: "
                f"se esperaban {rows_expected} "
                f"pero hay {rows_in_staging}"
            )

        logger.info(
            f"Staging OK: "
            f"{rows_in_staging:,} filas"
        )

def promote_to_fact(**context):

    with duckdb.connect(str(DB_PATH)) as conn:

        conn.execute(
            """
            INSERT INTO fact_streams (
                stream_sk,
                user_sk,
                track_sk,
                date_sk,
                listened_seconds,
                play_count,
                was_skipped,
                device_type
            )
            SELECT
                s.stream_sk,
                s.user_sk,
                s.track_sk,
                s.date_sk,
                s.listened_seconds,
                s.play_count,
                s.was_skipped,
                s.device_type
            FROM airflow_staging s
            LEFT JOIN fact_streams f
                ON s.stream_sk = f.stream_sk
            WHERE f.stream_sk IS NULL
            """
        )

        conn.execute(
            "DELETE FROM airflow_staging"
        )

        logger.info(
            "airflow_staging → fact_streams completado"
        )