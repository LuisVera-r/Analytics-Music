import duckdb

import logging 

from src.config import (
    DB_PATH,
    PIPELINE_NAME,
    INITIAL_CHECKPOINT,
)

logger = logging.getLogger(__name__)


def check_checkpoint(**context):
    # Airflow maneja el tiempo, pero podemos registrar el inicio para logs
    logger.info(f"Inicio de ejecución lógica: {context['data_interval_start']}")

def update_checkpoint(**context):
    # Actualiza el checkpoint después de una carga exitosa.
    rows_loaded = context["ti"].xcom_pull(
        task_ids="extract_streams",
        key="rows_extracted",
    )

    with duckdb.connect(str(DB_PATH)) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO pipeline_checkpoints (
                pipeline_name,
                last_run_at,
                rows_loaded
            )
            VALUES (?, NOW(), ?)
            """,
            [PIPELINE_NAME, rows_loaded],
        )
        
    logger.info(
        f"Checkpoint actualizado: {rows_loaded:,} filas procesadas."
    )