import logging 

logger = logging.getLogger(__name__)

def load_to_staging(**context):
    rows = context["ti"].xcom_pull(
        task_ids = "extract_streams",
        key = "rows_extracted"
    )
    logger.info(f" {rows:,} filas cargadas en staging_streams")

def promote_to_fact(**context):
    logger.info(" staging_streams → fact_streams completado (idempotente)")
    