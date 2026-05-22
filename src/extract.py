import random
import logging

logger = logging.getLogger(__name__)

def extract_streams (**context):
    last_run = context["ti"].xcom_pull(
        task_ids="check_checkpoint",
        key= "last_run"
    )

    #simula extraccion de nuevos streams
    new_rows = random.randint(1000,5000)
    context["ti"].xcom_push(key= "rows_extracted", value=new_rows)
    logger.info(f" Extraidas{new_rows:,} filas desde {last_run}")
