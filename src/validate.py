import logging

logger = logging.getLogger(__name__)

def validate_data(**context):
    rows = context["ti"].xcom_pull(
        task_ids = "extract_streams",
        key = "rows_extracted"
    )

    if rows == 0:
      raise ValueError("Sin datos nuevos — posible falla en la fuente")
    elif rows > 1_000_000:
      raise ValueError(f"Volumen anómalo: {rows:,} filas — revisar fuente")
    else:
       logger.info(f" Validación OK: {rows:,} filas dentro de rangos esperados")