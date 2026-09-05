import logging
import duckdb

from src.config import DB_PATH

logger = logging.getLogger(__name__)


def validate_data(**context):

    with duckdb.connect(str(DB_PATH)) as conn:

        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM airflow_staging
            """
        ).fetchone()

        rows = result[0] if result is not None else 0

        if rows == 0:
            raise ValueError(
                "Validación fallida: airflow_staging está vacía"
            )

        if rows > 10_000:
            raise ValueError(
                f"Validación fallida: volumen anómalo "
                f"({rows:,} filas)"
            )

        result_dates = conn.execute(
            """
            SELECT COUNT(*)
            FROM airflow_staging s
            LEFT JOIN dim_date d
                ON s.date_sk = d.date_sk
            WHERE d.date_sk IS NULL
            """
        ).fetchone()

        invalid_dates = (
            result_dates[0]
            if result_dates is not None
            else 0
        )

        if invalid_dates > 0:
            raise ValueError(
                f"Validación fallida: "
                f"{invalid_dates} streams tienen date_sk inválidos"
            )

        logger.info(
            f"Validación OK: {rows:,} filas íntegras"
        )