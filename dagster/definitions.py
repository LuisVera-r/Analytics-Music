from dagster import Definitions
from dagster_dbt import DbtCliResource
from pathlib import Path

from dagster_project.assets import musicflow_dbt_assets, fact_streams_source
from dagster_project.resources import DuckDBResource, DbtResource, DBT_PROJECT_DIR

DB_PATH = str(Path(__file__).resolve().parent.parent / "storage" / "musicflow.duckdb")

defs = Definitions(
    assets=[fact_streams_source, musicflow_dbt_assets],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
        "duckdb": DuckDBResource(db_path=DB_PATH),
    },
)