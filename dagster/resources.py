from dagster import ConfigurableResource
from dagster_dbt import DbtCliResource
from pathlib import Path

# Rutas base del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = BASE_DIR / "dbt_project"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

class DuckDBResource(ConfigurableResource):
    db_path: str

# Recurso para ejecutar comandos de dbt
class DbtResource(ConfigurableResource):
    def get_cli(self) -> DbtCliResource:
        return DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            # profiles_dir es usualmente ~/.dbt, pero lo dejamos explícito
        )