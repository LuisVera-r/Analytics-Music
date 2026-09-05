from dagster import asset, SourceAsset, AssetIn, Definitions
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator, DagsterDbtTranslatorSettings
from pathlib import Path

from dagster_project.resources import DBT_PROJECT_DIR, DBT_MANIFEST_PATH

# 1. Definimos la tabla que Airflow llena como un "Source Asset" 
# Esto permite que el grafo de Dagster sepa de dónde vienen los datos crudos
fact_streams_source = SourceAsset(
    key="fact_streams",
    description="Tabla cruda de streams inyectada por Airflow cada hora",
)

# 2. Traductor para que los assets de dbt se vean bien y se agrupen en Dagster
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        # Agrupa los modelos en la UI de Dagster según su carpeta en dbt
        return dbt_resource_props.get("fqn", ["default"])[-2]  # ej: 'staging' o 'marts'

    def get_asset_key(self, dbt_resource_props):
        # Usa el nombre del modelo dbt como clave del asset en Dagster
        return super().get_asset_key(dbt_resource_props)

# 3. El decorador que lo transforma todo
@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=CustomDagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    ),
)
def musicflow_dbt_assets(context, dbt: DbtCliResource):
    # Esto ejecuta efectivamente: dbt build --select <modelos_a_ejecutar>
    yield from dbt.cli(["build"], context=context).stream()