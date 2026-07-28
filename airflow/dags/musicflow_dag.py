from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


from src.checkpoint import (
    check_checkpoint,
    update_checkpoint
)
from src.extract import extract_streams
from src.validate import validate_data
from src.load import (
    load_to_staging, 
    promote_to_fact
)


#---------Configuraciones de produccion--------------

default_args = {
  "retries": 3,
  "retry_delay": timedelta(minutes = 2),
  "execution_timeout": timedelta(minutes = 30)
}

########### DEFINICION DEL DAG #################

with DAG(
    dag_id = "musicflow_hourly_load",
    schedule = "@hourly",
    start_date = datetime(2026,1,1),
    catchup = False,
    max_active_runs= 1,
    default_args = default_args,
    tags=["musicflow", "batch"],
    doc_md="""
    ## MusicFlow Hourly Load
    Pipeline batch que carga nuevos streams cada hora.
    Implementa checkpoint, validación, staging e idempotencia.
    """
) as dag:
    
    t1 = PythonOperator(
        task_id="check_checkpoint",
        python_callable=check_checkpoint
        )
    
    t2 = PythonOperator(
        task_id="extract_streams",   
        python_callable=extract_streams
        )
    
    t3 = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
        )
    
    t4 = PythonOperator(
        task_id="load_to_staging",  
        python_callable=load_to_staging
        )
    
    t5 = PythonOperator(
        task_id="promote_to_fact",
        python_callable=promote_to_fact
        )
    
    t6 = PythonOperator(
        task_id="update_checkpoint", 
        python_callable=update_checkpoint
        )

    # Orden de ejecucion de las tareas
    
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 # type: ignore
