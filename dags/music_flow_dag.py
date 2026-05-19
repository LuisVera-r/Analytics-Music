!pip install apache-airflow duckdb -q
!airflow db migrate #inicializa la base de datos de airflow
print("Airflow listo")

dag_code = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb, random

#---------Configuraciones de produccion--------------

default_args = {
  "retries": 3,
  "retry_delay": timedelta(minutes = 2),
  "execution_timeout": timedelta(minutes = 30)
}

#************** FUNCIONES DE CADA TAREA ********************

def check_checkpoint(**context):
  conn = duckdb.connect('musicflow.duckdb')
  conn.execute("""
      CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
        pipeline_name VARCHAR PRIMARY KEY,
        last_run_at   TIMESTAMPTZ,
        rows_loaded   INT
      )
  """)
  result = conn.execute("""
      SELECT last_run_at FROM pipeline_checkpoints
      WHERE pipeline_name = 'fact_streams_hourly'
      """).fetchone()

  last_run_at = result[0] if result else "2026-01-01 00:00:00"
  context["ti"].xcom_push(key="last_run", value=str(last_run_at))
  print(f" Checkpoint leido: {last_run_at}")

def extract_streams (**context):
    last_run = context["ti"].xcom_pull(
        task_ids="check_checkpoint",
        key= "last_run"
    )

    #simula extraccion de nuevos streams
    new_rows = random.randint(1000,5000)
    context["ti"].xcom_push(key= "rows_extracted", value=new_rows)
    print(f" Extraidas{new_rows:,} filas desde {last_run}")

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
      print(f" Validación OK: {rows:,} filas dentro de rangos esperados")

def load_to_staging(**context):
    rows = context["ti"].xcom_pull(
        task_ids = "extract_streams",
        key = "rows_extracted"
    )
    print(f" {rows:,} filas cargadas en staging_streams")

def promote_to_fact(**context):
    print(" staging_streams → fact_streams completado (idempotente)")

def update_checkpoint(**context):
    conn = duckdb.connect('muisicflow.duckdb')
    rows = context["ti"].xcom_pull(
        task_ids = "extract_streams",
        key = "rows_extracted"
    )

    conn.execute("""
        INSERT OR REPLACE INTO pipeline_checkpoints VALUES
        ('fact_streams_hourly', NOW(), ?)
    """, [rows])
    print(f" Checkpoint actualizado — {rows:,} filas procesadas")

########### DEFINICION DEL DAG #################

with DAG(
    dag_id = "musicflow_hourly_load",
    schedule = "@hourly",
    start_date = datetime(2026,1,1),
    catchup = True,
    max_active_runs= 1,
    default_args = default_args,
    tags=["musicflow", "batch", "semana2"],
    doc_md="""
    ## MusicFlow Hourly Load
    Pipeline batch que carga nuevos streams cada hora.
    Implementa checkpoint, validación, staging e idempotencia.
    """
) as dag:
    t1 = PythonOperator(task_id="check_checkpoint",  python_callable=check_checkpoint)
    t2 = PythonOperator(task_id="extract_streams",   python_callable=extract_streams)
    t3 = PythonOperator(task_id="validate_data",     python_callable=validate_data)
    t4 = PythonOperator(task_id="load_to_staging",   python_callable=load_to_staging)
    t5 = PythonOperator(task_id="promote_to_fact",   python_callable=promote_to_fact)
    t6 = PythonOperator(task_id="update_checkpoint", python_callable=update_checkpoint)

    # Orden de ejecucion de las tareas
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
'''
import os
os.makedirs(os.path.expanduser("~/airflow/dags"), exist_ok=True)
with open(os.path.expanduser("~/airflow/dags/musicflow_dag.py"), "w") as f:
    f.write(dag_code)
print(" DAG creado en ~/airflow/dags/musicflow_dag.py")
