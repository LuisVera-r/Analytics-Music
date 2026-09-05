from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys, os

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.config import get_duckdb_conn
from src.audit_checks import run_all_checks
from src.audit_slo import evaluar_slo_freshness, calcular_error_budget

def task_run_audit(**context):
    conn = get_duckdb_conn()
    errores = run_all_checks(conn)
    context["ti"].xcom_push(key="audit_errors", value=errores)
    conn.close()

def task_eval_slo(**context):
    errores = context["ti"].xcom_pull(task_ids="run_audit", key="audit_errors")
    conn = get_duckdb_conn()
    slo = evaluar_slo_freshness(conn, errores)
    budget = calcular_error_budget(conn)
    
    print("\nSLO Status:")
    for k, v in slo.items(): print(f"  {k}: {v}")
    
    print("\nError Budget:")
    for k, v in budget.items(): print(f"  {k}: {v}")
    conn.close()

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="musicflow_daily_audit",
    schedule="0 8 * * *", # Todos los días a las 8:00 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["musicflow", "governance"]
) as dag:

    t1 = PythonOperator(task_id="run_audit", python_callable=task_run_audit)
    t2 = PythonOperator(task_id="eval_slo", python_callable=task_eval_slo)

    t1 >> t2

    