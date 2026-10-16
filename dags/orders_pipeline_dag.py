
import sys
from datetime import datetime

sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl_project.generator import generate_orders
from etl_project.pipeline import run_pipeline
from etl_project.show_rows import show_processed_orders


with DAG(
    dag_id="orders_etl_elt_pipeline",
    description="Generate orders and process them into the warehouse",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ETL", "ELT", "Data Engineering"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_new_orders",
        python_callable=generate_orders,
    )

    process_orders = PythonOperator(
        task_id="validate_load_transform_and_audit",
        python_callable=run_pipeline,
    )

    show_rows = PythonOperator(
        task_id="show_processed_warehouse_rows",
        python_callable=show_processed_orders,
    )

    generate_data >> process_orders >> show_rows
