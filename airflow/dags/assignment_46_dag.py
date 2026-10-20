
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

with DAG(
    dag_id="assignment_46_pyspark_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["assignment46", "pyspark"],
) as dag:

    run_assignment46 = SparkSubmitOperator(
        task_id="run_assignment46",
        conn_id="spark_default",
        application=(
            "/opt/airflow/dags/scripts/assignment_46_pipeline.py"
        ),
        name="assignment46",
        verbose=True,
    )
