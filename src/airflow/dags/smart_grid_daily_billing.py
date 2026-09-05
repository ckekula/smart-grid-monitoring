from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator

from airflow import DAG

with DAG(
    dag_id="smart_grid_daily_billing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "smart-grid",
        "batch",
        "billing",
    ],
) as dag:

    run_daily_billing = BashOperator(
        task_id="run_daily_billing",

        bash_command="""
        echo "Processing simulated billing date: {{ dag_run.conf['billing_date'] }}"

        docker exec smart-grid-spark-submit \
          /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages \
          org.postgresql:postgresql:42.7.7 \
          /opt/spark/apps/src/spark/batch/billing_job.py
        """,
    )