import os
from datetime import UTC, datetime

import psycopg2
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowSkipException
from docker.types import Mount

PROJECT_DIR = os.getenv("AIRFLOW_PROJ_DIR", ".")
BATCH_DIR = os.path.join(PROJECT_DIR, "data", "batch")

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "smart_grid"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"


def get_next_billing_file():
    """
    Determine which simulated daily batch file should be processed next.

    The batch source writes the latest completed simulated date to:
        data/batch/.current_day

    PostgreSQL is used as the durable record of the last successfully
    processed billing date.
    """

    current_day_file = os.path.join(BATCH_DIR, ".current_day")

    if not os.path.exists(current_day_file):
        raise AirflowSkipException(
            "No completed simulated day is available yet."
        )

    current_day = open(
        current_day_file,
        "r",
        encoding="utf-8",
    ).read().strip()

    if not current_day:
        raise AirflowSkipException(
            ".current_day is empty."
        )

    batch_file = os.path.join(
        BATCH_DIR,
        f"smart_grid_{current_day}.csv",
    )

    if not os.path.exists(batch_file):
        raise AirflowSkipException(
            f"Batch file does not exist yet: {batch_file}"
        )

    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(billing_date)
                FROM smart_grid.daily_household_billing
                """
            )

            last_billing_date = cursor.fetchone()[0]

    finally:
        connection.close()

    if last_billing_date is not None and current_day <= last_billing_date.isoformat():
        raise AirflowSkipException(
            f"{current_day} has already been processed."
        )

    print(f"Next billing date: {current_day}")

    return current_day


with DAG(
    dag_id="smart_grid_daily_billing",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["smart-grid", "batch", "billing"],
) as dag:

    get_billing_date = PythonOperator(
        task_id="get_billing_date",
        python_callable=get_next_billing_file,
    )

    run_daily_billing = DockerOperator(
        task_id="run_daily_billing",

        image="spark:4.0.2-java21-python3",

        command="""
        /opt/spark/bin/spark-submit
        --master spark://spark-master:7077
        --packages org.postgresql:postgresql:42.7.7
        /opt/spark/apps/src/spark/batch/billing_job.py
        """,

        user="root",

        environment={
            "BATCH_FILE": (
                "/opt/spark/data/batch/"
                "smart_grid_{{ ti.xcom_pull(task_ids='get_billing_date') }}.csv"
            ),
            "POSTGRES_URL": "jdbc:postgresql://postgres:5432/smart_grid",
            "POSTGRES_USER": "airflow",
            "POSTGRES_PASSWORD": "airflow",
            "PYTHONPATH": "/opt/spark/apps/src",
            "HOME": "/tmp",
            "SPARK_USER": "root",
            "IVY_HOME": "/tmp/.ivy2",
        },

        mounts=[
            Mount(
                source=os.path.join(PROJECT_DIR, "src"),
                target="/opt/spark/apps/src",
                type="bind",
            ),
            Mount(
                source=os.path.join(PROJECT_DIR, "data"),
                target="/opt/spark/data",
                type="bind",
            ),
        ],

        network_mode="project_default",
        auto_remove="success",
        mount_tmp_dir=False,
    )

    get_billing_date >> run_daily_billing
