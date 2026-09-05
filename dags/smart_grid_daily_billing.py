import os
from datetime import UTC, datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    dag_id="smart_grid_daily_billing",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False
) as dag:

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
            "BATCH_FILE": "/opt/spark/data/batch/smart_grid_{{ dag_run.conf['billing_date'] }}.csv",
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
                source=os.getenv("AIRFLOW_PROJ_DIR", ".") + "/src",
                target="/opt/spark/apps/src",
                type="bind",
            ),
            Mount(
                source=os.getenv("AIRFLOW_PROJ_DIR", ".") + "/data",
                target="/opt/spark/data",
                type="bind",
            ),
        ],

        network_mode="project_default",
        auto_remove="success",
        mount_tmp_dir=False,
    )
