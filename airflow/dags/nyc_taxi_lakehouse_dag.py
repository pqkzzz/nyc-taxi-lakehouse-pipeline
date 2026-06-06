from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/home/iceberg/project"
SPARK_CONTAINER = "spark-iceberg"


with DAG(
    dag_id="nyc_taxi_lakehouse_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "lakehouse", "spark", "iceberg"],
) as dag:
    load_zone = BashOperator(
        task_id="load_taxi_zones",
        bash_command=(
            f'docker exec {SPARK_CONTAINER} bash -lc '
            f'"cd {PROJECT_DIR} && spark-submit spark/jobs/00_load_zone.py"'
        ),
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            f'docker exec {SPARK_CONTAINER} bash -lc '
            f'"cd {PROJECT_DIR} && spark-submit spark/jobs/01_bronze_ingest.py '
            f'--year {{{{ dag_run.conf.get(\'year\', \'2024\') }}}} '
            f'--month {{{{ dag_run.conf.get(\'month\', \'01\') }}}}"'
        ),
    )

    silver_clean = BashOperator(
        task_id="silver_clean",
        bash_command=(
            f'docker exec {SPARK_CONTAINER} bash -lc '
            f'"cd {PROJECT_DIR} && spark-submit spark/jobs/02_silver_clean.py"'
        ),
    )

    gold_marts = BashOperator(
        task_id="gold_marts",
        bash_command=(
            f'docker exec {SPARK_CONTAINER} bash -lc '
            f'"cd {PROJECT_DIR} && spark-submit spark/jobs/03_gold_marts.py"'
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/project/dbt_taxi && dbt test --profiles-dir .",
    )

    load_zone >> bronze_ingest >> silver_clean >> gold_marts >> dbt_test