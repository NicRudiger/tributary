from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import shutil

RAW_DIR = "/opt/airflow/data"
BUCKET = "first-test-tributary-nrudiger"          # from Phase A
STAGE_NAME = "TRIBUTARY.RAW.S3_TRIBUTARY_RAW"
def fetch_data(ds, **_):
    src = f"{RAW_DIR}/raw/superstore.csv"
    dst = f"{RAW_DIR}/raw/{ds}/superstore.csv"
    shutil.os.makedirs(f"{RAW_DIR}/raw/{ds}", exist_ok=True)
    shutil.copy(src, dst)

def upload_to_s3(ds, **_):
    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file(
        filename=f"{RAW_DIR}/raw/{ds}/superstore.csv",
        key=f"raw/{ds}/superstore.csv",
        bucket_name=BUCKET,
        replace=True,
    )

with DAG(
    "tributary_pipeline",
    start_date=datetime(2026, 9, 11),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    fetch = PythonOperator(task_id="fetch_data", python_callable=fetch_data)

    to_s3 = PythonOperator(task_id="upload_to_s3", python_callable=upload_to_s3)

    load = SnowflakeOperator(
        task_id="copy_into_snowflake",
        snowflake_conn_id="snowflake_default",
        sql=f"""
            COPY INTO RAW.SUPERSTORE_ORDERS
            FROM @{STAGE_NAME}/{{{{ ds }}}}/
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = 'ABORT_STATEMENT';
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/transform && dbt build",
        env={"DBT_PROFILES_DIR": "/opt/airflow/transform", "PATH": "/home/airflow/.local/bin:$PATH"},
    )

    fetch >> to_s3 >> load >> dbt_run