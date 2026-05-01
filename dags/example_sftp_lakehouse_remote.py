from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, task
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

LOCAL_FILEPATH = "/tmp/vendor_positions_{{ data_interval_start | ds_nodash }}.csv"
REMOTE_FILEPATH = "/outbound/vendor_positions_{{ data_interval_start | ds_nodash }}.csv"


with DAG(
    dag_id="example_sftp_lakehouse_remote",
    description="Example legacy-scheduler migration pattern for SFTP ingest, lakehouse processing, and Snowflake publish.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["legacy-migration", "sftp", "snowflake", "remote-execution"],
    doc_md="""
    ### Purpose
    Models a common migration pattern: vendor SFTP pull -> Bronze -> Silver -> Gold -> Snowflake.

    ### Remote Execution Notes
    - The SFTP and file-prep tasks are marked with the `remote_execution` pool to represent on-prem execution.
    - The Snowflake publish task is kept separate so cloud-side work can scale independently.
    """,
) as dag:
    download_vendor_file = SFTPOperator(
        task_id="download_vendor_file",
        ssh_conn_id="vendor_sftp_default",
        remote_filepath=REMOTE_FILEPATH,
        local_filepath=LOCAL_FILEPATH,
        operation="get",
        create_intermediate_dirs=True,
        pool="remote_execution",
    )

    @task(task_id="stage_to_bronze", pool="remote_execution")
    def stage_to_bronze(local_filepath: str, partition: str) -> dict[str, str]:
        return {
            "partition": partition,
            "source_path": local_filepath,
            "bronze_path": f"s3://bronze-lake/vendor_positions/dt={partition}/vendor_positions.csv",
        }

    @task(task_id="cleanse_to_silver", pool="remote_execution")
    def cleanse_to_silver(bronze_metadata: dict[str, str]) -> dict[str, str]:
        partition = bronze_metadata["partition"]
        return {
            "partition": partition,
            "silver_path": f"s3://silver-lake/vendor_positions/dt={partition}/vendor_positions.parquet",
            "load_mode": "MERGE",
        }

    @task(task_id="build_gold_sql")
    def build_gold_sql(silver_metadata: dict[str, str]) -> str:
        partition = silver_metadata["partition"]
        return f"CALL ANALYTICS.GOLD.SP_PUBLISH_VENDOR_POSITIONS('{partition}')"

    bronze_metadata = stage_to_bronze(
        LOCAL_FILEPATH,
        "{{ data_interval_start | ds_nodash }}",
    )
    silver_metadata = cleanse_to_silver(bronze_metadata)
    gold_sql = build_gold_sql(silver_metadata)

    publish_to_snowflake = SnowflakeSqlApiOperator(
        task_id="publish_to_snowflake",
        snowflake_conn_id="snowflake_default",
        sql=gold_sql,
        warehouse="TRANSFORMING",
        database="ANALYTICS",
        schema="GOLD",
        pool="snowflake_queries",
    )

    download_vendor_file >> bronze_metadata >> silver_metadata >> gold_sql >> publish_to_snowflake
