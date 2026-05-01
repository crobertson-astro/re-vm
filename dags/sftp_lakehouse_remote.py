from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import DAG

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

LOCAL_FILEPATH = "/tmp/vendor_positions_{{ data_interval_start | ds_nodash }}.csv"
REMOTE_FILEPATH = "/outbound/vendor_positions_{{ data_interval_start | ds_nodash }}.csv"


with DAG(
    dag_id="sftp_lakehouse_remote",
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

    stage_to_bronze = SSHOperator(
        task_id="stage_to_bronze",
        ssh_conn_id="linux_remote_executor",
        command=(
            'bash /opt/jobs/stage_to_bronze.sh '
            '"{{ data_interval_start | ds_nodash }}" '
            f'"{LOCAL_FILEPATH}"'
        ),
        pool="remote_execution",
    )

    cleanse_to_silver = SSHOperator(
        task_id="cleanse_to_silver",
        ssh_conn_id="linux_remote_executor",
        command='bash /opt/jobs/cleanse_to_silver.sh "{{ data_interval_start | ds_nodash }}"',
        pool="remote_execution",
    )

    publish_to_snowflake = SnowflakeSqlApiOperator(
        task_id="publish_to_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="CALL ANALYTICS.GOLD.SP_PUBLISH_VENDOR_POSITIONS('{{ data_interval_start | ds_nodash }}')",
        warehouse="TRANSFORMING",
        database="ANALYTICS",
        schema="GOLD",
        pool="snowflake_queries",
    )

    download_vendor_file >> stage_to_bronze >> cleanse_to_silver >> publish_to_snowflake
