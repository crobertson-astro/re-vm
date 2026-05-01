from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.sdk import DAG

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="example_databricks_snowflake_orchestration",
    description="Example cloud workload orchestration across Databricks and Snowflake.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="15 1 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["legacy-migration", "databricks", "snowflake", "cloud"],
    doc_md="""
    ### Purpose
    Shows Airflow as the control plane for Databricks compute followed by Snowflake publishing.
    """,
) as dag:
    run_databricks_transform = DatabricksSubmitRunOperator(
        task_id="run_databricks_transform",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "positions_{{ data_interval_start | ds_nodash }}",
            "existing_cluster_id": "{{ var.value.databricks_existing_cluster_id }}",
            "notebook_task": {
                "notebook_path": "/Shared/analytics/positions_bronze_to_silver",
                "base_parameters": {
                    "business_date": "{{ ds }}",
                    "dataset": "vendor_positions",
                },
            },
        },
        pool="databricks_jobs",
    )

    publish_curated_tables = SnowflakeSqlApiOperator(
        task_id="publish_curated_tables",
        snowflake_conn_id="snowflake_default",
        sql="CALL ANALYTICS.GOLD.SP_PUBLISH_POSITIONS('{{ ds }}')",
        warehouse="TRANSFORMING",
        database="ANALYTICS",
        schema="GOLD",
        pool="snowflake_queries",
    )

    run_databricks_transform >> publish_curated_tables
