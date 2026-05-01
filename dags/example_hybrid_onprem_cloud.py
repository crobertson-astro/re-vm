from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def parse_json_response(response):
    return response.json()


with DAG(
    dag_id="example_hybrid_onprem_cloud",
    description="Example hybrid orchestration across on-prem scripts, cloud APIs, Databricks, and Snowflake.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 3 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["legacy-migration", "hybrid", "remote-execution", "cloud"],
    doc_md="""
    ### Purpose
    Models a high-priority Remote Execution pattern: on-prem orchestration that hands off into cloud systems.
    """,
) as dag:
    export_from_onprem = SSHOperator(
        task_id="export_from_onprem",
        ssh_conn_id="linux_remote_executor",
        command='bash /opt/jobs/export_positions.sh "{{ ds }}"',
        pool="remote_execution",
    )

    register_manifest = HttpOperator(
        task_id="register_manifest",
        http_conn_id="ingestion_gateway",
        endpoint="/api/v1/manifests",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "business_date": "{{ ds }}",
                "dataset": "positions",
                "source_system": "onprem-linux",
                "manifest_id": "{{ run_id }}",
            }
        ),
        response_filter=parse_json_response,
        pool="cloud_control_plane",
    )

    run_cloud_enrichment = DatabricksSubmitRunOperator(
        task_id="run_cloud_enrichment",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "hybrid_positions_{{ data_interval_start | ds_nodash }}",
            "existing_cluster_id": "{{ var.value.databricks_existing_cluster_id }}",
            "notebook_task": {
                "notebook_path": "/Shared/analytics/hybrid_positions_enrichment",
                "base_parameters": {
                    "business_date": "{{ ds }}",
                    "manifest_id": "{{ run_id }}",
                },
            },
        },
        pool="databricks_jobs",
    )

    publish_to_snowflake = SnowflakeSqlApiOperator(
        task_id="publish_to_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="CALL ANALYTICS.GOLD.SP_LOAD_FROM_MANIFEST('{{ run_id }}')",
        warehouse="TRANSFORMING",
        database="ANALYTICS",
        schema="GOLD",
        pool="snowflake_queries",
    )

    emit_hybrid_summary = EmptyOperator(task_id="emit_hybrid_summary")

    export_from_onprem >> register_manifest >> run_cloud_enrichment >> publish_to_snowflake >> emit_hybrid_summary
