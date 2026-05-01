from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def parse_json_response(response):
    return response.json()


def batch_run_completed(response) -> bool:
    return response.json().get("state") == "COMPLETED"


with DAG(
    dag_id="example_long_running_batch_deferrable",
    description="Long-running batch pattern using deferrable polling so workers are not occupied for hours.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 22 * * 5",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=12),
    tags=["legacy-migration", "long-running", "deferrable", "remote-execution"],
    doc_md="""
    ### Purpose
    Represents a long-running batch process that may run for many hours.

    ### Remote Execution Notes
    - The job submission call is lightweight control-plane work.
    - The wait step is deferrable so it does not consume a worker slot while the external batch runs.
    """,
) as dag:
    submit_batch = HttpOperator(
        task_id="submit_batch",
        http_conn_id="batch_service_api",
        endpoint="/api/v1/batches/risk/runs",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "business_date": "{{ ds }}",
                "requested_by": "airflow",
                "batch_name": "weekly-risk-batch",
            }
        ),
        response_filter=parse_json_response,
        pool="cloud_control_plane",
    )

    wait_for_batch_completion = HttpSensor(
        task_id="wait_for_batch_completion",
        http_conn_id="batch_service_api",
        endpoint="/api/v1/batches/risk/runs/{{ ti.xcom_pull(task_ids='submit_batch')['run_id'] }}",
        response_check=batch_run_completed,
        poke_interval=300,
        timeout=int(timedelta(hours=10).total_seconds()),
        deferrable=True,
        pool="cloud_control_plane",
    )

    @task(task_id="publish_batch_summary")
    def publish_batch_summary(run_id: str) -> str:
        return f"Submitted and monitored batch run {run_id}."

    batch_summary = publish_batch_summary(submit_batch.output["run_id"])

    submit_batch >> wait_for_batch_completion >> batch_summary
