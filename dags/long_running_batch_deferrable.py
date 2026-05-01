from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import DAG

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="long_running_batch_deferrable",
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
                "client_run_id": "{{ run_id }}",
            }
        ),
        response_filter=lambda response: response.json(),
        pool="cloud_control_plane",
    )

    wait_for_batch_completion = HttpSensor(
        task_id="wait_for_batch_completion",
        http_conn_id="batch_service_api",
        endpoint="/api/v1/batches/risk/runs/{{ run_id }}",
        response_check=lambda response: response.json().get("state") == "COMPLETED",
        poke_interval=300,
        timeout=int(timedelta(hours=10).total_seconds()),
        deferrable=True,
        pool="cloud_control_plane",
    )

    submit_batch >> wait_for_batch_completion
