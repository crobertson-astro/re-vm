from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def parse_json_response(response):
    return response.json()


def service_request_completed(response) -> bool:
    return response.json().get("status") in {"COMPLETED", "SUCCEEDED"}


with DAG(
    dag_id="example_web_service_jobs",
    description="Example orchestration of vendor or internal web-service jobs from Airflow.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    tags=["legacy-migration", "http", "api", "remote-execution"],
    doc_md="""
    ### Purpose
    Models a web service job: submit a request, poll for completion, then write an audit summary.
    """,
) as dag:
    trigger_reconciliation = HttpOperator(
        task_id="trigger_reconciliation",
        http_conn_id="service_api_default",
        endpoint="/api/v1/reconciliations",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "business_date": "{{ ds }}",
                "workflow": "cash-balance-reconciliation",
                "request_id": "{{ run_id }}",
            }
        ),
        response_filter=parse_json_response,
        pool="cloud_control_plane",
    )

    wait_for_reconciliation = HttpSensor(
        task_id="wait_for_reconciliation",
        http_conn_id="service_api_default",
        endpoint="/api/v1/reconciliations/{{ run_id }}",
        response_check=service_request_completed,
        poke_interval=120,
        timeout=int(timedelta(hours=2).total_seconds()),
        deferrable=True,
        pool="cloud_control_plane",
    )

    build_audit_record = EmptyOperator(task_id="build_audit_record")

    trigger_reconciliation >> wait_for_reconciliation >> build_audit_record
