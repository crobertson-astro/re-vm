from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param, task

try:
    from airflow.sdk.exceptions import AirflowFailException
except (ImportError, AttributeError):
    from airflow.exceptions import AirflowFailException

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="example_powershell_and_shell_guardrails",
    description="Example guardrails for remote PowerShell and shell-script execution.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=10,
    params={
        "target_os": Param("linux", enum=["linux", "windows"]),
        "script_kind": Param("shell", enum=["shell", "powershell"]),
        "script_path": Param("/opt/jobs/example.sh", type="string"),
        "script_args": Param("", type="string"),
    },
    tags=["legacy-migration", "remote-execution", "powershell", "shell"],
    doc_md="""
    ### Purpose
    Demonstrates explicit guardrails so a PowerShell job is not accidentally routed to a Linux host and vice versa.
    """,
) as dag:
    @task(task_id="validate_request")
    def validate_request(target_os: str, script_kind: str) -> None:
        valid_combinations = {
            ("linux", "shell"),
            ("windows", "powershell"),
        }
        if (target_os, script_kind) not in valid_combinations:
            raise AirflowFailException(
                f"Invalid combination: target_os={target_os}, script_kind={script_kind}."
            )

    @task.branch(task_id="route_to_correct_runner")
    def route_to_correct_runner(target_os: str) -> str:
        if target_os == "windows":
            return "run_windows_powershell"
        return "run_linux_shell"

    run_windows_powershell = SSHOperator(
        task_id="run_windows_powershell",
        ssh_conn_id="windows_remote_executor",
        command='powershell.exe -File "{{ params.script_path }}" {{ params.script_args }}',
        pool="remote_execution",
    )

    run_linux_shell = SSHOperator(
        task_id="run_linux_shell",
        ssh_conn_id="linux_remote_executor",
        command='bash "{{ params.script_path }}" {{ params.script_args }}',
        pool="remote_execution",
    )

    finish = EmptyOperator(
        task_id="finish",
        trigger_rule="none_failed_min_one_success",
    )

    validated_request = validate_request("{{ params.target_os }}", "{{ params.script_kind }}")
    selected_runner = route_to_correct_runner("{{ params.target_os }}")

    validated_request >> selected_runner
    selected_runner >> [run_windows_powershell, run_linux_shell] >> finish
