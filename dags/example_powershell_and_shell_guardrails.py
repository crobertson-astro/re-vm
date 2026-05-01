from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import DAG, Param

try:
    from airflow.sdk.exceptions import AirflowFailException
except (ImportError, AttributeError):
    from airflow.exceptions import AirflowFailException

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


TARGET_OS = "linux"
SCRIPT_KIND = "shell"
VALID_COMBINATIONS = {
    ("linux", "shell"): {
        "ssh_conn_id": "linux_remote_executor",
        "command": 'bash "{{ params.script_path }}" {{ params.script_args }}',
    },
    ("windows", "powershell"): {
        "ssh_conn_id": "windows_remote_executor",
        "command": 'powershell.exe -File "{{ params.script_path }}" {{ params.script_args }}',
    },
}

if (TARGET_OS, SCRIPT_KIND) not in VALID_COMBINATIONS:
    raise AirflowFailException(
        f"Invalid combination: target_os={TARGET_OS}, script_kind={SCRIPT_KIND}."
    )

runner_config = VALID_COMBINATIONS[(TARGET_OS, SCRIPT_KIND)]

with DAG(
    dag_id="example_powershell_and_shell_guardrails",
    description="Example guardrails for remote PowerShell and shell-script execution.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=10,
    params={
        "script_path": Param("/opt/jobs/example.sh", type="string"),
        "script_args": Param("", type="string"),
    },
    tags=["legacy-migration", "remote-execution", "powershell", "shell"],
    doc_md="""
    ### Purpose
    Demonstrates explicit guardrails so a script is bound to the correct execution platform at authoring time.
    """,
) as dag:
    run_validated_script = SSHOperator(
        task_id="run_validated_script",
        ssh_conn_id=runner_config["ssh_conn_id"],
        command=runner_config["command"],
        pool="remote_execution",
    )
