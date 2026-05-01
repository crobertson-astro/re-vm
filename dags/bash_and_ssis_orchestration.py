from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="bash_and_ssis_orchestration",
    description="Example orchestration that stages a local control artifact with BashOperator and triggers an SSIS job on a Windows runner.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="30 1 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["legacy-migration", "bash", "ssis", "remote-execution"],
    doc_md="""
    ### Purpose
    Models a common migration pattern where Airflow prepares a lightweight local marker file,
    then hands off execution to an SSIS-trigger wrapper running on a Windows executor.

    ### Notes
    - Replace `C:/jobs/trigger_ssis_job.ps1` with the path to your PowerShell wrapper on the Windows runner.
    - The PowerShell wrapper should encapsulate the environment-specific SSIS trigger logic.
    """,
) as dag:
    create_trigger_marker = BashOperator(
        task_id="create_trigger_marker",
        bash_command="""
        set -euo pipefail
        mkdir -p /tmp/legacy_control
        printf '%s\n' '{{ ds }}|{{ run_id }}|daily_finance_load' > /tmp/legacy_control/ssis_trigger_{{ ds_nodash }}.txt
        """,
    )

    trigger_ssis_job = SSHOperator(
        task_id="trigger_ssis_job",
        ssh_conn_id="windows_remote_executor",
        command=(
            'powershell.exe -NoLogo -NoProfile -NonInteractive '
            '-File "C:/jobs/trigger_ssis_job.ps1" '
            '-JobName "daily_finance_load" '
            '-BusinessDate "{{ ds }}" '
            '-RequestId "{{ run_id }}"'
        ),
        pool="remote_execution",
    )

    create_trigger_marker >> trigger_ssis_job
