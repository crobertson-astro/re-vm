"""
## Bash command demo DAG

This DAG is meant for demos where you want to show that an Airflow deployment can
run a shell command inside a task environment and write runtime details to a
local file.
"""

from pendulum import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(
    dag_id="simple_bash_commands",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "Astro", "retries": 2},
    doc_md=__doc__,
    tags=["demo", "bash", "script", "foo2"],
)
def bash_command_demo():
    f = BashOperator(
        task_id="write_demo_file",
        do_xcom_push=False,
        bash_command="""
        set -euo pipefail

        output_dir="/tmp/airflow-bash-demo"
        output_file="${output_dir}/{{ dag.dag_id }}-{{ task.task_id }}-{{ run_id | replace(':', '_') }}.txt"

        mkdir -p "${output_dir}"

        cat <<FOO > "${output_file}"
        date=$(date -Iseconds)
        hostname=$(hostname)
        user=$(whoami)
        pwd=$(pwd)
        airflow_home=${AIRFLOW_HOME:-unset}
        python=$(python --version 2>&1)
        dag_id={{ dag.dag_id }}
        task_id={{ task.task_id }}
        run_id={{ run_id }}
        logical_date={{ logical_date }}
FOO

        echo "Created ${output_file}"
        """,
    )

    s = BashOperator(
        task_id="execute_script",
        do_xcom_push=False,
        bash_command="/usr/local/airflow/script.sh exec"
    )

    f >> s

bash_command_demo()

