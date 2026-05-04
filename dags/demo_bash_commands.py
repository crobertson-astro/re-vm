"""
## Bash command demo DAG

This DAG is meant for demos where you want to show that an Airflow deployment can
run shell commands inside a task environment.

The workflow demonstrates three common patterns:
- printing runtime details from the task container
- writing a file to the local filesystem using templated Airflow context
- passing a value between bash tasks with XCom
"""

from pendulum import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(
    dag_id="demo_bash_commands",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "Astro", "retries": 2},
    doc_md=__doc__,
    tags=["demo", "bash"],
)
def bash_command_demo():
    show_runtime_details = BashOperator(
        task_id="show_runtime_details",
        bash_command="""
        set -euo pipefail

        echo "date=$(date -Iseconds)"
        echo "hostname=$(hostname)"
        echo "user=$(whoami)"
        echo "pwd=$(pwd)"
        echo "airflow_home=${AIRFLOW_HOME:-unset}"
        echo "python=$(python --version 2>&1)"
        """,
    )

    write_demo_file = BashOperator(
        task_id="write_demo_file",
        do_xcom_push=True,
        bash_command="""
        set -euo pipefail

        output_dir="/tmp/airflow-bash-demo"
        output_file="${output_dir}/run-{{ run_id | replace(':', '_') }}.txt"

        mkdir -p "${output_dir}"

        cat <<EOF > "${output_file}"
        dag_id={{ dag.dag_id }}
        task_id={{ task.task_id }}
        run_id={{ run_id }}
        logical_date={{ logical_date }}
        EOF

        echo "Created ${output_file}"
        echo "${output_file}"
        """,
    )

    inspect_demo_file = BashOperator(
        task_id="inspect_demo_file",
        bash_command="""
        set -euo pipefail

        demo_file='{{ ti.xcom_pull(task_ids="write_demo_file") }}'

        echo "Reading ${demo_file}"
        test -f "${demo_file}"
        cat "${demo_file}"
        """,
    )

    show_runtime_details >> write_demo_file >> inspect_demo_file


bash_command_demo()
