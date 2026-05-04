from __future__ import annotations

from datetime import timedelta
import os
from pathlib import Path
import shutil
import subprocess

import pendulum
from airflow.sdk import dag, task

try:
    from airflow.sdk.exceptions import AirflowFailException
except (ImportError, AttributeError):
    from airflow.exceptions import AirflowFailException

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SAMPLE_VENDOR_CSV = """account_id,position\nA100,42\nB200,17\nC300,9\n"""


@dag(
    dag_id="standalone_windows_no_connections",
    description="Standalone example DAG with Bash, a connection-free SFTP-style transfer, and PowerShell for Windows workers.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["demo", "windows", "bash", "sftp", "powershell", "no-connections"],
    doc_md="""
    ### Purpose
    Demonstrates a self-contained Remote Execution-friendly example for Windows workers with no Airflow connections.

    ### Included task types
    - `bash_runtime_probe` uses TaskFlow's Bash task decorator to capture shell/runtime details.
    - `simulate_sftp_download` models an SFTP-style file download using only the local filesystem so the DAG stays runnable offline.
    - `powershell_windows_probe` runs PowerShell and captures Windows-specific runtime metadata.

    ### Why the SFTP task is simulated
    A real `SFTPOperator` requires an Airflow connection, which conflicts with the goal of a zero-setup example. This DAG keeps the same file-movement shape without requiring credentials or external systems.
    """,
)
def standalone_windows_no_connections():
    @task.bash(task_id="bash_runtime_probe")
    def bash_runtime_probe() -> str:
        return """
        set -euo pipefail

        output_dir="./tmp/standalone-windows-demo/{{ run_id | replace(':', '_') }}/bash"
        output_file="${output_dir}/runtime.txt"
        windows_pwd="$(pwd -W 2>/dev/null || pwd)"

        mkdir -p "${output_dir}"

        cat <<EOF > "${output_file}"
        timestamp=$(date -Iseconds)
        hostname=$(hostname)
        user=$(whoami)
        pwd=$(pwd)
        windows_pwd=${windows_pwd}
        shell=${SHELL:-unset}
        dag_id={{ dag.dag_id }}
        task_id={{ task.task_id }}
        run_id={{ run_id }}
        logical_date={{ logical_date }}
        EOF

        echo "Created ${output_file}"
        cat "${output_file}"
        """

    @task(task_id="simulate_sftp_download")
    def simulate_local_sftp_transfer(
        remote_dir: str,
        local_dir: str,
        filename: str,
        file_contents: str,
        overwrite: bool = True,
    ) -> None:
        """Simulate an SFTP get without requiring an Airflow connection."""

        remote_path = Path(remote_dir) / filename
        local_path = Path(local_dir) / filename

        remote_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if overwrite or not remote_path.exists():
            remote_path.write_text(file_contents.rstrip() + "\n", encoding="utf-8")

        shutil.copy2(remote_path, local_path)

        receipt_path = local_path.parent / f"{local_path.stem}.receipt.txt"
        receipt_path.write_text(
            "\n".join(
                [
                    f"source={remote_path}",
                    f"destination={local_path}",
                    "transport=sftp-simulation",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        print(f"Created simulated remote file: {remote_path}")
        print(f"Copied file into landing path: {local_path}")
        print(f"Wrote transfer receipt: {receipt_path}")

    @task(task_id="powershell_windows_probe")
    def run_local_powershell_report(
        input_filepath: str,
        report_filepath: str,
        seed_contents: str,
    ) -> None:
        """Run a local PowerShell report task on a Windows-capable worker."""

        input_path = Path(input_filepath)
        report_path = Path(report_filepath)

        input_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.parent.mkdir(parents=True, exist_ok=True)

        if not input_path.exists():
            input_path.write_text(seed_contents.rstrip() + "\n", encoding="utf-8")

        shell = next(
            (
                candidate
                for candidate in ("powershell.exe", "pwsh.exe", "pwsh", "powershell")
                if shutil.which(candidate)
            ),
            None,
        )
        if not shell:
            raise AirflowFailException(
                "No PowerShell executable was found on PATH. Expected one of powershell.exe, pwsh.exe, pwsh, or powershell."
            )

        script = """
$inputPath = $env:OTTO_INPUT_FILE
$reportPath = $env:OTTO_REPORT_FILE
$windowsInputPath = $inputPath -replace '/', '\\'
$inputExists = Test-Path -LiteralPath $inputPath
$lineCount = if ($inputExists) { (Get-Content -LiteralPath $inputPath).Count } else { 0 }
$reportLines = @(
    "timestamp=$(Get-Date -Format o)",
    "computer_name=$env:COMPUTERNAME",
    "user_name=$env:USERNAME",
    "temp_dir=$env:TEMP",
    "system_root=$env:SystemRoot",
    "powershell_version=$($PSVersionTable.PSVersion)",
    "os=$([System.Runtime.InteropServices.RuntimeInformation]::OSDescription)",
    "input_path=$inputPath",
    "input_path_windows=$windowsInputPath",
    "input_exists=$inputExists",
    "input_line_count=$lineCount"
)
Set-Content -LiteralPath $reportPath -Value $reportLines
Get-Content -LiteralPath $reportPath
""".strip()

        env = os.environ.copy()
        env["OTTO_INPUT_FILE"] = str(input_path)
        env["OTTO_REPORT_FILE"] = str(report_path)

        result = subprocess.run(
            [shell, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )

        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise AirflowFailException(
                f"PowerShell command failed with exit code {result.returncode}."
            )

    bash_task = bash_runtime_probe()
    sftp_task = simulate_local_sftp_transfer(
        remote_dir="./tmp/standalone-windows-demo/{{ run_id | replace(':', '_') }}/fake-sftp-remote",
        local_dir="./tmp/standalone-windows-demo/{{ run_id | replace(':', '_') }}/landing",
        filename="vendor_positions_{{ data_interval_start | ds_nodash }}.csv",
        file_contents=SAMPLE_VENDOR_CSV,
    )
    powershell_task = run_local_powershell_report(
        input_filepath="./tmp/standalone-windows-demo/{{ run_id | replace(':', '_') }}/windows-input/vendor_positions_{{ data_interval_start | ds_nodash }}.csv",
        report_filepath="./tmp/standalone-windows-demo/{{ run_id | replace(':', '_') }}/reports/powershell_report.txt",
        seed_contents=SAMPLE_VENDOR_CSV,
    )

    bash_task >> sftp_task >> powershell_task


standalone_windows_no_connections()
