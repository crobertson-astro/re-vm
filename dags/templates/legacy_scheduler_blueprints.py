from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess

from airflow.sdk import TaskGroup, get_current_context, task

try:
    from airflow.sdk.exceptions import AirflowFailException
except (ImportError, AttributeError):
    from airflow.exceptions import AirflowFailException
from blueprint import BaseModel, Blueprint, Field
from pydantic import ConfigDict

YamlScalar = str | int | float | bool | None


class LocalBashConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bash_command: str = Field(description="Bash command to execute locally.")
    env: dict[str, YamlScalar] = Field(default_factory=dict, description="Optional environment variables.")
    cwd: str | None = Field(default=None, description="Optional working directory for the command.")
    append_env: bool = Field(default=False, description="Append env values to the worker environment.")
    pool: str | None = Field(default=None, description="Optional pool for the bash task.")


class LocalBash(Blueprint[LocalBashConfig]):
    """Run a local Bash command with TaskFlow's bash decorator."""

    def render(self, config: LocalBashConfig):
        with TaskGroup(group_id=self.step_id) as group:
            @task.bash(
                task_id="run",
                env=config.env,
                cwd=config.cwd,
                append_env=config.append_env,
                pool=config.pool,
                params={"bash_command": config.bash_command},
            )
            def run() -> str:
                return self.param("bash_command")

            run()
        return group


class LocalFilesystemSftpConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    remote_dir: str = Field(description="Directory used as the simulated SFTP source.")
    local_dir: str = Field(description="Directory used as the landing destination.")
    filename: str = Field(description="Filename copied from the simulated remote directory.")
    file_contents: str = Field(description="Contents written into the simulated remote file before transfer.")
    overwrite: bool = Field(default=True, description="Overwrite the simulated remote file before copying it.")


class LocalFilesystemSftp(Blueprint[LocalFilesystemSftpConfig]):
    """Simulate an SFTP-style file download without requiring an Airflow connection."""

    name = "local_filesystem_sftp"

    @staticmethod
    def _transfer(remote_dir: str, local_dir: str, filename: str, file_contents: str, overwrite: bool = True) -> None:
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

    def render(self, config: LocalFilesystemSftpConfig):
        bp = self

        with TaskGroup(group_id=self.step_id) as group:
            @task(task_id="transfer")
            def transfer() -> None:
                resolved = bp.resolve_config(config, get_current_context())
                bp._transfer(
                    remote_dir=resolved.remote_dir,
                    local_dir=resolved.local_dir,
                    filename=resolved.filename,
                    file_contents=resolved.file_contents,
                    overwrite=resolved.overwrite,
                )

            transfer()
        return group


class LocalPowershellConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_filepath: str = Field(description="Path inspected from PowerShell.")
    report_filepath: str = Field(description="Path where the PowerShell report is written.")
    seed_contents: str = Field(description="Fallback file contents used when the input file does not already exist.")


class LocalPowershell(Blueprint[LocalPowershellConfig]):
    """Run a local PowerShell report step without requiring a connection."""

    name = "local_powershell"

    @staticmethod
    def _run_report(input_filepath: str, report_filepath: str, seed_contents: str) -> None:
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

    def render(self, config: LocalPowershellConfig):
        bp = self

        with TaskGroup(group_id=self.step_id) as group:
            @task(task_id="run")
            def run() -> None:
                resolved = bp.resolve_config(config, get_current_context())
                bp._run_report(
                    input_filepath=resolved.input_filepath,
                    report_filepath=resolved.report_filepath,
                    seed_contents=resolved.seed_contents,
                )

            run()
        return group


class SftpGetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ssh_conn_id: str = Field(default="vendor_sftp_default", description="SFTP connection ID.")
    remote_filepath: str = Field(description="Source path on the SFTP server.")
    local_filepath: str = Field(description="Local destination path.")
    create_intermediate_dirs: bool = Field(default=True, description="Create local directories if needed.")
    pool: str = Field(default="remote_execution", description="Pool for remote-executed tasks.")


class SftpGet(Blueprint[SftpGetConfig]):
    """Download a file from SFTP into a local staging path."""

    def render(self, config: SftpGetConfig):
        from airflow.providers.sftp.operators.sftp import SFTPOperator

        with TaskGroup(group_id=self.step_id) as group:
            SFTPOperator(
                task_id="download",
                ssh_conn_id=config.ssh_conn_id,
                remote_filepath=config.remote_filepath,
                local_filepath=config.local_filepath,
                operation="get",
                create_intermediate_dirs=config.create_intermediate_dirs,
                pool=config.pool,
            )
        return group


class RemoteCommandConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ssh_conn_id: str = Field(description="SSH connection ID for the remote runner.")
    command: str = Field(description="Command to execute remotely.")
    pool: str = Field(default="remote_execution", description="Pool for remote-executed tasks.")


class RemoteCommand(Blueprint[RemoteCommandConfig]):
    """Run a remote shell command through SSH."""

    def render(self, config: RemoteCommandConfig):
        from airflow.providers.ssh.operators.ssh import SSHOperator

        with TaskGroup(group_id=self.step_id) as group:
            SSHOperator(
                task_id="run",
                ssh_conn_id=config.ssh_conn_id,
                command=self.param("command"),
                params={"command": config.command},
                pool=config.pool,
            )
        return group


class SnowflakeSqlConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    sql: str = Field(description="SQL statement to execute.")
    snowflake_conn_id: str = Field(default="snowflake_default", description="Snowflake connection ID.")
    warehouse: str = Field(default="TRANSFORMING", description="Snowflake warehouse.")
    database: str = Field(default="ANALYTICS", description="Snowflake database.")
    schema_name: str = Field(default="GOLD", alias="schema", description="Snowflake schema.")
    pool: str = Field(default="snowflake_queries", description="Pool for Snowflake work.")


class SnowflakeSql(Blueprint[SnowflakeSqlConfig]):
    """Execute SQL in Snowflake via the SQL API operator."""

    def render(self, config: SnowflakeSqlConfig):
        from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator

        with TaskGroup(group_id=self.step_id) as group:
            SnowflakeSqlApiOperator(
                task_id="execute_sql",
                snowflake_conn_id=config.snowflake_conn_id,
                sql=self.param("sql"),
                params={"sql": config.sql},
                warehouse=config.warehouse,
                database=config.database,
                schema=config.schema_name,
                pool=config.pool,
            )
        return group


class HttpAsyncJobConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    http_conn_id: str = Field(description="HTTP connection ID.")
    submit_endpoint: str = Field(description="Submission endpoint.")
    status_endpoint_prefix: str = Field(description="Prefix for job-status polling endpoint.")
    payload: dict[str, YamlScalar] = Field(default_factory=dict, description="JSON payload for submission.")
    tracking_id: str = Field(default="{{ run_id }}", description="Deterministic identifier used for submission and polling.")
    tracking_payload_field: str | None = Field(default=None, description="Optional payload field that should receive the tracking identifier.")
    completion_key: str = Field(default="state", description="Response key that contains terminal state.")
    success_values: list[str] = Field(default_factory=lambda: ["COMPLETED", "SUCCEEDED"], description="Values treated as successful completion.")
    method: str = Field(default="POST", description="HTTP method for submission.")
    poke_interval_seconds: int = Field(default=300, ge=5, description="Polling interval in seconds.")
    timeout_seconds: int = Field(default=36000, ge=60, description="Polling timeout in seconds.")
    pool: str = Field(default="cloud_control_plane", description="Pool for API orchestration tasks.")


class HttpAsyncJob(Blueprint[HttpAsyncJobConfig]):
    """Submit an API-managed job and wait for completion with a deferrable sensor."""

    def render(self, config: HttpAsyncJobConfig):
        from airflow.providers.http.operators.http import HttpOperator
        from airflow.providers.http.sensors.http import HttpSensor

        payload = dict(config.payload)
        if config.tracking_payload_field:
            payload[config.tracking_payload_field] = config.tracking_id

        with TaskGroup(group_id=self.step_id) as group:
            submit_request = HttpOperator(
                task_id="submit_request",
                http_conn_id=config.http_conn_id,
                endpoint=config.submit_endpoint,
                method=config.method,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                response_filter=lambda response: response.json(),
                pool=config.pool,
            )

            wait_for_completion = HttpSensor(
                task_id="wait_for_completion",
                http_conn_id=config.http_conn_id,
                endpoint=f"{config.status_endpoint_prefix}/{config.tracking_id}",
                response_check=lambda response: response.json().get(config.completion_key) in set(config.success_values),
                poke_interval=config.poke_interval_seconds,
                timeout=config.timeout_seconds,
                deferrable=True,
                pool=config.pool,
            )

            submit_request >> wait_for_completion
        return group


class DatabricksNotebookRunConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    databricks_conn_id: str = Field(default="databricks_default", description="Databricks connection ID.")
    run_name: str = Field(description="Databricks run name.")
    existing_cluster_id: str = Field(description="Existing cluster ID.")
    notebook_path: str = Field(description="Notebook path.")
    base_parameters: dict[str, YamlScalar] = Field(default_factory=dict, description="Notebook parameters.")
    pool: str = Field(default="databricks_jobs", description="Pool for Databricks job submission and monitoring.")


class DatabricksNotebookRun(Blueprint[DatabricksNotebookRunConfig]):
    """Submit a Databricks notebook run."""

    def render(self, config: DatabricksNotebookRunConfig):
        from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

        with TaskGroup(group_id=self.step_id) as group:
            DatabricksSubmitRunOperator(
                task_id="submit_run",
                databricks_conn_id=config.databricks_conn_id,
                json={
                    "run_name": config.run_name,
                    "existing_cluster_id": config.existing_cluster_id,
                    "notebook_task": {
                        "notebook_path": config.notebook_path,
                        "base_parameters": config.base_parameters,
                    },
                },
                pool=config.pool,
            )
        return group


class ValidatedScriptExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_os: str = Field(description="Target operating system.")
    script_kind: str = Field(description="Script type, such as shell or powershell.")
    script_path: str = Field(description="Path to the script on the remote host.")
    script_args: str = Field(default="", description="Arguments passed to the script.")
    windows_conn_id: str = Field(default="windows_remote_executor", description="Connection for Windows execution.")
    linux_conn_id: str = Field(default="linux_remote_executor", description="Connection for Linux execution.")
    pool: str = Field(default="remote_execution", description="Pool for remote-executed tasks.")


class ValidatedScriptExecution(Blueprint[ValidatedScriptExecutionConfig]):
    """Validate a requested script/runtime combination and bind it to one execution target."""

    def render(self, config: ValidatedScriptExecutionConfig):
        valid_combinations = {
            ("linux", "shell"): {
                "ssh_conn_id": config.linux_conn_id,
                "command": f'bash "{config.script_path}" {config.script_args}',
            },
            ("windows", "powershell"): {
                "ssh_conn_id": config.windows_conn_id,
                "command": f'powershell.exe -File "{config.script_path}" {config.script_args}',
            },
        }

        if (config.target_os, config.script_kind) not in valid_combinations:
            raise AirflowFailException(
                f"Invalid combination: target_os={config.target_os}, script_kind={config.script_kind}."
            )

        runner_config = valid_combinations[(config.target_os, config.script_kind)]

        from airflow.providers.ssh.operators.ssh import SSHOperator

        with TaskGroup(group_id=self.step_id) as group:
            SSHOperator(
                task_id="run_validated_script",
                ssh_conn_id=runner_config["ssh_conn_id"],
                command=runner_config["command"],
                pool=config.pool,
            )
        return group
