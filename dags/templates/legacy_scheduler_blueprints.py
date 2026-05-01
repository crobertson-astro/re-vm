from __future__ import annotations

import json

from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup

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
    """Run a local Bash command with BashOperator."""

    def render(self, config: LocalBashConfig):
        with TaskGroup(group_id=self.step_id) as group:
            BashOperator(
                task_id="run",
                bash_command=self.param("bash_command"),
                params={"bash_command": config.bash_command},
                env=config.env,
                cwd=config.cwd,
                append_env=config.append_env,
                pool=config.pool,
            )
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

        with TaskGroup(group_id=self.step_id) as group:
            SSHOperator(
                task_id="run_validated_script",
                ssh_conn_id=runner_config["ssh_conn_id"],
                command=runner_config["command"],
                pool=config.pool,
            )
        return group
