from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from blueprint import BaseModel, BlueprintDagArgs, Field
from pydantic import ConfigDict


class ExampleDagArgsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schedule: str | None = Field(default=None, description="DAG schedule expression.")
    description: str | None = Field(default=None, description="DAG description.")
    tags: list[str] = Field(default_factory=list, description="DAG tags.")
    owner: str = Field(default="data-platform", description="Default task owner.")
    retries: int = Field(default=2, ge=2, description="Task retries.")
    retry_delay_minutes: int = Field(default=5, ge=1, description="Retry delay in minutes.")
    start_date: str = Field(default="2024-01-01", description="UTC start date in YYYY-MM-DD format.")
    catchup: bool = Field(default=False, description="Whether catchup is enabled.")
    max_active_runs: int = Field(default=1, ge=1, description="Maximum concurrent DAG runs.")


class ExampleDagArgs(BlueprintDagArgs[ExampleDagArgsConfig]):
    def render(self, config: ExampleDagArgsConfig) -> dict[str, Any]:
        return {
            "schedule": config.schedule,
            "description": config.description,
            "tags": config.tags,
            "start_date": pendulum.parse(config.start_date, tz="UTC"),
            "catchup": config.catchup,
            "max_active_runs": config.max_active_runs,
            "default_args": {
                "owner": config.owner,
                "retries": config.retries,
                "retry_delay": timedelta(minutes=config.retry_delay_minutes),
            },
        }
