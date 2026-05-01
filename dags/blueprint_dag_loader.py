from __future__ import annotations

from airflow.sdk import DAG # needed for dag discovery
from blueprint import build_all


def add_blueprint_metadata(dag, yaml_path) -> None:
    dag.tags = [*(dag.tags or []), "blueprint", f"yaml:{yaml_path.stem}"]

build_all(
    on_dag_built=add_blueprint_metadata,
)
