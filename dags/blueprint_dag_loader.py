from __future__ import annotations

from pathlib import Path

from airflow.sdk import DAG
from blueprint import build_all
from blueprint.registry import BlueprintRegistry

DAGS_DIR = Path(__file__).parent
TEMPLATES_DIR = DAGS_DIR / "templates"


def add_blueprint_metadata(dag: DAG, yaml_path: Path) -> None:
    existing_tags = list(dag.tags or [])
    for tag in ["blueprint", f"yaml:{yaml_path.stem}"]:
        if tag not in existing_tags:
            existing_tags.append(tag)
    dag.tags = existing_tags


build_all(
    search_path=DAGS_DIR,
    register_globals=globals(),
    bp_registry=BlueprintRegistry(
        template_dirs=[TEMPLATES_DIR],
        exclude_files={Path(__file__)},
    ),
    on_dag_built=add_blueprint_metadata,
)
