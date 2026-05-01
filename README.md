# Legacy Scheduler Migration Example DAGs

This repo contains example Airflow DAGs that model common legacy-scheduler-to-Astro migration patterns for deployments using Astronomer Remote Execution.

## Included DAGs

### Python-authored examples

- `example_sftp_lakehouse_remote` — SFTP pull, Bronze/Silver/Gold processing, Snowflake publish
- `example_long_running_batch_deferrable` — long-running batch with deferrable polling
- `example_web_service_jobs` — HTTP trigger + deferrable status polling
- `example_databricks_snowflake_orchestration` — Databricks compute plus Snowflake publish
- `example_powershell_and_shell_guardrails` — guardrails for Windows PowerShell vs Linux shell execution
- `example_hybrid_onprem_cloud` — hybrid on-prem to cloud orchestration chain

### Blueprint-authored examples

- `blueprint_sftp_lakehouse_remote` — YAML-composed SFTP to lakehouse to Snowflake flow
- `blueprint_long_running_batch_deferrable` — YAML-composed long-running API batch with deferrable polling
- `blueprint_databricks_snowflake_orchestration` — YAML-composed Databricks plus Snowflake flow
- `blueprint_hybrid_onprem_cloud` — YAML-composed on-prem to cloud orchestration flow
- `blueprint_script_execution_guardrails` — YAML-composed cross-platform script execution guardrails

### Blueprint files

- `dags/blueprint_dag_loader.py` — Blueprint loader that builds all YAML DAGs
- `dags/templates/example_dag_args.py` — custom DAG-level YAML config for tags, retries, owner, and scheduling
- `dags/templates/legacy_scheduler_blueprints.py` — reusable Blueprint templates for SFTP, remote commands, API jobs, Databricks, Snowflake, and script validation
- `dags/*.dag.yaml` — declarative DAG compositions built from those templates
- `blueprint/generated-schemas/*.schema.json` — checked-in Blueprint schemas for Astro IDE/editor support

## Example Connection IDs

- `vendor_sftp_default`
- `batch_service_api`
- `service_api_default`
- `databricks_default`
- `snowflake_default`
- `windows_remote_executor`
- `linux_remote_executor`
- `ingestion_gateway`

## Example Pools

These DAGs use pools to make execution domains visible in the graph and to make concurrency controls explicit:

- `remote_execution` — on-prem or remote-executed tasks
- `cloud_control_plane` — lightweight API orchestration tasks
- `databricks_jobs` — Databricks job submissions and monitoring
- `snowflake_queries` — Snowflake publish and reconciliation tasks

## Notes

- The DAG code is portable Airflow code. The actual Remote Execution routing is configured in Astro, not in DAG syntax.
- The examples avoid XCom-based coordination so they remain compatible with remote execution environments where XCom is unavailable.
- The `remote_execution` pool in these examples is a modeling aid for concurrency and intent.
- The Blueprint examples use `airflow-blueprint` and compose DAGs from YAML via `dags/blueprint_dag_loader.py`.
- `blueprint/generated-schemas/` is committed intentionally so Astro IDE and other editors can use the current Blueprint schemas without an extra generation step after clone.
- To regenerate schemas after editing Blueprint templates, run `blueprint schema <name> --template-dir dags/templates > blueprint/generated-schemas/<name>.schema.json` for each template.
- The Blueprint Databricks examples use a plain `REPLACE_WITH_DATABRICKS_CLUSTER_ID` placeholder so offline linting works cleanly in a public repo.
- Replace the placeholder endpoints, paths, job IDs, schemas, stored procedures, and cluster IDs with your own environment-specific values before deployment.
