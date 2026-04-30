# Legacy Scheduler Migration Example DAGs

This repo contains example Airflow DAGs that model common legacy-scheduler-to-Astro migration patterns for deployments using Astronomer Remote Execution.

## Included DAGs

- `example_sftp_lakehouse_remote` — SFTP pull, Bronze/Silver/Gold processing, Snowflake publish
- `example_long_running_batch_deferrable` — long-running batch with deferrable polling
- `example_web_service_jobs` — HTTP trigger + deferrable status polling
- `example_databricks_snowflake_orchestration` — Databricks compute plus Snowflake publish
- `example_powershell_and_shell_guardrails` — guardrails for Windows PowerShell vs Linux shell execution
- `example_hybrid_onprem_cloud` — hybrid on-prem to cloud orchestration chain

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
- The `remote_execution` pool in these examples is a modeling aid for concurrency and intent.
- Replace the placeholder endpoints, paths, job IDs, schemas, and stored procedures with your own environment-specific values before deployment.
