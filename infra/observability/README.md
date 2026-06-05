# Observability Stack

OpenTelemetry Collector + Prometheus + Grafana + exporters, wiring up the four
main technologies (Airflow, Spark, MinIO, Iceberg/Postgres). It runs separately
from the data infra and joins the same `datalake-network`.

```
Airflow  ──OTLP──►  otel-collector ──► Prometheus ──► Grafana
Spark    ──/metrics/prometheus──────►  Prometheus
MinIO    ──/minio/v2/metrics──────────► Prometheus
Postgres ──postgres-exporter─────────► Prometheus
                                        Grafana (also queries Postgres directly
                                        for the control-plane dashboard)
```

## Bring it up

```bash
make obs-up          # Grafana http://localhost:3000 (admin/admin), Prometheus :9090
make logs-obs
make obs-down
```

## Enabling each source

| Source | What to do | Notes |
|---|---|---|
| **Airflow** | `pip install 'apache-airflow[otel]'`, set `AIRFLOW__METRICS__OTEL_ON=True` + `AIRFLOW__TRACES__OTEL_ON=True` in `infra/airflow/airflow.env`, re-apply (`make airflow-install-services`) and restart. | OFF by default so Airflow doesn't log export errors before the collector is up. Custom spans: `from airflow.sdk.observability import trace`. |
| **Spark** | Already enabled (`spark.ui.prometheus.enabled=true` in `spark-defaults.conf`). | `:4040/metrics/prometheus` only exists **while** a Silver/Gold notebook runs. |
| **MinIO** | `MINIO_PROMETHEUS_AUTH_TYPE=public` is set; recreate MinIO (`make infra-down && make infra-up`) so it takes effect. | Otherwise scrapes get 403 (needs a bearer token). |
| **Postgres** | Set `PG_EXPORTER_DSN` (and the Grafana `PG_USER`/`PG_PASSWORD`) for your DB. | Exposes DB health + the control-plane dashboard. |

## Dashboards

`grafana/dashboards/pipeline_overview.json` is auto-provisioned (folder
"Football Pipeline") and reads the control plane directly:
- seasons by status, quality pass/warn/fail, completed count,
- failed seasons (stage + error), recent quality results.

## Spark History Server (opt-in)

To inspect a run after the ephemeral `jupyter-spark` container stops:
1. Create the event-log prefix and enable event logging in `spark-defaults.conf`:
   `spark.eventLog.enabled=true` / `spark.eventLog.dir=s3a://datalake-artifacts/spark-events`.
2. `docker compose --profile history up -d spark-history` → http://localhost:18080

## Iceberg maintenance

Table-format observability + hygiene (snapshot expiry, small-file compaction)
is handled by the **`iceberg_maintenance`** Airflow DAG (weekly), which runs
`rewrite_data_files` + `expire_snapshots` and logs per-table file/snapshot counts.

## Status

Configuration and the stack are authored here; **live verification (bringing
the stack up and confirming metrics/dashboards) is the remaining step** — the
data pipelines themselves are already validated end-to-end.
