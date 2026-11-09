"""Extract: bring the bronze layer up to date from REE and Open-Meteo, one mapped task per source."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from include.alerts import notify_failure
from include.settings import BRONZE, BRONZE_DIR, HISTORY_START

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="grid_pulse_extract",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Europe/Madrid"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["grid-pulse", "extract"],
    doc_md=__doc__,
)
def grid_pulse_extract():
    @task
    def check_sources() -> list[str]:
        """Fail fast if an API is down, before spending retries on every source."""
        from gridpulse.http import get_json
        from gridpulse.ingest import source_names
        from gridpulse.sources import OPEN_METEO_BASE_URL, REE_BASE_URL

        day = pendulum.yesterday("Europe/Madrid").to_date_string()
        get_json(f"{REE_BASE_URL}/demanda/evolucion",
                 {"start_date": f"{day}T00:00", "end_date": f"{day}T23:59", "time_trunc": "day"}, retries=2)
        get_json(OPEN_METEO_BASE_URL,
                 {"latitude": 40.42, "longitude": -3.70, "start_date": "2026-01-01", "end_date": "2026-01-01",
                  "daily": "temperature_2m_mean"}, retries=2)
        return source_names()

    @task(max_active_tis_per_dagrun=3)
    def extract(source: str, data_interval_end=None) -> dict:
        """One source per task: a failing source is retried alone and does not block the others."""
        from gridpulse.ingest import ingest_source

        until = data_interval_end.in_timezone("Europe/Madrid").date()
        files = ingest_source(BRONZE_DIR, source, HISTORY_START, until)
        return {"source": source, "files": files}

    @task(outlets=[BRONZE])
    def summarize(results: list[dict]) -> int:
        """Emit the BRONZE asset event that triggers the transform DAG."""
        total = sum(r["files"] for r in results)
        for r in results:
            print(f"{r['source']:<20} {r['files']:>3} file(s)")
        print(f"total: {total} file(s) written")
        return total

    summarize(extract.expand(source=check_sources()))


grid_pulse_extract()
