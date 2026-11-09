"""Transform and publish: dbt build (models + tests) whenever bronze changes, then rebuild the site."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from include.alerts import notify_failure
from include.settings import BRONZE, DBT_DIR, DUCKDB_PATH, GOLD, SITE_DIR

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="grid_pulse_transform",
    schedule=[BRONZE],  # data-aware: runs after every successful extraction
    start_date=pendulum.datetime(2026, 11, 1, tz="Europe/Madrid"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["grid-pulse", "dbt"],
    doc_md=__doc__,
)
def grid_pulse_transform():
    # dbt build runs every model and test in dependency order; a failed test fails the task,
    # so the site below is never rebuilt from bad data (quality gate).
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt build --project-dir . --profiles-dir . --no-use-colors",
        cwd=str(DBT_DIR),
    )

    @task
    def quality_report() -> dict:
        """Numbers worth watching after each run, kept in the task log and XCom."""
        import duckdb

        with duckdb.connect(str(DUCKDB_PATH), read_only=True) as con:
            last_price, hours, free_hours = con.execute("""
                SELECT max(price_date), count(*), count(*) FILTER (WHERE is_free_or_negative)
                FROM gold.fct_prices_hourly""").fetchone()
            renewable = con.execute("""
                SELECT round(100 * avg(renewable_share), 1) FROM gold.fct_energy_daily
                WHERE date_day > current_date - INTERVAL 30 DAY""").fetchone()[0]
        report = {"last_price_date": str(last_price), "hourly_prices": hours,
                  "free_or_negative_hours": free_hours, "renewable_share_30d_pct": renewable}
        print(report)
        return report

    @task(outlets=[GOLD])
    def publish_site() -> str:
        from gridpulse.site import build_site

        return str(build_site(DUCKDB_PATH, SITE_DIR))

    dbt_build >> quality_report() >> publish_site()


grid_pulse_transform()
