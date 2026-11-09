# 0004 — Pin Airflow and DuckDB

**Context.** The first end-to-end run in Airflow failed: `dbt build` ran out of memory while creating the staging views. The pipeline worked on its own, so the difference had to be the environment. The Airflow environment had resolved DuckDB 2.0, whose JSON schema inference used all 12 GB of RAM on the 300+ bronze files; the pipeline had been developed on 1.5.6.

**Decision.** Install Airflow with its official constraints file, pin `duckdb==1.5.6` and `dbt-duckdb==1.11.1` in `requirements.txt`, and declare `duckdb>=1.1,<2` in the pipeline package.

**Consequences.**
- The same versions run on a laptop, in Docker and in CI.
- Upgrades become deliberate changes that go through the tests, not surprises.
