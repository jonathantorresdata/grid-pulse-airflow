# 0002 — One mapped task per source

**Context.** Seven sources (three REE datasets, four weather points) fail independently: one API may be slow or down while the others are fine.

**Decision.** `check_sources` fails fast if an API is down, then returns the list of sources; `extract.expand(source=...)` creates one task per source at runtime (dynamic task mapping), at most three at a time to stay polite with the APIs. Each task retries with exponential backoff.

**Consequences.**
- A failing source is retried alone; the others are not re-downloaded.
- Adding a source to the pipeline package adds a task with no DAG change.
- The Airflow UI shows exactly which source failed.
