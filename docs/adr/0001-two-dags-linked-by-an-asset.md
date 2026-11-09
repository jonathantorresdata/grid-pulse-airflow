# 0001 — Two DAGs linked by an asset

**Context.** Extraction depends on external APIs and runs on a clock; transformation depends only on the data having changed.

**Decision.** `grid_pulse_extract` runs daily at 06:00 Europe/Madrid and, when every source succeeds, emits the `gridpulse://bronze` asset event. `grid_pulse_transform` is scheduled on that asset (data-aware scheduling), not on a clock.

**Consequences.**
- Transformation never runs on stale or half-loaded data, and never waits on a guessed time window.
- A manual re-extraction (or a backfill) automatically triggers a rebuild.
- Each DAG can be retried and reasoned about on its own.
