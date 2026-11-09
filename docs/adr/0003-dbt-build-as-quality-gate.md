# 0003 — dbt build as the quality gate

**Context.** A pipeline that publishes wrong numbers is worse than one that publishes nothing.

**Decision.** `dbt build` runs every model and its tests in dependency order; any failing test fails the task. `quality_report` and `publish_site` only run after it succeeds. Every task has retries, an execution timeout and a failure callback that logs the failure and posts it to a webhook when `ALERT_WEBHOOK_URL` is set. DAG integrity tests in CI enforce these rules.

**Consequences.**
- The public page is never rebuilt from data that failed its tests.
- Failures are visible within minutes, with the task and run that failed.
