# MongoClaw Ops Console

Visual dashboard for MongoClaw runtime operations.

## What it shows
- Cluster health (`/health`, `/health/detailed`)
- Agent inventory and status (`/api/v1/agents`) with one-click `Edit`
- Agent controls: enable, disable, validate
- Quick-create wizard for new `database.collection` agents (dropdown-first)
- Existing-agent editor (select + view + update + delete)
- Execution stream (`/api/v1/executions`)
- Execution filters by agent/status + adjustable stats window (`hours`)
- 24h execution distribution (`/api/v1/executions/stats`)
- Resilience metrics (`/metrics`) including DLQ, retries, loop-guard, SLO violations, quarantine, circuit breaker state
- Collection Explorer (`/api/v1/catalog/collection-profile`) showing:
  - inferred schema (human-readable fields + types)
  - applied agents for the selected collection
  - enrichment coverage stats
- Connection test actions for onboarding validation

## Run
1. Start MongoClaw API (usually at `http://127.0.0.1:8000`).
2. Serve this folder as static files:

```bash
cd /Users/supreethravi/supreeth/mongoclaw/ui
python3 -m http.server 4173
```

3. Open [http://127.0.0.1:4173](http://127.0.0.1:4173).
4. In the UI, set:
- `API Base URL` (example: `http://127.0.0.1:8000`)
- `X-API-Key` (example: `test-key` or your configured key)

Settings are stored in browser local storage.

## Notes
- This UI supports both onboarding and operations workflows.
- If endpoints fail with `401`, verify `X-API-Key` matches `MONGOCLAW_SECURITY__API_KEYS`.
- If CORS issues appear, ensure API CORS origins allow your dashboard URL.
- Auto-refresh runs continuously; right-click `Refresh` to pause/resume polling.
