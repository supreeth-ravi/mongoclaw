# E2E Scaled Testing Design

**Date:** 2026-02-26
**Status:** Approved
**Scope:** Full feature coverage + load/stress testing via Python SDK and Node SDK (Approach A — single pytest suite)

---

## Goals

1. Test every MongoClaw feature end-to-end against a live server (real MongoDB + Redis + AI provider)
2. Exercise both the Python SDK and the Node.js SDK with the same assertions
3. Validate load/stress scenarios: throughput, p95 latency, resilience under burst
4. Produce both HTML and JSON reports per run under `tests/e2e/reports/<timestamp>/`

---

## Test Environment

- MongoClaw server running (`mongoclaw server start`)
- MongoDB replica set (`rs0`) — required for change streams
- Redis running
- Real AI provider key configured (OpenRouter or OpenAI)
- `sdk-nodejs/dist/` compiled (`npm run build` in `sdk-nodejs/`)
- Environment variables:
  - `MONGOCLAW_BASE_URL` (default: `http://localhost:8000`)
  - `MONGOCLAW_API_KEY`
  - `MONGOCLAW_MONGODB__URI`
  - `MONGOCLAW_REDIS__URL`

---

## File Structure

```
tests/e2e/
├── conftest.py                # Session-scoped live fixtures, SDK clients, cleanup
├── node_adapter.py            # Python wrapper calling Node SDK via subprocess (.mjs)
├── test_A_health.py           # A1: health/readiness, A2: /metrics gauges
├── test_B_method_coverage.py  # B1: CLI, B2: Python SDK, B3: Node SDK, B4: REST parity
├── test_C_ai_execution.py     # C1: insert-driven, C2: update-driven, C3: schema-guided, C4: write strategies
├── test_D_consistency.py      # D1: eventual, D2: strict conflicts, D3: shadow, D4: hash guard
├── test_E_loop_idempotency.py # E1: loop guard, E2: duplicate delivery idempotency
├── test_F_resilience.py       # F1: baseline CRUD, F2: burst+retries, F3: backpressure, F4: DLQ, F5: quarantine
├── test_G_policy.py           # G1: enrich/block/tag, G2: simulation mode
├── test_H_observability.py    # H1: lifecycle fields, H2: counter verification
├── test_I_catalog.py          # Catalog overview + collection profile
└── test_scenarios.py          # Pack 1: support tickets, Pack 2: order risk, Pack 3: burst, Pack 4: policy flow
```

---

## conftest.py — Session Fixtures

| Fixture | Scope | Description |
|---|---|---|
| `live_server_url` | session | Reads `MONGOCLAW_BASE_URL`, default `http://localhost:8000` |
| `api_key` | session | Reads `MONGOCLAW_API_KEY` |
| `py_client` | session | `MongoClawClient(base_url, api_key)` |
| `node_client` | session | `NodeSDKClient` (subprocess wrapper) |
| `mongo_direct` | session | Motor `AsyncIOMotorClient` for pre-inserting docs + asserting writebacks |
| `redis_direct` | session | `redis.asyncio` client for queue state inspection |
| `wait_for_execution` | function | Polls `GET /api/v1/executions?agent_id=X` until terminal status or timeout |
| `cleanup_agents` | autouse, session | Deletes all agents with `e2e_` prefix after the session |

---

## node_adapter.py — Node SDK Subprocess Wrapper

`NodeSDKClient` mirrors `MongoClawClient` but runs each call as a Node.js subprocess:

- Generates a temp `.mjs` script per call
- Passes args as JSON via stdin or CLI args
- Captures stdout (JSON) and parses into equivalent Python dicts
- Raises `NodeSDKError` on non-zero exit or JSON parse failure

Methods: `health()`, `list_agents()`, `get_agent()`, `create_agent()`, `update_agent()`, `delete_agent()`, `enable_agent()`, `disable_agent()`, `validate_agent()`, `list_executions()`, `get_execution()`, `trigger_agent()`, `wait_for_execution()`

---

## Test Coverage Matrix

### A — Health (`test_A_health.py`)

| ID | Test | SDKs |
|---|---|---|
| A1 | `/health`, `/health/ready`, `/health/detailed` — all components healthy | Python, Node |
| A2 | `/metrics` — parse Prometheus text, assert key counters present | Python |

### B — Method Coverage (`test_B_method_coverage.py`)

| ID | Test | SDKs |
|---|---|---|
| B1 | CLI: create, list, validate, enable, disable, delete agent | CLI subprocess |
| B2 | Python SDK: full agent lifecycle + `trigger_agent` + assert writeback | Python |
| B3 | Node SDK: full agent lifecycle + `trigger_agent` + assert writeback | Node |
| B4 | REST API parity: same lifecycle via raw `httpx` calls | Direct HTTP |

### C — AI Execution (`test_C_ai_execution.py`)

| ID | Test | SDKs |
|---|---|---|
| C1 | Insert document → change stream triggers agent → `_ai_metadata` written | Python, Node |
| C2 | Update document → reprocessing → execution recorded | Python, Node |
| C3 | Strict `response_schema` → structured fields parsed and persisted | Python |
| C4 | Write strategy matrix: `merge`, `replace`, `append`, `nested`, `target_field` | Python, Node |

### D — Consistency (`test_D_consistency.py`)

| ID | Test | SDKs |
|---|---|---|
| D1 | `eventual` — high throughput, no stale-write blocks | Python |
| D2 | `strict_post_commit` — concurrent updates, stale writes blocked with conflict reason | Python |
| D3 | `shadow` mode — execution runs, write skipped, shadow counters increment | Python, Node |
| D4 | `require_document_hash_match` — hash mismatch blocks write, metrics reflect conflict | Python |

### E — Loop Safety & Idempotency (`test_E_loop_idempotency.py`)

| ID | Test | SDKs |
|---|---|---|
| E1 | Agent watches its own write target — no infinite cascade, loop-guard metrics present | Python |
| E2 | Same work item replayed twice — no duplicate enrichment, idempotency key deduplication | Python |

### F — Resilience (`test_F_resilience.py`)

| ID | Test | SDKs |
|---|---|---|
| F1 | Baseline concurrent CRUD: mixed insert/update/delete, multiple workers, no crash | Python |
| F2 | Burst + forced retries: tight timeout profile, retry path active, bounded failures | Python |
| F3 | Backpressure: queue flooded, admission decisions visible in `/metrics` | Python |
| F4 | DLQ path: force terminal failures, DLQ counters increment | Python |
| F5 | Quarantine: repeated agent failures → quarantine active, noisy agent contained | Python |

### G — Policy (`test_G_policy.py`)

| ID | Test | SDKs |
|---|---|---|
| G1 | Policy conditions: `enrich`, `block`, `tag` — correct action + persisted reason | Python, Node |
| G2 | Policy simulation mode: decisions logged, writes skipped per config | Python |

### H — Observability (`test_H_observability.py`)

| ID | Test | SDKs |
|---|---|---|
| H1 | Execution records: `status`, `lifecycle_state`, `reason`, `written`, `attempt` all present | Python, Node |
| H2 | Metrics completeness: induce retries/conflicts/quarantine, assert counters increment | Python |

### I — Catalog (`test_I_catalog.py`)

| ID | Test | SDKs |
|---|---|---|
| I1 | `GET /api/v1/catalog/overview` — runtime inventory present | Python, Node |
| I2 | `GET /api/v1/catalog/collection-profile` — schema + enrichment stats | Python |

### Scenario Packs (`test_scenarios.py`)

| Pack | Scenario | Load Profile |
|---|---|---|
| Pack 1 | Support ticket triage — insert tickets, assert category/priority/summary fields | 50 docs × 2 agents |
| Pack 2 | Commerce order risk tagging — concurrent CRUD, strict conflict behavior | 100 docs × 3 agents, concurrent |
| Pack 3 | Burst traffic + provider stress — burst writes, tight timeout, p50/p95/p99 latency | 200 docs, burst |
| Pack 4 | Policy-enforced flow — enrich/block/tag decisions, audit trail | 50 docs × policy agent |

---

## Load Test Parameters (F1 + Scenario Packs)

| Parameter | Default | Env Override |
|---|---|---|
| Agents per load test | 5 | `E2E_LOAD_AGENTS` |
| Documents per agent | 20 | `E2E_LOAD_DOCS` |
| Concurrency | 10 workers | `E2E_LOAD_CONCURRENCY` |
| Max execution wait | 120s | `E2E_LOAD_TIMEOUT` |
| p95 latency SLO | 10s | `E2E_LATENCY_SLO_MS` |
| Min success rate | 90% | `E2E_MIN_SUCCESS_RATE` |

---

## Reporting

- **HTML:** `pytest-html` → `tests/e2e/reports/<timestamp>/report.html`
- **JSON:** `pytest-json-report` → `tests/e2e/reports/<timestamp>/report.json`
- **Terminal:** verbose `-v` output with pass/fail per test ID

Run command:
```bash
pytest tests/e2e/ -v \
  --html=tests/e2e/reports/$(date +%Y%m%d_%H%M%S)/report.html \
  --json-report --json-report-file=tests/e2e/reports/$(date +%Y%m%d_%H%M%S)/report.json
```

---

## Out of Scope

- H3: UI real-time monitoring (requires browser automation)
- I1–I4: Product UI workflows (requires Playwright/Selenium)
- Multi-pod / cross-region coordination tests
