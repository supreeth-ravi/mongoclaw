const state = {
  timerId: null,
  pollSeconds: 5,
  pollEnabled: true,
  inFlight: false,
  catalog: { databases: [], watched_collections: [] },
  agents: [],
};

const elements = {
  baseUrl: document.getElementById("baseUrl"),
  apiKey: document.getElementById("apiKey"),
  pollingInterval: document.getElementById("pollingInterval"),
  executionLimit: document.getElementById("executionLimit"),
  saveConfigButton: document.getElementById("saveConfigButton"),
  connectionTestButton: document.getElementById("connectionTestButton"),
  refreshButton: document.getElementById("refreshButton"),
  autoRefreshState: document.getElementById("autoRefreshState"),
  lastUpdated: document.getElementById("lastUpdated"),
  healthValue: document.getElementById("healthValue"),
  healthDetail: document.getElementById("healthDetail"),
  agentsValue: document.getElementById("agentsValue"),
  agentsDetail: document.getElementById("agentsDetail"),
  executionsValue: document.getElementById("executionsValue"),
  successRateValue: document.getElementById("successRateValue"),
  dlqValue: document.getElementById("dlqValue"),
  retryValue: document.getElementById("retryValue"),
  statusBars: document.getElementById("statusBars"),
  loopSkips: document.getElementById("loopSkips"),
  sloViolations: document.getElementById("sloViolations"),
  quarantineActive: document.getElementById("quarantineActive"),
  openBreakers: document.getElementById("openBreakers"),
  apiP95: document.getElementById("apiP95"),
  agentsTableBody: document.getElementById("agentsTableBody"),
  executionsTableBody: document.getElementById("executionsTableBody"),
  eventsLog: document.getElementById("eventsLog"),
  executionAgentFilter: document.getElementById("executionAgentFilter"),
  executionStatusFilter: document.getElementById("executionStatusFilter"),
  executionHours: document.getElementById("executionHours"),
  applyExecutionFilterButton: document.getElementById("applyExecutionFilterButton"),
  qcDatabase: document.getElementById("qcDatabase"),
  qcCollection: document.getElementById("qcCollection"),
  qcProvider: document.getElementById("qcProvider"),
  qcConsistencyMode: document.getElementById("qcConsistencyMode"),
  qcAgentId: document.getElementById("qcAgentId"),
  qcAgentName: document.getElementById("qcAgentName"),
  qcModel: document.getElementById("qcModel"),
  qcTargetField: document.getElementById("qcTargetField"),
  qcFilterJson: document.getElementById("qcFilterJson"),
  qcPrompt: document.getElementById("qcPrompt"),
  opInsert: document.getElementById("opInsert"),
  opUpdate: document.getElementById("opUpdate"),
  opReplace: document.getElementById("opReplace"),
  opDelete: document.getElementById("opDelete"),
  qcCreateButton: document.getElementById("qcCreateButton"),
  agentSelect: document.getElementById("agentSelect"),
  loadAgentButton: document.getElementById("loadAgentButton"),
  updateAgentButton: document.getElementById("updateAgentButton"),
  deleteAgentButton: document.getElementById("deleteAgentButton"),
  agentJson: document.getElementById("agentJson"),
  explorerDatabase: document.getElementById("explorerDatabase"),
  explorerCollection: document.getElementById("explorerCollection"),
  explorerSampleSize: document.getElementById("explorerSampleSize"),
  loadCollectionProfileButton: document.getElementById("loadCollectionProfileButton"),
  collectionStats: document.getElementById("collectionStats"),
  collectionAgents: document.getElementById("collectionAgents"),
  schemaTableBody: document.getElementById("schemaTableBody"),
};

const saved = JSON.parse(localStorage.getItem("mongoclaw_ops_ui") || "{}");
elements.baseUrl.value = saved.baseUrl || "http://127.0.0.1:8000";
elements.apiKey.value = saved.apiKey || "test-key";
elements.pollingInterval.value = Number(saved.pollingInterval || 5);
elements.executionLimit.value = Number(saved.executionLimit || 50);
elements.executionHours.value = Number(saved.executionHours || 24);
elements.qcProvider.value = saved.qcProvider || "custom";
elements.qcConsistencyMode.value = saved.qcConsistencyMode || "eventual";
elements.qcModel.value = saved.qcModel || "openrouter/openai/gpt-4o-mini";
elements.qcTargetField.value = saved.qcTargetField || "ai_triage";
elements.qcPrompt.value =
  saved.qcPrompt ||
  "Classify and return strict JSON with category, priority, summary. id={{ document._id }}";

function logEvent(message) {
  const line = `[${new Date().toLocaleTimeString()}] ${message}`;
  elements.eventsLog.textContent = `${line}\n${elements.eventsLog.textContent || ""}`.trim();
}

function setAutoRefreshState() {
  elements.autoRefreshState.className = `chip ${state.pollEnabled ? "chip-ok" : "chip-warn"}`;
  elements.autoRefreshState.textContent = `auto: ${state.pollEnabled ? "on" : "off"}`;
}

function persistConfig() {
  const next = {
    baseUrl: elements.baseUrl.value.trim().replace(/\/$/, ""),
    apiKey: elements.apiKey.value.trim(),
    pollingInterval: Number(elements.pollingInterval.value || 5),
    executionLimit: Number(elements.executionLimit.value || 50),
    executionHours: Number(elements.executionHours.value || 24),
    qcProvider: elements.qcProvider.value,
    qcConsistencyMode: elements.qcConsistencyMode.value,
    qcModel: elements.qcModel.value.trim(),
    qcTargetField: elements.qcTargetField.value.trim(),
    qcPrompt: elements.qcPrompt.value.trim(),
  };
  localStorage.setItem("mongoclaw_ops_ui", JSON.stringify(next));
  return next;
}

function saveConfig() {
  const next = persistConfig();
  state.pollSeconds = Math.max(2, Math.min(120, next.pollingInterval));
  state.pollEnabled = state.pollSeconds > 0;
  restartPolling();
  setAutoRefreshState();
}

async function apiRequest(path, options = {}) {
  const baseUrl = elements.baseUrl.value.trim().replace(/\/$/, "");
  const apiKey = elements.apiKey.value.trim();
  const headers = new Headers(options.headers || {});
  if (apiKey) headers.set("X-API-Key", apiKey);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const response = await fetch(`${baseUrl}${path}`, { ...options, headers, signal: controller.signal });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`${response.status} ${response.statusText}: ${text.slice(0, 180)}`);
    }
    const contentType = response.headers.get("content-type") || "";
    return contentType.includes("application/json") ? response.json() : response.text();
  } finally {
    clearTimeout(timeout);
  }
}

function safe(v) {
  return String(v ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function statusChip(status) {
  const s = String(status || "").toLowerCase();
  let cls = "chip-warn";
  if (["healthy", "enabled", "completed"].includes(s)) cls = "chip-ok";
  if (["failed", "disabled", "unhealthy"].includes(s)) cls = "chip-bad";
  return `<span class="chip ${cls}">${status || "-"}</span>`;
}

function setOptions(selectEl, values, placeholder = "select") {
  const current = selectEl.value;
  const opts = [`<option value="">${placeholder}</option>`];
  for (const v of values) opts.push(`<option value="${safe(v)}">${safe(v)}</option>`);
  selectEl.innerHTML = opts.join("");
  if (values.includes(current)) selectEl.value = current;
}

function formatDate(value) {
  if (!value) return "-";
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? "-" : d.toLocaleString();
}

async function withErrorGuard(name, fn) {
  try {
    await fn();
  } catch (error) {
    logEvent(`${name} failed: ${error.message}`);
  }
}

async function loadCatalog() {
  const data = await apiRequest("/api/v1/catalog/overview");
  state.catalog = data;
  const dbs = data.databases.map((d) => d.database);
  setOptions(elements.qcDatabase, dbs, "database");
  setOptions(elements.explorerDatabase, dbs, "database");
  await refreshCollectionsForDb(elements.qcDatabase, elements.qcCollection);
  await refreshCollectionsForDb(elements.explorerDatabase, elements.explorerCollection);
}

async function refreshCollectionsForDb(dbSelect, colSelect) {
  const db = dbSelect.value;
  const found = state.catalog.databases.find((d) => d.database === db);
  const cols = found ? found.collections : [];
  setOptions(colSelect, cols, "collection");
}

async function loadHealth() {
  const [h, d] = await Promise.all([apiRequest("/health"), apiRequest("/health/detailed")]);
  elements.healthValue.innerHTML = statusChip(d.status || h.status);
  elements.healthDetail.textContent = `MongoDB: ${d.components?.mongodb?.status || "unknown"}, env: ${d.environment || "-"}`;
}

async function loadAgents() {
  const data = await apiRequest("/api/v1/agents?limit=1000");
  const agents = Array.isArray(data.agents) ? data.agents : [];
  state.agents = agents;
  const enabled = agents.filter((a) => a.enabled).length;
  elements.agentsValue.textContent = `${enabled} / ${agents.length}`;
  elements.agentsDetail.textContent = `disabled: ${agents.length - enabled}`;

  const agentIds = agents.map((a) => a.id);
  setOptions(elements.agentSelect, agentIds, "select agent");
  setOptions(elements.executionAgentFilter, ["", ...agentIds], "all agents");

  elements.agentsTableBody.innerHTML = agents.length
    ? agents
        .map(
          (a) => `<tr>
      <td>${safe(a.id)}</td>
      <td>${safe(a.name)}</td>
      <td>${statusChip(a.enabled ? "enabled" : "disabled")}</td>
      <td>${safe(a.database)}.${safe(a.collection)} (${safe((a.operations || []).join(","))})</td>
      <td>${safe(a.provider)}/${safe(a.model)}</td>
      <td>${safe((a.tags || []).join(", ") || "-")}</td>
      <td><div class="actions-inline">
      <button class="mini-button" data-action="edit" data-id="${safe(a.id)}">Edit</button>
      <button class="mini-button" data-action="${a.enabled ? "disable" : "enable"}" data-id="${safe(a.id)}">${a.enabled ? "Disable" : "Enable"}</button>
      <button class="mini-button" data-action="validate" data-id="${safe(a.id)}">Validate</button>
      </div></td>
      </tr>`
        )
        .join("")
    : `<tr><td colspan="7" class="muted">No agents found</td></tr>`;

  document.querySelectorAll("[data-action]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = btn.getAttribute("data-id");
      const action = btn.getAttribute("data-action");
      if (!id || !action) return;
      if (action === "edit") {
        elements.agentSelect.value = id;
        await loadSelectedAgent();
        return;
      }
      const suffix = action === "validate" ? "/validate" : `/${action}`;
      await withErrorGuard(`agent ${action}`, async () => {
        await apiRequest(`/api/v1/agents/${encodeURIComponent(id)}${suffix}`, { method: "POST" });
        await refreshAll();
      });
    });
  });
}

async function loadExecutions() {
  const limit = Math.max(10, Math.min(200, Number(elements.executionLimit.value || 50)));
  const agent = elements.executionAgentFilter.value.trim();
  const status = elements.executionStatusFilter.value.trim();
  const hours = Math.max(1, Math.min(720, Number(elements.executionHours.value || 24)));
  const params = new URLSearchParams({ limit: String(limit) });
  if (agent) params.set("agent_id", agent);
  if (status) params.set("status_filter", status);

  const [list, stats] = await Promise.all([
    apiRequest(`/api/v1/executions?${params.toString()}`),
    apiRequest(`/api/v1/executions/stats?hours=${hours}${agent ? `&agent_id=${encodeURIComponent(agent)}` : ""}`),
  ]);

  const executions = Array.isArray(list.executions) ? list.executions : [];
  const total = Number(stats.total || 0);
  const successCount = Number(stats.by_status?.completed?.count || 0) + Number(stats.by_status?.success?.count || 0);
  const successRate = total > 0 ? ((successCount / total) * 100).toFixed(1) : "-";
  elements.executionsValue.textContent = String(total);
  elements.successRateValue.textContent = `success rate: ${successRate === "-" ? "-" : `${successRate}%`}`;
  renderStatusBars(stats.by_status || {}, total);

  elements.executionsTableBody.innerHTML = executions.length
    ? executions
        .map(
          (e) => `<tr>
      <td>${safe(formatDate(e.started_at))}</td>
      <td>${safe(e.agent_id)}</td>
      <td>${statusChip(e.status || "-")}</td>
      <td>${safe(e.lifecycle_state || "-")}</td>
      <td>${e.duration_ms == null ? "-" : Number(e.duration_ms).toFixed(2)}</td>
      <td>${safe(e.attempt ?? 1)}</td>
      <td>${safe(e.written === true ? "yes" : e.written === false ? "no" : "-")}</td>
      <td title="${safe(e.error || "")}">${safe((e.error || "-").slice(0, 90))}</td>
      </tr>`
        )
        .join("")
    : `<tr><td colspan="8" class="muted">No executions</td></tr>`;
}

function renderStatusBars(byStatus, total) {
  const entries = Object.entries(byStatus || {});
  if (!entries.length || total <= 0) {
    elements.statusBars.innerHTML = `<p class="muted">No executions in window</p>`;
    return;
  }
  elements.statusBars.innerHTML = entries
    .sort((a, b) => (b[1].count || 0) - (a[1].count || 0))
    .map(([name, data]) => {
      const c = Number(data.count || 0);
      const pct = Math.max(0, Math.min(100, (c / total) * 100));
      return `<div class="status-bar"><span>${safe(name)}</span><div class="status-bar-track"><div class="status-bar-fill" style="width:${pct.toFixed(2)}%"></div></div><span>${c}</span></div>`;
    })
    .join("");
}

function parsePromMetrics(text) {
  const out = [];
  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const [left, right] = trimmed.split(/\s+/);
    if (!left || right === undefined) continue;
    const value = Number(right);
    if (Number.isNaN(value)) continue;
    const name = left.split("{")[0];
    out.push({ name, value });
  }
  return out;
}

function sumMetric(metrics, name) {
  return metrics.filter((m) => m.name === name).reduce((acc, m) => acc + m.value, 0);
}

async function loadMetrics() {
  const text = await apiRequest("/metrics");
  const metrics = parsePromMetrics(text);
  const dlq = sumMetric(metrics, "mongoclaw_dlq_size");
  let pending = sumMetric(metrics, "mongoclaw_queue_pending");
  if (pending === 0) pending = sumMetric(metrics, "mongoclaw_agent_stream_pending");
  elements.dlqValue.textContent = `${Math.round(dlq)} / ${Math.round(pending)}`;
  elements.retryValue.textContent = `retries: ${Math.round(sumMetric(metrics, "mongoclaw_retries_scheduled_total"))}`;
  elements.loopSkips.textContent = String(Math.round(sumMetric(metrics, "mongoclaw_loop_guard_skips_total")));
  elements.sloViolations.textContent = String(Math.round(sumMetric(metrics, "mongoclaw_agent_latency_slo_violations_total")));
  elements.quarantineActive.textContent = String(Math.round(sumMetric(metrics, "mongoclaw_agent_quarantine_active")));
  elements.openBreakers.textContent = String(Math.round(sumMetric(metrics, "mongoclaw_circuit_breaker_state")));
  elements.apiP95.textContent = "see /metrics histogram";
}

async function loadCollectionProfile() {
  const db = elements.explorerDatabase.value;
  const collection = elements.explorerCollection.value;
  if (!db || !collection) return;
  const sample = Math.max(5, Math.min(200, Number(elements.explorerSampleSize.value || 40)));
  const profile = await apiRequest(
    `/api/v1/catalog/collection-profile?database=${encodeURIComponent(db)}&collection=${encodeURIComponent(collection)}&sample_size=${sample}`
  );
  const s = profile.stats;
  elements.collectionStats.innerHTML = `
    <span class="chip chip-ok">total: ${s.total_docs}</span>
    <span class="chip chip-ok">enriched: ${s.enriched_docs}</span>
    <span class="chip chip-warn">ai_triage: ${s.ai_triage_docs}</span>
    <span class="chip chip-warn">enrichment: ${s.enrichment_pct}%</span>
  `;
  elements.collectionAgents.innerHTML = profile.applied_agents.length
    ? profile.applied_agents
        .map((a) => `<span class="chip ${a.enabled ? "chip-ok" : "chip-warn"}">${safe(a.id)} (${safe(a.consistency_mode)})</span>`)
        .join("")
    : `<span class="chip chip-bad">No agents applied</span>`;

  const schema = Array.isArray(profile.schema) ? profile.schema : [];
  elements.schemaTableBody.innerHTML = schema.length
    ? schema
        .slice(0, 120)
        .map(
          (f) => `<tr><td>${safe(f.path)}</td><td>${safe((f.types || []).join(", "))}</td><td>${safe(f.seen_in_samples)}</td><td>${safe(JSON.stringify(f.example))}</td></tr>`
        )
        .join("")
    : `<tr><td colspan="4" class="muted">No schema inferred</td></tr>`;
}

function selectedOperations() {
  const ops = [];
  if (elements.opInsert.checked) ops.push("insert");
  if (elements.opUpdate.checked) ops.push("update");
  if (elements.opReplace.checked) ops.push("replace");
  if (elements.opDelete.checked) ops.push("delete");
  return ops.length ? ops : ["insert", "update"];
}

function buildQuickCreatePayload() {
  const id = elements.qcAgentId.value.trim();
  const name = elements.qcAgentName.value.trim();
  const database = elements.qcDatabase.value;
  const collection = elements.qcCollection.value;
  const model = elements.qcModel.value.trim();
  const prompt = elements.qcPrompt.value.trim();
  if (!id || !name || !database || !collection || !model || !prompt) {
    throw new Error("agent id, name, database, collection, model, and prompt are required");
  }
  let filterObj = undefined;
  const filterRaw = elements.qcFilterJson.value.trim();
  if (filterRaw) filterObj = JSON.parse(filterRaw);
  const payload = {
    id,
    name,
    watch: {
      database,
      collection,
      operations: selectedOperations(),
    },
    ai: {
      provider: elements.qcProvider.value,
      model,
      prompt,
      temperature: 0,
      max_tokens: 250,
    },
    write: {
      strategy: "merge",
      target_field: elements.qcTargetField.value.trim() || "ai_triage",
      include_metadata: true,
    },
    execution: {
      timeout_seconds: 20,
      max_retries: 2,
      consistency_mode: elements.qcConsistencyMode.value,
    },
    enabled: true,
    tags: ["ui"],
  };
  if (filterObj && typeof filterObj === "object") payload.watch.filter = filterObj;
  return payload;
}

async function createAgentQuick() {
  const payload = buildQuickCreatePayload();
  await apiRequest("/api/v1/agents", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  logEvent(`Created agent ${payload.id} for ${payload.watch.database}.${payload.watch.collection}`);
  elements.agentJson.value = JSON.stringify(payload, null, 2);
  await refreshAll();
}

async function loadSelectedAgent() {
  const id = elements.agentSelect.value;
  if (!id) throw new Error("select an agent");
  const agent = await apiRequest(`/api/v1/agents/${encodeURIComponent(id)}`);
  elements.agentJson.value = JSON.stringify(agent, null, 2);
}

async function updateSelectedAgent() {
  const id = elements.agentSelect.value;
  if (!id) throw new Error("select an agent");
  const body = JSON.parse(elements.agentJson.value);
  await apiRequest(`/api/v1/agents/${encodeURIComponent(id)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  logEvent(`Updated agent ${id}`);
  await refreshAll();
}

async function deleteSelectedAgent() {
  const id = elements.agentSelect.value;
  if (!id) throw new Error("select an agent");
  await apiRequest(`/api/v1/agents/${encodeURIComponent(id)}`, { method: "DELETE" });
  logEvent(`Deleted agent ${id}`);
  elements.agentJson.value = "";
  await refreshAll();
}

async function testConnection() {
  await apiRequest("/health");
  await apiRequest("/api/v1/catalog/overview");
  logEvent("Connection test passed");
}

async function refreshAll() {
  if (state.inFlight) return;
  state.inFlight = true;
  elements.lastUpdated.textContent = "Last updated: refreshing...";
  await withErrorGuard("catalog", async () => {
    await loadCatalog();
  });
  const tasks = [
    withErrorGuard("health", loadHealth),
    withErrorGuard("agents", loadAgents),
    withErrorGuard("executions", loadExecutions),
    withErrorGuard("metrics", loadMetrics),
    withErrorGuard("collection profile", loadCollectionProfile),
  ];
  await Promise.allSettled(tasks);
  elements.lastUpdated.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
  state.inFlight = false;
}

function restartPolling() {
  if (state.timerId) clearInterval(state.timerId);
  if (!state.pollEnabled) return;
  state.timerId = setInterval(() => refreshAll(), state.pollSeconds * 1000);
}

elements.saveConfigButton.addEventListener("click", () => {
  saveConfig();
  logEvent("Saved config");
});
elements.connectionTestButton.addEventListener("click", () => withErrorGuard("connection test", testConnection));
elements.refreshButton.addEventListener("click", refreshAll);
elements.refreshButton.addEventListener("contextmenu", (event) => {
  event.preventDefault();
  state.pollEnabled = !state.pollEnabled;
  setAutoRefreshState();
  restartPolling();
  logEvent(`auto refresh ${state.pollEnabled ? "enabled" : "paused"}`);
});
elements.pollingInterval.addEventListener("change", saveConfig);
elements.qcDatabase.addEventListener("change", () => refreshCollectionsForDb(elements.qcDatabase, elements.qcCollection));
elements.explorerDatabase.addEventListener("change", () => refreshCollectionsForDb(elements.explorerDatabase, elements.explorerCollection));
elements.qcCreateButton.addEventListener("click", () => withErrorGuard("create agent", createAgentQuick));
elements.loadAgentButton.addEventListener("click", () => withErrorGuard("load agent", loadSelectedAgent));
elements.updateAgentButton.addEventListener("click", () => withErrorGuard("update agent", updateSelectedAgent));
elements.deleteAgentButton.addEventListener("click", () => withErrorGuard("delete agent", deleteSelectedAgent));
elements.applyExecutionFilterButton.addEventListener("click", async () => {
  persistConfig();
  await withErrorGuard("executions", loadExecutions);
});
elements.loadCollectionProfileButton.addEventListener("click", () =>
  withErrorGuard("collection profile", loadCollectionProfile)
);

saveConfig();
setAutoRefreshState();
refreshAll();
