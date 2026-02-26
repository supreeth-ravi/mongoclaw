"""Prometheus metrics for monitoring."""

from __future__ import annotations

from typing import Any

from prometheus_client import Counter, Gauge, Histogram, Info, CollectorRegistry, generate_latest

from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)

# Create a custom registry
REGISTRY = CollectorRegistry()


# Info metric
APP_INFO = Info(
    "mongoclaw",
    "MongoClaw application info",
    registry=REGISTRY,
)

# Agent metrics
AGENTS_TOTAL = Gauge(
    "mongoclaw_agents_total",
    "Total number of agents",
    ["status"],
    registry=REGISTRY,
)

AGENT_EXECUTIONS_TOTAL = Counter(
    "mongoclaw_agent_executions_total",
    "Total agent executions",
    ["agent_id", "status"],
    registry=REGISTRY,
)

AGENT_EXECUTION_DURATION = Histogram(
    "mongoclaw_agent_execution_duration_seconds",
    "Agent execution duration",
    ["agent_id"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
    registry=REGISTRY,
)

# AI metrics
AI_REQUESTS_TOTAL = Counter(
    "mongoclaw_ai_requests_total",
    "Total AI API requests",
    ["provider", "model", "status"],
    registry=REGISTRY,
)

AI_REQUEST_DURATION = Histogram(
    "mongoclaw_ai_request_duration_seconds",
    "AI API request duration",
    ["provider", "model"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
    registry=REGISTRY,
)

AI_TOKENS_TOTAL = Counter(
    "mongoclaw_ai_tokens_total",
    "Total AI tokens used",
    ["provider", "model", "type"],
    registry=REGISTRY,
)

AI_COST_TOTAL = Counter(
    "mongoclaw_ai_cost_usd_total",
    "Total AI cost in USD",
    ["provider", "model"],
    registry=REGISTRY,
)

# Queue metrics
QUEUE_SIZE = Gauge(
    "mongoclaw_queue_size",
    "Current queue size",
    ["queue"],
    registry=REGISTRY,
)

QUEUE_PENDING = Gauge(
    "mongoclaw_queue_pending",
    "Pending items in queue",
    ["queue", "consumer_group"],
    registry=REGISTRY,
)

QUEUE_PROCESSED_TOTAL = Counter(
    "mongoclaw_queue_processed_total",
    "Total items processed",
    ["queue", "status"],
    registry=REGISTRY,
)

DLQ_SIZE = Gauge(
    "mongoclaw_dlq_size",
    "Dead letter queue size",
    registry=REGISTRY,
)

# Worker metrics
WORKERS_ACTIVE = Gauge(
    "mongoclaw_workers_active",
    "Number of active workers",
    ["pool"],
    registry=REGISTRY,
)

WORKER_PROCESSING = Gauge(
    "mongoclaw_worker_processing",
    "Workers currently processing",
    ["pool"],
    registry=REGISTRY,
)

# Change stream metrics
CHANGE_EVENTS_TOTAL = Counter(
    "mongoclaw_change_events_total",
    "Total change events received",
    ["database", "collection", "operation"],
    registry=REGISTRY,
)

CHANGE_STREAM_LAG = Gauge(
    "mongoclaw_change_stream_lag_seconds",
    "Change stream lag",
    ["database", "collection"],
    registry=REGISTRY,
)

LOOP_GUARD_SKIPS_TOTAL = Counter(
    "mongoclaw_loop_guard_skips_total",
    "Total events skipped by loop guard",
    ["agent_id"],
    registry=REGISTRY,
)

SHADOW_WRITES_SKIPPED_TOTAL = Counter(
    "mongoclaw_shadow_writes_skipped_total",
    "Total writebacks skipped due to shadow mode",
    ["agent_id"],
    registry=REGISTRY,
)

POLICY_DECISIONS_TOTAL = Counter(
    "mongoclaw_policy_decisions_total",
    "Total policy decisions",
    ["agent_id", "action", "matched"],
    registry=REGISTRY,
)

AGENT_CONCURRENCY_WAITS_TOTAL = Counter(
    "mongoclaw_agent_concurrency_waits_total",
    "Total waits due to per-agent concurrency limits",
    ["agent_id"],
    registry=REGISTRY,
)

VERSION_CONFLICTS_TOTAL = Counter(
    "mongoclaw_version_conflicts_total",
    "Total strict consistency version conflicts",
    ["agent_id"],
    registry=REGISTRY,
)

RETRIES_SCHEDULED_TOTAL = Counter(
    "mongoclaw_retries_scheduled_total",
    "Total retry attempts scheduled",
    ["agent_id", "reason"],
    registry=REGISTRY,
)

HASH_CONFLICTS_TOTAL = Counter(
    "mongoclaw_hash_conflicts_total",
    "Total strict document-hash conflicts",
    ["agent_id"],
    registry=REGISTRY,
)

AGENT_STREAM_PENDING = Gauge(
    "mongoclaw_agent_stream_pending",
    "Pending queue items per agent stream",
    ["agent_id", "stream"],
    registry=REGISTRY,
)

AGENT_STREAM_INFLIGHT = Gauge(
    "mongoclaw_agent_stream_inflight",
    "In-flight work items per agent stream",
    ["agent_id", "stream"],
    registry=REGISTRY,
)

AGENT_STREAM_STARVATION_CYCLES_TOTAL = Counter(
    "mongoclaw_agent_stream_starvation_cycles_total",
    "Consecutive empty-read starvation signals per agent stream",
    ["agent_id", "stream"],
    registry=REGISTRY,
)

AGENT_STREAM_SATURATION_SKIPS_TOTAL = Counter(
    "mongoclaw_agent_stream_saturation_skips_total",
    "Times a stream was skipped due to in-flight cap",
    ["agent_id", "stream"],
    registry=REGISTRY,
)

DISPATCH_ADMISSION_TOTAL = Counter(
    "mongoclaw_dispatch_admission_total",
    "Dispatch admission decisions under backpressure control",
    ["agent_id", "stream", "decision"],
    registry=REGISTRY,
)

DISPATCH_QUEUE_FULLNESS = Gauge(
    "mongoclaw_dispatch_queue_fullness",
    "Dispatch-time queue fullness ratio (0-1)",
    ["stream"],
    registry=REGISTRY,
)

DISPATCH_ROUTED_TOTAL = Counter(
    "mongoclaw_dispatch_routed_total",
    "Total dispatched work items by routing strategy and stream",
    ["strategy", "stream"],
    registry=REGISTRY,
)

REPLAYED_DELIVERIES_TOTAL = Counter(
    "mongoclaw_replayed_deliveries_total",
    "Total replayed deliveries (attempt > 0), reflects at-least-once behavior",
    ["agent_id"],
    registry=REGISTRY,
)

AGENT_QUARANTINE_EVENTS_TOTAL = Counter(
    "mongoclaw_agent_quarantine_events_total",
    "Number of times agents entered temporary quarantine",
    ["agent_id"],
    registry=REGISTRY,
)

AGENT_QUARANTINE_ACTIVE = Gauge(
    "mongoclaw_agent_quarantine_active",
    "Whether an agent is currently quarantined (0/1)",
    ["agent_id"],
    registry=REGISTRY,
)

AGENT_LATENCY_SLO_VIOLATIONS_TOTAL = Counter(
    "mongoclaw_agent_latency_slo_violations_total",
    "Number of executions exceeding configured latency SLO",
    ["agent_id"],
    registry=REGISTRY,
)

# Circuit breaker metrics
CIRCUIT_BREAKER_STATE = Gauge(
    "mongoclaw_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=half-open, 2=open)",
    ["name"],
    registry=REGISTRY,
)

CIRCUIT_BREAKER_FAILURES = Counter(
    "mongoclaw_circuit_breaker_failures_total",
    "Circuit breaker failures",
    ["name"],
    registry=REGISTRY,
)

# HTTP metrics (for API)
HTTP_REQUESTS_TOTAL = Counter(
    "mongoclaw_http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
    registry=REGISTRY,
)

HTTP_REQUEST_DURATION = Histogram(
    "mongoclaw_http_request_duration_seconds",
    "HTTP request duration",
    ["method", "endpoint"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
    registry=REGISTRY,
)


class MetricsCollector:
    """
    Collects and exposes metrics.

    Provides methods to update metrics and generate output.
    """

    def __init__(self) -> None:
        self._initialized = False

    def initialize(self, version: str = "0.1.0") -> None:
        """Initialize metrics with app info."""
        if self._initialized:
            return

        APP_INFO.info({
            "version": version,
            "name": "mongoclaw",
        })
        self._initialized = True

    # Agent metrics

    def set_agent_count(self, enabled: int, disabled: int) -> None:
        """Update agent counts."""
        AGENTS_TOTAL.labels(status="enabled").set(enabled)
        AGENTS_TOTAL.labels(status="disabled").set(disabled)

    def record_execution(
        self,
        agent_id: str,
        success: bool,
        duration_seconds: float,
    ) -> None:
        """Record an agent execution."""
        status = "success" if success else "failure"
        AGENT_EXECUTIONS_TOTAL.labels(agent_id=agent_id, status=status).inc()
        AGENT_EXECUTION_DURATION.labels(agent_id=agent_id).observe(duration_seconds)

    # AI metrics

    def record_ai_request(
        self,
        provider: str,
        model: str,
        success: bool,
        duration_seconds: float,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        cost_usd: float = 0.0,
    ) -> None:
        """Record an AI API request."""
        status = "success" if success else "failure"
        AI_REQUESTS_TOTAL.labels(
            provider=provider, model=model, status=status
        ).inc()
        AI_REQUEST_DURATION.labels(
            provider=provider, model=model
        ).observe(duration_seconds)

        if prompt_tokens:
            AI_TOKENS_TOTAL.labels(
                provider=provider, model=model, type="prompt"
            ).inc(prompt_tokens)
        if completion_tokens:
            AI_TOKENS_TOTAL.labels(
                provider=provider, model=model, type="completion"
            ).inc(completion_tokens)
        if cost_usd:
            AI_COST_TOTAL.labels(provider=provider, model=model).inc(cost_usd)

    # Queue metrics

    def set_queue_size(self, queue: str, size: int) -> None:
        """Update queue size."""
        QUEUE_SIZE.labels(queue=queue).set(size)

    def set_queue_pending(
        self, queue: str, consumer_group: str, count: int
    ) -> None:
        """Update pending count."""
        QUEUE_PENDING.labels(queue=queue, consumer_group=consumer_group).set(count)

    def record_queue_processed(self, queue: str, success: bool) -> None:
        """Record queue item processed."""
        status = "success" if success else "failure"
        QUEUE_PROCESSED_TOTAL.labels(queue=queue, status=status).inc()

    def set_dlq_size(self, size: int) -> None:
        """Update DLQ size."""
        DLQ_SIZE.set(size)

    # Worker metrics

    def set_workers_active(self, pool: str, count: int) -> None:
        """Update active worker count."""
        WORKERS_ACTIVE.labels(pool=pool).set(count)

    def set_workers_processing(self, pool: str, count: int) -> None:
        """Update processing worker count."""
        WORKER_PROCESSING.labels(pool=pool).set(count)

    # Change stream metrics

    def record_change_event(
        self,
        database: str,
        collection: str,
        operation: str,
    ) -> None:
        """Record a change event."""
        CHANGE_EVENTS_TOTAL.labels(
            database=database, collection=collection, operation=operation
        ).inc()

    def record_loop_guard_skip(self, agent_id: str) -> None:
        """Record a loop guard skip event."""
        LOOP_GUARD_SKIPS_TOTAL.labels(agent_id=agent_id).inc()

    def record_shadow_write_skip(self, agent_id: str) -> None:
        """Record a shadow-mode writeback skip."""
        SHADOW_WRITES_SKIPPED_TOTAL.labels(agent_id=agent_id).inc()

    def record_policy_decision(
        self,
        agent_id: str,
        action: str,
        matched: bool,
    ) -> None:
        """Record a policy evaluation decision."""
        POLICY_DECISIONS_TOTAL.labels(
            agent_id=agent_id,
            action=action,
            matched="true" if matched else "false",
        ).inc()

    def record_agent_concurrency_wait(self, agent_id: str) -> None:
        """Record a wait on per-agent concurrency cap."""
        AGENT_CONCURRENCY_WAITS_TOTAL.labels(agent_id=agent_id).inc()

    def record_version_conflict(self, agent_id: str) -> None:
        """Record a strict-consistency version conflict."""
        VERSION_CONFLICTS_TOTAL.labels(agent_id=agent_id).inc()

    def record_retry_scheduled(self, agent_id: str, reason: str) -> None:
        """Record a scheduled retry event."""
        RETRIES_SCHEDULED_TOTAL.labels(agent_id=agent_id, reason=reason).inc()

    def record_hash_conflict(self, agent_id: str) -> None:
        """Record a strict hash conflict."""
        HASH_CONFLICTS_TOTAL.labels(agent_id=agent_id).inc()

    def set_agent_stream_pending(self, agent_id: str, stream: str, count: int) -> None:
        """Set pending depth for an agent stream."""
        AGENT_STREAM_PENDING.labels(agent_id=agent_id, stream=stream).set(count)

    def set_agent_stream_inflight(self, agent_id: str, stream: str, count: int) -> None:
        """Set in-flight count for an agent stream."""
        AGENT_STREAM_INFLIGHT.labels(agent_id=agent_id, stream=stream).set(count)

    def record_agent_stream_starvation_cycle(self, agent_id: str, stream: str) -> None:
        """Record a starvation signal for an agent stream."""
        AGENT_STREAM_STARVATION_CYCLES_TOTAL.labels(
            agent_id=agent_id,
            stream=stream,
        ).inc()

    def record_agent_stream_saturation_skip(self, agent_id: str, stream: str) -> None:
        """Record a skip due to per-stream in-flight saturation."""
        AGENT_STREAM_SATURATION_SKIPS_TOTAL.labels(
            agent_id=agent_id,
            stream=stream,
        ).inc()

    def record_dispatch_admission(
        self,
        agent_id: str,
        stream: str,
        decision: str,
    ) -> None:
        """Record dispatch admission outcome."""
        DISPATCH_ADMISSION_TOTAL.labels(
            agent_id=agent_id,
            stream=stream,
            decision=decision,
        ).inc()

    def set_dispatch_queue_fullness(self, stream: str, fullness: float) -> None:
        """Set dispatch-time stream fullness."""
        DISPATCH_QUEUE_FULLNESS.labels(stream=stream).set(fullness)

    def record_dispatch_routed(self, strategy: str, stream: str) -> None:
        """Record routing decision for a dispatched work item."""
        DISPATCH_ROUTED_TOTAL.labels(strategy=strategy, stream=stream).inc()

    def record_replayed_delivery(self, agent_id: str) -> None:
        """Record replayed delivery for at-least-once semantics visibility."""
        REPLAYED_DELIVERIES_TOTAL.labels(agent_id=agent_id).inc()

    def record_agent_quarantine_event(self, agent_id: str) -> None:
        """Record that an agent entered quarantine."""
        AGENT_QUARANTINE_EVENTS_TOTAL.labels(agent_id=agent_id).inc()

    def set_agent_quarantine_active(self, agent_id: str, active: bool) -> None:
        """Set whether an agent is actively quarantined."""
        AGENT_QUARANTINE_ACTIVE.labels(agent_id=agent_id).set(1 if active else 0)

    def record_agent_latency_slo_violation(self, agent_id: str) -> None:
        """Record an execution that violated latency SLO."""
        AGENT_LATENCY_SLO_VIOLATIONS_TOTAL.labels(agent_id=agent_id).inc()

    def set_change_stream_lag(
        self,
        database: str,
        collection: str,
        lag_seconds: float,
    ) -> None:
        """Update change stream lag."""
        CHANGE_STREAM_LAG.labels(
            database=database, collection=collection
        ).set(lag_seconds)

    # Circuit breaker metrics

    def set_circuit_breaker_state(self, name: str, state: str) -> None:
        """Update circuit breaker state."""
        state_value = {"closed": 0, "half_open": 1, "open": 2}.get(state, 0)
        CIRCUIT_BREAKER_STATE.labels(name=name).set(state_value)

    def record_circuit_breaker_failure(self, name: str) -> None:
        """Record circuit breaker failure."""
        CIRCUIT_BREAKER_FAILURES.labels(name=name).inc()

    # HTTP metrics

    def record_http_request(
        self,
        method: str,
        endpoint: str,
        status: int,
        duration_seconds: float,
    ) -> None:
        """Record HTTP request."""
        HTTP_REQUESTS_TOTAL.labels(
            method=method, endpoint=endpoint, status=str(status)
        ).inc()
        HTTP_REQUEST_DURATION.labels(
            method=method, endpoint=endpoint
        ).observe(duration_seconds)

    def get_metrics(self) -> bytes:
        """Generate metrics in Prometheus format."""
        return generate_latest(REGISTRY)


# Global metrics collector instance
_collector: MetricsCollector | None = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector."""
    global _collector
    if _collector is None:
        _collector = MetricsCollector()
    return _collector
