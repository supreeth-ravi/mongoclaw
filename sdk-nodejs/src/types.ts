/**
 * Type definitions for MongoClaw SDK
 */

export interface ClientOptions {
  baseUrl?: string;
  apiKey?: string;
  timeout?: number;
}

export interface AgentSummary {
  id: string;
  name: string;
  enabled: boolean;
  database: string;
  collection: string;
  model: string;
}

export interface AgentDetails {
  id: string;
  name: string;
  description?: string;
  enabled: boolean;
  watch: WatchConfig;
  ai: AIConfig;
  write: WriteConfig;
  execution: ExecutionConfig;
  created_at?: string;
  updated_at?: string;
}

export interface WatchConfig {
  database: string;
  collection: string;
  operations?: string[];
  filter?: Record<string, unknown>;
}

export interface AIConfig {
  provider?: string;
  model: string;
  prompt: string;
  system_prompt?: string;
  temperature?: number;
  max_tokens?: number;
  response_schema?: Record<string, unknown>;
}

export interface WriteConfig {
  strategy: 'merge' | 'replace' | 'append';
  target_field: string;
  idempotency_key?: string;
}

export interface ExecutionConfig {
  max_retries?: number;
  retry_delay_ms?: number;
  timeout_ms?: number;
  rate_limit_per_minute?: number;
  cost_limit_usd?: number;
}

export interface ExecutionRecord {
  id: string;
  agent_id: string;
  document_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'skipped';
  lifecycle_state?: string;
  reason?: string;
  written?: boolean;
  started_at: string;
  completed_at?: string;
  duration_ms?: number;
  tokens_used?: number;
  cost_usd?: number;
  error?: string;
}

export interface HealthStatus {
  status: 'healthy' | 'unhealthy' | 'degraded';
  version?: string;
  environment?: string;
  components?: Record<string, ComponentHealth>;
}

export interface ComponentHealth {
  status: 'healthy' | 'unhealthy';
  latency_ms?: number;
  error?: string;
}

export interface ListAgentsParams {
  enabled_only?: boolean;
  skip?: number;
  limit?: number;
}

export interface ListAgentsResponse {
  agents: AgentSummary[];
  total: number;
}

export interface ListExecutionsParams {
  agent_id?: string;
  status?: string;
  skip?: number;
  limit?: number;
}

export interface ListExecutionsResponse {
  executions: ExecutionRecord[];
  total: number;
}

export interface ValidationResult {
  valid: boolean;
  errors?: string[];
}

export interface TriggerRequest {
  document: Record<string, unknown>;
}

export interface AgentStats {
  total_executions: number;
  successful_executions: number;
  failed_executions: number;
  avg_duration_ms: number;
  total_tokens: number;
  total_cost_usd: number;
}
