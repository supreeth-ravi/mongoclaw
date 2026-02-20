/**
 * MongoClaw TypeScript/Node.js SDK Client
 */

import axios, { AxiosInstance, AxiosError } from 'axios';
import {
  ClientOptions,
  AgentSummary,
  AgentDetails,
  ExecutionRecord,
  HealthStatus,
  ListAgentsParams,
  ListAgentsResponse,
  ListExecutionsParams,
  ListExecutionsResponse,
  ValidationResult,
  AgentStats,
} from './types';

export class MongoClawError extends Error {
  constructor(
    message: string,
    public statusCode?: number,
    public response?: unknown
  ) {
    super(message);
    this.name = 'MongoClawError';
  }
}

export class MongoClawClient {
  private client: AxiosInstance;
  private baseUrl: string;

  constructor(options: ClientOptions = {}) {
    this.baseUrl = (options.baseUrl || 'http://localhost:8000').replace(/\/$/, '');

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    if (options.apiKey) {
      headers['Authorization'] = `Bearer ${options.apiKey}`;
    }

    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: options.timeout || 30000,
      headers,
    });
  }

  private handleError(error: unknown): never {
    if (axios.isAxiosError(error)) {
      const axiosError = error as AxiosError;
      throw new MongoClawError(
        axiosError.message,
        axiosError.response?.status,
        axiosError.response?.data
      );
    }
    throw error;
  }

  // Health endpoints

  async health(): Promise<HealthStatus> {
    try {
      const response = await this.client.get<HealthStatus>('/health');
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async healthDetailed(): Promise<HealthStatus> {
    try {
      const response = await this.client.get<HealthStatus>('/health/detailed');
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async isHealthy(): Promise<boolean> {
    try {
      const status = await this.health();
      return status.status === 'healthy';
    } catch {
      return false;
    }
  }

  // Agent endpoints

  async listAgents(params: ListAgentsParams = {}): Promise<ListAgentsResponse> {
    try {
      const response = await this.client.get<ListAgentsResponse>('/api/v1/agents', {
        params: {
          enabled_only: params.enabled_only || false,
          skip: params.skip || 0,
          limit: params.limit || 100,
        },
      });
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async getAgent(agentId: string): Promise<AgentDetails> {
    try {
      const response = await this.client.get<AgentDetails>(`/api/v1/agents/${agentId}`);
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async createAgent(config: Partial<AgentDetails>): Promise<AgentDetails> {
    try {
      const response = await this.client.post<AgentDetails>('/api/v1/agents', config);
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async updateAgent(agentId: string, config: Partial<AgentDetails>): Promise<AgentDetails> {
    try {
      const response = await this.client.put<AgentDetails>(`/api/v1/agents/${agentId}`, config);
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async deleteAgent(agentId: string): Promise<boolean> {
    try {
      await this.client.delete(`/api/v1/agents/${agentId}`);
      return true;
    } catch (error) {
      this.handleError(error);
    }
  }

  async enableAgent(agentId: string): Promise<AgentDetails> {
    try {
      const response = await this.client.post<AgentDetails>(`/api/v1/agents/${agentId}/enable`);
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async disableAgent(agentId: string): Promise<AgentDetails> {
    try {
      const response = await this.client.post<AgentDetails>(`/api/v1/agents/${agentId}/disable`);
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async validateAgent(config: Partial<AgentDetails>): Promise<ValidationResult> {
    try {
      const response = await this.client.post<ValidationResult>('/api/v1/agents/validate', config);
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async getAgentStats(agentId: string): Promise<AgentStats> {
    try {
      const response = await this.client.get<AgentStats>(`/api/v1/agents/${agentId}/stats`);
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  // Execution endpoints

  async listExecutions(params: ListExecutionsParams = {}): Promise<ListExecutionsResponse> {
    try {
      const response = await this.client.get<ListExecutionsResponse>('/api/v1/executions', {
        params: {
          agent_id: params.agent_id,
          status: params.status,
          skip: params.skip || 0,
          limit: params.limit || 100,
        },
      });
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async getExecution(executionId: string): Promise<ExecutionRecord> {
    try {
      const response = await this.client.get<ExecutionRecord>(`/api/v1/executions/${executionId}`);
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  async retryExecution(executionId: string): Promise<ExecutionRecord> {
    try {
      const response = await this.client.post<ExecutionRecord>(`/api/v1/executions/${executionId}/retry`);
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  // Webhook endpoints

  async triggerAgent(agentId: string, document: Record<string, unknown>): Promise<ExecutionRecord> {
    try {
      const response = await this.client.post<ExecutionRecord>(
        `/api/v1/webhooks/trigger/${agentId}`,
        { document }
      );
      return response.data;
    } catch (error) {
      this.handleError(error);
    }
  }

  // Utility methods

  async waitForExecution(
    executionId: string,
    options: { timeout?: number; pollInterval?: number } = {}
  ): Promise<ExecutionRecord> {
    const timeout = options.timeout || 60000;
    const pollInterval = options.pollInterval || 1000;
    const startTime = Date.now();

    while (true) {
      const execution = await this.getExecution(executionId);

      if (execution.status === 'completed' || execution.status === 'failed') {
        return execution;
      }

      const elapsed = Date.now() - startTime;
      if (elapsed >= timeout) {
        throw new MongoClawError(
          `Execution ${executionId} did not complete within ${timeout}ms`
        );
      }

      await this.sleep(pollInterval);
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
