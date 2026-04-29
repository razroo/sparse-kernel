export type SparseKernelClientOptions = {
  baseUrl?: string;
  fetchImpl?: typeof fetch;
};

export type SparseKernelHealth = {
  ok: boolean;
  service: string;
  version: string;
};

export type SparseKernelInspect = {
  path: string;
  schema_version: number;
  counts: Record<string, number>;
};

export type SparseKernelTask = {
  id: string;
  agent_id?: string | null;
  session_id?: string | null;
  kind: string;
  priority: number;
  status: string;
  lease_owner?: string | null;
  lease_until?: string | null;
  attempts: number;
  result_artifact_id?: string | null;
  created_at: string;
  updated_at: string;
};

export type SparseKernelSession = {
  id: string;
  agent_id: string;
  session_key?: string | null;
  channel?: string | null;
  status: string;
  current_token_count: number;
  last_activity_at?: string | null;
  created_at: string;
  updated_at: string;
};

export type SparseKernelTranscriptEvent = {
  id: number;
  session_id: string;
  parent_event_id?: number | null;
  seq: number;
  role: string;
  event_type: string;
  content?: unknown;
  tool_call_id?: string | null;
  token_count?: number | null;
  created_at: string;
};

export type SparseKernelUpsertSessionInput = {
  id: string;
  agent_id: string;
  session_key?: string | null;
  channel?: string | null;
  status?: string | null;
  current_token_count?: number | null;
  last_activity_at?: string | null;
};

export type SparseKernelAppendTranscriptEventInput = {
  session_id: string;
  parent_event_id?: number | null;
  role: string;
  event_type: string;
  content?: unknown;
  tool_call_id?: string | null;
  token_count?: number | null;
  created_at?: string | null;
};

export type SparseKernelToolCall = {
  id: string;
  task_id?: string | null;
  session_id?: string | null;
  agent_id?: string | null;
  tool_name: string;
  status: string;
  input?: unknown;
  output?: unknown;
  error?: string | null;
  started_at?: string | null;
  ended_at?: string | null;
  created_at: string;
};

export type SparseKernelAuditEvent = {
  id: number;
  actor_type?: string | null;
  actor_id?: string | null;
  action: string;
  object_type?: string | null;
  object_id?: string | null;
  payload?: unknown;
  created_at: string;
};

export type SparseKernelArtifact = {
  id: string;
  sha256: string;
  mime_type?: string | null;
  size_bytes: number;
  storage_ref: string;
  classification?: string | null;
  retention_policy?: string | null;
  created_at: string;
};

export type SparseKernelArtifactSubject = {
  subject_type: string;
  subject_id: string;
  permission?: string | null;
};

export type SparseKernelCreateArtifactInput = {
  content_base64?: string;
  content_text?: string;
  mime_type?: string | null;
  retention_policy?: "ephemeral" | "session" | "durable" | "debug" | string | null;
  subject?: SparseKernelArtifactSubject;
};

export type SparseKernelArtifactAccessInput = {
  id: string;
  subject?: SparseKernelArtifactSubject;
};

export type SparseKernelReadArtifactResult = {
  artifact: SparseKernelArtifact;
  content_base64: string;
};

export type SparseKernelBrowserContext = {
  id: string;
  pool_id: string;
  agent_id?: string | null;
  session_id?: string | null;
  task_id?: string | null;
  profile_mode: string;
  allowed_origins?: unknown;
  allowedOrigins?: unknown;
  status: string;
  created_at: string;
};

export type SparseKernelBrowserPool = {
  id: string;
  trust_zone_id: string;
  browser_kind: string;
  status: string;
  max_contexts: number;
  cdp_endpoint?: string | null;
  created_at: string;
  updated_at: string;
};

export type SparseKernelBrowserEndpointProbe = {
  endpoint: string;
  reachable: boolean;
  status_code?: number | null;
  browser?: string | null;
  web_socket_debugger_url?: string | null;
  error?: string | null;
};

export type SparseKernelBrowserTarget = {
  id: string;
  context_id: string;
  target_id: string;
  opener_target_id?: string | null;
  url?: string | null;
  title?: string | null;
  status: string;
  close_reason?: string | null;
  console_count: number;
  network_count: number;
  artifact_count: number;
  created_at: string;
  updated_at: string;
  closed_at?: string | null;
};

export type SparseKernelBrowserObservation = {
  id: number;
  context_id: string;
  target_id?: string | null;
  observation_type: string;
  payload?: unknown;
  created_at: string;
};

export type SparseKernelAcquireBrowserContextInput = {
  agent_id?: string | null;
  session_id?: string | null;
  task_id?: string | null;
  trust_zone_id: string;
  max_contexts?: number;
  cdp_endpoint?: string | null;
  allowed_origins?: unknown;
};

export type SparseKernelBrowserObservationInput = {
  context_id: string;
  target_id?: string | null;
  observation_type: string;
  payload?: unknown;
  created_at?: string | null;
};

export type SparseKernelRecordBrowserTargetInput = {
  context_id: string;
  target_id: string;
  opener_target_id?: string | null;
  url?: string | null;
  title?: string | null;
  status?: string | null;
  close_reason?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  closed_at?: string | null;
};

export type SparseKernelCloseBrowserTargetInput = {
  context_id: string;
  target_id: string;
  reason?: string | null;
  closed_at?: string | null;
};

export type SparseKernelListBrowserTargetsInput = {
  context_id?: string | null;
  session_id?: string | null;
  task_id?: string | null;
  status?: string | null;
  limit?: number;
};

export type SparseKernelListBrowserObservationsInput = {
  context_id?: string | null;
  target_id?: string | null;
  observation_type?: string | null;
  since?: string | null;
  limit?: number;
};

export type SparseKernelEnqueueTaskInput = {
  id?: string;
  agent_id?: string | null;
  session_id?: string | null;
  kind: string;
  priority?: number;
  idempotency_key?: string | null;
  input?: unknown;
};

export type SparseKernelClaimTaskInput = {
  worker_id: string;
  kinds?: string[];
  lease_seconds?: number;
};

export type SparseKernelClaimTaskByIdInput = {
  task_id: string;
  worker_id: string;
  lease_seconds?: number;
};

export type SparseKernelHeartbeatTaskInput = {
  task_id: string;
  worker_id: string;
  lease_seconds?: number;
};

export type SparseKernelCompleteTaskInput = {
  task_id: string;
  worker_id: string;
  result_artifact_id?: string | null;
};

export type SparseKernelFailTaskInput = {
  task_id: string;
  worker_id: string;
  error: string;
};

export type SparseKernelReleaseExpiredLeasesInput = {
  now?: string;
};

export type SparseKernelReleaseExpiredLeasesResult = {
  tasks: number;
  resources: number;
};

export type SparseKernelSandboxAllocation = {
  id: string;
  task_id?: string | null;
  trust_zone_id: string;
  backend: string;
  status: string;
  created_at: string;
};

export type SparseKernelAllocateSandboxInput = {
  agent_id?: string | null;
  task_id?: string | null;
  trust_zone_id: string;
  backend?: string | null;
  docker_image?: string | null;
  max_runtime_ms?: number | null;
  max_bytes_out?: number | null;
};

export type SparseKernelRunSandboxCommandInput = {
  allocation_id: string;
  backend?: string | null;
  docker_image?: string | null;
  command: string;
  args?: string[];
  cwd?: string | null;
  env?: Record<string, string>;
  stdin_text?: string;
  stdin_base64?: string;
  timeout_ms?: number | null;
  max_output_bytes?: number | null;
};

export type SparseKernelRunSandboxCommandResult = {
  allocation_id: string;
  exit_code: number | null;
  signal?: string | null;
  stdout: string;
  stderr: string;
  timed_out: boolean;
  duration_ms: number;
};

export type SparseKernelCreateToolCallInput = {
  id?: string;
  task_id?: string | null;
  session_id?: string | null;
  agent_id?: string | null;
  tool_name: string;
  input?: unknown;
};

export type SparseKernelCompleteToolCallInput = {
  id: string;
  output?: unknown;
  artifact_ids?: string[];
};

export type SparseKernelCapability = {
  id: string;
  subject_type: string;
  subject_id: string;
  resource_type: string;
  resource_id?: string | null;
  action: string;
  constraints?: unknown;
  expires_at?: string | null;
  created_at: string;
};

export type SparseKernelGrantCapabilityInput = {
  subject_type: string;
  subject_id: string;
  resource_type: string;
  resource_id?: string | null;
  action: string;
  constraints?: unknown;
  expires_at?: string | null;
};

export type SparseKernelCapabilityCheckInput = {
  subject_type: string;
  subject_id: string;
  resource_type: string;
  resource_id?: string | null;
  action: string;
  context?: unknown;
  audit_denied?: boolean;
};

export class SparseKernelClient {
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;

  constructor(options: SparseKernelClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "http://127.0.0.1:8765").replace(/\/+$/, "");
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  async health(): Promise<SparseKernelHealth> {
    return await this.getJson<SparseKernelHealth>("/health");
  }

  async status(): Promise<SparseKernelInspect> {
    return await this.getJson<SparseKernelInspect>("/status");
  }

  async tasks(): Promise<SparseKernelTask[]> {
    return await this.getJson<SparseKernelTask[]>("/tasks");
  }

  async sessions(): Promise<SparseKernelSession[]> {
    return await this.getJson<SparseKernelSession[]>("/sessions");
  }

  async upsertSession(input: SparseKernelUpsertSessionInput): Promise<SparseKernelSession> {
    return await this.postJson<SparseKernelSession>("/sessions/upsert", input);
  }

  async appendTranscriptEvent(
    input: SparseKernelAppendTranscriptEventInput,
  ): Promise<SparseKernelTranscriptEvent> {
    return await this.postJson<SparseKernelTranscriptEvent>("/transcript-events/append", input);
  }

  async transcriptEvents(input: {
    session_id: string;
    limit?: number;
  }): Promise<SparseKernelTranscriptEvent[]> {
    return await this.postJson<SparseKernelTranscriptEvent[]>("/transcript-events/list", input);
  }

  async toolCalls(): Promise<SparseKernelToolCall[]> {
    return await this.getJson<SparseKernelToolCall[]>("/tool-calls");
  }

  async audit(): Promise<SparseKernelAuditEvent[]> {
    return await this.getJson<SparseKernelAuditEvent[]>("/audit");
  }

  async createArtifact(input: SparseKernelCreateArtifactInput): Promise<SparseKernelArtifact> {
    return await this.postJson<SparseKernelArtifact>("/artifacts/create", input);
  }

  async readArtifact(
    input: SparseKernelArtifactAccessInput,
  ): Promise<SparseKernelReadArtifactResult> {
    return await this.postJson<SparseKernelReadArtifactResult>("/artifacts/read", input);
  }

  async artifactMetadata(input: SparseKernelArtifactAccessInput): Promise<SparseKernelArtifact> {
    return await this.postJson<SparseKernelArtifact>("/artifacts/metadata", input);
  }

  async browserContexts(): Promise<SparseKernelBrowserContext[]> {
    return await this.getJson<SparseKernelBrowserContext[]>("/browser/contexts");
  }

  async browserPools(): Promise<SparseKernelBrowserPool[]> {
    return await this.getJson<SparseKernelBrowserPool[]>("/browser/pools");
  }

  async probeBrowserPool(input: {
    cdp_endpoint: string;
  }): Promise<SparseKernelBrowserEndpointProbe> {
    return await this.postJson<SparseKernelBrowserEndpointProbe>("/browser/pools/probe", input);
  }

  async acquireBrowserContext(
    input: SparseKernelAcquireBrowserContextInput,
  ): Promise<SparseKernelBrowserContext> {
    return await this.postJson<SparseKernelBrowserContext>("/browser/contexts/acquire", input);
  }

  async releaseBrowserContext(contextId: string): Promise<boolean> {
    const response = await this.postJson<{ released: boolean }>("/browser/contexts/release", {
      context_id: contextId,
    });
    return response.released;
  }

  async recordBrowserObservation(input: SparseKernelBrowserObservationInput): Promise<void> {
    await this.postJson<{ ok: boolean }>("/browser/contexts/observe", input);
  }

  async recordBrowserTarget(
    input: SparseKernelRecordBrowserTargetInput,
  ): Promise<SparseKernelBrowserTarget> {
    return await this.postJson<SparseKernelBrowserTarget>("/browser/targets/record", input);
  }

  async closeBrowserTarget(
    input: SparseKernelCloseBrowserTargetInput,
  ): Promise<SparseKernelBrowserTarget> {
    return await this.postJson<SparseKernelBrowserTarget>("/browser/targets/close", input);
  }

  async browserTargets(
    input: SparseKernelListBrowserTargetsInput = {},
  ): Promise<SparseKernelBrowserTarget[]> {
    return await this.postJson<SparseKernelBrowserTarget[]>("/browser/targets/list", input);
  }

  async browserObservations(
    input: SparseKernelListBrowserObservationsInput = {},
  ): Promise<SparseKernelBrowserObservation[]> {
    return await this.postJson<SparseKernelBrowserObservation[]>(
      "/browser/observations/list",
      input,
    );
  }

  async enqueueTask(input: SparseKernelEnqueueTaskInput): Promise<SparseKernelTask> {
    return await this.postJson<SparseKernelTask>("/tasks/enqueue", input);
  }

  async claimNextTask(input: SparseKernelClaimTaskInput): Promise<SparseKernelTask | null> {
    return await this.postJson<SparseKernelTask | null>("/tasks/claim", input);
  }

  async claimTask(input: SparseKernelClaimTaskByIdInput): Promise<SparseKernelTask | null> {
    return await this.postJson<SparseKernelTask | null>("/tasks/claim-id", input);
  }

  async heartbeatTask(input: SparseKernelHeartbeatTaskInput): Promise<boolean> {
    const response = await this.postJson<{ ok: boolean }>("/tasks/heartbeat", input);
    return response.ok;
  }

  async completeTask(input: SparseKernelCompleteTaskInput): Promise<boolean> {
    const response = await this.postJson<{ ok: boolean }>("/tasks/complete", input);
    return response.ok;
  }

  async failTask(input: SparseKernelFailTaskInput): Promise<boolean> {
    const response = await this.postJson<{ ok: boolean }>("/tasks/fail", input);
    return response.ok;
  }

  async releaseExpiredLeases(
    input: SparseKernelReleaseExpiredLeasesInput = {},
  ): Promise<SparseKernelReleaseExpiredLeasesResult> {
    return await this.postJson<SparseKernelReleaseExpiredLeasesResult>(
      "/leases/release-expired",
      input,
    );
  }

  async allocateSandbox(
    input: SparseKernelAllocateSandboxInput,
  ): Promise<SparseKernelSandboxAllocation> {
    return await this.postJson<SparseKernelSandboxAllocation>("/sandbox/allocate", input);
  }

  async releaseSandbox(allocationId: string): Promise<boolean> {
    const response = await this.postJson<{ released: boolean }>("/sandbox/release", {
      allocation_id: allocationId,
    });
    return response.released;
  }

  async runSandboxCommand(
    input: SparseKernelRunSandboxCommandInput,
  ): Promise<SparseKernelRunSandboxCommandResult> {
    return await this.postJson<SparseKernelRunSandboxCommandResult>("/sandbox/run-command", input);
  }

  async createToolCall(input: SparseKernelCreateToolCallInput): Promise<SparseKernelToolCall> {
    return await this.postJson<SparseKernelToolCall>("/tool-calls/create", input);
  }

  async startToolCall(id: string): Promise<SparseKernelToolCall> {
    return await this.postJson<SparseKernelToolCall>("/tool-calls/start", { id });
  }

  async completeToolCall(input: SparseKernelCompleteToolCallInput): Promise<SparseKernelToolCall> {
    return await this.postJson<SparseKernelToolCall>("/tool-calls/complete", input);
  }

  async failToolCall(id: string, error: string): Promise<SparseKernelToolCall> {
    return await this.postJson<SparseKernelToolCall>("/tool-calls/fail", { id, error });
  }

  async grantCapability(input: SparseKernelGrantCapabilityInput): Promise<SparseKernelCapability> {
    return await this.postJson<SparseKernelCapability>("/capabilities/grant", input);
  }

  async checkCapability(input: SparseKernelCapabilityCheckInput): Promise<boolean> {
    const response = await this.postJson<{ allowed: boolean }>("/capabilities/check", input);
    return response.allowed;
  }

  async revokeCapability(id: string): Promise<boolean> {
    const response = await this.postJson<{ revoked: boolean }>("/capabilities/revoke", { id });
    return response.revoked;
  }

  async listCapabilities(subject: {
    subject_type: string;
    subject_id: string;
  }): Promise<SparseKernelCapability[]> {
    return await this.postJson<SparseKernelCapability[]>("/capabilities/list", subject);
  }

  private async getJson<T>(path: string): Promise<T> {
    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      method: "GET",
      headers: { accept: "application/json" },
    });
    if (!response.ok) {
      throw new Error(`SparseKernel request failed: ${response.status} ${response.statusText}`);
    }
    return (await response.json()) as T;
  }

  private async postJson<T>(path: string, body: unknown): Promise<T> {
    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(`SparseKernel request failed: ${response.status} ${response.statusText}`);
    }
    return (await response.json()) as T;
  }
}
