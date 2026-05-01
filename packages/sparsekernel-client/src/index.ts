export type SparseKernelClientOptions = {
  baseUrl?: string;
  fetchImpl?: typeof fetch;
};

export const SPARSEKERNEL_PROTOCOL_VERSION = "2026-04-29.v1";

export type SparseKernelHealth = {
  ok: boolean;
  service: string;
  version: string;
  protocol_version?: string;
  schema_version?: number;
  features?: string[];
};

export type SparseKernelInspect = {
  path: string;
  schema_version: number;
  counts: Record<string, number>;
};

export type SparseKernelResourceBudgets = {
  logical_agents_max: number;
  active_agent_steps_max: number;
  model_calls_in_flight_max: number;
  file_patch_jobs_max: number;
  test_jobs_max: number;
  browser_contexts_max: number;
  heavy_sandboxes_max: number;
};

export type SparseKernelResourceBudgetUpdate = Partial<SparseKernelResourceBudgets>;

export type SparseKernelNetworkDefaultAction = "allow" | "deny" | (string & {});
export type SparseKernelArtifactRetentionPolicy =
  | "ephemeral"
  | "session"
  | "durable"
  | "debug"
  | (string & {});

export type SparseKernelNetworkPolicy = {
  id: string;
  default_action: SparseKernelNetworkDefaultAction;
  allow_private_network: boolean;
  allowed_hosts?: string[];
  denied_cidrs?: string[];
  proxy_ref?: string | null;
  created_at: string;
};

export type SparseKernelTrustZoneProxyAttachment = {
  trust_zone_id: string;
  network_policy_id: string;
  proxy_ref?: string | null;
};

export type SparseKernelSupervisedEgressProxy = {
  trust_zone_id: string;
  proxy_ref: string;
  pid?: number;
  already_running?: boolean;
  stopped?: boolean;
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
  retention_policy?: SparseKernelArtifactRetentionPolicy | null;
  subject?: SparseKernelArtifactSubject;
};

export type SparseKernelImportArtifactFileInput = {
  staged_path: string;
  mime_type?: string | null;
  retention_policy?: SparseKernelArtifactRetentionPolicy | null;
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

export type SparseKernelExportArtifactFileInput = SparseKernelArtifactAccessInput & {
  file_name?: string | null;
};

export type SparseKernelExportArtifactFileResult = {
  artifact: SparseKernelArtifact;
  staged_path: string;
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

export type SparseKernelSandboxBackendProbe = {
  backend: string;
  available: boolean;
  command?: string | null;
  hard_boundary?: boolean;
  isolation?: string | null;
  notes?: string[];
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

function protocolMajor(version: string): string {
  const trimmed = version.trim();
  const match = /(?:^|[._-])v?(\d+)$/i.exec(trimmed);
  return match?.[1] ?? trimmed;
}

export function isSparseKernelProtocolCompatible(
  health: Pick<SparseKernelHealth, "protocol_version">,
): boolean {
  if (!health.protocol_version) {
    return true;
  }
  return protocolMajor(health.protocol_version) === protocolMajor(SPARSEKERNEL_PROTOCOL_VERSION);
}

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

  async assertCompatible(): Promise<SparseKernelHealth> {
    const health = await this.health();
    if (!isSparseKernelProtocolCompatible(health)) {
      throw new Error(
        `SparseKernel protocol mismatch: client ${SPARSEKERNEL_PROTOCOL_VERSION}, daemon ${
          health.protocol_version ?? "unknown"
        }`,
      );
    }
    return health;
  }

  async status(): Promise<SparseKernelInspect> {
    return await this.getJson<SparseKernelInspect>("/status");
  }

  async resourceBudgets(): Promise<SparseKernelResourceBudgets> {
    return await this.getJson<SparseKernelResourceBudgets>("/runtime/budgets");
  }

  async updateResourceBudgets(
    input: SparseKernelResourceBudgetUpdate,
  ): Promise<SparseKernelResourceBudgets> {
    return await this.postJson<SparseKernelResourceBudgets>("/runtime/budgets/update", input);
  }

  async trustZoneNetworkPolicy(input: {
    trust_zone_id: string;
  }): Promise<SparseKernelNetworkPolicy | null> {
    return await this.postJson<SparseKernelNetworkPolicy | null>(
      "/trust-zones/network-policy",
      input,
    );
  }

  async attachTrustZoneProxyRef(input: {
    trust_zone_id: string;
    proxy_ref?: string | null;
  }): Promise<SparseKernelTrustZoneProxyAttachment> {
    return await this.postJson<SparseKernelTrustZoneProxyAttachment>(
      "/trust-zones/proxy-ref",
      input,
    );
  }

  async egressProxies(): Promise<SparseKernelSupervisedEgressProxy[]> {
    return await this.getJson<SparseKernelSupervisedEgressProxy[]>("/egress-proxies");
  }

  async startEgressProxy(input: {
    trust_zone_id: string;
    host?: string | null;
    port?: number | null;
    command?: string | null;
    args?: string[];
  }): Promise<SparseKernelSupervisedEgressProxy> {
    return await this.postJson<SparseKernelSupervisedEgressProxy>("/egress-proxies/start", input);
  }

  async stopEgressProxy(input: {
    trust_zone_id: string;
    clear_proxy_ref?: boolean;
  }): Promise<SparseKernelSupervisedEgressProxy> {
    return await this.postJson<SparseKernelSupervisedEgressProxy>("/egress-proxies/stop", input);
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

  async importArtifactFile(
    input: SparseKernelImportArtifactFileInput,
  ): Promise<SparseKernelArtifact> {
    return await this.postJson<SparseKernelArtifact>("/artifacts/import-file", input);
  }

  async readArtifact(
    input: SparseKernelArtifactAccessInput,
  ): Promise<SparseKernelReadArtifactResult> {
    return await this.postJson<SparseKernelReadArtifactResult>("/artifacts/read", input);
  }

  async exportArtifactFile(
    input: SparseKernelExportArtifactFileInput,
  ): Promise<SparseKernelExportArtifactFileResult> {
    return await this.postJson<SparseKernelExportArtifactFileResult>(
      "/artifacts/export-file",
      input,
    );
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

  async probeSandboxBackends(): Promise<SparseKernelSandboxBackendProbe[]> {
    return await this.getJson<SparseKernelSandboxBackendProbe[]>("/sandbox/backends/probe");
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
      throw await sparseKernelRequestError(response);
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
      throw await sparseKernelRequestError(response);
    }
    return (await response.json()) as T;
  }
}

const MAX_ERROR_DETAIL_LENGTH = 1000;

async function sparseKernelRequestError(response: Response): Promise<Error> {
  const detail = await responseErrorDetail(response);
  const status = [response.status, response.statusText.trim()].filter(Boolean).join(" ");
  return new Error(
    `SparseKernel request failed: ${status}${detail ? `: ${truncateErrorDetail(detail)}` : ""}`,
  );
}

async function responseErrorDetail(response: Response): Promise<string | undefined> {
  let text: string;
  try {
    text = await response.text();
  } catch {
    return undefined;
  }
  const trimmed = text.trim();
  if (!trimmed) {
    return undefined;
  }
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    return formatErrorPayload(parsed);
  } catch {
    return trimmed;
  }
}

function formatErrorPayload(payload: unknown): string | undefined {
  if (typeof payload === "string") {
    return payload.trim() || undefined;
  }
  if (!payload || typeof payload !== "object") {
    return undefined;
  }
  const record = payload as Record<string, unknown>;
  for (const key of ["error", "message", "detail"]) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  try {
    return JSON.stringify(payload);
  } catch {
    return undefined;
  }
}

function truncateErrorDetail(detail: string): string {
  return detail.length > MAX_ERROR_DETAIL_LENGTH
    ? `${detail.slice(0, MAX_ERROR_DETAIL_LENGTH)}...`
    : detail;
}
