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

  async enqueueTask(input: SparseKernelEnqueueTaskInput): Promise<SparseKernelTask> {
    return await this.postJson<SparseKernelTask>("/tasks/enqueue", input);
  }

  async claimNextTask(input: SparseKernelClaimTaskInput): Promise<SparseKernelTask | null> {
    return await this.postJson<SparseKernelTask | null>("/tasks/claim", input);
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
