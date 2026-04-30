export type KernelActor = {
  type: string;
  id?: string;
};

export type KernelSubject = {
  subjectType: string;
  subjectId: string;
};

export type KernelResource = {
  resourceType: string;
  resourceId?: string;
};

export type RuntimeRetentionPolicy = "ephemeral" | "session" | "durable" | "debug";

export type KernelAuditInput = {
  actor?: KernelActor;
  action: string;
  objectType?: string;
  objectId?: string;
  payload?: unknown;
  createdAt?: string;
};

export type KernelAgentInput = {
  id: string;
  name?: string;
  role?: string;
  status?: string;
  now?: string;
};

export type KernelSessionInput = {
  id: string;
  agentId: string;
  sessionKey?: string;
  channel?: string;
  status?: string;
  currentTokenCount?: number;
  lastActivityAt?: string;
  createdAt?: string;
  updatedAt?: string;
};

export type KernelSessionRecord = {
  id: string;
  agentId: string;
  sessionKey?: string;
  channel?: string;
  status: string;
  currentTokenCount: number;
  compactedUntilEventId?: number;
  lastActivityAt?: string;
  createdAt: string;
  updatedAt: string;
};

export type TranscriptEventInput = {
  sessionId: string;
  parentEventId?: number;
  role: string;
  eventType: string;
  content?: unknown;
  toolCallId?: string;
  tokenCount?: number;
  createdAt?: string;
};

export type TranscriptEventRecord = {
  id: number;
  sessionId: string;
  parentEventId?: number;
  seq: number;
  role: string;
  eventType: string;
  content?: unknown;
  toolCallId?: string;
  tokenCount?: number;
  createdAt: string;
};

export type TaskStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";

export type EnqueueTaskInput = {
  id?: string;
  agentId?: string;
  sessionId?: string;
  kind: string;
  priority?: number;
  idempotencyKey?: string;
  input?: unknown;
  createdAt?: string;
};

export type KernelTaskRecord = {
  id: string;
  agentId?: string;
  sessionId?: string;
  kind: string;
  priority: number;
  status: TaskStatus;
  idempotencyKey?: string;
  leaseOwner?: string;
  leaseUntil?: string;
  attempts: number;
  input?: unknown;
  resultArtifactId?: string;
  createdAt: string;
  updatedAt: string;
};

export type ClaimTaskInput = {
  workerId: string;
  kinds?: string[];
  leaseMs?: number;
  now?: string;
};

export type ClaimTaskByIdInput = {
  taskId: string;
  workerId: string;
  leaseMs?: number;
  now?: string;
};

export type ArtifactRecordInput = {
  id?: string;
  sha256: string;
  mimeType?: string;
  sizeBytes: number;
  storageRef: string;
  createdByTaskId?: string;
  createdByToolCallId?: string;
  classification?: string;
  retentionPolicy?: RuntimeRetentionPolicy;
  createdAt?: string;
};

export type ArtifactRecord = Required<
  Pick<ArtifactRecordInput, "id" | "sha256" | "sizeBytes" | "storageRef">
> & {
  mimeType?: string;
  createdByTaskId?: string;
  createdByToolCallId?: string;
  classification?: string;
  retentionPolicy?: RuntimeRetentionPolicy;
  createdAt: string;
};

export type ArtifactAccessRecord = {
  artifactId: string;
  subjectType: string;
  subjectId: string;
  permission: string;
  expiresAt?: string;
  createdAt: string;
};

export type GrantCapabilityInput = KernelSubject &
  KernelResource & {
    id?: string;
    action: string;
    constraints?: unknown;
    expiresAt?: string;
    createdAt?: string;
    actor?: KernelActor;
  };

export type CapabilityCheckInput = KernelSubject &
  KernelResource & {
    action: string;
    context?: unknown;
    now?: string;
    auditDenied?: boolean;
    actor?: KernelActor;
  };

export type BrowserContextRecord = {
  id: string;
  poolId: string;
  agentId?: string;
  sessionId?: string;
  taskId?: string;
  profileMode: string;
  allowedOrigins?: unknown;
  status: string;
  createdAt: string;
  expiresAt?: string;
};

export type BrowserPoolRecord = {
  id: string;
  trustZoneId: string;
  browserKind: string;
  status: string;
  maxContexts: number;
  activeContexts: number;
  cdpEndpoint?: string;
  createdAt: string;
  updatedAt: string;
};

export type BrowserTargetInput = {
  contextId: string;
  targetId: string;
  openerTargetId?: string;
  url?: string;
  title?: string;
  status?: "active" | "closed" | "blocked" | "crashed";
  closeReason?: string;
  createdAt?: string;
  updatedAt?: string;
  closedAt?: string;
};

export type BrowserTargetRecord = {
  id: string;
  contextId: string;
  targetId: string;
  openerTargetId?: string;
  url?: string;
  title?: string;
  status: string;
  closeReason?: string;
  consoleCount: number;
  networkCount: number;
  artifactCount: number;
  createdAt: string;
  updatedAt: string;
  closedAt?: string;
};

export type BrowserObservationInput = {
  contextId: string;
  targetId?: string;
  observationType: string;
  payload?: unknown;
  createdAt?: string;
};

export type BrowserObservationRecord = {
  id: number;
  contextId: string;
  targetId?: string;
  observationType: string;
  payload?: unknown;
  createdAt: string;
};

export type SandboxAllocationRecord = {
  id: string;
  taskId: string;
  trustZoneId: string;
  backend: string;
  status: string;
  createdAt: string;
  leaseUntil?: string;
};

export type ResourceLeaseRecord = {
  id: string;
  resourceType: string;
  resourceId: string;
  ownerTaskId?: string;
  ownerAgentId?: string;
  trustZoneId?: string;
  status: string;
  leaseUntil?: string;
  maxRuntimeMs?: number;
  maxBytesOut?: number;
  maxTokens?: number;
  metadata?: unknown;
  createdAt: string;
  updatedAt: string;
};

export type TrustZoneRecord = {
  id: string;
  description?: string;
  sandboxBackend: string;
  networkPolicyId?: string;
  filesystemPolicy?: unknown;
  maxProcesses?: number;
  maxMemoryMb?: number;
  maxRuntimeSeconds?: number;
  createdAt: string;
};

export type UsageSummaryRecord = {
  resourceType: string;
  unit: string;
  amount: number;
};

export type ArtifactRetentionSummaryRecord = {
  retentionPolicy: RuntimeRetentionPolicy | "unknown";
  count: number;
  sizeBytes: number;
};

export type RuntimeInfoRecord = {
  key: string;
  value: string;
  updatedAt: string;
};

export type NetworkPolicyRecord = {
  id: string;
  defaultAction: "allow" | "deny";
  allowPrivateNetwork: boolean;
  allowedHosts?: string[];
  deniedCidrs?: string[];
  proxyRef?: string;
  createdAt: string;
};

export type NetworkPolicyInput = {
  id: string;
  defaultAction: "allow" | "deny";
  allowPrivateNetwork?: boolean;
  allowedHosts?: string[];
  deniedCidrs?: string[];
  proxyRef?: string;
  createdAt?: string;
  actor?: KernelActor;
};
