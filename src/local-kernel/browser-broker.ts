import type { ContentAddressedArtifactStore } from "./artifact-store.js";
import type { LocalKernelDatabase } from "./database.js";
import { checkTrustZoneNetworkUrl } from "./network-policy.js";
import type { BrowserContextRecord, RuntimeRetentionPolicy } from "./types.js";

export type BrowserContextLeaseRequest = {
  agentId: string;
  sessionId?: string;
  taskId?: string;
  trustZoneId: string;
  browserKind?: string;
  profileMode?: string;
  allowedOrigins?: unknown;
  maxContexts?: number;
  expiresAt?: string;
};

export interface BrowserBroker {
  acquireContext(request: BrowserContextLeaseRequest): BrowserContextRecord;
  releaseContext(contextId: string): boolean;
  recordObservation(contextId: string, observation: unknown): void;
  captureArtifact(
    contextId: string,
    artifactType: string,
    bytes: Buffer | Uint8Array | string,
    metadata?: {
      mimeType?: string;
      retentionPolicy?: RuntimeRetentionPolicy;
      classification?: string;
    },
  ): Promise<{ artifactId: string; sha256: string }>;
}

export class LocalBrowserBroker implements BrowserBroker {
  constructor(
    private readonly db: LocalKernelDatabase,
    private readonly artifactStore?: ContentAddressedArtifactStore,
  ) {}

  acquireContext(request: BrowserContextLeaseRequest): BrowserContextRecord {
    const origins = Array.isArray(request.allowedOrigins) ? request.allowedOrigins : [];
    for (const origin of origins) {
      if (typeof origin !== "string" || !origin.trim()) {
        continue;
      }
      const decision = checkTrustZoneNetworkUrl({
        db: this.db,
        trustZoneId: request.trustZoneId,
        url: origin,
        actor: { type: "agent", id: request.agentId },
      });
      if (!decision.allowed) {
        throw new Error(`Browser context origin denied by network policy: ${origin}`);
      }
    }

    const allowed = this.db.checkCapability({
      subjectType: "agent",
      subjectId: request.agentId,
      resourceType: "browser_context",
      resourceId: request.trustZoneId,
      action: "allocate",
      context: { taskId: request.taskId, sessionId: request.sessionId },
    });
    if (!allowed) {
      throw new Error(`Agent ${request.agentId} lacks browser_context allocate capability`);
    }

    const { browserContextsMax } = this.db.getResourceBudgetSnapshot();
    const activeBrowserLeases = this.db.listResourceLeases({
      resourceType: "browser_context",
      status: "active",
      limit: browserContextsMax + 1,
    }).length;
    if (activeBrowserLeases >= browserContextsMax) {
      this.db.recordAudit({
        actor: { type: "agent", id: request.agentId },
        action: "resource_lease.denied_budget_exhausted",
        objectType: "browser_context",
        payload: {
          resourceType: "browser_context",
          active: activeBrowserLeases,
          limit: browserContextsMax,
        },
      });
      throw new Error(
        `Browser context budget exhausted: ${activeBrowserLeases}/${browserContextsMax} active`,
      );
    }

    return this.db.withTransaction(() => {
      const poolId = this.db.ensureBrowserPool({
        trustZoneId: request.trustZoneId,
        browserKind: request.browserKind,
        maxContexts: request.maxContexts,
      });
      const activeContexts = this.db.countActiveBrowserContexts(poolId);
      const maxContexts = this.db.getBrowserPoolMaxContexts(poolId);
      if (activeContexts >= maxContexts) {
        this.db.recordAudit({
          actor: { type: "agent", id: request.agentId },
          action: "browser_context.denied_pool_full",
          objectType: "browser_pool",
          objectId: poolId,
          payload: { activeContexts, maxContexts },
        });
        throw new Error(`Browser pool ${poolId} has no available contexts`);
      }
      const context = this.db.createBrowserContext({
        poolId,
        agentId: request.agentId,
        sessionId: request.sessionId,
        taskId: request.taskId,
        profileMode: request.profileMode ?? "ephemeral",
        allowedOrigins: request.allowedOrigins,
        expiresAt: request.expiresAt,
      });
      this.db.createResourceLease({
        resourceType: "browser_context",
        resourceId: context.id,
        ownerTaskId: request.taskId,
        ownerAgentId: request.agentId,
        trustZoneId: request.trustZoneId,
        leaseUntil: request.expiresAt,
      });
      this.db.recordAudit({
        actor: { type: "agent", id: request.agentId },
        action: "browser_context.acquired",
        objectType: "browser_context",
        objectId: context.id,
        payload: { trustZoneId: request.trustZoneId, poolId },
      });
      return context;
    });
  }

  releaseContext(contextId: string): boolean {
    const released = this.db.releaseBrowserContext(contextId);
    if (released) {
      this.db.releaseResourceLeasesByResource({
        resourceType: "browser_context",
        resourceId: contextId,
      });
    }
    return released;
  }

  recordObservation(contextId: string, observation: unknown): void {
    const record =
      observation && typeof observation === "object"
        ? (observation as Record<string, unknown>)
        : {};
    this.db.recordBrowserObservation({
      contextId,
      targetId: typeof record.targetId === "string" ? record.targetId : undefined,
      observationType:
        typeof record.observationType === "string" ? record.observationType : "browser_observation",
      payload: record.payload ?? observation,
      createdAt: typeof record.createdAt === "string" ? record.createdAt : undefined,
    });
  }

  async captureArtifact(
    contextId: string,
    artifactType: string,
    bytes: Buffer | Uint8Array | string,
    metadata: {
      mimeType?: string;
      retentionPolicy?: RuntimeRetentionPolicy;
      classification?: string;
    } = {},
  ): Promise<{ artifactId: string; sha256: string }> {
    if (!this.artifactStore) {
      throw new Error("Browser artifact capture requires an artifact store");
    }
    const context = this.db.getBrowserContext(contextId);
    if (!context) {
      throw new Error(`Browser context not found: ${contextId}`);
    }
    const artifact = await this.artifactStore.write({
      bytes,
      mimeType: metadata.mimeType,
      classification: metadata.classification ?? artifactType,
      retentionPolicy: metadata.retentionPolicy ?? "debug",
      createdByTaskId: context.taskId,
      subject: context.agentId
        ? { subjectType: "agent", subjectId: context.agentId, permission: "read" }
        : undefined,
    });
    this.db.recordAudit({
      actor: { type: "runtime" },
      action: "browser_context.artifact_captured",
      objectType: "browser_context",
      objectId: contextId,
      payload: { artifactId: artifact.id, artifactType },
    });
    return { artifactId: artifact.id, sha256: artifact.sha256 };
  }
}
