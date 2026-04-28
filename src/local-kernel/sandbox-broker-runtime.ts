import { openLocalKernelDatabase, type LocalKernelDatabase } from "./database.js";
import { LocalSandboxBroker, type SandboxBackendKind } from "./sandbox-broker.js";
import type { SandboxAllocationRecord } from "./types.js";

export type AccountSandboxForRunInput = {
  agentId: string;
  sessionId: string;
  sessionKey?: string;
  runId?: string;
  taskId?: string;
  backendId?: string;
  leaseMs?: number;
  dbPath?: string;
  env?: NodeJS.ProcessEnv;
};

export type AccountedSandboxRun = {
  allocation: SandboxAllocationRecord | null;
  db: LocalKernelDatabase;
  release: () => void;
  close: () => void;
};

function mapSandboxBackendKind(backendId: string | undefined): SandboxBackendKind {
  switch (backendId) {
    case "docker":
      return "docker";
    case "ssh":
      return "ssh";
    case "openshell":
      return "openshell";
    case "bwrap":
    case "bubblewrap":
      return "bwrap";
    case "minijail":
    case "minijail0":
      return "minijail";
    case "vm":
      return "vm";
    case "local":
    case "none":
    case "no_isolation":
    case "local/no_isolation":
      return "local/no_isolation";
    case undefined:
    case "":
      return "local/no_isolation";
    default:
      return "other";
  }
}

export function accountSandboxForRun(input: AccountSandboxForRunInput): AccountedSandboxRun {
  const db = openLocalKernelDatabase({ dbPath: input.dbPath, env: input.env });
  let released = false;
  let closed = false;
  const close = () => {
    if (closed) {
      return;
    }
    closed = true;
    db.close();
  };
  const release = () => {
    if (released) {
      return;
    }
    released = true;
    if (allocation) {
      new LocalSandboxBroker(db).releaseSandbox(allocation.id);
    }
  };
  const taskId = input.taskId?.trim() || input.runId?.trim() || input.sessionId;
  const leaseUntil = new Date(Date.now() + (input.leaseMs ?? 60 * 60 * 1000)).toISOString();
  db.upsertSession({
    id: input.sessionId,
    agentId: input.agentId,
    sessionKey: input.sessionKey,
    status: "active",
    lastActivityAt: new Date().toISOString(),
  });
  db.grantCapability({
    subjectType: "agent",
    subjectId: input.agentId,
    resourceType: "sandbox",
    resourceId: "code_execution",
    action: "allocate",
    constraints: {
      sessionId: input.sessionId,
      sessionKey: input.sessionKey,
      runId: input.runId,
    },
    expiresAt: leaseUntil,
  });
  let allocation: SandboxAllocationRecord | null = null;
  try {
    allocation = new LocalSandboxBroker(db).allocateSandbox({
      taskId,
      agentId: input.agentId,
      trustZoneId: "code_execution",
      requirements: {
        backend: mapSandboxBackendKind(input.backendId),
        leaseUntil,
      },
    });
    return { allocation, db, release, close };
  } catch (err) {
    db.recordAudit({
      actor: { type: "agent", id: input.agentId },
      action: "sandbox.accounting_failed",
      objectType: "trust_zone",
      objectId: "code_execution",
      payload: {
        backendId: input.backendId,
        error: err instanceof Error ? err.message : String(err),
      },
    });
    return { allocation: null, db, release, close };
  }
}
