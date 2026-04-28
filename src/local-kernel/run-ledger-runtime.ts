import { SparseKernelClient } from "../../packages/sparsekernel-client/src/index.js";
import type {
  SparseKernelAppendTranscriptEventInput,
  SparseKernelSession,
  SparseKernelTask,
  SparseKernelTranscriptEvent,
  SparseKernelUpsertSessionInput,
} from "../../packages/sparsekernel-client/src/index.js";
import { formatErrorMessage } from "../infra/errors.js";
import { openLocalKernelDatabase, type LocalKernelDatabase } from "./database.js";
import { resolveRuntimeToolBrokerMode } from "./tool-broker-runtime.js";

const EMBEDDED_RUN_TASK_KIND = "openclaw.embedded_run";
const DEFAULT_LEDGER_CONTENT_LIMIT_BYTES = 64 * 1024;

export type EmbeddedRunKernelLedgerClient = {
  upsertSession(input: SparseKernelUpsertSessionInput): Promise<SparseKernelSession>;
  enqueueTask(input: {
    id?: string;
    agent_id?: string | null;
    session_id?: string | null;
    kind: string;
    priority?: number;
    idempotency_key?: string | null;
    input?: unknown;
  }): Promise<SparseKernelTask>;
  claimTask(input: {
    task_id: string;
    worker_id: string;
    lease_seconds?: number;
  }): Promise<SparseKernelTask | null>;
  heartbeatTask(input: {
    task_id: string;
    worker_id: string;
    lease_seconds?: number;
  }): Promise<boolean>;
  completeTask(input: {
    task_id: string;
    worker_id: string;
    result_artifact_id?: string | null;
  }): Promise<boolean>;
  failTask(input: { task_id: string; worker_id: string; error: string }): Promise<boolean>;
  appendTranscriptEvent(
    input: SparseKernelAppendTranscriptEventInput,
  ): Promise<SparseKernelTranscriptEvent>;
};

export type MaterializeEmbeddedRunInKernelInput = {
  agentId: string;
  sessionId: string;
  sessionKey?: string;
  channel?: string;
  runId?: string;
  provider?: string;
  modelId?: string;
  trigger?: string;
  timeoutMs?: number;
  dbPath?: string;
  sparseKernelBaseUrl?: string;
  daemonKernel?: EmbeddedRunKernelLedgerClient;
  env?: NodeJS.ProcessEnv;
  onWarning?: (message: string) => void;
};

export type KernelTranscriptEventInput = {
  role: string;
  eventType: string;
  content?: unknown;
  parentEventId?: number;
  toolCallId?: string;
  tokenCount?: number;
};

export type EmbeddedRunKernelLedger = {
  mode: "local" | "daemon";
  taskId: string;
  workerId: string;
  db?: LocalKernelDatabase;
  appendTranscriptEvent: (input: KernelTranscriptEventInput) => Promise<void>;
  complete: (payload?: { resultArtifactId?: string; output?: unknown }) => Promise<void>;
  fail: (error: unknown) => Promise<void>;
  close: () => void;
};

type LedgerBackend =
  | {
      mode: "local";
      db: LocalKernelDatabase;
    }
  | {
      mode: "daemon";
      kernel: EmbeddedRunKernelLedgerClient;
    };

export function compactLedgerContent(
  content: unknown,
  limitBytes = DEFAULT_LEDGER_CONTENT_LIMIT_BYTES,
): unknown {
  if (content === undefined) {
    return undefined;
  }
  if (typeof content === "string") {
    const bytes = Buffer.byteLength(content);
    if (bytes <= limitBytes) {
      return content;
    }
    return {
      type: "truncated_text",
      sizeBytes: bytes,
      preview: content.slice(0, Math.max(0, Math.floor(limitBytes / 4))),
    };
  }
  try {
    const serialized = JSON.stringify(content);
    if (!serialized) {
      return content;
    }
    const bytes = Buffer.byteLength(serialized);
    if (bytes <= limitBytes) {
      return content;
    }
    return {
      type: "truncated_json",
      sizeBytes: bytes,
      preview: serialized.slice(0, Math.max(0, Math.floor(limitBytes / 4))),
    };
  } catch (err) {
    return {
      type: "unserializable",
      error: formatErrorMessage(err),
    };
  }
}

export async function materializeEmbeddedRunInKernel(
  input: MaterializeEmbeddedRunInKernelInput,
): Promise<EmbeddedRunKernelLedger | undefined> {
  const mode = resolveRuntimeToolBrokerMode(input.env);
  if (mode === "off") {
    return undefined;
  }
  if (mode === "daemon") {
    try {
      return await createDaemonRunLedger(input);
    } catch (err) {
      input.onWarning?.(
        `SparseKernel daemon run ledger unavailable; falling back to local runtime ledger: ${formatErrorMessage(
          err,
        )}`,
      );
      return createLocalRunLedger(input);
    }
  }
  try {
    return createLocalRunLedger(input);
  } catch (err) {
    input.onWarning?.(
      `local runtime run ledger unavailable; continuing without SparseKernel run ledger: ${formatErrorMessage(
        err,
      )}`,
    );
    return undefined;
  }
}

function resolveTaskId(input: MaterializeEmbeddedRunInKernelInput): string {
  const runId = input.runId?.trim();
  if (runId) {
    return runId;
  }
  return `embedded_run:${input.sessionId}:${Date.now()}`;
}

function resolveLeaseSeconds(input: MaterializeEmbeddedRunInKernelInput): number {
  const timeoutMs = Math.max(30_000, input.timeoutMs ?? 10 * 60 * 1000);
  return Math.ceil(timeoutMs / 1000) + 60;
}

function runTaskInput(input: MaterializeEmbeddedRunInKernelInput): unknown {
  return {
    runId: input.runId,
    sessionKey: input.sessionKey,
    channel: input.channel,
    provider: input.provider,
    modelId: input.modelId,
    trigger: input.trigger,
  };
}

function createHeartbeat(params: {
  taskId: string;
  workerId: string;
  leaseSeconds: number;
  heartbeat: () => Promise<boolean> | boolean;
  onWarning?: (message: string) => void;
}): () => void {
  const intervalMs = Math.max(5_000, Math.min(60_000, Math.floor(params.leaseSeconds * 333)));
  const timer = setInterval(() => {
    Promise.resolve(params.heartbeat()).then(
      (ok) => {
        if (!ok) {
          params.onWarning?.(
            `SparseKernel task heartbeat was not accepted for task ${params.taskId}`,
          );
        }
      },
      (err: unknown) => {
        params.onWarning?.(
          `SparseKernel task heartbeat failed for task ${params.taskId}: ${formatErrorMessage(err)}`,
        );
      },
    );
  }, intervalMs);
  timer.unref?.();
  return () => clearInterval(timer);
}

function buildLedger(params: {
  backend: LedgerBackend;
  taskId: string;
  workerId: string;
  leaseSeconds: number;
  input: MaterializeEmbeddedRunInKernelInput;
}): EmbeddedRunKernelLedger {
  let closed = false;
  let finished = false;
  let disabled = false;
  const warn = (message: string) => params.input.onWarning?.(message);
  const disableAfter = (operation: string, err: unknown) => {
    if (!disabled) {
      warn(
        `SparseKernel run ledger ${operation} failed; disabling run ledger writes: ${formatErrorMessage(err)}`,
      );
    }
    disabled = true;
  };
  const heartbeat = createHeartbeat({
    taskId: params.taskId,
    workerId: params.workerId,
    leaseSeconds: params.leaseSeconds,
    onWarning: warn,
    heartbeat: async () => {
      if (params.backend.mode === "local") {
        return params.backend.db.heartbeatTask(
          params.taskId,
          params.workerId,
          params.leaseSeconds * 1000,
        );
      }
      return await params.backend.kernel.heartbeatTask({
        task_id: params.taskId,
        worker_id: params.workerId,
        lease_seconds: params.leaseSeconds,
      });
    },
  });

  const appendTranscriptEvent = async (event: KernelTranscriptEventInput) => {
    if (closed || disabled) {
      return;
    }
    try {
      const content = compactLedgerContent(event.content);
      if (params.backend.mode === "local") {
        params.backend.db.appendTranscriptEvent({
          sessionId: params.input.sessionId,
          parentEventId: event.parentEventId,
          role: event.role,
          eventType: event.eventType,
          content,
          toolCallId: event.toolCallId,
          tokenCount: event.tokenCount,
        });
        return;
      }
      await params.backend.kernel.appendTranscriptEvent({
        session_id: params.input.sessionId,
        parent_event_id: event.parentEventId,
        role: event.role,
        event_type: event.eventType,
        content,
        tool_call_id: event.toolCallId,
        token_count: event.tokenCount,
      });
    } catch (err) {
      disableAfter("append", err);
    }
  };

  const complete = async (payload: { resultArtifactId?: string; output?: unknown } = {}) => {
    if (closed || finished) {
      return;
    }
    finished = true;
    heartbeat();
    await appendTranscriptEvent({
      role: "system",
      eventType: "run.completed",
      content: {
        runId: params.input.runId,
        resultArtifactId: payload.resultArtifactId,
        output: compactLedgerContent(payload.output),
      },
    });
    try {
      if (params.backend.mode === "local") {
        params.backend.db.completeTask(params.taskId, params.workerId, {
          artifactId: payload.resultArtifactId,
          output: payload.output,
        });
        return;
      }
      await params.backend.kernel.completeTask({
        task_id: params.taskId,
        worker_id: params.workerId,
        result_artifact_id: payload.resultArtifactId,
      });
    } catch (err) {
      disableAfter("complete", err);
    }
  };

  const fail = async (error: unknown) => {
    if (closed || finished) {
      return;
    }
    finished = true;
    heartbeat();
    await appendTranscriptEvent({
      role: "system",
      eventType: "run.failed",
      content: {
        runId: params.input.runId,
        error: formatErrorMessage(error),
      },
    });
    try {
      if (params.backend.mode === "local") {
        params.backend.db.failTask(params.taskId, params.workerId, error);
        return;
      }
      await params.backend.kernel.failTask({
        task_id: params.taskId,
        worker_id: params.workerId,
        error: formatErrorMessage(error),
      });
    } catch (err) {
      disableAfter("fail", err);
    }
  };

  const close = () => {
    if (closed) {
      return;
    }
    closed = true;
    heartbeat();
    if (params.backend.mode === "local") {
      params.backend.db.close();
    }
  };

  return {
    mode: params.backend.mode,
    taskId: params.taskId,
    workerId: params.workerId,
    ...(params.backend.mode === "local" ? { db: params.backend.db } : {}),
    appendTranscriptEvent,
    complete,
    fail,
    close,
  };
}

async function createDaemonRunLedger(
  input: MaterializeEmbeddedRunInKernelInput,
): Promise<EmbeddedRunKernelLedger> {
  const taskId = resolveTaskId(input);
  const workerId = `openclaw:${process.pid}:${taskId}`;
  const leaseSeconds = resolveLeaseSeconds(input);
  const kernel =
    input.daemonKernel ??
    new SparseKernelClient({
      baseUrl:
        input.sparseKernelBaseUrl ??
        input.env?.OPENCLAW_SPARSEKERNEL_BASE_URL ??
        input.env?.SPARSEKERNEL_BASE_URL,
    });
  await kernel.upsertSession({
    id: input.sessionId,
    agent_id: input.agentId,
    session_key: input.sessionKey,
    channel: input.channel,
    status: "running",
    last_activity_at: new Date().toISOString(),
  });
  await kernel.enqueueTask({
    id: taskId,
    agent_id: input.agentId,
    session_id: input.sessionId,
    kind: EMBEDDED_RUN_TASK_KIND,
    priority: 0,
    idempotency_key: input.runId,
    input: runTaskInput(input),
  });
  const claimed = await kernel.claimTask({
    task_id: taskId,
    worker_id: workerId,
    lease_seconds: leaseSeconds,
  });
  if (!claimed) {
    throw new Error(`SparseKernel task was not claimable: ${taskId}`);
  }
  const ledger = buildLedger({
    backend: { mode: "daemon", kernel },
    taskId,
    workerId,
    leaseSeconds,
    input,
  });
  await ledger.appendTranscriptEvent({
    role: "system",
    eventType: "run.started",
    content: runTaskInput(input),
  });
  return ledger;
}

function createLocalRunLedger(
  input: MaterializeEmbeddedRunInKernelInput,
): EmbeddedRunKernelLedger | undefined {
  const taskId = resolveTaskId(input);
  const workerId = `openclaw:${process.pid}:${taskId}`;
  const leaseSeconds = resolveLeaseSeconds(input);
  const db = openLocalKernelDatabase({ dbPath: input.dbPath, env: input.env });
  try {
    db.upsertSession({
      id: input.sessionId,
      agentId: input.agentId,
      sessionKey: input.sessionKey,
      channel: input.channel,
      status: "running",
      lastActivityAt: new Date().toISOString(),
    });
    db.enqueueTask({
      id: taskId,
      agentId: input.agentId,
      sessionId: input.sessionId,
      kind: EMBEDDED_RUN_TASK_KIND,
      priority: 0,
      idempotencyKey: input.runId,
      input: runTaskInput(input),
    });
    const claimed = db.claimTask({
      taskId,
      workerId,
      leaseMs: leaseSeconds * 1000,
    });
    if (!claimed) {
      input.onWarning?.(`SparseKernel local task was not claimable: ${taskId}`);
      db.close();
      return undefined;
    }
    const ledger = buildLedger({
      backend: { mode: "local", db },
      taskId,
      workerId,
      leaseSeconds,
      input,
    });
    void ledger.appendTranscriptEvent({
      role: "system",
      eventType: "run.started",
      content: runTaskInput(input),
    });
    return ledger;
  } catch (err) {
    db.close();
    throw err;
  }
}
