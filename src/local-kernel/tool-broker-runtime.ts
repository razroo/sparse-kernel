import {
  createOpenClawSparseKernelToolBroker,
  OpenClawSparseKernelToolBroker,
  type OpenClawSparseKernelToolBrokerClient,
} from "../../packages/openclaw-sparsekernel-adapter/src/index.js";
import type { AnyAgentTool } from "../agents/tools/common.js";
import { openLocalKernelDatabase, type LocalKernelDatabase } from "./database.js";
import { CapabilityToolBroker } from "./tool-broker.js";

export type RuntimeToolBrokerMode = "off" | "local" | "daemon";

export type BrokerToolsForRunInput = {
  tools: AnyAgentTool[];
  agentId: string;
  sessionId: string;
  sessionKey?: string;
  channel?: string;
  runId?: string;
  taskId?: string;
  dbPath?: string;
  artifactRootDir?: string;
  outputArtifactThresholdBytes?: number;
  sparseKernelBaseUrl?: string;
  daemonKernel?: OpenClawSparseKernelToolBrokerClient;
  env?: NodeJS.ProcessEnv;
};

export type BrokeredToolsForRun = {
  tools: AnyAgentTool[];
  mode: "local" | "daemon";
  db?: LocalKernelDatabase;
  close: () => void;
};

export type LocalBrokeredToolsForRun = BrokeredToolsForRun & {
  mode: "local";
  db: LocalKernelDatabase;
};

export type BrokerEffectiveToolsForRunInput = BrokerToolsForRunInput & {
  onWarning?: (message: string) => void;
};

function isTruthyBrokerFlag(raw: string | undefined): boolean {
  const normalized = raw?.trim().toLowerCase();
  return normalized === "on" || normalized === "1" || normalized === "true";
}

function isFalsyBrokerFlag(raw: string | undefined): boolean {
  const normalized = raw?.trim().toLowerCase();
  return normalized === "off" || normalized === "0" || normalized === "false";
}

export function resolveRuntimeToolBrokerMode(
  env: NodeJS.ProcessEnv = process.env,
): RuntimeToolBrokerMode {
  const raw = env.OPENCLAW_RUNTIME_TOOL_BROKER?.trim().toLowerCase();
  if (raw && isFalsyBrokerFlag(raw)) {
    return "off";
  }
  if (raw === "daemon" || raw === "sparsekernel" || raw === "sparse-kernel") {
    return "daemon";
  }
  if (isTruthyBrokerFlag(env.OPENCLAW_SPARSEKERNEL_TOOL_BROKER)) {
    return "daemon";
  }
  if (raw === "local" || (raw && isTruthyBrokerFlag(raw))) {
    return "local";
  }
  return env.VITEST || env.NODE_ENV === "test" ? "off" : "local";
}

export function shouldUseRuntimeToolBroker(env: NodeJS.ProcessEnv = process.env): boolean {
  return resolveRuntimeToolBrokerMode(env) !== "off";
}

export function shouldUseSparseKernelDaemonToolBroker(
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  return resolveRuntimeToolBrokerMode(env) === "daemon";
}

export function brokerToolsForRun(input: BrokerToolsForRunInput): LocalBrokeredToolsForRun {
  const db = openLocalKernelDatabase({ dbPath: input.dbPath, env: input.env });
  const capabilityExpiresAt = new Date(Date.now() + 12 * 60 * 60 * 1000).toISOString();
  let closed = false;
  const close = () => {
    if (closed) {
      return;
    }
    closed = true;
    db.close();
  };
  try {
    db.upsertSession({
      id: input.sessionId,
      agentId: input.agentId,
      sessionKey: input.sessionKey,
      channel: input.channel,
      status: "active",
      lastActivityAt: new Date().toISOString(),
    });
    const subject = {
      subjectType: "run",
      subjectId: input.runId?.trim() || input.sessionId,
    };
    for (const tool of input.tools) {
      db.grantCapability({
        subjectType: subject.subjectType,
        subjectId: subject.subjectId,
        resourceType: "tool",
        resourceId: tool.name,
        action: "invoke",
        constraints: {
          agentId: input.agentId,
          sessionId: input.sessionId,
          sessionKey: input.sessionKey,
          runId: input.runId,
        },
        expiresAt: capabilityExpiresAt,
      });
      if (tool.name === "browser") {
        for (const trustZoneId of ["public_web", "authenticated_web", "user_browser_profile"]) {
          db.grantCapability({
            subjectType: "agent",
            subjectId: input.agentId,
            resourceType: "browser_context",
            resourceId: trustZoneId,
            action: "allocate",
            constraints: {
              sessionId: input.sessionId,
              sessionKey: input.sessionKey,
              runId: input.runId,
              reason: "browser tool available for run",
            },
            expiresAt: capabilityExpiresAt,
          });
        }
      }
    }
    const broker = new CapabilityToolBroker(db, {
      artifactRootDir: input.artifactRootDir,
      outputArtifactThresholdBytes: input.outputArtifactThresholdBytes,
      env: input.env,
    });
    return {
      tools: broker.wrapTools(input.tools, {
        agentId: input.agentId,
        sessionId: input.sessionId,
        taskId: input.taskId,
        subject,
        runId: input.runId,
      }),
      mode: "local",
      db,
      close,
    };
  } catch (err) {
    close();
    throw err;
  }
}

export async function brokerToolsForRunWithDaemon(
  input: BrokerToolsForRunInput,
): Promise<BrokeredToolsForRun> {
  const capabilityExpiresAt = new Date(Date.now() + 12 * 60 * 60 * 1000).toISOString();
  const broker = input.daemonKernel
    ? new OpenClawSparseKernelToolBroker({
        kernel: input.daemonKernel,
        agentId: input.agentId,
        sessionId: input.sessionId,
        sessionKey: input.sessionKey,
        channel: input.channel,
        runId: input.runId,
        taskId: input.taskId,
        capabilityExpiresAt,
        outputArtifactThresholdBytes: input.outputArtifactThresholdBytes,
      })
    : createOpenClawSparseKernelToolBroker({
        baseUrl:
          input.sparseKernelBaseUrl ??
          input.env?.OPENCLAW_SPARSEKERNEL_BASE_URL ??
          input.env?.SPARSEKERNEL_BASE_URL,
        agentId: input.agentId,
        sessionId: input.sessionId,
        sessionKey: input.sessionKey,
        channel: input.channel,
        runId: input.runId,
        taskId: input.taskId,
        capabilityExpiresAt,
        outputArtifactThresholdBytes: input.outputArtifactThresholdBytes,
      });
  await broker.prepareRun(input.tools);
  return {
    tools: broker.wrapTools(input.tools),
    mode: "daemon",
    close: () => {},
  };
}

export async function brokerEffectiveToolsForRun(
  input: BrokerEffectiveToolsForRunInput,
): Promise<BrokeredToolsForRun | undefined> {
  const mode = resolveRuntimeToolBrokerMode(input.env);
  if (mode === "off") {
    return undefined;
  }
  if (mode === "daemon") {
    try {
      return await brokerToolsForRunWithDaemon(input);
    } catch (err) {
      input.onWarning?.(
        `SparseKernel daemon tool broker unavailable; falling back to local runtime broker: ${formatBrokerError(err)}`,
      );
      try {
        return brokerToolsForRun(input);
      } catch (localErr) {
        input.onWarning?.(
          `local runtime tool broker unavailable; continuing with existing in-process tool execution: ${formatBrokerError(
            localErr,
          )}`,
        );
        return undefined;
      }
    }
  }
  try {
    return brokerToolsForRun(input);
  } catch (err) {
    input.onWarning?.(
      `runtime tool broker unavailable; continuing with existing in-process tool execution: ${formatBrokerError(
        err,
      )}`,
    );
    return undefined;
  }
}

function formatBrokerError(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
