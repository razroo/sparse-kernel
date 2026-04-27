import type { AnyAgentTool } from "../agents/tools/common.js";
import { openLocalKernelDatabase, type LocalKernelDatabase } from "./database.js";
import { CapabilityToolBroker } from "./tool-broker.js";

export type BrokerToolsForRunInput = {
  tools: AnyAgentTool[];
  agentId: string;
  sessionId: string;
  sessionKey?: string;
  runId?: string;
  taskId?: string;
  dbPath?: string;
  artifactRootDir?: string;
  outputArtifactThresholdBytes?: number;
  env?: NodeJS.ProcessEnv;
};

export type BrokeredToolsForRun = {
  tools: AnyAgentTool[];
  db: LocalKernelDatabase;
  close: () => void;
};

export function shouldUseRuntimeToolBroker(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env.OPENCLAW_RUNTIME_TOOL_BROKER?.trim().toLowerCase();
  if (raw === "off" || raw === "0" || raw === "false") {
    return false;
  }
  if (raw === "on" || raw === "1" || raw === "true") {
    return true;
  }
  return !(env.VITEST || env.NODE_ENV === "test");
}

export function brokerToolsForRun(input: BrokerToolsForRunInput): BrokeredToolsForRun {
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
      db,
      close,
    };
  } catch (err) {
    close();
    throw err;
  }
}
