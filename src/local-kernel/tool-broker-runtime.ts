import {
  OpenClawSparseKernelToolBroker,
  type OpenClawSparseKernelBrowserProxyFactory,
  type OpenClawSparseKernelToolBrokerClient,
} from "../../packages/openclaw-sparsekernel-adapter/src/index.js";
import { SparseKernelClient } from "../../packages/sparsekernel-client/src/index.js";
import type { AnyAgentTool } from "../agents/tools/common.js";
import { getPluginToolMeta } from "../plugins/tools.js";
import { resolveSparseKernelBrowserCdpEndpoint } from "./browser-managed-cdp.js";
import type { acquireNativeBrowserProcess } from "./browser-process-pool.js";
import { openLocalKernelDatabase, type LocalKernelDatabase } from "./database.js";
import {
  buildSandboxSpawnPlan,
  runSandboxSpawnPlan,
  type SandboxBackendKind,
} from "./sandbox-broker.js";
import {
  CapabilityToolBroker,
  isSandboxCommandToolName,
  requiresPluginSubprocess,
  resolvePluginSandboxConfig,
  resolvePluginSubprocessPlan,
  type PluginSubprocessPlan,
} from "./tool-broker.js";

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
  browserNativeAcquire?: typeof acquireNativeBrowserProcess;
};

export type BrokeredToolsForRun = {
  tools: AnyAgentTool[];
  mode: "local" | "daemon";
  db?: LocalKernelDatabase;
  close: () => void | Promise<void>;
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

const SENSITIVE_TOOL_NAMES = new Set([
  "browser",
  "exec",
  "bash",
  "shell",
  "read",
  "write",
  "edit",
  "patch",
  "apply_patch",
]);

export function isSparseKernelSensitiveTool(toolName: string): boolean {
  const normalized = toolName.trim().toLowerCase();
  if (SENSITIVE_TOOL_NAMES.has(normalized)) {
    return true;
  }
  return (
    normalized.startsWith("mcp__") ||
    normalized.includes("filesystem") ||
    normalized.includes("sandbox")
  );
}

export function shouldAutoGrantToolCapability(
  toolName: string,
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  const mode =
    env.OPENCLAW_RUNTIME_TOOL_CAPABILITY_MODE?.trim().toLowerCase() ??
    env.OPENCLAW_SPARSEKERNEL_TOOL_CAPABILITY_MODE?.trim().toLowerCase();
  if (mode !== "strict" && mode !== "fail-closed") {
    return true;
  }
  if (!isSparseKernelSensitiveTool(toolName)) {
    return true;
  }
  return isTruthyBrokerFlag(env.OPENCLAW_RUNTIME_TOOL_ALLOW_SENSITIVE);
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
  let broker: CapabilityToolBroker | undefined;
  const close = async () => {
    if (closed) {
      return;
    }
    closed = true;
    try {
      await broker?.close();
    } finally {
      db.close();
    }
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
      if (shouldAutoGrantToolCapability(tool.name, input.env)) {
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
      }
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
      if (isSandboxCommandToolName(tool.name)) {
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
            reason: "exec-shaped tool available for brokered run",
          },
          expiresAt: capabilityExpiresAt,
        });
      }
      const pluginMeta = getPluginToolMeta(tool);
      if (pluginMeta?.subprocess) {
        const trustZoneId =
          pluginMeta.subprocess.sandbox?.trustZoneId?.trim() || "plugin_untrusted";
        db.grantCapability({
          subjectType: "agent",
          subjectId: input.agentId,
          resourceType: "sandbox",
          resourceId: trustZoneId,
          action: "allocate",
          constraints: {
            sessionId: input.sessionId,
            sessionKey: input.sessionKey,
            runId: input.runId,
            pluginId: pluginMeta.pluginId,
            reason: "plugin subprocess worker available for brokered run",
          },
          expiresAt: capabilityExpiresAt,
        });
      }
    }
    broker = new CapabilityToolBroker(db, {
      artifactRootDir: input.artifactRootDir,
      outputArtifactThresholdBytes: input.outputArtifactThresholdBytes,
      env: input.env,
      browserNativeAcquire: input.browserNativeAcquire,
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
    closed = true;
    db.close();
    throw err;
  }
}

export async function brokerToolsForRunWithDaemon(
  input: BrokerToolsForRunInput,
): Promise<BrokeredToolsForRun> {
  const capabilityExpiresAt = new Date(Date.now() + 12 * 60 * 60 * 1000).toISOString();
  const browserProxyFactory = createDaemonBrowserProxyFactory(input);
  const kernel =
    input.daemonKernel ??
    new SparseKernelClient({
      baseUrl:
        input.sparseKernelBaseUrl ??
        input.env?.OPENCLAW_SPARSEKERNEL_BASE_URL ??
        input.env?.SPARSEKERNEL_BASE_URL,
    });
  const daemonTools = wrapDaemonPluginSubprocessTools(input, kernel, capabilityExpiresAt);
  const broker = new OpenClawSparseKernelToolBroker({
    kernel,
    agentId: input.agentId,
    sessionId: input.sessionId,
    sessionKey: input.sessionKey,
    channel: input.channel,
    runId: input.runId,
    taskId: input.taskId,
    capabilityExpiresAt,
    autoGrantToolCapability: (toolName) => shouldAutoGrantToolCapability(toolName, input.env),
    outputArtifactThresholdBytes: input.outputArtifactThresholdBytes,
    browserProxyFactory,
  });
  await broker.prepareRun(daemonTools);
  return {
    tools: broker.wrapTools(daemonTools),
    mode: "daemon",
    close: () => broker.close(),
  };
}

function wrapDaemonPluginSubprocessTools(
  input: BrokerToolsForRunInput,
  kernel: OpenClawSparseKernelToolBrokerClient,
  capabilityExpiresAt: string,
): AnyAgentTool[] {
  const env = input.env ?? process.env;
  const strictPluginBoundary =
    requiresPluginSubprocess(env.OPENCLAW_RUNTIME_PLUGIN_PROCESS_BOUNDARY) ||
    requiresPluginSubprocess(env.OPENCLAW_RUNTIME_PLUGIN_PROCESS);
  return input.tools.map((tool) => {
    const pluginMeta = getPluginToolMeta(tool);
    if (!pluginMeta) {
      return tool;
    }
    const plan = resolvePluginSubprocessPlan(pluginMeta);
    if (!plan && !strictPluginBoundary) {
      return tool;
    }
    return {
      ...tool,
      execute: async (toolCallId, params, signal) => {
        if (!plan) {
          throw new Error(
            `Plugin tool ${tool.name} from ${pluginMeta.pluginId} requires out-of-process execution, but no subprocess worker is configured.`,
          );
        }
        return await runDaemonPluginToolSubprocess({
          input,
          kernel,
          capabilityExpiresAt,
          plan,
          pluginId: pluginMeta.pluginId,
          tool,
          toolCallId,
          params,
          signal,
        });
      },
    } as AnyAgentTool;
  });
}

async function runDaemonPluginToolSubprocess(params: {
  input: BrokerToolsForRunInput;
  kernel: OpenClawSparseKernelToolBrokerClient;
  capabilityExpiresAt: string;
  plan: PluginSubprocessPlan;
  pluginId: string;
  tool: AnyAgentTool;
  toolCallId: string;
  params: unknown;
  signal?: AbortSignal;
}): Promise<unknown> {
  const env = params.input.env ?? process.env;
  const sandboxConfig = resolvePluginSandboxConfig({ plan: params.plan, env });
  const timeoutMs = Math.max(1, sandboxConfig.maxRuntimeMs ?? params.plan.timeoutMs ?? 30_000);
  if (sandboxConfig.backend === "local/no_isolation" && sandboxConfig.requireIsolated) {
    throw new Error(
      `Plugin subprocess for ${params.tool.name} requires an isolated sandbox backend; install bwrap/minijail, configure Docker with OPENCLAW_RUNTIME_PLUGIN_DOCKER_IMAGE, or set OPENCLAW_RUNTIME_PLUGIN_ALLOW_NO_ISOLATION=1 only for trusted local workers.`,
    );
  }
  await params.kernel.grantCapability({
    subject_type: "agent",
    subject_id: params.input.agentId,
    resource_type: "sandbox",
    resource_id: sandboxConfig.trustZoneId,
    action: "allocate",
    constraints: {
      sessionId: params.input.sessionId,
      sessionKey: params.input.sessionKey,
      runId: params.input.runId,
      taskId: params.input.taskId,
      pluginId: params.pluginId,
      reason: "plugin subprocess worker available for daemon brokered run",
    },
    expires_at: params.capabilityExpiresAt,
  });
  const allocation = await params.kernel.allocateSandbox({
    agent_id: params.input.agentId,
    task_id: params.input.taskId ?? params.input.runId ?? params.input.sessionId,
    trust_zone_id: sandboxConfig.trustZoneId,
    backend: sandboxConfig.backend,
    docker_image: sandboxConfig.dockerImage,
    max_runtime_ms: timeoutMs,
    max_bytes_out: sandboxConfig.maxBytesOut,
  });
  try {
    const payload = JSON.stringify({
      protocol: "openclaw.sparsekernel.plugin_tool.v1",
      pluginId: params.pluginId,
      toolName: params.tool.name,
      toolCallId: params.toolCallId,
      params: params.params,
      context: {
        agentId: params.input.agentId,
        sessionId: params.input.sessionId,
        taskId: params.input.taskId,
        runId: params.input.runId,
        subject: { subjectType: "agent", subjectId: params.input.agentId },
      },
    });
    const spawnPlan = buildSandboxSpawnPlan({
      backend: sandboxConfig.backend,
      command: params.plan.command,
      args: params.plan.args ?? [],
      cwd: params.plan.cwd,
      stdin: true,
      dockerImage: sandboxConfig.dockerImage,
    });
    const result = await runSandboxSpawnPlan({
      allocationId: allocation.id,
      backend: sandboxConfig.backend as SandboxBackendKind,
      spawnPlan,
      cwd: params.plan.cwd,
      stdin: payload,
      signal: params.signal,
      timeoutMs,
      maxOutputBytes: sandboxConfig.maxBytesOut,
    });
    if (result.exitCode !== 0 || result.timedOut) {
      throw new Error(
        `Plugin subprocess failed: exit=${
          result.exitCode ?? result.signal ?? "unknown"
        } stderr=${result.stderr.trim()}`,
      );
    }
    try {
      return JSON.parse(result.stdout) as unknown;
    } catch (err) {
      throw new Error(
        `Plugin subprocess returned invalid JSON: ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    }
  } finally {
    await params.kernel.releaseSandbox(allocation.id);
  }
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

function createDaemonBrowserProxyFactory(
  input: BrokerToolsForRunInput,
): OpenClawSparseKernelBrowserProxyFactory | undefined {
  const env = input.env ?? process.env;
  const browserBrokerMode = env.OPENCLAW_RUNTIME_BROWSER_BROKER?.trim().toLowerCase();
  const shouldUseCdpBroker =
    !isFalsyBrokerFlag(browserBrokerMode) &&
    (browserBrokerMode === "cdp" ||
      browserBrokerMode === "sparsekernel" ||
      browserBrokerMode === "sparse-kernel" ||
      browserBrokerMode === "managed" ||
      browserBrokerMode === "managed-cdp" ||
      browserBrokerMode === "sparsekernel-managed" ||
      browserBrokerMode === "native" ||
      browserBrokerMode === "native-cdp" ||
      browserBrokerMode === "sparsekernel-native" ||
      browserBrokerMode === "sparse-kernel-native" ||
      isTruthyBrokerFlag(env.OPENCLAW_SPARSEKERNEL_BROWSER_NATIVE) ||
      Boolean(env.OPENCLAW_SPARSEKERNEL_BROWSER_CONTROL_URL?.trim()) ||
      Boolean(env.OPENCLAW_BROWSER_CONTROL_URL?.trim()));
  if (!shouldUseCdpBroker) {
    return undefined;
  }
  return async ({ toolName, toolParams, agentId, sessionId, taskId }) => {
    if (toolName !== "browser") {
      return null;
    }
    const record =
      toolParams && typeof toolParams === "object" ? (toolParams as Record<string, unknown>) : {};
    const target = typeof record.target === "string" ? record.target.trim().toLowerCase() : "";
    if (target && target !== "host") {
      return null;
    }
    const profile = typeof record.profile === "string" ? record.profile.trim().toLowerCase() : "";
    const action = typeof record.action === "string" ? record.action.trim() : "";
    const urlCandidate =
      typeof record.targetUrl === "string"
        ? record.targetUrl
        : typeof record.url === "string"
          ? record.url
          : undefined;
    const shouldEnforceNetwork =
      env.OPENCLAW_RUNTIME_BROWSER_POLICY_ENFORCE === "1" ||
      env.OPENCLAW_RUNTIME_BROWSER_POLICY_ENFORCE === "true";
    const allowedOrigins =
      shouldEnforceNetwork && urlCandidate && (action === "open" || action === "navigate")
        ? [urlCandidate]
        : undefined;
    const trustZoneId =
      profile === "user"
        ? "user_browser_profile"
        : profile || target === "host"
          ? "authenticated_web"
          : "public_web";
    const managedCdp = await resolveSparseKernelBrowserCdpEndpoint({
      env,
      nativeAcquire: input.browserNativeAcquire,
      profile: profile || undefined,
      trustZoneId,
    });
    if (!managedCdp?.cdpEndpoint) {
      return null;
    }
    const { createSparseKernelBrowserToolCdpProxy } = await import("./browser-tool-cdp-proxy.js");
    const proxyLease = await createSparseKernelBrowserToolCdpProxy({
      agentId,
      sessionId,
      taskId,
      trustZoneId,
      cdpEndpoint: managedCdp.cdpEndpoint,
      baseUrl:
        input.sparseKernelBaseUrl ??
        env.OPENCLAW_SPARSEKERNEL_BASE_URL ??
        env.SPARSEKERNEL_BASE_URL,
      initialUrl:
        urlCandidate && (action === "open" || action === "navigate") ? urlCandidate : undefined,
      allowedOrigins,
      subject: {
        subject_type: "agent",
        subject_id: agentId,
        permission: "read",
      },
    });
    if (!managedCdp.release) {
      return proxyLease;
    }
    return {
      ...proxyLease,
      release: async () => {
        try {
          await proxyLease.release();
        } finally {
          await managedCdp.release?.();
        }
      },
    };
  };
}
