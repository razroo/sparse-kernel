import type { AgentToolUpdateCallback } from "@mariozechner/pi-agent-core";
import type { AnyAgentTool } from "../agents/tools/common.js";
import { copyPluginToolMeta, getPluginToolMeta } from "../plugins/tools.js";
import { ContentAddressedArtifactStore } from "./artifact-store.js";
import { LocalBrowserBroker } from "./browser-broker.js";
import { resolveSparseKernelBrowserCdpEndpoint } from "./browser-managed-cdp.js";
import type { SparseKernelBrowserToolCdpProxyInput } from "./browser-tool-cdp-proxy.js";
import { attachSparseKernelBrowserProxyRequest } from "./browser-tool-proxy.js";
import type { LocalKernelDatabase } from "./database.js";

const DEFAULT_TOOL_OUTPUT_ARTIFACT_THRESHOLD_BYTES = 256 * 1024;

export type ToolBrokerSubject = {
  subjectType: string;
  subjectId: string;
};

export type ToolBrokerContext = {
  agentId?: string;
  sessionId?: string;
  taskId?: string;
  runId?: string;
  subject: ToolBrokerSubject;
};

export type CapabilityToolBrokerOptions = {
  artifactRootDir?: string;
  outputArtifactThresholdBytes?: number;
  env?: NodeJS.ProcessEnv;
  browserControlFetch?: typeof fetch;
  browserCdpProxyFactory?: (
    input: SparseKernelBrowserToolCdpProxyInput,
  ) => Promise<BrowserToolLease>;
};

export type BrowserToolLease = {
  id: string;
  proxyRequest?: (opts: {
    method: string;
    path: string;
    query?: Record<string, string | number | boolean | undefined>;
    body?: unknown;
    timeoutMs?: number;
    profile?: string;
  }) => Promise<unknown>;
  release: () => Promise<void> | void;
};

function resolveOutputArtifactThresholdBytes(options: CapabilityToolBrokerOptions): number {
  if (options.outputArtifactThresholdBytes !== undefined) {
    return Math.max(0, Math.floor(options.outputArtifactThresholdBytes));
  }
  const raw =
    options.env?.OPENCLAW_RUNTIME_TOOL_OUTPUT_ARTIFACT_BYTES ??
    process.env.OPENCLAW_RUNTIME_TOOL_OUTPUT_ARTIFACT_BYTES;
  if (!raw?.trim()) {
    return DEFAULT_TOOL_OUTPUT_ARTIFACT_THRESHOLD_BYTES;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed >= 0
    ? parsed
    : DEFAULT_TOOL_OUTPUT_ARTIFACT_THRESHOLD_BYTES;
}

function isBrowserBrokerDisabled(raw: string | undefined): boolean {
  const normalized = raw?.trim().toLowerCase();
  return normalized === "off" || normalized === "0" || normalized === "false";
}

export class CapabilityToolBroker {
  private readonly outputArtifactThresholdBytes: number;
  private readonly activeBrowserLeases = new Map<string, BrowserToolLease>();

  constructor(
    private readonly db: LocalKernelDatabase,
    private readonly options: CapabilityToolBrokerOptions = {},
  ) {
    this.outputArtifactThresholdBytes = resolveOutputArtifactThresholdBytes(options);
  }

  wrapTool(tool: AnyAgentTool, context: ToolBrokerContext): AnyAgentTool {
    const execute = tool.execute;
    const wrapped: AnyAgentTool = {
      ...tool,
      execute: async (
        toolCallId: string,
        params: unknown,
        signal?: AbortSignal,
        onUpdate?: AgentToolUpdateCallback<unknown>,
      ) => {
        const providerToolCallId = toolCallId.trim() || `tool_call:${Date.now()}`;
        const toolCallDbId = this.db.insertToolCall({
          id: context.runId ? `${context.runId}:${providerToolCallId}` : providerToolCallId,
          taskId: context.taskId,
          sessionId: context.sessionId,
          agentId: context.agentId,
          toolName: tool.name,
          input: { providerToolCallId, params },
        });
        const pluginMeta = getPluginToolMeta(tool);
        const allowed = this.db.checkCapability({
          subjectType: context.subject.subjectType,
          subjectId: context.subject.subjectId,
          resourceType: "tool",
          resourceId: tool.name,
          action: "invoke",
          context: { pluginId: pluginMeta?.pluginId, taskId: context.taskId },
        });
        if (!allowed) {
          const err = new Error(`Tool invocation denied: ${tool.name}`);
          this.db.failToolCall(toolCallDbId, err);
          throw err;
        }
        this.db.startToolCall(toolCallDbId);
        const browserContext = await this.maybeAcquireBrowserContext(tool, context, params);
        try {
          const executionParams = browserContext?.proxyRequest
            ? attachSparseKernelBrowserProxyRequest(params, browserContext.proxyRequest)
            : params;
          const result = await execute(toolCallId, executionParams, signal, onUpdate);
          const ledgerOutput = await this.prepareToolOutputForLedger(toolCallDbId, context, result);
          this.db.finishToolCall(toolCallDbId, ledgerOutput);
          return result;
        } catch (err) {
          this.db.failToolCall(toolCallDbId, err);
          throw err;
        }
      },
    };
    copyPluginToolMeta(tool, wrapped);
    return wrapped;
  }

  wrapTools(tools: AnyAgentTool[], context: ToolBrokerContext): AnyAgentTool[] {
    return tools.map((tool) => this.wrapTool(tool, context));
  }

  async close(): Promise<void> {
    const leases = [...this.activeBrowserLeases.values()];
    this.activeBrowserLeases.clear();
    const results = await Promise.allSettled(
      leases.map((lease) => Promise.resolve(lease.release())),
    );
    const rejected = results.find((result) => result.status === "rejected");
    if (rejected?.status === "rejected") {
      throw rejected.reason;
    }
  }

  private async prepareToolOutputForLedger(
    toolCallDbId: string,
    context: ToolBrokerContext,
    result: unknown,
  ): Promise<unknown> {
    if (this.outputArtifactThresholdBytes <= 0) {
      return result;
    }
    const serialized = JSON.stringify(result);
    const sizeBytes = Buffer.byteLength(serialized);
    if (sizeBytes <= this.outputArtifactThresholdBytes) {
      return result;
    }
    const artifact = await new ContentAddressedArtifactStore(
      this.db,
      this.options.artifactRootDir,
    ).write({
      bytes: serialized,
      mimeType: "application/json",
      createdByTaskId: context.taskId,
      createdByToolCallId: toolCallDbId,
      classification: "tool_output",
      retentionPolicy: "debug",
      subject: {
        subjectType: context.subject.subjectType,
        subjectId: context.subject.subjectId,
        permission: "read",
      },
    });
    if (
      context.agentId &&
      !(context.subject.subjectType === "agent" && context.subject.subjectId === context.agentId)
    ) {
      this.db.grantArtifactAccess({
        artifactId: artifact.id,
        subjectType: "agent",
        subjectId: context.agentId,
        permission: "read",
      });
    }
    this.db.recordAudit({
      actor: { type: "runtime" },
      action: "tool_call.output_artifactized",
      objectType: "tool_call",
      objectId: toolCallDbId,
      payload: {
        artifactId: artifact.id,
        sizeBytes,
        thresholdBytes: this.outputArtifactThresholdBytes,
      },
    });
    return {
      type: "artifact_ref",
      artifactType: "tool_output",
      artifactId: artifact.id,
      sha256: artifact.sha256,
      sizeBytes: artifact.sizeBytes,
      mimeType: artifact.mimeType,
    };
  }

  private async maybeAcquireBrowserContext(
    tool: AnyAgentTool,
    context: ToolBrokerContext,
    params: unknown,
  ): Promise<BrowserToolLease | null> {
    if (tool.name !== "browser" || !context.agentId) {
      return null;
    }
    const record = params && typeof params === "object" ? (params as Record<string, unknown>) : {};
    const profile = typeof record.profile === "string" ? record.profile.trim().toLowerCase() : "";
    const target = typeof record.target === "string" ? record.target.trim().toLowerCase() : "";
    const trustZoneId =
      profile === "user"
        ? "user_browser_profile"
        : profile || target === "host"
          ? "authenticated_web"
          : "public_web";
    const action = typeof record.action === "string" ? record.action.trim() : "";
    const urlCandidate =
      typeof record.targetUrl === "string"
        ? record.targetUrl
        : typeof record.url === "string"
          ? record.url
          : undefined;
    const env = this.options.env ?? process.env;
    const shouldEnforceNetwork =
      env.OPENCLAW_RUNTIME_BROWSER_POLICY_ENFORCE === "1" ||
      env.OPENCLAW_RUNTIME_BROWSER_POLICY_ENFORCE === "true";
    const allowedOrigins =
      shouldEnforceNetwork && urlCandidate && (action === "open" || action === "navigate")
        ? [urlCandidate]
        : undefined;
    const browserBrokerMode = env.OPENCLAW_RUNTIME_BROWSER_BROKER?.trim().toLowerCase();
    const cdpEligibleTarget = !target || target === "host";
    const cdpBrokerDisabled = isBrowserBrokerDisabled(browserBrokerMode);
    const managedCdp =
      cdpEligibleTarget && !cdpBrokerDisabled
        ? await resolveSparseKernelBrowserCdpEndpoint({
            env,
            fetchImpl: this.options.browserControlFetch,
            profile: profile || undefined,
          })
        : null;
    const shouldUseCdpBroker =
      cdpEligibleTarget &&
      !cdpBrokerDisabled &&
      Boolean(managedCdp?.cdpEndpoint) &&
      (browserBrokerMode === "cdp" ||
        browserBrokerMode === "sparsekernel" ||
        browserBrokerMode === "sparse-kernel" ||
        browserBrokerMode === "managed" ||
        browserBrokerMode === "managed-cdp" ||
        browserBrokerMode === "sparsekernel-managed" ||
        Boolean(env.OPENCLAW_SPARSEKERNEL_BROWSER_CONTROL_URL?.trim()) ||
        Boolean(env.OPENCLAW_BROWSER_CONTROL_URL?.trim()));
    const leaseKey = [
      shouldUseCdpBroker ? "cdp" : "local",
      trustZoneId,
      profile || "default",
      target || "default",
      context.sessionId ?? "",
      context.taskId ?? "",
    ].join(":");
    const activeLease = this.activeBrowserLeases.get(leaseKey);
    if (activeLease) {
      return activeLease;
    }
    try {
      if (shouldUseCdpBroker && managedCdp?.cdpEndpoint) {
        const createBrowserProxy =
          this.options.browserCdpProxyFactory ??
          (async (input: SparseKernelBrowserToolCdpProxyInput) => {
            const { createSparseKernelBrowserToolCdpProxy } =
              await import("./browser-tool-cdp-proxy.js");
            return await createSparseKernelBrowserToolCdpProxy(input);
          });
        const lease = await createBrowserProxy({
          agentId: context.agentId,
          sessionId: context.sessionId,
          taskId: context.taskId,
          trustZoneId,
          cdpEndpoint: managedCdp.cdpEndpoint,
          baseUrl: env.OPENCLAW_SPARSEKERNEL_BASE_URL ?? env.SPARSEKERNEL_BASE_URL,
          initialUrl:
            urlCandidate && (action === "open" || action === "navigate") ? urlCandidate : undefined,
          subject: {
            subject_type: context.subject.subjectType,
            subject_id: context.subject.subjectId,
            permission: "read",
          },
        });
        this.db.recordAudit({
          actor: { type: "agent", id: context.agentId },
          action: "browser_context.materialized_cdp",
          objectType: "browser_context",
          objectId: lease.id,
          payload: {
            trustZoneId,
            endpointSource: managedCdp.source,
            taskId: context.taskId,
            sessionId: context.sessionId,
          },
        });
        this.activeBrowserLeases.set(leaseKey, lease);
        return lease;
      }
      const localBroker = new LocalBrowserBroker(this.db);
      const record = localBroker.acquireContext({
        agentId: context.agentId,
        sessionId: context.sessionId,
        taskId: context.taskId,
        trustZoneId,
        profileMode: profile === "user" ? "user" : "ephemeral",
        allowedOrigins,
      });
      const lease = {
        id: record.id,
        release: () => {
          localBroker.releaseContext(record.id);
        },
      };
      this.activeBrowserLeases.set(leaseKey, lease);
      return lease;
    } catch (err) {
      this.db.recordAudit({
        actor: { type: "agent", id: context.agentId },
        action: "browser_context.accounting_failed",
        objectType: "tool",
        objectId: tool.name,
        payload: {
          trustZoneId,
          error: err instanceof Error ? err.message : String(err),
        },
      });
      if (shouldEnforceNetwork) {
        throw err;
      }
      return null;
    }
  }
}
