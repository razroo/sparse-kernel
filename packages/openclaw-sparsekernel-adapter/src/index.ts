import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";
import {
  SparseKernelClient,
  type SparseKernelArtifact,
  type SparseKernelCapability,
  type SparseKernelCompleteToolCallInput,
  type SparseKernelCreateArtifactInput,
  type SparseKernelCreateToolCallInput,
  type SparseKernelGrantCapabilityInput,
  type SparseKernelAllocateSandboxInput,
  type SparseKernelSession,
  type SparseKernelSandboxAllocation,
  type SparseKernelToolCall,
  type SparseKernelUpsertSessionInput,
} from "../../sparsekernel-client/src/index.js";

const DEFAULT_TOOL_OUTPUT_ARTIFACT_THRESHOLD_BYTES = 256 * 1024;

export type OpenClawSparseKernelToolUpdateCallback<T = unknown> = (
  update: T,
) => void | Promise<void>;

export type OpenClawSparseKernelTool = {
  name: string;
  execute: (
    toolCallId: string,
    params: unknown,
    signal?: AbortSignal,
    onUpdate?: OpenClawSparseKernelToolUpdateCallback<unknown>,
  ) => Promise<unknown>;
};

export type OpenClawSparseKernelBrowserProxyRequest = (opts: {
  method: string;
  path: string;
  query?: Record<string, string | number | boolean | undefined>;
  body?: unknown;
  timeoutMs?: number;
  profile?: string;
}) => Promise<unknown>;

export type OpenClawSparseKernelBrowserProxyLease = {
  id: string;
  proxyRequest: OpenClawSparseKernelBrowserProxyRequest;
  release: () => Promise<void> | void;
};

export type OpenClawSparseKernelBrowserProxyFactoryInput = {
  toolName: string;
  toolParams: unknown;
  agentId: string;
  sessionId: string;
  sessionKey?: string;
  runId?: string;
  taskId?: string;
};

export type OpenClawSparseKernelBrowserProxyFactory = (
  input: OpenClawSparseKernelBrowserProxyFactoryInput,
) => Promise<OpenClawSparseKernelBrowserProxyLease | null>;

export type OpenClawSparseKernelToolBrokerClient = {
  upsertSession(input: SparseKernelUpsertSessionInput): Promise<SparseKernelSession>;
  grantCapability(input: SparseKernelGrantCapabilityInput): Promise<SparseKernelCapability>;
  createToolCall(input: SparseKernelCreateToolCallInput): Promise<SparseKernelToolCall>;
  startToolCall(id: string): Promise<SparseKernelToolCall>;
  completeToolCall(input: SparseKernelCompleteToolCallInput): Promise<SparseKernelToolCall>;
  failToolCall(id: string, error: string): Promise<SparseKernelToolCall>;
  createArtifact(input: SparseKernelCreateArtifactInput): Promise<SparseKernelArtifact>;
  allocateSandbox(input: SparseKernelAllocateSandboxInput): Promise<SparseKernelSandboxAllocation>;
  releaseSandbox(allocationId: string): Promise<boolean>;
};

export type OpenClawSparseKernelToolBrokerOptions = {
  kernel: OpenClawSparseKernelToolBrokerClient;
  agentId: string;
  sessionId: string;
  sessionKey?: string;
  channel?: string;
  runId?: string;
  taskId?: string;
  capabilityExpiresAt?: string;
  autoGrantToolCapability?: (toolName: string) => boolean;
  outputArtifactThresholdBytes?: number;
  browserProxyFactory?: OpenClawSparseKernelBrowserProxyFactory;
};

export type OpenClawSparseKernelToolBrokerFactoryOptions = Omit<
  OpenClawSparseKernelToolBrokerOptions,
  "kernel"
> & {
  baseUrl?: string;
  fetchImpl?: typeof fetch;
};

type SerializedToolOutput =
  | {
      output: unknown;
      serialized?: undefined;
      sizeBytes?: undefined;
    }
  | {
      output: unknown;
      serialized: string;
      sizeBytes: number;
    };

export function createOpenClawSparseKernelToolBroker(
  options: OpenClawSparseKernelToolBrokerFactoryOptions,
): OpenClawSparseKernelToolBroker {
  return new OpenClawSparseKernelToolBroker({
    ...options,
    kernel: new SparseKernelClient({ baseUrl: options.baseUrl, fetchImpl: options.fetchImpl }),
  });
}

export class OpenClawSparseKernelToolBroker {
  private readonly preparedTools = new Set<string>();
  private readonly activeBrowserLeases = new Map<string, OpenClawSparseKernelBrowserProxyLease>();
  private sessionPrepared = false;
  private artifactWriteCapabilityPrepared = false;
  private readonly outputArtifactThresholdBytes: number;

  constructor(private readonly options: OpenClawSparseKernelToolBrokerOptions) {
    this.outputArtifactThresholdBytes = Math.max(
      0,
      Math.floor(
        options.outputArtifactThresholdBytes ?? DEFAULT_TOOL_OUTPUT_ARTIFACT_THRESHOLD_BYTES,
      ),
    );
  }

  wrapTool<T extends OpenClawSparseKernelTool>(tool: T): T {
    const execute = tool.execute;
    return {
      ...tool,
      execute: async (toolCallId, params, signal, onUpdate) => {
        await this.prepareTool(tool.name);
        const providerToolCallId = toolCallId.trim() || `tool_call_${randomUUID()}`;
        const ledgerToolCallId = this.ledgerToolCallId(providerToolCallId);
        await this.options.kernel.createToolCall({
          id: ledgerToolCallId,
          task_id: this.options.taskId,
          session_id: this.options.sessionId,
          agent_id: this.options.agentId,
          tool_name: tool.name,
          input: { providerToolCallId, params },
        });
        try {
          await this.options.kernel.startToolCall(ledgerToolCallId);
          const browserProxy = await this.maybeAcquireBrowserProxy(tool.name, params);
          const executionParams = browserProxy
            ? attachSparseKernelBrowserProxyRequest(params, browserProxy.proxyRequest)
            : params;
          const result = await execute(toolCallId, executionParams, signal, onUpdate);
          const { output, artifactIds } = await this.prepareCompletionOutput(result);
          await this.options.kernel.completeToolCall({
            id: ledgerToolCallId,
            output,
            artifact_ids: artifactIds,
          });
          return result;
        } catch (err) {
          await this.options.kernel.failToolCall(ledgerToolCallId, formatError(err));
          throw err;
        }
      },
    };
  }

  wrapTools<T extends OpenClawSparseKernelTool>(tools: T[]): T[] {
    return tools.map((tool) => this.wrapTool(tool));
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

  async prepareRun(tools: OpenClawSparseKernelTool[]): Promise<void> {
    await this.ensureSession();
    for (const tool of tools) {
      await this.ensureToolCapability(tool.name);
    }
  }

  private async prepareTool(toolName: string): Promise<void> {
    await this.ensureSession();
    await this.ensureToolCapability(toolName);
  }

  private async ensureSession(): Promise<void> {
    if (this.sessionPrepared) {
      return;
    }
    await this.options.kernel.upsertSession({
      id: this.options.sessionId,
      agent_id: this.options.agentId,
      session_key: this.options.sessionKey,
      channel: this.options.channel,
      status: "active",
      last_activity_at: new Date().toISOString(),
    });
    this.sessionPrepared = true;
  }

  private async ensureToolCapability(toolName: string): Promise<void> {
    if (this.preparedTools.has(toolName)) {
      return;
    }
    if (this.options.autoGrantToolCapability?.(toolName) === false) {
      this.preparedTools.add(toolName);
      return;
    }
    await this.options.kernel.grantCapability({
      subject_type: "agent",
      subject_id: this.options.agentId,
      resource_type: "tool",
      resource_id: toolName,
      action: "invoke",
      constraints: {
        sessionId: this.options.sessionId,
        sessionKey: this.options.sessionKey,
        runId: this.options.runId,
        taskId: this.options.taskId,
      },
      expires_at: this.options.capabilityExpiresAt,
    });
    this.preparedTools.add(toolName);
  }

  private async ensureArtifactWriteCapability(): Promise<void> {
    if (this.artifactWriteCapabilityPrepared) {
      return;
    }
    await this.options.kernel.grantCapability({
      subject_type: "agent",
      subject_id: this.options.agentId,
      resource_type: "artifact",
      resource_id: null,
      action: "write",
      constraints: {
        sessionId: this.options.sessionId,
        sessionKey: this.options.sessionKey,
        runId: this.options.runId,
        taskId: this.options.taskId,
      },
      expires_at: this.options.capabilityExpiresAt,
    });
    this.artifactWriteCapabilityPrepared = true;
  }

  private ledgerToolCallId(providerToolCallId: string): string {
    return this.options.runId ? `${this.options.runId}:${providerToolCallId}` : providerToolCallId;
  }

  private async maybeAcquireBrowserProxy(
    toolName: string,
    params: unknown,
  ): Promise<OpenClawSparseKernelBrowserProxyLease | null> {
    if (toolName !== "browser" || !this.options.browserProxyFactory) {
      return null;
    }
    const key = browserProxyLeaseKey(params, this.options.sessionId, this.options.taskId);
    const active = this.activeBrowserLeases.get(key);
    if (active) {
      return active;
    }
    const lease = await this.options.browserProxyFactory({
      toolName,
      toolParams: params,
      agentId: this.options.agentId,
      sessionId: this.options.sessionId,
      sessionKey: this.options.sessionKey,
      runId: this.options.runId,
      taskId: this.options.taskId,
    });
    if (lease) {
      this.activeBrowserLeases.set(key, lease);
    }
    return lease;
  }

  private async prepareCompletionOutput(
    result: unknown,
  ): Promise<{ output: unknown; artifactIds: string[] }> {
    const serialized = serializeToolOutput(result);
    if (
      this.outputArtifactThresholdBytes <= 0 ||
      !serialized.serialized ||
      serialized.sizeBytes <= this.outputArtifactThresholdBytes
    ) {
      return { output: serialized.output, artifactIds: [] };
    }
    await this.ensureArtifactWriteCapability();
    const artifact = await this.options.kernel.createArtifact({
      content_text: serialized.serialized,
      mime_type: "application/json",
      retention_policy: "debug",
      subject: {
        subject_type: "agent",
        subject_id: this.options.agentId,
        permission: "read",
      },
    });
    return {
      output: {
        type: "artifact_ref",
        artifact_type: "tool_output",
        artifact_id: artifact.id,
        sha256: artifact.sha256,
        size_bytes: artifact.size_bytes,
        mime_type: artifact.mime_type,
      },
      artifactIds: [artifact.id],
    };
  }
}

function serializeToolOutput(result: unknown): SerializedToolOutput {
  try {
    const serialized = JSON.stringify(result);
    if (serialized === undefined) {
      return { output: result };
    }
    return {
      output: result,
      serialized,
      sizeBytes: Buffer.byteLength(serialized),
    };
  } catch (err) {
    return {
      output: {
        type: "unserializable_tool_output",
        error: formatError(err),
      },
    };
  }
}

function formatError(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

const SPARSEKERNEL_BROWSER_PROXY_REQUEST_SYMBOL = Symbol.for(
  "openclaw.sparsekernel.browserProxyRequest",
);

function attachSparseKernelBrowserProxyRequest(
  params: unknown,
  proxyRequest: OpenClawSparseKernelBrowserProxyRequest,
): unknown {
  if (!params || typeof params !== "object" || Array.isArray(params)) {
    return params;
  }
  const copy = { ...(params as Record<string, unknown>) };
  Object.defineProperty(copy, SPARSEKERNEL_BROWSER_PROXY_REQUEST_SYMBOL, {
    value: proxyRequest,
    enumerable: false,
  });
  return copy;
}

function browserProxyLeaseKey(params: unknown, sessionId: string, taskId?: string): string {
  const record = params && typeof params === "object" ? (params as Record<string, unknown>) : {};
  const profile = typeof record.profile === "string" ? record.profile.trim() : "";
  const target = typeof record.target === "string" ? record.target.trim() : "";
  return [sessionId, taskId ?? "", profile || "default", target || "default"].join(":");
}
