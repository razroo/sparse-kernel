import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import type {
  SparseKernelArtifact,
  SparseKernelCapability,
  SparseKernelCompleteToolCallInput,
  SparseKernelCreateArtifactInput,
  SparseKernelCreateToolCallInput,
  SparseKernelGrantCapabilityInput,
  SparseKernelSession,
  SparseKernelToolCall,
  SparseKernelUpsertSessionInput,
} from "../../sparsekernel-client/src/index.js";
import type { OpenClawSparseKernelTool, OpenClawSparseKernelToolBrokerClient } from "./index.js";
import { OpenClawSparseKernelToolBroker } from "./index.js";

class FakeKernel implements OpenClawSparseKernelToolBrokerClient {
  readonly sessions: SparseKernelUpsertSessionInput[] = [];
  readonly grants: SparseKernelGrantCapabilityInput[] = [];
  readonly creates: SparseKernelCreateToolCallInput[] = [];
  readonly starts: string[] = [];
  readonly completes: SparseKernelCompleteToolCallInput[] = [];
  readonly failures: Array<{ id: string; error: string }> = [];
  readonly artifacts: SparseKernelCreateArtifactInput[] = [];

  async upsertSession(input: SparseKernelUpsertSessionInput): Promise<SparseKernelSession> {
    this.sessions.push(input);
    return {
      id: input.id,
      agent_id: input.agent_id,
      session_key: input.session_key,
      channel: input.channel,
      status: input.status ?? "active",
      current_token_count: input.current_token_count ?? 0,
      last_activity_at: input.last_activity_at,
      created_at: "2026-04-27T00:00:00Z",
      updated_at: "2026-04-27T00:00:00Z",
    };
  }

  async grantCapability(input: SparseKernelGrantCapabilityInput): Promise<SparseKernelCapability> {
    this.grants.push(input);
    return {
      id: `cap_${this.grants.length}`,
      subject_type: input.subject_type,
      subject_id: input.subject_id,
      resource_type: input.resource_type,
      resource_id: input.resource_id,
      action: input.action,
      constraints: input.constraints,
      expires_at: input.expires_at,
      created_at: "2026-04-27T00:00:00Z",
    };
  }

  async createToolCall(input: SparseKernelCreateToolCallInput): Promise<SparseKernelToolCall> {
    const allowed = this.grants.some(
      (grant) =>
        grant.subject_type === "agent" &&
        grant.subject_id === input.agent_id &&
        grant.resource_type === "tool" &&
        grant.resource_id === input.tool_name &&
        grant.action === "invoke",
    );
    if (!allowed) {
      throw new Error(`denied: ${input.tool_name}`);
    }
    this.creates.push(input);
    return this.toolCall(input.id ?? `tool_call_${this.creates.length}`, input, "created");
  }

  async startToolCall(id: string): Promise<SparseKernelToolCall> {
    this.starts.push(id);
    const input = this.creates.find((entry) => entry.id === id);
    if (!input) {
      throw new Error(`missing tool call ${id}`);
    }
    return this.toolCall(id, input, "running");
  }

  async completeToolCall(input: SparseKernelCompleteToolCallInput): Promise<SparseKernelToolCall> {
    this.completes.push(input);
    const created = this.creates.find((entry) => entry.id === input.id);
    if (!created) {
      throw new Error(`missing tool call ${input.id}`);
    }
    return {
      ...this.toolCall(input.id, created, "completed"),
      output: input.output,
    };
  }

  async failToolCall(id: string, error: string): Promise<SparseKernelToolCall> {
    this.failures.push({ id, error });
    const input = this.creates.find((entry) => entry.id === id);
    if (!input) {
      throw new Error(`missing tool call ${id}`);
    }
    return {
      ...this.toolCall(id, input, "failed"),
      error,
    };
  }

  async createArtifact(input: SparseKernelCreateArtifactInput): Promise<SparseKernelArtifact> {
    this.artifacts.push(input);
    return {
      id: `artifact_${this.artifacts.length}`,
      sha256: `sha_${this.artifacts.length}`,
      mime_type: input.mime_type,
      size_bytes: Buffer.byteLength(input.content_text ?? input.content_base64 ?? ""),
      storage_ref: `sha256/aa/bb/sha_${this.artifacts.length}`,
      retention_policy: input.retention_policy,
      created_at: "2026-04-27T00:00:00Z",
    };
  }

  private toolCall(
    id: string,
    input: SparseKernelCreateToolCallInput,
    status: string,
  ): SparseKernelToolCall {
    return {
      id,
      task_id: input.task_id,
      session_id: input.session_id,
      agent_id: input.agent_id,
      tool_name: input.tool_name,
      status,
      input: input.input,
      created_at: "2026-04-27T00:00:00Z",
    };
  }
}

function makeBroker(kernel: FakeKernel, overrides: Partial<OpenClawSparseKernelTool> = {}) {
  const broker = new OpenClawSparseKernelToolBroker({
    kernel,
    agentId: "agent-a",
    sessionId: "session-a",
    sessionKey: "agent:agent-a:main",
    runId: "run-a",
    taskId: "task-a",
    outputArtifactThresholdBytes: 64,
  });
  const tool: OpenClawSparseKernelTool = {
    name: "demo_tool",
    execute: async () => ({ content: [{ type: "text", text: "ok" }] }),
    ...overrides,
  };
  return { broker, tool };
}

describe("@openclaw/openclaw-sparsekernel-adapter", () => {
  it("wraps a real tool execution with daemon session, capability, and tool-call lifecycle", async () => {
    const kernel = new FakeKernel();
    const { broker, tool } = makeBroker(kernel);

    const wrapped = broker.wrapTool(tool);
    await expect(wrapped.execute("provider-call-1", { q: "hello" })).resolves.toEqual({
      content: [{ type: "text", text: "ok" }],
    });

    expect(kernel.sessions).toHaveLength(1);
    expect(kernel.sessions[0]).toMatchObject({
      id: "session-a",
      agent_id: "agent-a",
      session_key: "agent:agent-a:main",
    });
    expect(kernel.grants).toContainEqual(
      expect.objectContaining({
        subject_type: "agent",
        subject_id: "agent-a",
        resource_type: "tool",
        resource_id: "demo_tool",
        action: "invoke",
      }),
    );
    expect(kernel.creates[0]).toMatchObject({
      id: "run-a:provider-call-1",
      task_id: "task-a",
      session_id: "session-a",
      agent_id: "agent-a",
      tool_name: "demo_tool",
      input: { providerToolCallId: "provider-call-1", params: { q: "hello" } },
    });
    expect(kernel.starts).toEqual(["run-a:provider-call-1"]);
    expect(kernel.completes[0]).toMatchObject({
      id: "run-a:provider-call-1",
      output: { content: [{ type: "text", text: "ok" }] },
      artifact_ids: [],
    });
  });

  it("fails the ledger tool call when the wrapped tool throws", async () => {
    const kernel = new FakeKernel();
    const { broker, tool } = makeBroker(kernel, {
      execute: async () => {
        throw new Error("tool exploded");
      },
    });

    const wrapped = broker.wrapTool(tool);
    await expect(wrapped.execute("provider-call-2", {})).rejects.toThrow("tool exploded");

    expect(kernel.failures).toEqual([{ id: "run-a:provider-call-2", error: "tool exploded" }]);
    expect(kernel.completes).toEqual([]);
  });

  it("stores large tool outputs as SparseKernel artifacts while returning the original result", async () => {
    const kernel = new FakeKernel();
    const largeText = "x".repeat(256);
    const { broker, tool } = makeBroker(kernel, {
      execute: async () => ({ content: [{ type: "text", text: largeText }] }),
    });

    const wrapped = broker.wrapTool(tool);
    await expect(wrapped.execute("provider-call-3", {})).resolves.toEqual({
      content: [{ type: "text", text: largeText }],
    });

    expect(kernel.artifacts[0]).toMatchObject({
      mime_type: "application/json",
      retention_policy: "debug",
      subject: {
        subject_type: "agent",
        subject_id: "agent-a",
        permission: "read",
      },
    });
    expect(kernel.grants).toContainEqual(
      expect.objectContaining({
        subject_type: "agent",
        subject_id: "agent-a",
        resource_type: "artifact",
        resource_id: null,
        action: "write",
      }),
    );
    expect(kernel.completes[0]).toMatchObject({
      id: "run-a:provider-call-3",
      output: {
        type: "artifact_ref",
        artifact_type: "tool_output",
        artifact_id: "artifact_1",
      },
      artifact_ids: ["artifact_1"],
    });
  });
});
