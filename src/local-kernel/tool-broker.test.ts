import { Buffer } from "node:buffer";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Type } from "typebox";
import { describe, expect, it } from "vitest";
import type { OpenClawSparseKernelToolBrokerClient } from "../../packages/openclaw-sparsekernel-adapter/src/index.js";
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
} from "../../packages/sparsekernel-client/src/index.js";
import type { AnyAgentTool } from "../agents/tools/common.js";
import {
  brokerEffectiveToolsForRun,
  brokerToolsForRun,
  CapabilityToolBroker,
  LocalKernelDatabase,
  resolveRuntimeToolBrokerMode,
} from "./index.js";

function makeTool(): AnyAgentTool {
  return {
    name: "sensitive_tool",
    label: "Sensitive Tool",
    description: "test",
    parameters: Type.Object({}),
    execute: async () => ({ content: [{ type: "text", text: "ok" }], details: {} }),
  } as AnyAgentTool;
}

class FakeDaemonKernel implements OpenClawSparseKernelToolBrokerClient {
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

  protected toolCall(
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

class FailingDaemonKernel extends FakeDaemonKernel {
  override async upsertSession(): Promise<SparseKernelSession> {
    throw new Error("daemon unavailable");
  }
}

describe("CapabilityToolBroker", () => {
  it("resolves daemon, local, and off runtime broker modes", () => {
    expect(
      resolveRuntimeToolBrokerMode({
        OPENCLAW_RUNTIME_TOOL_BROKER: "daemon",
      } as NodeJS.ProcessEnv),
    ).toBe("daemon");
    expect(
      resolveRuntimeToolBrokerMode({
        OPENCLAW_SPARSEKERNEL_TOOL_BROKER: "1",
      } as NodeJS.ProcessEnv),
    ).toBe("daemon");
    expect(
      resolveRuntimeToolBrokerMode({
        OPENCLAW_RUNTIME_TOOL_BROKER: "local",
      } as NodeJS.ProcessEnv),
    ).toBe("local");
    expect(
      resolveRuntimeToolBrokerMode({
        OPENCLAW_RUNTIME_TOOL_BROKER: "off",
      } as NodeJS.ProcessEnv),
    ).toBe("off");
    expect(resolveRuntimeToolBrokerMode({ VITEST: "true" } as NodeJS.ProcessEnv)).toBe("off");
  });

  it("routes embedded tools through SparseKernel daemon mode when explicitly enabled", async () => {
    const kernel = new FakeDaemonKernel();
    const run = await brokerEffectiveToolsForRun({
      tools: [makeTool()],
      agentId: "main",
      sessionId: "session-a",
      sessionKey: "agent:main:main",
      channel: "discord",
      runId: "run-a",
      taskId: "task-a",
      daemonKernel: kernel,
      env: { OPENCLAW_RUNTIME_TOOL_BROKER: "daemon" } as NodeJS.ProcessEnv,
    });
    expect(run?.mode).toBe("daemon");
    await expect(run?.tools[0]?.execute("provider-call", {})).resolves.toEqual({
      content: [{ type: "text", text: "ok" }],
      details: {},
    });
    expect(kernel.sessions[0]).toMatchObject({
      id: "session-a",
      agent_id: "main",
      channel: "discord",
    });
    expect(kernel.creates[0]).toMatchObject({
      id: "run-a:provider-call",
      task_id: "task-a",
      session_id: "session-a",
      agent_id: "main",
      tool_name: "sensitive_tool",
    });
    expect(kernel.completes[0]).toMatchObject({
      id: "run-a:provider-call",
      artifact_ids: [],
    });
  });

  it("falls back from daemon broker mode to the local broker", async () => {
    const warnings: string[] = [];
    const run = await brokerEffectiveToolsForRun({
      tools: [makeTool()],
      agentId: "main",
      sessionId: "session-a",
      sessionKey: "agent:main:main",
      runId: "run-a",
      dbPath: ":memory:",
      daemonKernel: new FailingDaemonKernel(),
      env: { OPENCLAW_RUNTIME_TOOL_BROKER: "daemon" } as NodeJS.ProcessEnv,
      onWarning: (message) => warnings.push(message),
    });
    try {
      expect(run?.mode).toBe("local");
      expect(warnings[0]).toContain("falling back to local runtime broker");
      await expect(run?.tools[0]?.execute("provider-call", {})).resolves.toMatchObject({
        details: {},
      });
      if (run?.mode !== "local" || !run.db) {
        throw new Error("expected local broker fallback");
      }
      const db = run.db;
      const row = db.db
        .prepare("SELECT status FROM tool_calls WHERE id = ?")
        .get("run-a:provider-call") as { status: string };
      expect(row.status).toBe("succeeded");
    } finally {
      run?.close();
    }
  });

  it("denies tool invocation without capability and records audit/tool state", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    try {
      db.ensureAgent({ id: "main" });
      const broker = new CapabilityToolBroker(db);
      const tool = broker.wrapTool(makeTool(), {
        subject: { subjectType: "agent", subjectId: "main" },
        agentId: "main",
      });
      await expect(tool.execute("call-1", {})).rejects.toThrow(/denied/);
      const row = db.db
        .prepare("SELECT status, error FROM tool_calls WHERE id = ?")
        .get("call-1") as {
        status: string;
        error: string;
      };
      expect(row.status).toBe("failed");
      expect(row.error).toContain("denied");
    } finally {
      db.close();
    }
  });

  it("allows tool invocation with capability", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    try {
      db.ensureAgent({ id: "main" });
      db.grantCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "tool",
        resourceId: "sensitive_tool",
        action: "invoke",
      });
      const broker = new CapabilityToolBroker(db);
      const tool = broker.wrapTool(makeTool(), {
        subject: { subjectType: "agent", subjectId: "main" },
        agentId: "main",
      });
      await expect(tool.execute("call-2", {})).resolves.toEqual({
        content: [{ type: "text", text: "ok" }],
        details: {},
      });
      const row = db.db.prepare("SELECT status FROM tool_calls WHERE id = ?").get("call-2") as {
        status: string;
      };
      expect(row.status).toBe("succeeded");
    } finally {
      db.close();
    }
  });

  it("artifactizes large tool outputs in the ledger without changing the tool result", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-tool-broker-"));
    const db = new LocalKernelDatabase({ dbPath: path.join(root, "runtime.sqlite") });
    try {
      db.ensureAgent({ id: "main" });
      db.grantCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "tool",
        resourceId: "sensitive_tool",
        action: "invoke",
      });
      const largeText = "x".repeat(256);
      const broker = new CapabilityToolBroker(db, {
        artifactRootDir: path.join(root, "artifacts"),
        outputArtifactThresholdBytes: 64,
      });
      const tool = broker.wrapTool(
        {
          ...makeTool(),
          execute: async () => ({ content: [{ type: "text", text: largeText }], details: {} }),
        },
        {
          subject: { subjectType: "agent", subjectId: "main" },
          agentId: "main",
        },
      );
      await expect(tool.execute("call-large", {})).resolves.toEqual({
        content: [{ type: "text", text: largeText }],
        details: {},
      });
      const row = db.db
        .prepare("SELECT output_json FROM tool_calls WHERE id = ?")
        .get("call-large") as { output_json: string };
      const output = JSON.parse(row.output_json) as {
        type: string;
        artifactType: string;
        artifactId: string;
      };
      expect(output).toMatchObject({ type: "artifact_ref", artifactType: "tool_output" });
      const artifact = db.getArtifact(output.artifactId);
      expect(artifact).toMatchObject({
        mimeType: "application/json",
        classification: "tool_output",
        retentionPolicy: "debug",
      });
      const audit = db.db
        .prepare("SELECT action FROM audit_log WHERE action = ?")
        .get("tool_call.output_artifactized") as { action: string } | undefined;
      expect(audit?.action).toBe("tool_call.output_artifactized");
    } finally {
      db.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("accounts browser tool invocations with brokered context leases", async () => {
    const run = brokerToolsForRun({
      tools: [
        {
          name: "browser",
          label: "Browser",
          description: "test",
          parameters: Type.Object({}),
          execute: async () => ({ content: [{ type: "text", text: "ok" }], details: {} }),
        } as AnyAgentTool,
      ],
      agentId: "main",
      sessionId: "session-a",
      sessionKey: "agent:main:main",
      runId: "run-a",
      dbPath: ":memory:",
    });
    try {
      await expect(
        run.tools[0]?.execute("call-browser", {
          action: "status",
        }),
      ).resolves.toMatchObject({ details: {} });
      const context = run.db.db
        .prepare("SELECT status FROM browser_contexts ORDER BY created_at DESC LIMIT 1")
        .get() as { status: string };
      expect(context.status).toBe("released");
      const lease = run.db.db
        .prepare(
          "SELECT status FROM resource_leases WHERE resource_type = 'browser_context' LIMIT 1",
        )
        .get() as { status: string };
      expect(lease.status).toBe("released");
    } finally {
      run.close();
    }
  });
});
