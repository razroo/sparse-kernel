import { Buffer } from "node:buffer";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Type } from "typebox";
import { describe, expect, it, vi } from "vitest";
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
import { setPluginToolMeta } from "../plugins/tools.js";
import type {
  NativeBrowserProcessAcquireInput,
  NativeBrowserProcessLease,
} from "./browser-process-pool.js";
import type { SparseKernelBrowserToolCdpProxyInput } from "./browser-tool-cdp-proxy.js";
import { SPARSEKERNEL_BROWSER_PROXY_REQUEST_SYMBOL } from "./browser-tool-proxy.js";
import {
  brokerEffectiveToolsForRun,
  brokerToolsForRun,
  CapabilityToolBroker,
  LocalKernelDatabase,
  resolveRuntimeToolBrokerMode,
} from "./index.js";
import { resolvePluginSandboxConfig } from "./tool-broker.js";

function makeTool(name = "sensitive_tool"): AnyAgentTool {
  return {
    name,
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
      await run?.close();
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

  it("fails closed for plugin tools when subprocess execution is required", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    try {
      db.ensureAgent({ id: "main" });
      db.grantCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "tool",
        resourceId: "plugin_tool",
        action: "invoke",
      });
      const rawTool = makeTool("plugin_tool");
      setPluginToolMeta(rawTool, { pluginId: "community-plugin", optional: false });
      const broker = new CapabilityToolBroker(db, {
        env: { OPENCLAW_RUNTIME_PLUGIN_PROCESS_BOUNDARY: "subprocess" } as NodeJS.ProcessEnv,
      });
      const tool = broker.wrapTool(rawTool, {
        subject: { subjectType: "agent", subjectId: "main" },
        agentId: "main",
      });
      await expect(tool.execute("call-plugin", {})).rejects.toThrow(/out-of-process execution/);
      expect(db.listAudit({ limit: 20 }).map((entry) => entry.action)).toEqual(
        expect.arrayContaining(["plugin_tool.subprocess_required", "tool_call.failed"]),
      );
    } finally {
      db.close();
    }
  });

  it("auto-selects an available isolated backend for plugin subprocess workers", () => {
    expect(
      resolvePluginSandboxConfig({
        plan: { command: "worker" },
        env: {} as NodeJS.ProcessEnv,
        backendAvailable: (backend) => backend === "minijail",
      }),
    ).toMatchObject({
      backend: "minijail",
      selection: "auto",
      trustZoneId: "plugin_untrusted",
      requireIsolated: true,
    });

    expect(
      resolvePluginSandboxConfig({
        plan: { command: "worker" },
        env: { OPENCLAW_RUNTIME_PLUGIN_SANDBOX_BACKENDS: "docker,bwrap" } as NodeJS.ProcessEnv,
        backendAvailable: () => true,
      }),
    ).toMatchObject({
      backend: "bwrap",
      selection: "auto",
      candidateBackends: ["docker", "bwrap"],
    });

    expect(
      resolvePluginSandboxConfig({
        plan: { command: "worker" },
        env: {
          OPENCLAW_RUNTIME_PLUGIN_SANDBOX_BACKENDS: "docker,bwrap",
          OPENCLAW_RUNTIME_PLUGIN_DOCKER_IMAGE: "openclaw-plugin-worker:local",
        } as NodeJS.ProcessEnv,
        backendAvailable: () => true,
      }),
    ).toMatchObject({
      backend: "docker",
      dockerImage: "openclaw-plugin-worker:local",
      selection: "auto",
    });
  });

  it("can run an opt-in plugin tool through a subprocess worker", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-plugin-worker-"));
    const workerPath = path.join(root, "worker.mjs");
    fs.writeFileSync(
      workerPath,
      [
        "let input = '';",
        "process.stdin.setEncoding('utf8');",
        "process.stdin.on('data', chunk => { input += chunk; });",
        "process.stdin.on('end', () => {",
        "  const request = JSON.parse(input);",
        "  process.stdout.write(JSON.stringify({",
        "    content: [{ type: 'text', text: `worker:${request.params.value}` }],",
        "    details: { pluginId: request.pluginId, toolName: request.toolName }",
        "  }));",
        "});",
      ].join("\n"),
    );
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    try {
      db.ensureAgent({ id: "main" });
      db.grantCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "tool",
        resourceId: "plugin_tool",
        action: "invoke",
      });
      db.grantCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "sandbox",
        resourceId: "plugin_untrusted",
        action: "allocate",
      });
      const rawTool = makeTool("plugin_tool");
      setPluginToolMeta(rawTool, {
        pluginId: "community-plugin",
        optional: false,
        subprocess: {
          command: process.execPath,
          args: [workerPath],
          timeoutMs: 5_000,
          sandbox: {
            backend: "local/no_isolation",
            requireIsolated: false,
            maxBytesOut: 16_384,
          },
        },
      });
      const broker = new CapabilityToolBroker(db, {
        env: {
          OPENCLAW_RUNTIME_PLUGIN_PROCESS_BOUNDARY: "subprocess",
          OPENCLAW_RUNTIME_PLUGIN_SANDBOX_BACKENDS: "local/no_isolation",
        } as NodeJS.ProcessEnv,
      });
      const tool = broker.wrapTool(rawTool, {
        subject: { subjectType: "agent", subjectId: "main" },
        agentId: "main",
      });
      await expect(tool.execute("call-plugin-worker", { value: "ok" })).resolves.toEqual({
        content: [{ type: "text", text: "worker:ok" }],
        details: { pluginId: "community-plugin", toolName: "plugin_tool" },
      });
      expect(db.listAudit({ limit: 20 }).map((entry) => entry.action)).toEqual(
        expect.arrayContaining([
          "plugin_tool.subprocess_started",
          "plugin_tool.subprocess_finished",
          "sandbox.allocated",
          "sandbox.command_started",
          "sandbox.command_completed",
          "sandbox.released",
          "tool_call.completed",
        ]),
      );
    } finally {
      db.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("fails closed for plugin subprocess workers without an isolated sandbox", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    try {
      db.ensureAgent({ id: "main" });
      db.grantCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "tool",
        resourceId: "plugin_tool",
        action: "invoke",
      });
      const rawTool = makeTool("plugin_tool");
      setPluginToolMeta(rawTool, {
        pluginId: "community-plugin",
        optional: false,
        subprocess: {
          command: process.execPath,
          args: ["-e", "process.stdout.write('{}')"],
        },
      });
      const broker = new CapabilityToolBroker(db, {
        env: { OPENCLAW_RUNTIME_PLUGIN_PROCESS_BOUNDARY: "subprocess" } as NodeJS.ProcessEnv,
      });
      const tool = broker.wrapTool(rawTool, {
        subject: { subjectType: "agent", subjectId: "main" },
        agentId: "main",
      });
      await expect(tool.execute("call-plugin-worker-unsafe", {})).rejects.toThrow(
        /requires an isolated sandbox backend/,
      );
      expect(db.listAudit({ limit: 20 }).map((entry) => entry.action)).toEqual(
        expect.arrayContaining(["plugin_tool.sandbox_required", "tool_call.failed"]),
      );
    } finally {
      db.close();
    }
  });

  it("does not auto-grant sensitive tools in strict capability mode", async () => {
    const run = brokerToolsForRun({
      tools: [makeTool("exec")],
      agentId: "main",
      sessionId: "session-a",
      runId: "run-a",
      dbPath: ":memory:",
      env: { OPENCLAW_RUNTIME_TOOL_CAPABILITY_MODE: "strict" } as NodeJS.ProcessEnv,
    });
    try {
      await expect(run.tools[0]?.execute("call-exec", {})).rejects.toThrow(/denied/);
      const grants = run.db.db
        .prepare("SELECT COUNT(*) AS count FROM capabilities WHERE resource_type = 'tool'")
        .get() as { count: number };
      expect(grants.count).toBe(0);
    } finally {
      await run.close();
    }
  });

  it("can route exec-shaped tools through a brokered sandbox command", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    try {
      db.ensureAgent({ id: "main" });
      db.upsertSession({ id: "session-a", agentId: "main" });
      db.enqueueTask({ id: "task-a", kind: "exec", sessionId: "session-a" });
      db.grantCapability({
        subjectType: "run",
        subjectId: "run-a",
        resourceType: "tool",
        resourceId: "exec",
        action: "invoke",
      });
      db.grantCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "sandbox",
        resourceId: "code_execution",
        action: "allocate",
      });
      const broker = new CapabilityToolBroker(db, {
        env: { OPENCLAW_RUNTIME_TOOL_SANDBOX_EXEC: "1" } as NodeJS.ProcessEnv,
      });
      const tool = broker.wrapTool(
        {
          name: "exec",
          label: "Exec",
          description: "test",
          parameters: Type.Object({}),
          execute: async () => {
            throw new Error("ambient exec should not run");
          },
        } as AnyAgentTool,
        {
          agentId: "main",
          sessionId: "session-a",
          taskId: "task-a",
          subject: { subjectType: "run", subjectId: "run-a" },
          runId: "run-a",
        },
      );

      await expect(
        tool.execute("call-exec", {
          argv: [process.execPath, "-e", "process.stdout.write('sandboxed')"],
          maxOutputBytes: 1024,
        }),
      ).resolves.toMatchObject({
        details: { sparsekernelSandbox: true, exitCode: 0, stdout: "sandboxed" },
      });
      const actions = db.listAudit({ limit: 50 }).map((entry) => entry.action);
      expect(actions).toEqual(
        expect.arrayContaining([
          "sandbox.allocated",
          "sandbox.command_started",
          "sandbox.command_completed",
          "sandbox.released",
          "tool_call.completed",
        ]),
      );
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
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    const broker = new CapabilityToolBroker(db);
    try {
      db.upsertSession({
        id: "session-a",
        agentId: "main",
        status: "active",
      });
      db.grantCapability({
        subjectType: "run",
        subjectId: "run-a",
        resourceType: "tool",
        resourceId: "browser",
        action: "invoke",
      });
      db.grantCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "browser_context",
        resourceId: "public_web",
        action: "allocate",
      });
      const tool = broker.wrapTool(
        {
          name: "browser",
          label: "Browser",
          description: "test",
          parameters: Type.Object({}),
          execute: async () => ({ content: [{ type: "text", text: "ok" }], details: {} }),
        } as AnyAgentTool,
        {
          agentId: "main",
          sessionId: "session-a",
          subject: { subjectType: "run", subjectId: "run-a" },
          runId: "run-a",
        },
      );
      await expect(
        tool.execute("call-browser", {
          action: "status",
        }),
      ).resolves.toMatchObject({ details: {} });
      const context = db.db
        .prepare("SELECT status FROM browser_contexts ORDER BY created_at DESC LIMIT 1")
        .get() as { status: string };
      expect(context.status).toBe("active");
      const lease = db.db
        .prepare(
          "SELECT status FROM resource_leases WHERE resource_type = 'browser_context' LIMIT 1",
        )
        .get() as { status: string };
      expect(lease.status).toBe("active");
      await broker.close();
      const releasedContext = db.db
        .prepare("SELECT status FROM browser_contexts ORDER BY created_at DESC LIMIT 1")
        .get() as { status: string };
      expect(releasedContext.status).toBe("released");
      const releasedLease = db.db
        .prepare(
          "SELECT status FROM resource_leases WHERE resource_type = 'browser_context' LIMIT 1",
        )
        .get() as { status: string };
      expect(releasedLease.status).toBe("released");
    } finally {
      await broker.close();
      db.close();
    }
  });

  it("injects the SparseKernel CDP proxy into browser tool execution", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    const proxyRequest = vi.fn(async () => ({ ok: true, transport: "sparsekernel-cdp" }));
    const proxyInputs: SparseKernelBrowserToolCdpProxyInput[] = [];
    const released: string[] = [];
    const broker = new CapabilityToolBroker(db, {
      env: {
        OPENCLAW_RUNTIME_BROWSER_BROKER: "cdp",
        OPENCLAW_SPARSEKERNEL_BROWSER_CDP_ENDPOINT: "http://127.0.0.1:9222",
      } as NodeJS.ProcessEnv,
      browserCdpProxyFactory: async (input) => {
        proxyInputs.push(input);
        return {
          id: "browser_ctx_cdp",
          proxyRequest,
          release: async () => {
            released.push("browser_ctx_cdp");
          },
        };
      },
    });
    try {
      db.upsertSession({
        id: "session-a",
        agentId: "main",
        status: "active",
      });
      db.enqueueTask({
        id: "task-a",
        agentId: "main",
        sessionId: "session-a",
        kind: "test",
      });
      db.grantCapability({
        subjectType: "run",
        subjectId: "run-a",
        resourceType: "tool",
        resourceId: "browser",
        action: "invoke",
      });
      const tool = broker.wrapTool(
        {
          name: "browser",
          label: "Browser",
          description: "test",
          parameters: Type.Object({}),
          execute: async (_toolCallId, params) => {
            const injected =
              params && typeof params === "object"
                ? (params as Record<PropertyKey, unknown>)[
                    SPARSEKERNEL_BROWSER_PROXY_REQUEST_SYMBOL
                  ]
                : undefined;
            if (typeof injected !== "function") {
              throw new Error("missing SparseKernel browser proxy");
            }
            return {
              content: [{ type: "text", text: "ok" }],
              details: await injected({ method: "GET", path: "/" }),
            };
          },
        } as AnyAgentTool,
        {
          agentId: "main",
          sessionId: "session-a",
          taskId: "task-a",
          subject: { subjectType: "run", subjectId: "run-a" },
          runId: "run-a",
        },
      );

      await expect(
        tool.execute("call-browser", {
          action: "status",
          target: "host",
        }),
      ).resolves.toMatchObject({
        details: { ok: true, transport: "sparsekernel-cdp" },
      });
      await expect(
        tool.execute("call-browser-2", {
          action: "tabs",
          target: "host",
        }),
      ).resolves.toMatchObject({
        details: { ok: true, transport: "sparsekernel-cdp" },
      });

      expect(proxyInputs).toEqual([
        expect.objectContaining({
          agentId: "main",
          sessionId: "session-a",
          taskId: "task-a",
          trustZoneId: "authenticated_web",
          cdpEndpoint: "http://127.0.0.1:9222",
        }),
      ]);
      expect(proxyRequest).toHaveBeenCalledTimes(2);
      await broker.close();
      expect(released).toEqual(["browser_ctx_cdp"]);
    } finally {
      await broker.close();
      db.close();
    }
  });

  it("checks trust-zone network policy before CDP browser navigation", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    const proxyFactory = vi.fn(async () => ({
      id: "browser_ctx_cdp",
      proxyRequest: async () => ({ ok: true }),
      release: () => {},
    }));
    const broker = new CapabilityToolBroker(db, {
      env: {
        OPENCLAW_RUNTIME_BROWSER_BROKER: "cdp",
        OPENCLAW_RUNTIME_BROWSER_POLICY_ENFORCE: "1",
        OPENCLAW_SPARSEKERNEL_BROWSER_CDP_ENDPOINT: "http://127.0.0.1:9222",
      } as NodeJS.ProcessEnv,
      browserCdpProxyFactory: proxyFactory,
    });
    try {
      db.upsertSession({ id: "session-a", agentId: "main", status: "active" });
      db.enqueueTask({ id: "task-a", agentId: "main", sessionId: "session-a", kind: "test" });
      db.grantCapability({
        subjectType: "run",
        subjectId: "run-a",
        resourceType: "tool",
        resourceId: "browser",
        action: "invoke",
      });
      const tool = broker.wrapTool(makeTool("browser"), {
        agentId: "main",
        sessionId: "session-a",
        taskId: "task-a",
        subject: { subjectType: "run", subjectId: "run-a" },
        runId: "run-a",
      });

      await expect(
        tool.execute("call-browser", {
          action: "navigate",
          target: "host",
          url: "http://127.0.0.1/private",
        }),
      ).rejects.toThrow(/network policy/);
      expect(proxyFactory).not.toHaveBeenCalled();
      const call = db.db
        .prepare("SELECT status FROM tool_calls WHERE id = ?")
        .get("run-a:call-browser") as { status: string } | undefined;
      expect(call?.status).toBe("failed");
    } finally {
      await broker.close();
      db.close();
    }
  });

  it("does not inject the CDP proxy when browser broker mode is off", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    const proxyFactory = vi.fn(async () => ({
      id: "browser_ctx_cdp",
      proxyRequest: async () => ({ ok: true }),
      release: () => {},
    }));
    const broker = new CapabilityToolBroker(db, {
      env: {
        OPENCLAW_RUNTIME_BROWSER_BROKER: "off",
        OPENCLAW_SPARSEKERNEL_BROWSER_CDP_ENDPOINT: "http://127.0.0.1:9222",
      } as NodeJS.ProcessEnv,
      browserCdpProxyFactory: proxyFactory,
    });
    try {
      db.upsertSession({
        id: "session-a",
        agentId: "main",
        status: "active",
      });
      db.grantCapability({
        subjectType: "run",
        subjectId: "run-a",
        resourceType: "tool",
        resourceId: "browser",
        action: "invoke",
      });
      db.grantCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "browser_context",
        resourceId: "authenticated_web",
        action: "allocate",
      });
      const tool = broker.wrapTool(
        {
          name: "browser",
          label: "Browser",
          description: "test",
          parameters: Type.Object({}),
          execute: async (_toolCallId, params) => ({
            content: [{ type: "text", text: "ok" }],
            details: {
              hasSparseKernelProxy: Boolean(
                params && typeof params === "object"
                  ? (params as Record<PropertyKey, unknown>)[
                      SPARSEKERNEL_BROWSER_PROXY_REQUEST_SYMBOL
                    ]
                  : undefined,
              ),
            },
          }),
        } as AnyAgentTool,
        {
          agentId: "main",
          sessionId: "session-a",
          subject: { subjectType: "run", subjectId: "run-a" },
          runId: "run-a",
        },
      );

      await expect(
        tool.execute("call-browser", {
          action: "status",
          target: "host",
        }),
      ).resolves.toMatchObject({
        details: { hasSparseKernelProxy: false },
      });
      expect(proxyFactory).not.toHaveBeenCalled();
    } finally {
      await broker.close();
      db.close();
    }
  });

  it("can resolve a managed browser CDP endpoint through browser control", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    const fetchCalls: Array<{ url: string; method?: string }> = [];
    const proxyInputs: SparseKernelBrowserToolCdpProxyInput[] = [];
    const broker = new CapabilityToolBroker(db, {
      env: {
        OPENCLAW_RUNTIME_BROWSER_BROKER: "managed",
        OPENCLAW_SPARSEKERNEL_BROWSER_CONTROL_URL: "http://127.0.0.1:18791",
      } as NodeJS.ProcessEnv,
      browserControlFetch: (async (input, init) => {
        fetchCalls.push({ url: input.toString(), method: init?.method });
        return Response.json({
          running: true,
          cdpReady: true,
          transport: "cdp",
          cdpUrl: "http://127.0.0.1:18800",
        });
      }) as typeof fetch,
      browserCdpProxyFactory: async (input) => {
        proxyInputs.push(input);
        return {
          id: "browser_ctx_managed",
          proxyRequest: async () => ({ ok: true, transport: "sparsekernel-cdp" }),
          release: () => {},
        };
      },
    });
    try {
      db.upsertSession({
        id: "session-a",
        agentId: "main",
        status: "active",
      });
      db.enqueueTask({
        id: "task-a",
        agentId: "main",
        sessionId: "session-a",
        kind: "test",
      });
      db.grantCapability({
        subjectType: "run",
        subjectId: "run-a",
        resourceType: "tool",
        resourceId: "browser",
        action: "invoke",
      });
      const tool = broker.wrapTool(
        {
          name: "browser",
          label: "Browser",
          description: "test",
          parameters: Type.Object({}),
          execute: async (_toolCallId, params) => {
            const injected =
              params && typeof params === "object"
                ? (params as Record<PropertyKey, unknown>)[
                    SPARSEKERNEL_BROWSER_PROXY_REQUEST_SYMBOL
                  ]
                : undefined;
            if (typeof injected !== "function") {
              throw new Error("missing SparseKernel browser proxy");
            }
            return {
              content: [{ type: "text", text: "ok" }],
              details: await injected({ method: "GET", path: "/" }),
            };
          },
        } as AnyAgentTool,
        {
          agentId: "main",
          sessionId: "session-a",
          taskId: "task-a",
          subject: { subjectType: "run", subjectId: "run-a" },
          runId: "run-a",
        },
      );

      await expect(
        tool.execute("call-browser", {
          action: "status",
          profile: "openclaw",
        }),
      ).resolves.toMatchObject({
        details: { ok: true, transport: "sparsekernel-cdp" },
      });

      expect(fetchCalls).toEqual([
        { url: "http://127.0.0.1:18791/start?profile=openclaw", method: "POST" },
        { url: "http://127.0.0.1:18791/?profile=openclaw", method: "GET" },
      ]);
      expect(proxyInputs[0]).toMatchObject({
        cdpEndpoint: "http://127.0.0.1:18800",
        trustZoneId: "authenticated_web",
      });
    } finally {
      await broker.close();
      db.close();
    }
  });

  it("can materialize a native SparseKernel browser process for CDP brokered tools", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    const processRelease = vi.fn(async () => {});
    const proxyRelease = vi.fn(async () => {});
    const nativeAcquire = vi.fn(
      async (input: NativeBrowserProcessAcquireInput): Promise<NativeBrowserProcessLease> => {
        expect(input.trustZoneId).toBe("authenticated_web");
        expect(input.proxyServer).toBe("http://127.0.0.1:18080/");
        return {
          cdpEndpoint: "http://127.0.0.1:19222",
          trustZoneId: input.trustZoneId,
          poolKey: `${input.trustZoneId}:default`,
          userDataDir: "/tmp/openclaw-browser-pool",
          release: processRelease,
        };
      },
    );
    const proxyInputs: SparseKernelBrowserToolCdpProxyInput[] = [];
    const broker = new CapabilityToolBroker(db, {
      env: {
        OPENCLAW_RUNTIME_BROWSER_BROKER: "native",
        OPENCLAW_RUNTIME_BROWSER_REQUIRE_PROXY: "1",
      } as NodeJS.ProcessEnv,
      browserNativeAcquire: nativeAcquire,
      browserCdpProxyFactory: async (input) => {
        proxyInputs.push(input);
        return {
          id: "browser_ctx_native",
          proxyRequest: async () => ({ ok: true, transport: "sparsekernel-cdp" }),
          release: proxyRelease,
        };
      },
    });
    try {
      db.upsertSession({
        id: "session-a",
        agentId: "main",
        status: "active",
      });
      db.enqueueTask({
        id: "task-a",
        agentId: "main",
        sessionId: "session-a",
        kind: "test",
      });
      db.grantCapability({
        subjectType: "run",
        subjectId: "run-a",
        resourceType: "tool",
        resourceId: "browser",
        action: "invoke",
      });
      db.db
        .prepare("UPDATE network_policies SET proxy_ref = ? WHERE id = 'authenticated_web_default'")
        .run("http://127.0.0.1:18080/");
      const tool = broker.wrapTool(
        {
          name: "browser",
          label: "Browser",
          description: "test",
          parameters: Type.Object({}),
          execute: async (_toolCallId, params) => {
            const injected =
              params && typeof params === "object"
                ? (params as Record<PropertyKey, unknown>)[
                    SPARSEKERNEL_BROWSER_PROXY_REQUEST_SYMBOL
                  ]
                : undefined;
            if (typeof injected !== "function") {
              throw new Error("missing SparseKernel browser proxy");
            }
            return {
              content: [{ type: "text", text: "ok" }],
              details: await injected({ method: "GET", path: "/" }),
            };
          },
        } as AnyAgentTool,
        {
          agentId: "main",
          sessionId: "session-a",
          taskId: "task-a",
          subject: { subjectType: "run", subjectId: "run-a" },
          runId: "run-a",
        },
      );

      await expect(
        tool.execute("call-browser", {
          action: "status",
          target: "host",
        }),
      ).resolves.toMatchObject({
        details: { ok: true, transport: "sparsekernel-cdp" },
      });

      expect(nativeAcquire).toHaveBeenCalledTimes(1);
      expect(proxyInputs[0]).toMatchObject({
        cdpEndpoint: "http://127.0.0.1:19222",
        trustZoneId: "authenticated_web",
      });
      await broker.close();
      expect(proxyRelease).toHaveBeenCalledTimes(1);
      expect(processRelease).toHaveBeenCalledTimes(1);
    } finally {
      await broker.close();
      db.close();
    }
  });
});
