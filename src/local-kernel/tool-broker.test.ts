import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Type } from "typebox";
import { describe, expect, it } from "vitest";
import type { AnyAgentTool } from "../agents/tools/common.js";
import { brokerToolsForRun, CapabilityToolBroker, LocalKernelDatabase } from "./index.js";

function makeTool(): AnyAgentTool {
  return {
    name: "sensitive_tool",
    label: "Sensitive Tool",
    description: "test",
    parameters: Type.Object({}),
    execute: async () => ({ content: [{ type: "text", text: "ok" }], details: {} }),
  } as AnyAgentTool;
}

describe("CapabilityToolBroker", () => {
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
