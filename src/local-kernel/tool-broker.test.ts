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
