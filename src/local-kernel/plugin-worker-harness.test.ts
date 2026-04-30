import { describe, expect, it } from "vitest";
import { createSparseKernelPluginWorkerHandler } from "./plugin-worker-harness.js";

describe("SparseKernel plugin worker harness", () => {
  it("dispatches v1 worker requests to registered tools", async () => {
    const handler = createSparseKernelPluginWorkerHandler([
      {
        pluginId: "demo",
        name: "echo",
        execute: async ({ toolCallId, params }) => ({
          content: [{ type: "text", text: `${toolCallId}:${JSON.stringify(params)}` }],
          details: { worker: true },
        }),
      },
    ]);

    await expect(
      handler({
        protocol: "openclaw.sparsekernel.plugin_tool.v1",
        pluginId: "demo",
        toolName: "echo",
        toolCallId: "call-a",
        params: { value: "ok" },
      }),
    ).resolves.toMatchObject({
      content: [{ type: "text", text: 'call-a:{"value":"ok"}' }],
      details: { worker: true },
    });
  });

  it("fails closed for unsupported protocols or missing tools", async () => {
    const handler = createSparseKernelPluginWorkerHandler([]);
    await expect(
      handler({
        protocol: "openclaw.sparsekernel.plugin_tool.v1",
        pluginId: "demo",
        toolName: "missing",
        toolCallId: "call-a",
      }),
    ).rejects.toThrow(/no tool/);
    await expect(
      handler({
        protocol: "unsupported" as "openclaw.sparsekernel.plugin_tool.v1",
        pluginId: "demo",
        toolName: "missing",
        toolCallId: "call-a",
      }),
    ).rejects.toThrow(/protocol/);
  });
});
