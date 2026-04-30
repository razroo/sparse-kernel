import type { Readable, Writable } from "node:stream";
import type { AgentToolResult } from "@mariozechner/pi-agent-core";

export type SparseKernelPluginWorkerRequest = {
  protocol: "openclaw.sparsekernel.plugin_tool.v1";
  pluginId: string;
  toolName: string;
  toolCallId: string;
  params?: unknown;
  context?: unknown;
};

export type SparseKernelPluginWorkerTool = {
  pluginId?: string;
  name: string;
  execute: (input: {
    toolCallId: string;
    params: unknown;
    context: unknown;
  }) => Promise<AgentToolResult<unknown>> | AgentToolResult<unknown>;
};

export type SparseKernelPluginWorkerStreams = {
  stdin?: Readable;
  stdout?: Writable;
  stderr?: Writable;
};

export function createSparseKernelPluginWorkerHandler(
  tools: SparseKernelPluginWorkerTool[],
): (request: SparseKernelPluginWorkerRequest) => Promise<AgentToolResult<unknown>> {
  const registry = new Map<string, SparseKernelPluginWorkerTool>();
  for (const tool of tools) {
    registry.set(pluginWorkerToolKey(tool.pluginId, tool.name), tool);
    if (!tool.pluginId) {
      registry.set(pluginWorkerToolKey(undefined, tool.name), tool);
    }
  }
  return async (request) => {
    assertPluginWorkerRequest(request);
    const tool =
      registry.get(pluginWorkerToolKey(request.pluginId, request.toolName)) ??
      registry.get(pluginWorkerToolKey(undefined, request.toolName));
    if (!tool) {
      throw new Error(
        `SparseKernel plugin worker has no tool named ${request.pluginId}:${request.toolName}`,
      );
    }
    return await tool.execute({
      toolCallId: request.toolCallId,
      params: request.params,
      context: request.context,
    });
  };
}

export async function runSparseKernelPluginWorker(
  tools: SparseKernelPluginWorkerTool[],
  streams: SparseKernelPluginWorkerStreams = {},
): Promise<void> {
  const stdin = streams.stdin ?? process.stdin;
  const stdout = streams.stdout ?? process.stdout;
  const stderr = streams.stderr ?? process.stderr;
  try {
    const request = parsePluginWorkerRequest(await readAll(stdin));
    const result = await createSparseKernelPluginWorkerHandler(tools)(request);
    stdout.write(`${JSON.stringify(result)}\n`);
  } catch (error) {
    stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}

function pluginWorkerToolKey(pluginId: string | undefined, toolName: string): string {
  return `${pluginId?.trim() || "*"}:${toolName.trim()}`;
}

function parsePluginWorkerRequest(raw: string): SparseKernelPluginWorkerRequest {
  const parsed = JSON.parse(raw) as unknown;
  assertPluginWorkerRequest(parsed);
  return parsed;
}

function assertPluginWorkerRequest(
  value: unknown,
): asserts value is SparseKernelPluginWorkerRequest {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("SparseKernel plugin worker request must be an object.");
  }
  const record = value as Partial<Record<keyof SparseKernelPluginWorkerRequest, unknown>>;
  if (record.protocol !== "openclaw.sparsekernel.plugin_tool.v1") {
    throw new Error("SparseKernel plugin worker request protocol is not supported.");
  }
  for (const key of ["pluginId", "toolName", "toolCallId"] as const) {
    if (typeof record[key] !== "string" || !record[key].trim()) {
      throw new Error(`SparseKernel plugin worker request is missing ${key}.`);
    }
  }
}

async function readAll(stream: Readable): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}
