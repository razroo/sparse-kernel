import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type {
  SparseKernelArtifact,
  SparseKernelBrowserContext,
  SparseKernelCreateArtifactInput,
} from "../../sparsekernel-client/src/index.js";
import type { CdpTransport, SparseKernelBrowserKernelClient } from "./index.js";
import { normalizeLoopbackCdpEndpoint, SparseKernelCdpBrowserBroker } from "./index.js";

class FakeKernel implements SparseKernelBrowserKernelClient {
  readonly artifactInputs: SparseKernelCreateArtifactInput[] = [];
  readonly releasedContextIds: string[] = [];

  async probeBrowserPool() {
    return {
      endpoint: "http://127.0.0.1:9222",
      reachable: true,
      status_code: 200,
      browser: "Chrome/123.0",
      web_socket_debugger_url: "ws://127.0.0.1/devtools/browser/test",
    };
  }

  async acquireBrowserContext(): Promise<SparseKernelBrowserContext> {
    return {
      id: "browser_ctx_1",
      pool_id: "browser_pool_public_web",
      agent_id: "agent-a",
      task_id: "task-a",
      profile_mode: "ephemeral",
      status: "active",
      created_at: "2026-04-27T00:00:00Z",
    };
  }

  async releaseBrowserContext(contextId: string): Promise<boolean> {
    this.releasedContextIds.push(contextId);
    return true;
  }

  async createArtifact(input: SparseKernelCreateArtifactInput): Promise<SparseKernelArtifact> {
    this.artifactInputs.push(input);
    return {
      id: `artifact_${this.artifactInputs.length}`,
      sha256: `sha_${this.artifactInputs.length}`,
      size_bytes: Buffer.from(input.content_base64 ?? "", "base64").length,
      storage_ref: `sha256/aa/bb/sha_${this.artifactInputs.length}`,
      mime_type: input.mime_type,
      retention_policy: input.retention_policy,
      created_at: "2026-04-27T00:00:00Z",
    };
  }
}

class FakeCdpTransport implements CdpTransport {
  readonly sent: unknown[] = [];
  private readonly messageListeners: Array<(data: string) => void> = [];
  private readonly closeListeners: Array<() => void> = [];
  private readonly errorListeners: Array<(error: Error) => void> = [];
  private downloadPath: string | undefined;

  send(data: string): void {
    const message = JSON.parse(data) as {
      id: number;
      method: string;
      params?: Record<string, unknown>;
      sessionId?: string;
    };
    this.sent.push(message);
    switch (message.method) {
      case "Target.createBrowserContext":
        this.respond(message.id, { browserContextId: "cdp-context-1" });
        break;
      case "Target.createTarget":
        this.respond(message.id, { targetId: "target-1" });
        break;
      case "Target.attachToTarget":
        this.respond(message.id, { sessionId: "session-1" });
        break;
      case "Page.enable":
        this.respond(message.id, {});
        break;
      case "Browser.setDownloadBehavior":
        this.downloadPath =
          typeof message.params?.downloadPath === "string"
            ? message.params.downloadPath
            : undefined;
        this.respond(message.id, {});
        break;
      case "Page.captureScreenshot":
        this.respond(message.id, { data: Buffer.from("pixels").toString("base64") });
        break;
      case "Page.navigate":
        this.respond(message.id, { frameId: "frame-1" });
        this.handleNavigate(String(message.params?.url ?? ""));
        break;
      case "Target.closeTarget":
      case "Target.disposeBrowserContext":
        this.respond(message.id, { success: true });
        break;
      default:
        this.respondError(message.id, `unexpected CDP method: ${message.method}`);
        break;
    }
  }

  close(): void {
    for (const listener of this.closeListeners) {
      listener();
    }
  }

  onMessage(listener: (data: string) => void): void {
    this.messageListeners.push(listener);
  }

  onClose(listener: () => void): void {
    this.closeListeners.push(listener);
  }

  onError(listener: (error: Error) => void): void {
    this.errorListeners.push(listener);
  }

  private respond(id: number, result: Record<string, unknown>): void {
    this.emit({ id, result });
  }

  private respondError(id: number, message: string): void {
    this.emit({ id, error: { message } });
  }

  private emit(message: Record<string, unknown>): void {
    for (const listener of this.messageListeners) {
      listener(JSON.stringify(message));
    }
  }

  private handleNavigate(url: string): void {
    if (url.endsWith("/download")) {
      const downloadPath = this.downloadPath;
      if (!downloadPath) {
        return;
      }
      setTimeout(() => {
        this.emit({
          method: "Browser.downloadWillBegin",
          params: {
            guid: "download-guid",
            suggestedFilename: "report.txt",
            url,
          },
        });
        void mkdir(downloadPath, { recursive: true })
          .then(() => writeFile(join(downloadPath, "download-guid"), "download body"))
          .then(() => {
            this.emit({
              method: "Browser.downloadProgress",
              params: {
                guid: "download-guid",
                state: "completed",
                url,
              },
            });
          });
      }, 0);
      return;
    }
    setTimeout(() => {
      this.emit({
        method: "Page.loadEventFired",
        params: {},
        sessionId: "session-1",
      });
    }, 0);
  }
}

describe("@openclaw/sparsekernel-browser-broker", () => {
  it("normalizes only loopback CDP endpoints", () => {
    expect(normalizeLoopbackCdpEndpoint("http://127.0.0.1:9222/json/version")).toBe(
      "http://127.0.0.1:9222",
    );
    expect(() => normalizeLoopbackCdpEndpoint("https://127.0.0.1:9222")).toThrow("http://");
    expect(() => normalizeLoopbackCdpEndpoint("http://10.0.0.8:9222")).toThrow("loopback");
  });

  it("creates a CDP browser context and stores screenshots as SparseKernel artifacts", async () => {
    const kernel = new FakeKernel();
    const transport = new FakeCdpTransport();
    const broker = new SparseKernelCdpBrowserBroker({
      kernel,
      fetchImpl: async () =>
        Response.json({
          webSocketDebuggerUrl: "ws://127.0.0.1/devtools/browser/test",
        }),
      transportFactory: async () => transport,
    });

    const context = await broker.acquireContext({
      agent_id: "agent-a",
      task_id: "task-a",
      trust_zone_id: "public_web",
      cdp_endpoint: "http://127.0.0.1:9222",
    });
    expect(context.cdp_browser_context_id).toBe("cdp-context-1");
    expect(context.target_id).toBe("target-1");

    const result = await broker.captureScreenshotArtifact(context.ledger_context.id, {
      retention_policy: "session",
      url: "https://example.com/",
    });
    expect(result.artifact_type).toBe("screenshot");
    expect(result.artifact.mime_type).toBe("image/png");
    expect(kernel.artifactInputs[0]).toMatchObject({
      content_base64: Buffer.from("pixels").toString("base64"),
      mime_type: "image/png",
      retention_policy: "session",
    });

    await expect(broker.releaseContext(context.ledger_context.id)).resolves.toBe(true);
    expect(kernel.releasedContextIds).toEqual(["browser_ctx_1"]);
    expect(transport.sent).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ method: "Target.createBrowserContext" }),
        expect.objectContaining({ method: "Target.createTarget" }),
        expect.objectContaining({ method: "Target.disposeBrowserContext" }),
      ]),
    );
  });

  it("stores completed CDP downloads as SparseKernel artifacts", async () => {
    const kernel = new FakeKernel();
    const transport = new FakeCdpTransport();
    const broker = new SparseKernelCdpBrowserBroker({
      kernel,
      fetchImpl: async () =>
        Response.json({
          webSocketDebuggerUrl: "ws://127.0.0.1/devtools/browser/test",
        }),
      transportFactory: async () => transport,
    });

    const context = await broker.acquireContext({
      trust_zone_id: "public_web",
      cdp_endpoint: "http://127.0.0.1:9222",
    });
    const result = await broker.captureDownloadArtifact(context.ledger_context.id, {
      mime_type: "text/plain",
      retention_policy: "durable",
      url: "https://example.com/download",
    });

    expect(result).toMatchObject({
      artifact_type: "download",
      filename: "report.txt",
      source_url: "https://example.com/download",
    });
    expect(kernel.artifactInputs[0]).toMatchObject({
      content_base64: Buffer.from("download body").toString("base64"),
      mime_type: "text/plain",
      retention_policy: "durable",
    });
  });
});
