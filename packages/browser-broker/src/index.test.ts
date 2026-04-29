import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type {
  SparseKernelArtifact,
  SparseKernelBrowserContext,
  SparseKernelBrowserTarget,
  SparseKernelCreateArtifactInput,
  SparseKernelImportArtifactFileInput,
  SparseKernelRecordBrowserTargetInput,
} from "../../sparsekernel-client/src/index.js";
import type {
  CdpTransport,
  SparseKernelBrowserKernelClient,
  SparseKernelBrowserObservationInput,
} from "./index.js";
import {
  createSparseKernelCdpBrowserBroker,
  normalizeLoopbackCdpEndpoint,
  SparseKernelCdpBrowserBroker,
} from "./index.js";

class FakeKernel implements SparseKernelBrowserKernelClient {
  readonly artifactInputs: SparseKernelCreateArtifactInput[] = [];
  readonly observations: SparseKernelBrowserObservationInput[] = [];
  readonly targets: SparseKernelRecordBrowserTargetInput[] = [];
  readonly closedTargets: Array<{ context_id: string; target_id: string; reason?: string | null }> =
    [];
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

  async recordBrowserObservation(input: SparseKernelBrowserObservationInput): Promise<void> {
    this.observations.push(input);
  }

  async recordBrowserTarget(
    input: SparseKernelRecordBrowserTargetInput,
  ): Promise<SparseKernelBrowserTarget> {
    this.targets.push(input);
    return fakeBrowserTarget(input);
  }

  async closeBrowserTarget(input: {
    context_id: string;
    target_id: string;
    reason?: string | null;
  }): Promise<SparseKernelBrowserTarget> {
    this.closedTargets.push(input);
    return fakeBrowserTarget({ ...input, status: "closed", close_reason: input.reason });
  }
}

function fakeBrowserTarget(input: SparseKernelRecordBrowserTargetInput): SparseKernelBrowserTarget {
  return {
    id: `${input.context_id}:${input.target_id}`,
    context_id: input.context_id,
    target_id: input.target_id,
    opener_target_id: input.opener_target_id,
    url: input.url,
    title: input.title,
    status: input.status ?? "active",
    close_reason: input.close_reason,
    console_count: 0,
    network_count: 0,
    artifact_count: 0,
    created_at: input.created_at ?? "2026-04-27T00:00:00Z",
    updated_at: input.updated_at ?? "2026-04-27T00:00:00Z",
    closed_at: input.closed_at,
  };
}

class FakeCdpTransport implements CdpTransport {
  readonly sent: unknown[] = [];
  private readonly messageListeners: Array<(data: string) => void> = [];
  private readonly closeListeners: Array<() => void> = [];
  private readonly errorListeners: Array<(error: Error) => void> = [];
  private downloadPath: string | undefined;
  private currentUrl = "about:blank";
  private readonly targetUrls = new Map<string, string>();
  private readonly sessionTargets = new Map<string, string>();
  private nextActionNavigationUrl: string | undefined;
  private nextActionNewTarget: { targetId: string; url: string } | undefined;

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
        this.currentUrl = String(message.params?.url ?? this.currentUrl);
        this.targetUrls.set("target-1", this.currentUrl);
        this.respond(message.id, { targetId: "target-1" });
        break;
      case "Target.attachToTarget":
        this.respondAttachToTarget(message);
        break;
      case "Page.enable":
      case "Runtime.enable":
      case "Network.enable":
      case "Fetch.enable":
      case "Fetch.continueRequest":
      case "Fetch.failRequest":
      case "Log.enable":
      case "Target.setDiscoverTargets":
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
      case "Page.printToPDF":
        this.respond(message.id, { data: Buffer.from("pdf body").toString("base64") });
        break;
      case "Page.navigate":
        this.respond(message.id, { frameId: "frame-1" });
        this.handleNavigate(String(message.params?.url ?? ""), message.sessionId);
        break;
      case "Runtime.evaluate":
        this.respondRuntimeEvaluate(message);
        break;
      case "Input.dispatchMouseEvent":
      case "Input.dispatchKeyEvent":
      case "Emulation.setDeviceMetricsOverride":
      case "Page.handleJavaScriptDialog":
      case "DOM.setFileInputFiles":
        this.respond(message.id, {});
        break;
      case "DOM.getDocument":
        this.respond(message.id, { root: { nodeId: 1 } });
        break;
      case "DOM.querySelector":
        this.respond(message.id, { nodeId: 2 });
        break;
      case "Target.closeTarget":
        this.closeTarget(String(message.params?.targetId ?? ""));
        this.respond(message.id, { success: true });
        break;
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

  emitConsole(text: string, sessionId = "session-1"): void {
    this.emit({
      method: "Runtime.consoleAPICalled",
      sessionId,
      params: {
        type: "log",
        args: [{ value: text }],
      },
    });
  }

  emitDialog(): void {
    this.emit({
      method: "Page.javascriptDialogOpening",
      sessionId: "session-1",
      params: {
        message: "Continue?",
      },
    });
  }

  emitNetworkRequest(requestId: string, sessionId = "session-1", url = "https://example.com/api") {
    this.emit({
      method: "Network.requestWillBeSent",
      sessionId,
      params: { requestId, request: { method: "GET", url } },
    });
  }

  emitNetworkFinished(requestId: string, sessionId = "session-1"): void {
    this.emit({
      method: "Network.loadingFinished",
      sessionId,
      params: { requestId },
    });
  }

  emitFetchRequest(requestId: string, url: string, sessionId = "session-1"): void {
    this.emit({
      method: "Fetch.requestPaused",
      sessionId,
      params: { requestId, request: { method: "GET", url } },
    });
  }

  queueActionNavigation(url: string): void {
    this.nextActionNavigationUrl = url;
  }

  queueActionNewTarget(url: string, targetId = "target-popup"): void {
    this.nextActionNewTarget = { targetId, url };
  }

  private respondRuntimeEvaluate(message: {
    id: number;
    params?: Record<string, unknown>;
    sessionId?: string;
  }): void {
    const expression = String(message.params?.expression ?? "");
    const url = this.urlForSession(
      typeof message.sessionId === "string" ? message.sessionId : "session-1",
    );
    if (expression.includes("const links =")) {
      this.respond(message.id, {
        result: {
          value: JSON.stringify({
            title: url.includes("example.com") ? "Example" : "",
            url,
            text: "Example page body",
            links: [
              {
                text: "Example link",
                href: "https://example.com/link",
                selector: "body > a:nth-of-type(1)",
              },
            ],
            buttons: [{ text: "Submit", selector: "body > button:nth-of-type(1)" }],
            inputs: [
              {
                label: "Search",
                name: "q",
                type: "text",
                selector: "body > input:nth-of-type(1)",
              },
            ],
            truncated: false,
          }),
        },
      });
      return;
    }
    if (expression.includes("title: document.title")) {
      this.respond(message.id, {
        result: {
          value: JSON.stringify({
            title: url.includes("example.com") ? "Example" : "",
            url,
          }),
        },
      });
      return;
    }
    if (expression.includes("() => 42")) {
      this.respond(message.id, {
        result: { value: 42 },
      });
      return;
    }
    if (expression.includes("waitForActionTarget")) {
      const navigationUrl = this.nextActionNavigationUrl;
      const newTarget = this.nextActionNewTarget;
      this.nextActionNavigationUrl = undefined;
      this.nextActionNewTarget = undefined;
      this.respond(message.id, {
        result: { value: { ok: true, x: 42, y: 24 } },
      });
      if (navigationUrl) {
        setTimeout(() => this.emitFrameNavigation(navigationUrl, message.sessionId), 0);
      }
      if (newTarget) {
        setTimeout(() => this.emitNewTarget(newTarget.targetId, newTarget.url), 0);
      }
      return;
    }
    this.respond(message.id, {
      result: { value: { ok: true } },
    });
  }

  private respond(id: number, result: Record<string, unknown>): void {
    this.emit({ id, result });
  }

  private respondError(id: number, message: string): void {
    this.emit({ id, error: { message } });
  }

  private respondAttachToTarget(message: { id: number; params?: Record<string, unknown> }): void {
    const targetId = String(message.params?.targetId ?? "target-1");
    const sessionId = targetId === "target-1" ? "session-1" : `session-${targetId}`;
    this.sessionTargets.set(sessionId, targetId);
    this.respond(message.id, { sessionId });
  }

  private emit(message: Record<string, unknown>): void {
    for (const listener of this.messageListeners) {
      listener(JSON.stringify(message));
    }
  }

  private handleNavigate(url: string, sessionId = "session-1"): void {
    this.currentUrl = url;
    const targetId = this.sessionTargets.get(sessionId) ?? "target-1";
    this.targetUrls.set(targetId, url);
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
        sessionId,
      });
    }, 0);
  }

  private emitFrameNavigation(url: string, sessionId = "session-1"): void {
    this.currentUrl = url;
    const targetId = this.sessionTargets.get(sessionId) ?? "target-1";
    this.targetUrls.set(targetId, url);
    this.emit({
      method: "Page.frameNavigated",
      params: {
        frame: {
          id: "frame-1",
          url,
        },
      },
      sessionId,
    });
    setTimeout(() => {
      this.emit({
        method: "Page.loadEventFired",
        params: {},
        sessionId,
      });
    }, 0);
  }

  private emitNewTarget(targetId: string, url: string): void {
    this.targetUrls.set(targetId, url);
    this.emit({
      method: "Target.targetCreated",
      params: {
        targetInfo: {
          targetId,
          type: "page",
          browserContextId: "cdp-context-1",
          url,
        },
      },
    });
  }

  private closeTarget(targetId: string): void {
    this.targetUrls.delete(targetId);
    for (const [sessionId, mappedTargetId] of [...this.sessionTargets.entries()]) {
      if (mappedTargetId === targetId) {
        this.sessionTargets.delete(sessionId);
      }
    }
  }

  private urlForSession(sessionId: string): string {
    const targetId = this.sessionTargets.get(sessionId) ?? "target-1";
    return this.targetUrls.get(targetId) ?? this.currentUrl;
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
    expect(kernel.targets).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ context_id: "browser_ctx_1", target_id: "target-1" }),
        expect.objectContaining({ target_id: "target-1", url: "https://example.com/" }),
      ]),
    );
    expect(kernel.observations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          target_id: "target-1",
          observation_type: "browser_artifact.created",
        }),
      ]),
    );
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

  it("uses staged artifact import when the kernel supports local file transfer", async () => {
    const kernel = new (class extends FakeKernel {
      readonly importedFiles: SparseKernelImportArtifactFileInput[] = [];

      async importArtifactFile(
        input: SparseKernelImportArtifactFileInput,
      ): Promise<SparseKernelArtifact> {
        this.importedFiles.push(input);
        const bytes = await readFile(input.staged_path);
        return {
          id: `imported_${this.importedFiles.length}`,
          sha256: `imported_sha_${this.importedFiles.length}`,
          size_bytes: bytes.length,
          storage_ref: `sha256/aa/bb/imported_${this.importedFiles.length}`,
          mime_type: input.mime_type,
          retention_policy: input.retention_policy,
          created_at: "2026-04-27T00:00:00Z",
        };
      }
    })();
    const transport = new FakeCdpTransport();
    const stageRoot = await mkdtemp(join(tmpdir(), "sparsekernel-browser-stage-"));
    const broker = new SparseKernelCdpBrowserBroker({
      kernel,
      artifactStagingDir: stageRoot,
      fetchImpl: async () =>
        Response.json({
          webSocketDebuggerUrl: "ws://127.0.0.1/devtools/browser/test",
        }),
      transportFactory: async () => transport,
    });
    try {
      const context = await broker.acquireContext({
        trust_zone_id: "public_web",
        cdp_endpoint: "http://127.0.0.1:9222",
      });
      const result = await broker.captureScreenshotArtifact(context.ledger_context.id, {
        retention_policy: "session",
      });

      expect(result.artifact).toMatchObject({
        id: "imported_1",
        mime_type: "image/png",
        size_bytes: 6,
      });
      expect(kernel.importedFiles[0]).toMatchObject({
        mime_type: "image/png",
        retention_policy: "session",
      });
      expect(kernel.artifactInputs).toEqual([]);
    } finally {
      await rm(stageRoot, { recursive: true, force: true });
    }
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

  it("stores printed PDFs as SparseKernel artifacts", async () => {
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
    const result = await broker.capturePdfArtifact(context.ledger_context.id, {
      retention_policy: "debug",
    });

    expect(result.artifact_type).toBe("pdf");
    expect(result.artifact.mime_type).toBe("application/pdf");
    expect(kernel.artifactInputs[0]).toMatchObject({
      content_base64: Buffer.from("pdf body").toString("base64"),
      mime_type: "application/pdf",
      retention_policy: "debug",
    });
    expect(transport.sent).toEqual(
      expect.arrayContaining([expect.objectContaining({ method: "Page.printToPDF" })]),
    );
  });

  it("records console events from the leased CDP context", async () => {
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
    transport.emitConsole("hello from page");

    expect(broker.listConsoleMessages(context.ledger_context.id)).toMatchObject({
      ok: true,
      targetId: "target-1",
      messages: [
        {
          targetId: "target-1",
          type: "log",
          text: "hello from page",
        },
      ],
    });
    expect(kernel.observations).toEqual([
      expect.objectContaining({
        context_id: "browser_ctx_1",
        target_id: "target-1",
        observation_type: "browser_console",
        payload: expect.objectContaining({ text: "hello from page", targetId: "target-1" }),
      }),
    ]);
  });

  it("uploads files and handles dialogs through the leased CDP context", async () => {
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
      initial_url: "https://example.com/",
    });
    await broker.snapshotContext(context.ledger_context.id);

    await expect(
      broker.uploadFiles(context.ledger_context.id, {
        input_ref: "e3",
        paths: ["/tmp/openclaw-browser-uploads/report.txt"],
      }),
    ).resolves.toEqual({ ok: true, targetId: "target-1" });

    const armed = broker.armDialog(context.ledger_context.id, {
      accept: true,
      prompt_text: "ok",
      timeout_ms: 1_000,
    });
    transport.emitDialog();
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(armed).toEqual({ ok: true, targetId: "target-1", armed: true });
    expect(transport.sent).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          method: "DOM.querySelector",
          params: expect.objectContaining({ selector: "body > input:nth-of-type(1)" }),
        }),
        expect.objectContaining({
          method: "DOM.setFileInputFiles",
          params: expect.objectContaining({
            files: ["/tmp/openclaw-browser-uploads/report.txt"],
          }),
        }),
        expect.objectContaining({
          method: "Page.handleJavaScriptDialog",
          params: expect.objectContaining({ accept: true, promptText: "ok" }),
        }),
      ]),
    );
  });

  it("navigates and lists the leased CDP context tab", async () => {
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
    const tab = await broker.navigateContext(context.ledger_context.id, {
      url: "https://example.com/",
    });
    const tabs = await broker.listTabs(context.ledger_context.id);

    expect(tab).toMatchObject({
      targetId: "target-1",
      suggestedTargetId: "target-1",
      title: "Example",
      url: "https://example.com/",
      sparsekernelContextId: "browser_ctx_1",
    });
    expect(tabs).toEqual([tab]);
    expect(transport.sent).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ method: "Page.navigate" }),
        expect.objectContaining({ method: "Runtime.evaluate" }),
      ]),
    );
  });

  it("captures a brokered text snapshot from the leased CDP context", async () => {
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
      initial_url: "https://example.com/",
    });
    const snapshot = await broker.snapshotContext(context.ledger_context.id, {
      format: "ai",
      max_chars: 1000,
      interactive: true,
    });

    expect(snapshot).toMatchObject({
      ok: true,
      format: "ai",
      targetId: "target-1",
      url: "https://example.com/",
      title: "Example",
      truncated: false,
    });
    expect(snapshot.snapshot).toContain("Example page body");
    expect(snapshot.refs).toMatchObject({
      e1: { role: "link", name: "Example link", selector: "body > a:nth-of-type(1)" },
      e2: { role: "button", name: "Submit", selector: "body > button:nth-of-type(1)" },
    });
    expect(snapshot.stats).toMatchObject({
      linkCount: 1,
      buttonCount: 1,
      inputCount: 1,
    });
  });

  it("performs basic actions against refs from the brokered snapshot", async () => {
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
      initial_url: "https://example.com/",
    });
    await broker.snapshotContext(context.ledger_context.id);
    const clicked = await broker.actContext(context.ledger_context.id, {
      kind: "click",
      ref: "e1",
      modifiers: ["Shift"],
    });
    const pressed = await broker.actContext(context.ledger_context.id, {
      kind: "press",
      key: "Enter",
    });

    expect(clicked).toMatchObject({ ok: true, targetId: "target-1", kind: "click" });
    expect(pressed).toMatchObject({ ok: true, targetId: "target-1", kind: "press" });
    expect(transport.sent).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          method: "Input.dispatchMouseEvent",
          params: expect.objectContaining({ type: "mousePressed", modifiers: 8 }),
        }),
        expect.objectContaining({ method: "Input.dispatchKeyEvent" }),
      ]),
    );
  });

  it("accepts same-origin post-action navigation within the context policy", async () => {
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
      initial_url: "https://example.com/start",
      allowed_origins: ["https://example.com"],
    });
    transport.queueActionNavigation("https://example.com/next");

    await expect(
      broker.actContext(context.ledger_context.id, {
        kind: "click",
        selector: "#continue",
      }),
    ).resolves.toMatchObject({ ok: true, kind: "click" });
    await expect(broker.listTabs(context.ledger_context.id)).resolves.toEqual([
      expect.objectContaining({ url: "https://example.com/next" }),
    ]);
  });

  it("releases a context when post-action navigation leaves the allowed origins", async () => {
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
      initial_url: "https://example.com/start",
      allowed_origins: ["https://example.com"],
    });
    transport.queueActionNavigation("https://blocked.example/next");

    await expect(
      broker.actContext(context.ledger_context.id, {
        kind: "click",
        selector: "#escape",
      }),
    ).rejects.toThrow(/post-action navigation blocked by allowed origins/);
    await expect(broker.listTabs(context.ledger_context.id)).rejects.toThrow(/not materialized/);
    expect(kernel.releasedContextIds).toEqual(["browser_ctx_1"]);
  });

  it("attaches same-policy tabs opened by brokered actions", async () => {
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
      initial_url: "https://example.com/start",
      allowed_origins: ["https://example.com"],
    });
    transport.queueActionNewTarget("https://example.com/popup", "target-popup");

    await expect(
      broker.actContext(context.ledger_context.id, {
        kind: "click",
        selector: "#popup",
      }),
    ).resolves.toMatchObject({ ok: true, kind: "click" });
    await expect(broker.listTabs(context.ledger_context.id)).resolves.toEqual([
      expect.objectContaining({ targetId: "target-1" }),
      expect.objectContaining({ targetId: "target-popup" }),
    ]);
    expect(kernel.observations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          target_id: "target-popup",
          observation_type: "browser_target.attached",
        }),
      ]),
    );
  });

  it("keeps console and network observations scoped to their CDP targets", async () => {
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
      initial_url: "https://example.com/start",
      allowed_origins: ["https://example.com"],
    });
    transport.emitConsole("primary console");
    transport.emitNetworkRequest("req-primary", "session-1", "https://example.com/api");
    transport.emitNetworkFinished("req-primary");
    transport.queueActionNewTarget("https://example.com/popup", "target-popup");
    await broker.actContext(context.ledger_context.id, {
      kind: "click",
      selector: "#popup",
    });
    transport.emitConsole("popup console", "session-target-popup");
    transport.emitConsole("orphan console", "session-detached");
    transport.emitNetworkRequest("req-orphan", "session-detached", "https://example.com/orphan");

    expect(broker.listConsoleMessages(context.ledger_context.id)).toMatchObject({
      targetId: "target-popup",
      messages: [expect.objectContaining({ targetId: "target-popup", text: "popup console" })],
    });
    await broker.focusContext(context.ledger_context.id, { target_id: "target-1" });
    expect(broker.listConsoleMessages(context.ledger_context.id)).toMatchObject({
      targetId: "target-1",
      messages: [expect.objectContaining({ targetId: "target-1", text: "primary console" })],
    });
    expect(kernel.observations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          target_id: "target-1",
          observation_type: "browser_network.request",
          payload: expect.objectContaining({ requestId: "req-primary" }),
        }),
        expect.objectContaining({
          target_id: "target-1",
          observation_type: "browser_network.finished",
          payload: expect.objectContaining({ requestId: "req-primary" }),
        }),
        expect.objectContaining({
          target_id: "target-popup",
          observation_type: "browser_console",
          payload: expect.objectContaining({ text: "popup console" }),
        }),
      ]),
    );
    expect(kernel.observations).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          payload: expect.objectContaining({ requestId: "req-orphan" }),
        }),
      ]),
    );
  });

  it("blocks target-scoped CDP requests outside the allowed origin policy", async () => {
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
      initial_url: "https://example.com/start",
      allowed_origins: ["https://example.com"],
    });
    transport.emitFetchRequest("fetch-allowed", "https://example.com/style.css");
    transport.emitFetchRequest("fetch-denied", "https://tracker.example.net/pixel");
    await delay(0);

    expect(transport.sent).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ method: "Fetch.enable" }),
        expect.objectContaining({
          method: "Fetch.continueRequest",
          params: expect.objectContaining({ requestId: "fetch-allowed" }),
        }),
        expect.objectContaining({
          method: "Fetch.failRequest",
          params: expect.objectContaining({ requestId: "fetch-denied" }),
        }),
      ]),
    );
    expect(kernel.observations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          context_id: context.ledger_context.id,
          target_id: "target-1",
          observation_type: "browser_network.blocked",
          payload: expect.objectContaining({ requestId: "fetch-denied" }),
        }),
      ]),
    );
  });

  it("closes one broker-owned tab without releasing the whole context", async () => {
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
      initial_url: "https://example.com/start",
      allowed_origins: ["https://example.com"],
    });
    transport.queueActionNewTarget("https://example.com/popup", "target-popup");
    await broker.actContext(context.ledger_context.id, {
      kind: "click",
      selector: "#popup",
    });

    await expect(
      broker.closeTarget(context.ledger_context.id, { target_id: "target-popup" }),
    ).resolves.toMatchObject({
      ok: true,
      targetId: "target-popup",
      releasedContext: false,
      activeTargetId: "target-1",
    });
    expect(kernel.releasedContextIds).toEqual([]);
    expect(kernel.closedTargets).toEqual([
      expect.objectContaining({ context_id: "browser_ctx_1", target_id: "target-popup" }),
    ]);
    await expect(broker.listTabs(context.ledger_context.id)).resolves.toEqual([
      expect.objectContaining({ targetId: "target-1" }),
    ]);
    await expect(
      broker.closeTarget(context.ledger_context.id, { target_id: "target-1" }),
    ).resolves.toMatchObject({
      ok: true,
      targetId: "target-1",
      releasedContext: true,
    });
    expect(kernel.releasedContextIds).toEqual(["browser_ctx_1"]);
  });

  it("closes new tabs that violate the context policy", async () => {
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
      initial_url: "https://example.com/start",
      allowed_origins: ["https://example.com"],
    });
    transport.queueActionNewTarget("https://blocked.example/popup", "target-popup");

    await expect(
      broker.actContext(context.ledger_context.id, {
        kind: "click",
        selector: "#popup",
      }),
    ).rejects.toThrow(/popup navigation blocked by allowed origins/);
    expect(transport.sent).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          method: "Target.closeTarget",
          params: expect.objectContaining({ targetId: "target-popup" }),
        }),
      ]),
    );
    await expect(broker.listTabs(context.ledger_context.id)).resolves.toEqual([
      expect.objectContaining({ targetId: "target-1" }),
    ]);
  });

  it("performs the broader brokered action contract against the leased context", async () => {
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
      initial_url: "https://example.com/",
    });
    await broker.snapshotContext(context.ledger_context.id);

    await broker.actContext(context.ledger_context.id, {
      kind: "clickCoords",
      x: 10,
      y: 20,
      doubleClick: true,
    });
    await broker.actContext(context.ledger_context.id, {
      kind: "scrollIntoView",
      ref: "e1",
    });
    await broker.actContext(context.ledger_context.id, {
      kind: "drag",
      startRef: "e1",
      endRef: "e2",
    });
    await broker.actContext(context.ledger_context.id, {
      kind: "select",
      ref: "e3",
      values: ["choice"],
    });
    await broker.actContext(context.ledger_context.id, {
      kind: "fill",
      fields: [{ ref: "e3", type: "text", value: "filled" }],
    });
    const evaluated = await broker.actContext(context.ledger_context.id, {
      kind: "evaluate",
      fn: "() => 42",
    });
    const batch = await broker.actContext(context.ledger_context.id, {
      kind: "batch",
      actions: [
        { kind: "press", key: "Enter" },
        { kind: "wait", selector: "body", url: "example.com", loadState: "load" },
      ],
    });

    expect(evaluated.value).toBe(42);
    expect(batch.value).toMatchObject({
      results: [
        { ok: true, kind: "press" },
        { ok: true, kind: "wait" },
      ],
    });
    expect(transport.sent).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ method: "Input.dispatchMouseEvent" }),
        expect.objectContaining({
          method: "Runtime.evaluate",
          params: expect.objectContaining({
            expression: expect.stringContaining("scrollIntoView"),
          }),
        }),
        expect.objectContaining({
          method: "Runtime.evaluate",
          params: expect.objectContaining({
            expression: expect.stringContaining("DataTransfer"),
          }),
        }),
        expect.objectContaining({
          method: "Runtime.evaluate",
          params: expect.objectContaining({
            expression: expect.stringContaining("filled"),
          }),
        }),
      ]),
    );
  });

  it("retries selector-backed actions inside the leased page context", async () => {
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
      initial_url: "https://example.com/",
    });
    await broker.actContext(context.ledger_context.id, {
      kind: "click",
      selector: "#eventual",
      timeoutMs: 1_234,
    });

    const runtimeEvaluations = transport.sent.filter(
      (message): message is { method: string; params: { expression: string } } =>
        typeof message === "object" &&
        message !== null &&
        (message as { method?: unknown }).method === "Runtime.evaluate" &&
        typeof (message as { params?: { expression?: unknown } }).params?.expression === "string",
    );
    const actionExpression =
      runtimeEvaluations.find((message) =>
        message.params.expression.includes("waitForActionTarget"),
      )?.params.expression ?? "";
    expect(actionExpression).toContain("waitForActionTarget");
    expect(actionExpression).toContain("const timeoutMs = 1234");
    expect(actionExpression).toContain("document.querySelector(selector)");
    expect(actionExpression).toContain("getBoundingClientRect");
    expect(actionExpression).toContain("aria-disabled");
    expect(actionExpression).toContain("elementFromPoint");
  });

  it("waits for CDP network idle instead of treating document load as enough", async () => {
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
      initial_url: "https://example.com/",
    });
    transport.emitNetworkRequest("req-1");
    const wait = broker.actContext(context.ledger_context.id, {
      kind: "wait",
      loadState: "networkidle",
      timeoutMs: 2_500,
    });

    await expect(
      Promise.race([wait.then(() => "resolved"), delay(150).then(() => "pending")]),
    ).resolves.toBe("pending");

    transport.emitNetworkFinished("req-1");
    await expect(wait).resolves.toMatchObject({
      ok: true,
      kind: "wait",
      value: { ok: true },
    });
  });

  it("constructs from the SparseKernel daemon client", async () => {
    const calls: Array<{ url: string; body?: unknown }> = [];
    const transport = new FakeCdpTransport();
    const stageRoot = await mkdtemp(join(tmpdir(), "sparsekernel-browser-client-stage-"));
    const broker = createSparseKernelCdpBrowserBroker({
      baseUrl: "http://127.0.0.1:8765",
      artifactStagingDir: stageRoot,
      fetchImpl: async (input, init) => {
        const url = input.toString();
        const body =
          typeof init?.body === "string" ? (JSON.parse(init.body) as Record<string, string>) : {};
        calls.push({ url, body });
        if (url.endsWith("/browser/pools/probe")) {
          return Response.json({
            endpoint: body.cdp_endpoint,
            reachable: true,
            status_code: 200,
          });
        }
        if (url.endsWith("/browser/contexts/acquire")) {
          return Response.json({
            id: "browser_ctx_client",
            pool_id: "browser_pool_public_web",
            profile_mode: "ephemeral",
            status: "active",
            created_at: "2026-04-27T00:00:00Z",
          });
        }
        if (url.endsWith("/json/version")) {
          return Response.json({
            webSocketDebuggerUrl: "ws://127.0.0.1/devtools/browser/test",
          });
        }
        if (url.endsWith("/artifacts/import-file")) {
          return Response.json({
            id: "artifact_client",
            sha256: "sha_client",
            size_bytes: 6,
            storage_ref: "sha256/aa/bb/sha_client",
            mime_type: body.mime_type,
            retention_policy: body.retention_policy,
            created_at: "2026-04-27T00:00:00Z",
          });
        }
        if (url.endsWith("/browser/contexts/observe")) {
          return Response.json({ ok: true });
        }
        if (url.endsWith("/browser/targets/record") || url.endsWith("/browser/targets/close")) {
          return Response.json({
            id: `${body.context_id}:${body.target_id}`,
            context_id: body.context_id,
            target_id: body.target_id,
            status: body.status ?? "active",
            console_count: 0,
            network_count: 0,
            artifact_count: 0,
            created_at: "2026-04-27T00:00:00Z",
            updated_at: "2026-04-27T00:00:00Z",
          });
        }
        if (url.endsWith("/browser/contexts/release")) {
          return Response.json({ released: true });
        }
        return Response.json({ error: "not found" }, { status: 404 });
      },
      transportFactory: async () => transport,
    });

    try {
      const context = await broker.acquireContext({
        trust_zone_id: "public_web",
        cdp_endpoint: "http://127.0.0.1:9222",
      });
      const screenshot = await broker.captureScreenshotArtifact(context.ledger_context.id);
      await expect(broker.releaseContext(context.ledger_context.id)).resolves.toBe(true);

      expect(screenshot.artifact.id).toBe("artifact_client");
      expect(calls.map((call) => new URL(call.url).pathname)).toEqual(
        expect.arrayContaining([
          "/browser/pools/probe",
          "/browser/contexts/acquire",
          "/json/version",
          "/artifacts/import-file",
          "/browser/contexts/observe",
          "/browser/targets/record",
          "/browser/targets/close",
          "/browser/contexts/release",
        ]),
      );
      expect(
        calls.find((call) => call.url.endsWith("/browser/contexts/observe"))?.body,
      ).toMatchObject({
        context_id: "browser_ctx_client",
        target_id: "target-1",
        observation_type: "browser_artifact.created",
      });
      expect(calls.find((call) => call.url.endsWith("/artifacts/import-file"))?.body).toMatchObject(
        {
          staged_path: expect.stringContaining("browser-artifact-"),
          mime_type: "image/png",
        },
      );
    } finally {
      await rm(stageRoot, { recursive: true, force: true });
    }
  });
});

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
