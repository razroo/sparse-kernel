import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type {
  SparseKernelArtifact,
  SparseKernelBrowserContext,
  SparseKernelCreateArtifactInput,
} from "../../sparsekernel-client/src/index.js";
import type { CdpTransport, SparseKernelBrowserKernelClient } from "./index.js";
import {
  createSparseKernelCdpBrowserBroker,
  normalizeLoopbackCdpEndpoint,
  SparseKernelCdpBrowserBroker,
} from "./index.js";

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
  private currentUrl = "about:blank";

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
        this.respond(message.id, { targetId: "target-1" });
        break;
      case "Target.attachToTarget":
        this.respond(message.id, { sessionId: "session-1" });
        break;
      case "Page.enable":
      case "Runtime.enable":
      case "Network.enable":
      case "Log.enable":
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
        this.handleNavigate(String(message.params?.url ?? ""));
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

  emitConsole(text: string): void {
    this.emit({
      method: "Runtime.consoleAPICalled",
      sessionId: "session-1",
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

  emitNetworkRequest(requestId: string): void {
    this.emit({
      method: "Network.requestWillBeSent",
      sessionId: "session-1",
      params: { requestId },
    });
  }

  emitNetworkFinished(requestId: string): void {
    this.emit({
      method: "Network.loadingFinished",
      sessionId: "session-1",
      params: { requestId },
    });
  }

  private respondRuntimeEvaluate(message: { id: number; params?: Record<string, unknown> }): void {
    const expression = String(message.params?.expression ?? "");
    if (expression.includes("const links =")) {
      this.respond(message.id, {
        result: {
          value: JSON.stringify({
            title: this.currentUrl.includes("example.com") ? "Example" : "",
            url: this.currentUrl,
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
            title: this.currentUrl.includes("example.com") ? "Example" : "",
            url: this.currentUrl,
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

  private emit(message: Record<string, unknown>): void {
    for (const listener of this.messageListeners) {
      listener(JSON.stringify(message));
    }
  }

  private handleNavigate(url: string): void {
    this.currentUrl = url;
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
          type: "log",
          text: "hello from page",
        },
      ],
    });
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
          method: "Runtime.evaluate",
          params: expect.objectContaining({
            expression: expect.stringContaining("body > a:nth-of-type(1)"),
          }),
        }),
        expect.objectContaining({ method: "Input.dispatchKeyEvent" }),
      ]),
    );
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
    const actionExpression = runtimeEvaluations.at(-1)?.params.expression ?? "";
    expect(actionExpression).toContain("waitForActionTarget");
    expect(actionExpression).toContain("const timeoutMs = 1234");
    expect(actionExpression).toContain("document.querySelector(selector)");
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
    const broker = createSparseKernelCdpBrowserBroker({
      baseUrl: "http://127.0.0.1:8765",
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
        if (url.endsWith("/artifacts/create")) {
          return Response.json({
            id: "artifact_client",
            sha256: "sha_client",
            size_bytes: Buffer.from(body.content_base64, "base64").length,
            storage_ref: "sha256/aa/bb/sha_client",
            mime_type: body.mime_type,
            retention_policy: body.retention_policy,
            created_at: "2026-04-27T00:00:00Z",
          });
        }
        if (url.endsWith("/browser/contexts/release")) {
          return Response.json({ released: true });
        }
        return Response.json({ error: "not found" }, { status: 404 });
      },
      transportFactory: async () => transport,
    });

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
        "/artifacts/create",
        "/browser/contexts/release",
      ]),
    );
    expect(calls.find((call) => call.url.endsWith("/artifacts/create"))?.body).toMatchObject({
      content_base64: Buffer.from("pixels").toString("base64"),
      mime_type: "image/png",
    });
  });
});

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
