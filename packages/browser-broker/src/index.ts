import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import WebSocket, { type RawData } from "ws";
import {
  SparseKernelClient,
  type SparseKernelClientOptions,
  type SparseKernelAcquireBrowserContextInput,
  type SparseKernelArtifact,
  type SparseKernelArtifactSubject,
  type SparseKernelBrowserContext,
  type SparseKernelBrowserEndpointProbe,
  type SparseKernelCreateArtifactInput,
} from "../../sparsekernel-client/src/index.js";

export type {
  SparseKernelAcquireBrowserContextInput,
  SparseKernelArtifact,
  SparseKernelArtifactSubject,
  SparseKernelBrowserContext,
  SparseKernelBrowserEndpointProbe,
  SparseKernelCreateArtifactInput,
} from "../../sparsekernel-client/src/index.js";

type JsonRecord = Record<string, unknown>;

export type SparseKernelBrowserKernelClient = {
  probeBrowserPool(input: { cdp_endpoint: string }): Promise<SparseKernelBrowserEndpointProbe>;
  acquireBrowserContext(
    input: SparseKernelAcquireBrowserContextInput,
  ): Promise<SparseKernelBrowserContext>;
  releaseBrowserContext(contextId: string): Promise<boolean>;
  createArtifact(input: SparseKernelCreateArtifactInput): Promise<SparseKernelArtifact>;
};

export type CdpTransport = {
  send(data: string): void;
  close(): void;
  onMessage(listener: (data: string) => void): void;
  onClose(listener: () => void): void;
  onError(listener: (error: Error) => void): void;
};

export type CdpTransportFactory = (webSocketUrl: string) => Promise<CdpTransport>;

export type SparseKernelBrowserBrokerOptions = {
  kernel: SparseKernelBrowserKernelClient;
  fetchImpl?: typeof fetch;
  transportFactory?: CdpTransportFactory;
};

export type SparseKernelCdpBrowserBrokerFactoryOptions = SparseKernelClientOptions & {
  transportFactory?: CdpTransportFactory;
};

export type AcquireMaterializedBrowserContextInput = {
  agent_id?: string | null;
  session_id?: string | null;
  task_id?: string | null;
  trust_zone_id: string;
  cdp_endpoint: string;
  initial_url?: string;
  max_contexts?: number;
  download_dir?: string;
};

export type MaterializedBrowserContext = {
  ledger_context: SparseKernelBrowserContext;
  cdp_endpoint: string;
  cdp_browser_context_id: string;
  target_id: string;
};

export type CaptureScreenshotArtifactInput = {
  url?: string;
  format?: "png" | "jpeg";
  full_page?: boolean;
  retention_policy?: "ephemeral" | "session" | "durable" | "debug" | string;
  subject?: SparseKernelArtifactSubject;
  timeout_ms?: number;
};

export type CaptureDownloadArtifactInput = {
  url: string;
  mime_type?: string;
  retention_policy?: "ephemeral" | "session" | "durable" | "debug" | string;
  subject?: SparseKernelArtifactSubject;
  timeout_ms?: number;
};

export type BrowserArtifactResult = {
  context_id: string;
  artifact: SparseKernelArtifact;
  artifact_type: "screenshot" | "download" | "pdf";
  filename?: string;
  source_url?: string;
};

export type SparseKernelBrowserConsoleMessage = {
  type: string;
  text: string;
  timestamp: string;
  level?: string;
};

export type SparseKernelBrowserDialogInput = {
  accept: boolean;
  prompt_text?: string;
  timeout_ms?: number;
};

export type SparseKernelBrowserUploadInput = {
  paths: string[];
  ref?: string;
  input_ref?: string;
  selector?: string;
  target_id?: string;
  timeout_ms?: number;
};

export type SparseKernelBrowserSnapshotInput = {
  format?: "ai" | "aria";
  max_chars?: number;
  limit?: number;
  selector?: string;
  interactive?: boolean;
  compact?: boolean;
  timeout_ms?: number;
};

export type SparseKernelBrowserSnapshotResult = {
  ok: true;
  format: "ai" | "aria";
  targetId: string;
  url?: string;
  title?: string;
  snapshot?: string;
  nodes: Array<Record<string, unknown>>;
  refs?: Record<string, Record<string, unknown>>;
  truncated?: boolean;
  stats: Record<string, number>;
};

export type SparseKernelBrowserActRequest =
  | {
      kind: "click";
      ref?: string;
      selector?: string;
      targetId?: string;
      doubleClick?: boolean;
      timeoutMs?: number;
    }
  | {
      kind: "type";
      ref?: string;
      selector?: string;
      text: string;
      targetId?: string;
      submit?: boolean;
      timeoutMs?: number;
    }
  | { kind: "press"; key: string; targetId?: string; delayMs?: number }
  | { kind: "hover"; ref?: string; selector?: string; targetId?: string; timeoutMs?: number }
  | { kind: "wait"; timeMs?: number; text?: string; textGone?: string; timeoutMs?: number }
  | { kind: "resize"; width: number; height: number; targetId?: string }
  | { kind: "close"; targetId?: string };

export type SparseKernelBrowserActResult = {
  ok: true;
  targetId: string;
  kind: string;
  value?: unknown;
};

export type SparseKernelBrowserTab = {
  targetId: string;
  suggestedTargetId: string;
  title?: string;
  url?: string;
  type: "page";
  sparsekernelContextId: string;
};

export function createSparseKernelCdpBrowserBroker(
  options: SparseKernelCdpBrowserBrokerFactoryOptions = {},
): SparseKernelCdpBrowserBroker {
  const client = new SparseKernelClient(options);
  return new SparseKernelCdpBrowserBroker({
    kernel: client,
    fetchImpl: options.fetchImpl,
    transportFactory: options.transportFactory,
  });
}

type CdpResponseWaiter = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout: NodeJS.Timeout;
};

type CdpEventWaiter = {
  method: string;
  predicate: (message: CdpEventMessage) => boolean;
  resolve: (message: CdpEventMessage) => void;
  reject: (error: Error) => void;
  timeout: NodeJS.Timeout;
};

type CdpEventMessage = {
  method: string;
  params: JsonRecord;
  sessionId?: string;
};

type LiveBrowserContext = MaterializedBrowserContext & {
  connection: CdpConnection;
  page_session_id: string;
  download_dir: string;
  owns_download_dir: boolean;
  snapshot_refs: Map<string, SnapshotRef>;
  console_messages: SparseKernelBrowserConsoleMessage[];
};

export class SparseKernelCdpBrowserBroker {
  private readonly kernel: SparseKernelBrowserKernelClient;
  private readonly fetchImpl: typeof fetch;
  private readonly transportFactory: CdpTransportFactory;
  private readonly contexts = new Map<string, LiveBrowserContext>();

  constructor(options: SparseKernelBrowserBrokerOptions) {
    this.kernel = options.kernel;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.transportFactory = options.transportFactory ?? createWebSocketTransport;
  }

  async acquireContext(
    input: AcquireMaterializedBrowserContextInput,
  ): Promise<MaterializedBrowserContext> {
    const cdpEndpoint = normalizeLoopbackCdpEndpoint(input.cdp_endpoint);
    const probe = await this.kernel.probeBrowserPool({ cdp_endpoint: cdpEndpoint });
    if (!probe.reachable) {
      throw new Error(`CDP endpoint is not reachable: ${probe.error ?? cdpEndpoint}`);
    }

    const ledgerContext = await this.kernel.acquireBrowserContext({
      agent_id: input.agent_id,
      session_id: input.session_id,
      task_id: input.task_id,
      trust_zone_id: input.trust_zone_id,
      max_contexts: input.max_contexts,
      cdp_endpoint: cdpEndpoint,
    });

    let connection: CdpConnection | undefined;
    let downloadDir: string | undefined;
    let ownsDownloadDir = false;
    try {
      const webSocketUrl = await discoverBrowserWebSocketUrl(cdpEndpoint, this.fetchImpl);
      connection = await CdpConnection.connect(webSocketUrl, this.transportFactory);
      const { browserContextId } = await connection.command<{ browserContextId: string }>(
        "Target.createBrowserContext",
      );
      const { targetId } = await connection.command<{ targetId: string }>("Target.createTarget", {
        browserContextId,
        url: input.initial_url ?? "about:blank",
      });
      const { sessionId } = await connection.command<{ sessionId: string }>(
        "Target.attachToTarget",
        {
          flatten: true,
          targetId,
        },
      );
      await connection.command("Page.enable", {}, sessionId);
      await connection.command("Runtime.enable", {}, sessionId);
      await connection.command("Log.enable", {}, sessionId).catch(() => {});
      if (input.download_dir) {
        downloadDir = input.download_dir;
      } else {
        downloadDir = await mkdtemp(join(tmpdir(), "sparsekernel-browser-"));
        ownsDownloadDir = true;
      }
      await connection.command("Browser.setDownloadBehavior", {
        behavior: "allowAndName",
        browserContextId,
        downloadPath: downloadDir,
        eventsEnabled: true,
      });

      const materialized: LiveBrowserContext = {
        ledger_context: ledgerContext,
        cdp_endpoint: cdpEndpoint,
        cdp_browser_context_id: browserContextId,
        target_id: targetId,
        connection,
        page_session_id: sessionId,
        download_dir: downloadDir,
        owns_download_dir: ownsDownloadDir,
        snapshot_refs: new Map(),
        console_messages: [],
      };
      connection.onEvent((event) => recordConsoleEvent(materialized, event));
      this.contexts.set(ledgerContext.id, materialized);
      return publicContext(materialized);
    } catch (error) {
      connection?.close();
      if (ownsDownloadDir && downloadDir) {
        await rm(downloadDir, { force: true, recursive: true });
      }
      await this.kernel.releaseBrowserContext(ledgerContext.id).catch(() => false);
      throw error;
    }
  }

  async captureScreenshotArtifact(
    contextId: string,
    input: CaptureScreenshotArtifactInput = {},
  ): Promise<BrowserArtifactResult> {
    const context = this.requireContext(contextId);
    if (input.url) {
      await this.navigate(context, input.url, input.timeout_ms);
    }
    const format = input.format ?? "png";
    const screenshot = await context.connection.command<{ data: string }>(
      "Page.captureScreenshot",
      {
        captureBeyondViewport: input.full_page ?? false,
        format,
      },
      context.page_session_id,
    );
    const artifact = await this.kernel.createArtifact({
      content_base64: screenshot.data,
      mime_type: format === "jpeg" ? "image/jpeg" : "image/png",
      retention_policy: input.retention_policy ?? "debug",
      subject: input.subject,
    });
    return {
      context_id: contextId,
      artifact,
      artifact_type: "screenshot",
      source_url: input.url,
    };
  }

  async captureDownloadArtifact(
    contextId: string,
    input: CaptureDownloadArtifactInput,
  ): Promise<BrowserArtifactResult> {
    const context = this.requireContext(contextId);
    const timeoutMs = input.timeout_ms ?? 15_000;
    const willBegin = context.connection.waitForEvent(
      "Browser.downloadWillBegin",
      (event) => readString(event.params.url) === input.url,
      timeoutMs,
    );
    await this.navigate(context, input.url, timeoutMs, { allowDownloadAbort: true });
    const beginEvent = await willBegin;
    const guid = requiredString(beginEvent.params.guid, "Browser.downloadWillBegin.guid");
    const complete = await context.connection.waitForEvent(
      "Browser.downloadProgress",
      (event) =>
        readString(event.params.guid) === guid && readString(event.params.state) === "completed",
      timeoutMs,
    );
    const downloadedPath = join(context.download_dir, guid);
    const bytes = await readFile(downloadedPath);
    const artifact = await this.kernel.createArtifact({
      content_base64: bytes.toString("base64"),
      mime_type: input.mime_type ?? "application/octet-stream",
      retention_policy: input.retention_policy ?? "session",
      subject: input.subject,
    });
    return {
      context_id: contextId,
      artifact,
      artifact_type: "download",
      filename: readString(beginEvent.params.suggestedFilename),
      source_url: readString(complete.params.url) ?? input.url,
    };
  }

  async capturePdfArtifact(
    contextId: string,
    input: {
      retention_policy?: "ephemeral" | "session" | "durable" | "debug" | string;
      subject?: SparseKernelArtifactSubject;
    } = {},
  ): Promise<BrowserArtifactResult> {
    const context = this.requireContext(contextId);
    const pdf = await context.connection.command<{ data: string }>(
      "Page.printToPDF",
      { printBackground: true },
      context.page_session_id,
    );
    const artifact = await this.kernel.createArtifact({
      content_base64: pdf.data,
      mime_type: "application/pdf",
      retention_policy: input.retention_policy ?? "debug",
      subject: input.subject,
    });
    return {
      context_id: contextId,
      artifact,
      artifact_type: "pdf",
    };
  }

  listConsoleMessages(
    contextId: string,
    input: { level?: string; limit?: number } = {},
  ): { ok: true; targetId: string; messages: SparseKernelBrowserConsoleMessage[] } {
    const context = this.requireContext(contextId);
    const level = input.level?.trim().toLowerCase();
    const limit = Math.max(1, Math.min(500, Math.floor(input.limit ?? 100)));
    const messages = context.console_messages
      .filter((message) => !level || message.type.toLowerCase() === level)
      .slice(-limit);
    return {
      ok: true,
      targetId: context.target_id,
      messages,
    };
  }

  armDialog(
    contextId: string,
    input: SparseKernelBrowserDialogInput,
  ): { ok: true; targetId: string; armed: true } {
    const context = this.requireContext(contextId);
    void context.connection
      .waitForEvent(
        "Page.javascriptDialogOpening",
        (event) => event.sessionId === context.page_session_id,
        input.timeout_ms ?? 30_000,
      )
      .then(async () => {
        await context.connection.command(
          "Page.handleJavaScriptDialog",
          {
            accept: input.accept,
            promptText: input.prompt_text,
          },
          context.page_session_id,
        );
      })
      .catch(() => {});
    return {
      ok: true,
      targetId: context.target_id,
      armed: true,
    };
  }

  async uploadFiles(
    contextId: string,
    input: SparseKernelBrowserUploadInput,
  ): Promise<{ ok: true; targetId: string }> {
    const context = this.requireContext(contextId);
    const targetId = input.target_id?.trim();
    if (targetId && targetId !== context.target_id) {
      throw new Error(`SparseKernel CDP browser context does not own target: ${targetId}`);
    }
    if (input.paths.length === 0) {
      throw new Error("SparseKernel browser upload requires at least one file path.");
    }
    const selector = this.resolveActionSelector(context, {
      ref: input.ref ?? input.input_ref,
      selector: input.selector,
    });
    const { root } = await context.connection.command<{ root: { nodeId: number } }>(
      "DOM.getDocument",
      { depth: 1 },
      context.page_session_id,
      input.timeout_ms,
    );
    const rootNodeId = root?.nodeId;
    if (typeof rootNodeId !== "number") {
      throw new Error("SparseKernel browser upload could not read DOM root.");
    }
    const { nodeId } = await context.connection.command<{ nodeId: number }>(
      "DOM.querySelector",
      { nodeId: rootNodeId, selector },
      context.page_session_id,
      input.timeout_ms,
    );
    if (typeof nodeId !== "number" || nodeId <= 0) {
      throw new Error("SparseKernel browser upload target not found.");
    }
    await context.connection.command(
      "DOM.setFileInputFiles",
      { nodeId, files: input.paths },
      context.page_session_id,
      input.timeout_ms,
    );
    return {
      ok: true,
      targetId: context.target_id,
    };
  }

  async navigateContext(
    contextId: string,
    input: { url: string; timeout_ms?: number } = { url: "about:blank" },
  ): Promise<SparseKernelBrowserTab> {
    const context = this.requireContext(contextId);
    await this.navigate(context, input.url, input.timeout_ms);
    return await this.describeContextTab(context);
  }

  async listTabs(contextId: string): Promise<SparseKernelBrowserTab[]> {
    return [await this.describeContextTab(this.requireContext(contextId))];
  }

  async snapshotContext(
    contextId: string,
    input: SparseKernelBrowserSnapshotInput = {},
  ): Promise<SparseKernelBrowserSnapshotResult> {
    const context = this.requireContext(contextId);
    const limit = Math.max(1, Math.min(200, Math.floor(input.limit ?? 50)));
    const maxChars =
      typeof input.max_chars === "number" && Number.isFinite(input.max_chars)
        ? Math.max(1, Math.floor(input.max_chars))
        : 40_000;
    const selector = input.selector?.trim() || undefined;
    const expression = buildSnapshotExpression({ limit, maxChars, selector });
    const evaluated = await context.connection.command<{
      result?: { value?: string };
    }>(
      "Runtime.evaluate",
      {
        expression,
        returnByValue: true,
        awaitPromise: false,
      },
      context.page_session_id,
      input.timeout_ms,
    );
    const raw = evaluated.result?.value;
    const data = typeof raw === "string" ? parseSnapshotPayload(raw) : emptySnapshotPayload();
    const refs = buildSnapshotRefs(data, {
      interactive: input.interactive,
      compact: input.compact,
    });
    context.snapshot_refs = buildSnapshotRefMap(data);
    const nodes = buildSnapshotNodes(data, refs);
    const snapshotText = buildAiSnapshotText(data, maxChars);
    return {
      ok: true,
      format: input.format === "aria" ? "aria" : "ai",
      targetId: context.target_id,
      ...(data.url ? { url: data.url } : {}),
      ...(data.title ? { title: data.title } : {}),
      snapshot: snapshotText.text,
      nodes,
      refs,
      truncated: data.truncated || snapshotText.truncated,
      stats: {
        textChars: data.text.length,
        linkCount: data.links.length,
        buttonCount: data.buttons.length,
        inputCount: data.inputs.length,
        nodeCount: nodes.length,
      },
    };
  }

  async actContext(
    contextId: string,
    request: SparseKernelBrowserActRequest,
  ): Promise<SparseKernelBrowserActResult> {
    const context = this.requireContext(contextId);
    const targetId =
      "targetId" in request && typeof request.targetId === "string" ? request.targetId : undefined;
    if (targetId && targetId !== context.target_id) {
      throw new Error(`SparseKernel CDP browser context does not own target: ${targetId}`);
    }
    switch (request.kind) {
      case "click":
      case "type":
      case "hover": {
        const selector = this.resolveActionSelector(context, request);
        const evaluated = await context.connection.command<{ result?: { value?: unknown } }>(
          "Runtime.evaluate",
          {
            expression: buildActionExpression(request, selector),
            returnByValue: true,
            awaitPromise: true,
          },
          context.page_session_id,
          "timeoutMs" in request ? request.timeoutMs : undefined,
        );
        return {
          ok: true,
          targetId: context.target_id,
          kind: request.kind,
          value: evaluated.result?.value,
        };
      }
      case "press": {
        await context.connection.command(
          "Input.dispatchKeyEvent",
          { type: "keyDown", key: request.key },
          context.page_session_id,
        );
        await context.connection.command(
          "Input.dispatchKeyEvent",
          { type: "keyUp", key: request.key },
          context.page_session_id,
        );
        return { ok: true, targetId: context.target_id, kind: request.kind };
      }
      case "wait": {
        const evaluated = await context.connection.command<{ result?: { value?: unknown } }>(
          "Runtime.evaluate",
          {
            expression: buildWaitExpression(request),
            returnByValue: true,
            awaitPromise: true,
          },
          context.page_session_id,
          request.timeoutMs,
        );
        return {
          ok: true,
          targetId: context.target_id,
          kind: request.kind,
          value: evaluated.result?.value,
        };
      }
      case "resize":
        await context.connection.command(
          "Emulation.setDeviceMetricsOverride",
          {
            width: Math.max(1, Math.floor(request.width)),
            height: Math.max(1, Math.floor(request.height)),
            deviceScaleFactor: 1,
            mobile: false,
          },
          context.page_session_id,
        );
        return { ok: true, targetId: context.target_id, kind: request.kind };
      case "close":
        await this.releaseContext(context.ledger_context.id);
        return { ok: true, targetId: context.target_id, kind: request.kind };
    }
  }

  async releaseContext(contextId: string): Promise<boolean> {
    const context = this.contexts.get(contextId);
    if (!context) {
      return await this.kernel.releaseBrowserContext(contextId);
    }
    this.contexts.delete(contextId);
    try {
      await context.connection.command("Target.closeTarget", { targetId: context.target_id });
      await context.connection.command("Target.disposeBrowserContext", {
        browserContextId: context.cdp_browser_context_id,
      });
    } finally {
      context.connection.close();
      if (context.owns_download_dir) {
        await rm(context.download_dir, { force: true, recursive: true });
      }
    }
    return await this.kernel.releaseBrowserContext(contextId);
  }

  private requireContext(contextId: string): LiveBrowserContext {
    const context = this.contexts.get(contextId);
    if (!context) {
      throw new Error(`SparseKernel browser context is not materialized: ${contextId}`);
    }
    return context;
  }

  private async navigate(
    context: LiveBrowserContext,
    url: string,
    timeoutMs = 10_000,
    options: { allowDownloadAbort?: boolean } = {},
  ): Promise<void> {
    const load = options.allowDownloadAbort
      ? undefined
      : context.connection.waitForEvent(
          "Page.loadEventFired",
          (event) => event.sessionId === context.page_session_id,
          timeoutMs,
        );
    const result = await context.connection.command<{ errorText?: string }>(
      "Page.navigate",
      { url },
      context.page_session_id,
    );
    if (
      result.errorText &&
      !(options.allowDownloadAbort && result.errorText.includes("ERR_ABORTED"))
    ) {
      throw new Error(`CDP navigation failed: ${result.errorText}`);
    }
    if (load) {
      await load;
    }
  }

  private async describeContextTab(context: LiveBrowserContext): Promise<SparseKernelBrowserTab> {
    let title: string | undefined;
    let url: string | undefined;
    try {
      const evaluated = await context.connection.command<{
        result?: { value?: string };
      }>(
        "Runtime.evaluate",
        {
          expression: "JSON.stringify({ title: document.title, url: location.href })",
          returnByValue: true,
        },
        context.page_session_id,
      );
      const raw = evaluated.result?.value;
      if (typeof raw === "string") {
        const parsed = JSON.parse(raw) as { title?: unknown; url?: unknown };
        title = typeof parsed.title === "string" ? parsed.title : undefined;
        url = typeof parsed.url === "string" ? parsed.url : undefined;
      }
    } catch {
      // Best-effort tab metadata; target id is the durable handle for the lease.
    }
    return {
      targetId: context.target_id,
      suggestedTargetId: context.target_id,
      ...(title ? { title } : {}),
      ...(url ? { url } : {}),
      type: "page",
      sparsekernelContextId: context.ledger_context.id,
    };
  }

  private resolveActionSelector(
    context: LiveBrowserContext,
    request: { ref?: string; selector?: string },
  ): string {
    const selector = request.selector?.trim();
    if (selector) {
      return selector;
    }
    const ref = request.ref?.trim();
    if (ref) {
      const resolved = context.snapshot_refs.get(ref);
      if (resolved?.selector) {
        return resolved.selector;
      }
      throw new Error(`SparseKernel browser ref is unknown or stale: ${ref}`);
    }
    throw new Error("SparseKernel browser action requires ref or selector");
  }
}

class CdpConnection {
  private nextId = 1;
  private readonly pending = new Map<number, CdpResponseWaiter>();
  private readonly eventWaiters: CdpEventWaiter[] = [];
  private readonly eventBacklog: CdpEventMessage[] = [];
  private readonly eventObservers: Array<(message: CdpEventMessage) => void> = [];

  private constructor(private readonly transport: CdpTransport) {
    transport.onMessage((data) => this.handleMessage(data));
    transport.onClose(() => this.failAll(new Error("CDP connection closed")));
    transport.onError((error) => this.failAll(error));
  }

  static async connect(webSocketUrl: string, factory: CdpTransportFactory): Promise<CdpConnection> {
    return new CdpConnection(await factory(webSocketUrl));
  }

  command<T = JsonRecord>(
    method: string,
    params: JsonRecord = {},
    sessionId?: string,
    timeoutMs = 10_000,
  ): Promise<T> {
    const id = this.nextId++;
    const message: JsonRecord = { id, method, params };
    if (sessionId) {
      message.sessionId = sessionId;
    }
    const promise = new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`CDP command timed out: ${method}`));
      }, timeoutMs);
      this.pending.set(id, {
        resolve: (value) => resolve(value as T),
        reject,
        timeout,
      });
    });
    this.transport.send(JSON.stringify(message));
    return promise;
  }

  waitForEvent(
    method: string,
    predicate: (message: CdpEventMessage) => boolean,
    timeoutMs = 10_000,
  ): Promise<CdpEventMessage> {
    const existing = this.eventBacklog.find((event) => event.method === method && predicate(event));
    if (existing) {
      return Promise.resolve(existing);
    }
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.removeEventWaiter(waiter);
        reject(new Error(`CDP event timed out: ${method}`));
      }, timeoutMs);
      const waiter: CdpEventWaiter = {
        method,
        predicate,
        resolve,
        reject,
        timeout,
      };
      this.eventWaiters.push(waiter);
    });
  }

  onEvent(listener: (message: CdpEventMessage) => void): void {
    this.eventObservers.push(listener);
  }

  close(): void {
    this.transport.close();
    this.failAll(new Error("CDP connection closed"));
  }

  private handleMessage(data: string): void {
    const message = parseJsonRecord(data);
    if (!message) {
      return;
    }
    const id = typeof message.id === "number" ? message.id : undefined;
    if (id !== undefined) {
      const waiter = this.pending.get(id);
      if (!waiter) {
        return;
      }
      this.pending.delete(id);
      clearTimeout(waiter.timeout);
      const error = isRecord(message.error) ? readString(message.error.message) : undefined;
      if (error) {
        waiter.reject(new Error(error));
      } else {
        waiter.resolve(isRecord(message.result) ? message.result : {});
      }
      return;
    }
    const method = readString(message.method);
    if (!method) {
      return;
    }
    const event: CdpEventMessage = {
      method,
      params: isRecord(message.params) ? message.params : {},
      sessionId: readString(message.sessionId),
    };
    this.eventBacklog.push(event);
    if (this.eventBacklog.length > 100) {
      this.eventBacklog.shift();
    }
    for (const observer of this.eventObservers) {
      try {
        observer(event);
      } catch {
        // CDP event observers are best-effort side channels such as console capture.
      }
    }
    for (const waiter of [...this.eventWaiters]) {
      if (waiter.method === method && waiter.predicate(event)) {
        this.removeEventWaiter(waiter);
        clearTimeout(waiter.timeout);
        waiter.resolve(event);
      }
    }
  }

  private removeEventWaiter(waiter: CdpEventWaiter): void {
    const index = this.eventWaiters.indexOf(waiter);
    if (index >= 0) {
      this.eventWaiters.splice(index, 1);
    }
  }

  private failAll(error: Error): void {
    for (const waiter of this.pending.values()) {
      clearTimeout(waiter.timeout);
      waiter.reject(error);
    }
    this.pending.clear();
    for (const waiter of this.eventWaiters.splice(0)) {
      clearTimeout(waiter.timeout);
      waiter.reject(error);
    }
  }
}

class WebSocketCdpTransport implements CdpTransport {
  constructor(private readonly socket: WebSocket) {}

  send(data: string): void {
    this.socket.send(data);
  }

  close(): void {
    this.socket.close();
  }

  onMessage(listener: (data: string) => void): void {
    this.socket.on("message", (data: RawData) => listener(rawDataToString(data)));
  }

  onClose(listener: () => void): void {
    this.socket.on("close", listener);
  }

  onError(listener: (error: Error) => void): void {
    this.socket.on("error", listener);
  }
}

async function createWebSocketTransport(webSocketUrl: string): Promise<CdpTransport> {
  const socket = new WebSocket(webSocketUrl);
  return await new Promise<CdpTransport>((resolve, reject) => {
    const onError = (error: Error) => {
      socket.off("open", onOpen);
      reject(error);
    };
    const onOpen = () => {
      socket.off("error", onError);
      resolve(new WebSocketCdpTransport(socket));
    };
    socket.once("error", onError);
    socket.once("open", onOpen);
  });
}

async function discoverBrowserWebSocketUrl(
  endpoint: string,
  fetchImpl: typeof fetch,
): Promise<string> {
  const url = new URL(endpoint);
  url.pathname = "/json/version";
  url.search = "";
  url.hash = "";
  const response = await fetchImpl(url);
  if (!response.ok) {
    throw new Error(`CDP /json/version failed: HTTP ${response.status}`);
  }
  const payload = (await response.json()) as unknown;
  const webSocketUrl = isRecord(payload) ? readString(payload.webSocketDebuggerUrl) : undefined;
  if (!webSocketUrl) {
    throw new Error("CDP /json/version did not return webSocketDebuggerUrl");
  }
  return webSocketUrl;
}

export function normalizeLoopbackCdpEndpoint(endpoint: string): string {
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch {
    throw new Error("CDP endpoint must be a valid URL");
  }
  if (url.protocol !== "http:") {
    throw new Error("CDP endpoint must use http:// for v0 loopback attachment");
  }
  if (!isLoopbackHost(url.hostname)) {
    throw new Error("CDP endpoint must be loopback for v0 attachment");
  }
  url.pathname = "";
  url.search = "";
  url.hash = "";
  return url.toString().replace(/\/$/, "");
}

function isLoopbackHost(host: string): boolean {
  const normalized = host.toLowerCase();
  return normalized === "localhost" || normalized === "127.0.0.1" || normalized === "::1";
}

function publicContext(context: LiveBrowserContext): MaterializedBrowserContext {
  return {
    ledger_context: context.ledger_context,
    cdp_endpoint: context.cdp_endpoint,
    cdp_browser_context_id: context.cdp_browser_context_id,
    target_id: context.target_id,
  };
}

function recordConsoleEvent(context: LiveBrowserContext, event: CdpEventMessage): void {
  if (event.sessionId && event.sessionId !== context.page_session_id) {
    return;
  }
  if (event.method === "Runtime.consoleAPICalled") {
    const type = readString(event.params.type) ?? "log";
    const args = Array.isArray(event.params.args) ? event.params.args : [];
    const text = args
      .filter(isRecord)
      .map((arg) => readString(arg.value) ?? readString(arg.description) ?? "")
      .filter(Boolean)
      .join(" ");
    context.console_messages.push({
      type,
      text,
      timestamp: new Date().toISOString(),
    });
  } else if (event.method === "Log.entryAdded" && isRecord(event.params.entry)) {
    const entry = event.params.entry;
    const level = readString(entry.level) ?? "log";
    context.console_messages.push({
      type: level,
      level,
      text: readString(entry.text) ?? "",
      timestamp: new Date().toISOString(),
    });
  } else {
    return;
  }
  if (context.console_messages.length > 500) {
    context.console_messages.splice(0, context.console_messages.length - 500);
  }
}

function parseJsonRecord(raw: string): JsonRecord | undefined {
  try {
    const parsed = JSON.parse(raw) as unknown;
    return isRecord(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function requiredString(value: unknown, field: string): string {
  const text = readString(value);
  if (!text) {
    throw new Error(`Missing CDP field: ${field}`);
  }
  return text;
}

type SnapshotPayload = {
  title?: string;
  url?: string;
  text: string;
  links: Array<{ text?: string; href?: string; selector?: string }>;
  buttons: Array<{ text?: string; selector?: string }>;
  inputs: Array<{
    label?: string;
    placeholder?: string;
    name?: string;
    type?: string;
    selector?: string;
  }>;
  truncated?: boolean;
};

type SnapshotRef = {
  role: string;
  name: string;
  selector: string;
  href?: string;
  type?: string;
};

function buildSnapshotExpression(input: {
  limit: number;
  maxChars: number;
  selector?: string;
}): string {
  const selector = JSON.stringify(input.selector ?? "");
  return `(() => {
  const limit = ${input.limit};
  const maxChars = ${input.maxChars};
  const selector = ${selector};
  const quote = (value) => String(value || "").replace(/\\\\/g, "\\\\\\\\").replace(/"/g, "\\\\\"");
  const clean = (value) => String(value || "").replace(/\\s+/g, " ").trim();
  const selectorFor = (node) => {
    if (!node || !node.tagName) return "";
    if (node.id) return '[id="' + quote(node.id) + '"]';
    const parts = [];
    let current = node;
    while (current && current.nodeType === 1 && current !== document.documentElement) {
      const tag = current.tagName.toLowerCase();
      let index = 1;
      let previous = current.previousElementSibling;
      while (previous) {
        if (previous.tagName === current.tagName) index += 1;
        previous = previous.previousElementSibling;
      }
      parts.unshift(tag + ":nth-of-type(" + index + ")");
      if (tag === "body") break;
      current = current.parentElement;
    }
    return parts.join(" > ");
  };
  let root = document.body || document.documentElement;
  if (selector) {
    try {
      root = document.querySelector(selector) || root;
    } catch {
      root = root || document.body || document.documentElement;
    }
  }
  const textSource = clean(root?.innerText || root?.textContent || "");
  const text = textSource.slice(0, maxChars);
  const links = Array.from(root?.querySelectorAll?.("a[href]") || [])
    .slice(0, limit)
    .map((node) => ({ text: clean(node.innerText || node.textContent), href: node.href, selector: selectorFor(node) }));
  const buttons = Array.from(root?.querySelectorAll?.("button,[role=button]") || [])
    .slice(0, limit)
    .map((node) => ({ text: clean(node.innerText || node.textContent || node.getAttribute("aria-label")), selector: selectorFor(node) }));
  const inputs = Array.from(root?.querySelectorAll?.("input,textarea,select") || [])
    .slice(0, limit)
    .map((node) => ({
      label: clean(node.getAttribute("aria-label") || ""),
      placeholder: clean(node.getAttribute("placeholder") || ""),
      name: clean(node.getAttribute("name") || node.id || ""),
      type: clean(node.getAttribute("type") || node.tagName.toLowerCase()),
      selector: selectorFor(node),
    }));
  return JSON.stringify({
    title: document.title || "",
    url: location.href,
    text,
    links,
    buttons,
    inputs,
    truncated: textSource.length > text.length,
  });
})()`;
}

function parseSnapshotPayload(raw: string): SnapshotPayload {
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!isRecord(parsed)) {
      return emptySnapshotPayload();
    }
    return {
      title: readString(parsed.title),
      url: readString(parsed.url),
      text: readString(parsed.text) ?? "",
      links: readSnapshotList(parsed.links, (entry) => ({
        text: readString(entry.text),
        href: readString(entry.href),
        selector: readString(entry.selector),
      })),
      buttons: readSnapshotList(parsed.buttons, (entry) => ({
        text: readString(entry.text),
        selector: readString(entry.selector),
      })),
      inputs: readSnapshotList(parsed.inputs, (entry) => ({
        label: readString(entry.label),
        placeholder: readString(entry.placeholder),
        name: readString(entry.name),
        type: readString(entry.type),
        selector: readString(entry.selector),
      })),
      truncated: parsed.truncated === true,
    };
  } catch {
    return emptySnapshotPayload();
  }
}

function emptySnapshotPayload(): SnapshotPayload {
  return {
    text: "",
    links: [],
    buttons: [],
    inputs: [],
  };
}

function readSnapshotList<T>(value: unknown, map: (entry: JsonRecord) => T): T[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter(isRecord).map(map);
}

function buildSnapshotRefs(
  data: SnapshotPayload,
  input: { interactive?: boolean; compact?: boolean },
): Record<string, Record<string, unknown>> {
  const refs: Record<string, Record<string, unknown>> = {};
  let next = 1;
  for (const link of data.links) {
    refs[`e${next++}`] = {
      role: "link",
      name: link.text ?? link.href ?? "",
      href: link.href,
      selector: link.selector,
    };
  }
  for (const button of data.buttons) {
    refs[`e${next++}`] = {
      role: "button",
      name: button.text ?? "",
      selector: button.selector,
    };
  }
  for (const field of data.inputs) {
    refs[`e${next++}`] = {
      role: field.type === "textarea" ? "textbox" : "input",
      name: field.label ?? field.placeholder ?? field.name ?? "",
      type: field.type,
      selector: field.selector,
    };
  }
  if (!input.interactive && input.compact && Object.keys(refs).length === 0) {
    return {};
  }
  return refs;
}

function buildSnapshotRefMap(data: SnapshotPayload): Map<string, SnapshotRef> {
  const refs = new Map<string, SnapshotRef>();
  let next = 1;
  for (const link of data.links) {
    if (link.selector) {
      refs.set(`e${next}`, {
        role: "link",
        name: link.text ?? link.href ?? "",
        selector: link.selector,
        href: link.href,
      });
    }
    next += 1;
  }
  for (const button of data.buttons) {
    if (button.selector) {
      refs.set(`e${next}`, {
        role: "button",
        name: button.text ?? "",
        selector: button.selector,
      });
    }
    next += 1;
  }
  for (const field of data.inputs) {
    if (field.selector) {
      refs.set(`e${next}`, {
        role: field.type === "textarea" ? "textbox" : "input",
        name: field.label ?? field.placeholder ?? field.name ?? "",
        selector: field.selector,
        type: field.type,
      });
    }
    next += 1;
  }
  return refs;
}

function buildSnapshotNodes(
  data: SnapshotPayload,
  refs: Record<string, Record<string, unknown>>,
): Array<Record<string, unknown>> {
  const nodes: Array<Record<string, unknown>> = [
    {
      role: "document",
      name: data.title ?? data.url ?? "page",
      url: data.url,
    },
  ];
  for (const [ref, node] of Object.entries(refs)) {
    nodes.push({ ref, ...node });
  }
  if (data.text) {
    nodes.push({
      role: "text",
      name: data.text,
    });
  }
  return nodes;
}

function buildAiSnapshotText(
  data: SnapshotPayload,
  maxChars: number,
): { text: string; truncated: boolean } {
  const lines: string[] = [];
  if (data.title) {
    lines.push(`Title: ${data.title}`);
  }
  if (data.url) {
    lines.push(`URL: ${data.url}`);
  }
  if (data.text) {
    lines.push("", data.text);
  }
  if (data.links.length > 0) {
    lines.push("", "Links:");
    let ref = 1;
    for (const link of data.links) {
      lines.push(
        `- [e${ref++}] ${link.text || link.href || "link"}${link.href ? ` -> ${link.href}` : ""}`,
      );
    }
  }
  if (data.buttons.length > 0) {
    lines.push("", "Buttons:");
    let ref = data.links.length + 1;
    for (const button of data.buttons) {
      lines.push(`- [e${ref++}] ${button.text || "button"}`);
    }
  }
  if (data.inputs.length > 0) {
    lines.push("", "Inputs:");
    let ref = data.links.length + data.buttons.length + 1;
    for (const input of data.inputs) {
      lines.push(
        `- [e${ref++}] ${input.label || input.placeholder || input.name || input.type || "input"}`,
      );
    }
  }
  const text = lines.join("\n").slice(0, maxChars);
  return {
    text,
    truncated: data.truncated === true || text.length < lines.join("\n").length,
  };
}

function buildActionExpression(request: SparseKernelBrowserActRequest, selector: string): string {
  const selectorJson = JSON.stringify(selector);
  if (request.kind === "click" || request.kind === "hover") {
    const eventName = request.kind === "hover" ? "mouseover" : "click";
    const repeat = request.kind === "click" && request.doubleClick ? 2 : 1;
    return `(() => {
  const node = document.querySelector(${selectorJson});
  if (!node) throw new Error("SparseKernel browser action target not found");
  node.scrollIntoView({ block: "center", inline: "center" });
  for (let i = 0; i < ${repeat}; i += 1) {
    node.dispatchEvent(new MouseEvent(${JSON.stringify(eventName)}, { bubbles: true, cancelable: true, view: window }));
  }
  return { ok: true };
})()`;
  }
  if (request.kind === "type") {
    const text = JSON.stringify(request.text);
    const submit = request.submit === true;
    return `(() => {
  const node = document.querySelector(${selectorJson});
  if (!node) throw new Error("SparseKernel browser action target not found");
  node.scrollIntoView({ block: "center", inline: "center" });
  node.focus?.();
  const value = ${text};
  if ("value" in node) {
    node.value = value;
    node.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText", data: value }));
    node.dispatchEvent(new Event("change", { bubbles: true }));
  } else {
    node.textContent = value;
    node.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText", data: value }));
  }
  if (${submit}) {
    const form = node.form || node.closest?.("form");
    if (form?.requestSubmit) form.requestSubmit();
    else form?.submit?.();
  }
  return { ok: true };
})()`;
  }
  throw new Error(`SparseKernel CDP browser action does not support ${request.kind} yet.`);
}

function buildWaitExpression(request: Extract<SparseKernelBrowserActRequest, { kind: "wait" }>) {
  const timeMs = Math.max(0, Math.min(60_000, Math.floor(request.timeMs ?? 0)));
  const text = JSON.stringify(request.text ?? "");
  const textGone = JSON.stringify(request.textGone ?? "");
  return `(async () => {
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const wanted = ${text};
  const gone = ${textGone};
  if (${timeMs} > 0) await delay(${timeMs});
  const deadline = Date.now() + ${Math.max(1, Math.floor(request.timeoutMs ?? 10_000))};
  while (Date.now() < deadline) {
    const body = document.body?.innerText || document.documentElement?.textContent || "";
    const hasWanted = !wanted || body.includes(wanted);
    const hasGone = gone ? body.includes(gone) : false;
    if (hasWanted && !hasGone) return { ok: true };
    await delay(100);
  }
  throw new Error("SparseKernel browser wait timed out");
})()`;
}

function rawDataToString(data: RawData): string {
  if (typeof data === "string") {
    return data;
  }
  if (Buffer.isBuffer(data)) {
    return data.toString("utf8");
  }
  if (Array.isArray(data)) {
    return Buffer.concat(data).toString("utf8");
  }
  return Buffer.from(data).toString("utf8");
}
