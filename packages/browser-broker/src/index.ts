import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import WebSocket, { type RawData } from "ws";
import type {
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
  artifact_type: "screenshot" | "download";
  filename?: string;
  source_url?: string;
};

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
      };
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
}

class CdpConnection {
  private nextId = 1;
  private readonly pending = new Map<number, CdpResponseWaiter>();
  private readonly eventWaiters: CdpEventWaiter[] = [];
  private readonly eventBacklog: CdpEventMessage[] = [];

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
