import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
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
  type SparseKernelBrowserObservationInput,
  type SparseKernelBrowserTarget,
  type SparseKernelCloseBrowserTargetInput,
  type SparseKernelCreateArtifactInput,
  type SparseKernelImportArtifactFileInput,
  type SparseKernelRecordBrowserTargetInput,
} from "../../sparsekernel-client/src/index.js";
import { defaultSparseKernelArtifactStagingDir } from "../../sparsekernel-client/src/node-artifacts.js";

export type {
  SparseKernelAcquireBrowserContextInput,
  SparseKernelArtifact,
  SparseKernelArtifactSubject,
  SparseKernelBrowserContext,
  SparseKernelBrowserEndpointProbe,
  SparseKernelBrowserObservationInput,
  SparseKernelBrowserTarget,
  SparseKernelCloseBrowserTargetInput,
  SparseKernelCreateArtifactInput,
  SparseKernelImportArtifactFileInput,
  SparseKernelRecordBrowserTargetInput,
} from "../../sparsekernel-client/src/index.js";

type JsonRecord = Record<string, unknown>;

export type SparseKernelBrowserKernelClient = {
  probeBrowserPool(input: { cdp_endpoint: string }): Promise<SparseKernelBrowserEndpointProbe>;
  acquireBrowserContext(
    input: SparseKernelAcquireBrowserContextInput,
  ): Promise<SparseKernelBrowserContext>;
  releaseBrowserContext(contextId: string): Promise<boolean>;
  createArtifact(input: SparseKernelCreateArtifactInput): Promise<SparseKernelArtifact>;
  importArtifactFile?(input: SparseKernelImportArtifactFileInput): Promise<SparseKernelArtifact>;
  recordBrowserObservation?(input: SparseKernelBrowserObservationInput): Promise<void>;
  recordBrowserTarget?(
    input: SparseKernelRecordBrowserTargetInput,
  ): Promise<SparseKernelBrowserTarget>;
  closeBrowserTarget?(
    input: SparseKernelCloseBrowserTargetInput,
  ): Promise<SparseKernelBrowserTarget>;
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
  artifactStagingDir?: string | false;
};

export type SparseKernelCdpBrowserBrokerFactoryOptions = SparseKernelClientOptions & {
  transportFactory?: CdpTransportFactory;
  artifactStagingDir?: string | false;
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
  allowed_origins?: unknown;
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
  filename?: string;
  retention_policy?: "ephemeral" | "session" | "durable" | "debug" | string;
  subject?: SparseKernelArtifactSubject;
  timeout_ms?: number;
};

export type BrowserArtifactResult = {
  context_id: string;
  target_id?: string;
  artifact: SparseKernelArtifact;
  artifact_type: "screenshot" | "download" | "pdf";
  filename?: string;
  source_url?: string;
};

export type SparseKernelBrowserConsoleMessage = {
  type: string;
  text: string;
  timestamp: string;
  targetId?: string;
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
      frameSelector?: string;
      targetId?: string;
      doubleClick?: boolean;
      button?: string;
      modifiers?: string[];
      delayMs?: number;
      timeoutMs?: number;
    }
  | {
      kind: "clickCoords";
      x: number;
      y: number;
      targetId?: string;
      doubleClick?: boolean;
      button?: string;
      modifiers?: string[];
      delayMs?: number;
      timeoutMs?: number;
    }
  | {
      kind: "type";
      ref?: string;
      selector?: string;
      frameSelector?: string;
      text: string;
      targetId?: string;
      submit?: boolean;
      slowly?: boolean;
      timeoutMs?: number;
    }
  | { kind: "press"; key: string; targetId?: string; delayMs?: number; modifiers?: string[] }
  | {
      kind: "hover";
      ref?: string;
      selector?: string;
      frameSelector?: string;
      targetId?: string;
      timeoutMs?: number;
    }
  | {
      kind: "check";
      ref?: string;
      selector?: string;
      frameSelector?: string;
      targetId?: string;
      timeoutMs?: number;
    }
  | {
      kind: "uncheck";
      ref?: string;
      selector?: string;
      frameSelector?: string;
      targetId?: string;
      timeoutMs?: number;
    }
  | {
      kind: "scrollIntoView";
      ref?: string;
      selector?: string;
      frameSelector?: string;
      targetId?: string;
      timeoutMs?: number;
    }
  | {
      kind: "drag";
      startRef?: string;
      startSelector?: string;
      endRef?: string;
      endSelector?: string;
      targetId?: string;
      timeoutMs?: number;
    }
  | {
      kind: "select";
      ref?: string;
      selector?: string;
      frameSelector?: string;
      values: string[];
      targetId?: string;
      timeoutMs?: number;
    }
  | {
      kind: "fill";
      fields: SparseKernelBrowserFormField[];
      targetId?: string;
      timeoutMs?: number;
    }
  | { kind: "resize"; width: number; height: number; targetId?: string }
  | {
      kind: "wait";
      timeMs?: number;
      text?: string;
      textGone?: string;
      selector?: string;
      url?: string;
      loadState?: "load" | "domcontentloaded" | "networkidle";
      fn?: string;
      targetId?: string;
      timeoutMs?: number;
    }
  | {
      kind: "evaluate";
      fn: string;
      ref?: string;
      selector?: string;
      frameSelector?: string;
      targetId?: string;
      timeoutMs?: number;
    }
  | {
      kind: "batch";
      actions: SparseKernelBrowserActRequest[];
      targetId?: string;
      stopOnError?: boolean;
    }
  | { kind: "reload"; targetId?: string; timeoutMs?: number }
  | { kind: "goBack"; targetId?: string; timeoutMs?: number }
  | { kind: "goForward"; targetId?: string; timeoutMs?: number }
  | { kind: "close"; targetId?: string };

export type SparseKernelBrowserFormField = {
  ref: string;
  type?: string;
  value?: string | number | boolean;
};

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
    artifactStagingDir: options.artifactStagingDir,
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

type CdpEventWatch = {
  promise: Promise<CdpEventMessage | undefined>;
  cancel: () => void;
};

type CdpEventMessage = {
  method: string;
  params: JsonRecord;
  sessionId?: string;
};

type LiveBrowserPage = {
  target_id: string;
  page_session_id: string;
  console_messages: SparseKernelBrowserConsoleMessage[];
  network_request_ids: Set<string>;
  last_network_activity_at: number;
};

type LiveBrowserContext = MaterializedBrowserContext & {
  connection: CdpConnection;
  page_session_id: string;
  download_dir: string;
  owns_download_dir: boolean;
  snapshot_refs: Map<string, SnapshotRef>;
  allowed_origins: string[];
  pages: Map<string, LiveBrowserPage>;
};

const NETWORK_IDLE_QUIET_MS = 500;
const POST_ACTION_NAVIGATION_SETTLE_MS = 300;
const POST_ACTION_LOAD_TIMEOUT_MS = 5_000;

type PostActionNavigationObservation =
  | { kind: "same-target"; url?: string }
  | { kind: "new-target"; targetId: string; url?: string };

type CdpActionPoint = {
  x: number;
  y: number;
};

export class SparseKernelCdpBrowserBroker {
  private readonly kernel: SparseKernelBrowserKernelClient;
  private readonly fetchImpl: typeof fetch;
  private readonly transportFactory: CdpTransportFactory;
  private readonly artifactStagingDir: string | false;
  private readonly contexts = new Map<string, LiveBrowserContext>();

  constructor(options: SparseKernelBrowserBrokerOptions) {
    this.kernel = options.kernel;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.transportFactory = options.transportFactory ?? createWebSocketTransport;
    this.artifactStagingDir =
      options.artifactStagingDir === undefined
        ? defaultSparseKernelArtifactStagingDir()
        : options.artifactStagingDir;
  }

  async acquireContext(
    input: AcquireMaterializedBrowserContextInput,
  ): Promise<MaterializedBrowserContext> {
    const cdpEndpoint = normalizeLoopbackCdpEndpoint(input.cdp_endpoint);
    const allowedOrigins = normalizeAllowedOrigins(input.allowed_origins);
    if (input.initial_url) {
      assertUrlAllowedByOrigins(input.initial_url, allowedOrigins, "initial navigation");
    }
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
      allowed_origins: input.allowed_origins,
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
      await connection.command("Network.enable", {}, sessionId).catch(() => {});
      if (allowedOrigins.length > 0) {
        await connection
          .command(
            "Fetch.enable",
            { patterns: [{ urlPattern: "*", requestStage: "Request" }] },
            sessionId,
          )
          .catch(() => {});
      }
      await connection.command("Log.enable", {}, sessionId).catch(() => {});
      await connection.command("Target.setDiscoverTargets", { discover: true }).catch(() => {});
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
        allowed_origins: allowedOrigins,
        pages: new Map([[targetId, createLiveBrowserPage(targetId, sessionId)]]),
      };
      connection.onEvent((event) => {
        void this.handleFetchRequestPaused(materialized, event).catch((error) => {
          this.recordObservation(materialized, undefined, "browser_network.policy_error", {
            error: error instanceof Error ? error.message : String(error),
          });
        });
        const consoleObservation = recordConsoleEvent(materialized, event);
        if (consoleObservation) {
          this.recordObservation(
            materialized,
            consoleObservation.targetId,
            "browser_console",
            consoleObservation.message,
          );
        }
        const networkObservation = recordNetworkEvent(materialized, event);
        if (networkObservation) {
          this.recordObservation(
            materialized,
            networkObservation.targetId,
            networkObservation.observationType,
            networkObservation.payload,
          );
        }
      });
      this.contexts.set(ledgerContext.id, materialized);
      this.recordTarget(materialized, targetId, {
        url: input.initial_url ?? "about:blank",
        status: "active",
      });
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
    const artifact = await this.createArtifactFromBytes({
      bytes: Buffer.from(screenshot.data, "base64"),
      mime_type: format === "jpeg" ? "image/jpeg" : "image/png",
      retention_policy: input.retention_policy ?? "debug",
      subject: input.subject,
    });
    this.recordObservation(context, context.target_id, "browser_artifact.created", {
      artifactId: artifact.id,
      artifactType: "screenshot",
      sha256: artifact.sha256,
      sourceUrl: input.url,
    });
    return {
      context_id: contextId,
      target_id: context.target_id,
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
    const artifact = await this.createArtifactFromBytes({
      bytes,
      mime_type: input.mime_type ?? "application/octet-stream",
      retention_policy: input.retention_policy ?? "session",
      subject: input.subject,
    });
    const sourceUrl = readString(complete.params.url) ?? input.url;
    const filename =
      input.filename ??
      readString(beginEvent.params.suggestedFilename) ??
      sanitizeDownloadName(input.url);
    const totalBytes = readNumber(complete.params.totalBytes);
    const receivedBytes = readNumber(complete.params.receivedBytes);
    this.recordObservation(context, context.target_id, "browser_artifact.created", {
      artifactId: artifact.id,
      artifactType: "download",
      sha256: artifact.sha256,
      sourceUrl,
      filename,
      guid,
      ...(totalBytes !== undefined ? { totalBytes } : {}),
      ...(receivedBytes !== undefined ? { receivedBytes } : {}),
      sizeBytes: artifact.size_bytes,
      mimeType: artifact.mime_type,
    });
    return {
      context_id: contextId,
      target_id: context.target_id,
      artifact,
      artifact_type: "download",
      filename,
      source_url: sourceUrl,
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
    const artifact = await this.createArtifactFromBytes({
      bytes: Buffer.from(pdf.data, "base64"),
      mime_type: "application/pdf",
      retention_policy: input.retention_policy ?? "debug",
      subject: input.subject,
    });
    this.recordObservation(context, context.target_id, "browser_artifact.created", {
      artifactId: artifact.id,
      artifactType: "pdf",
      sha256: artifact.sha256,
    });
    return {
      context_id: contextId,
      target_id: context.target_id,
      artifact,
      artifact_type: "pdf",
    };
  }

  private async createArtifactFromBytes(input: {
    bytes: Buffer;
    mime_type: string;
    retention_policy?: "ephemeral" | "session" | "durable" | "debug" | string;
    subject?: SparseKernelArtifactSubject;
  }): Promise<SparseKernelArtifact> {
    if (!this.kernel.importArtifactFile || this.artifactStagingDir === false) {
      return await this.kernel.createArtifact({
        content_base64: input.bytes.toString("base64"),
        mime_type: input.mime_type,
        retention_policy: input.retention_policy,
        subject: input.subject,
      });
    }
    await mkdir(this.artifactStagingDir, { recursive: true });
    const stageDir = await mkdtemp(join(this.artifactStagingDir, "browser-artifact-"));
    const stagedPath = join(stageDir, "artifact.bin");
    try {
      await writeFile(stagedPath, input.bytes);
      return await this.kernel.importArtifactFile({
        staged_path: stagedPath,
        mime_type: input.mime_type,
        retention_policy: input.retention_policy,
        subject: input.subject,
      });
    } finally {
      await rm(stageDir, { recursive: true, force: true });
    }
  }

  listConsoleMessages(
    contextId: string,
    input: { level?: string; limit?: number; target_id?: string } = {},
  ): { ok: true; targetId: string; messages: SparseKernelBrowserConsoleMessage[] } {
    const context = this.requireContext(contextId);
    const page = input.target_id
      ? this.requirePage(context, input.target_id)
      : this.activePage(context);
    const level = input.level?.trim().toLowerCase();
    const limit = Math.max(1, Math.min(500, Math.floor(input.limit ?? 100)));
    const messages = page.console_messages
      .filter((message) => !level || message.type.toLowerCase() === level)
      .slice(-limit);
    return {
      ok: true,
      targetId: page.target_id,
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
    if (targetId) {
      this.activateTarget(context, targetId);
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
    const context = this.requireContext(contextId);
    const tabs: SparseKernelBrowserTab[] = [];
    for (const page of context.pages.values()) {
      tabs.push(await this.describePageTab(context, page));
    }
    return tabs;
  }

  async focusContext(
    contextId: string,
    input: { target_id: string },
  ): Promise<SparseKernelBrowserTab> {
    const context = this.requireContext(contextId);
    this.activateTarget(context, input.target_id);
    return await this.describeContextTab(context);
  }

  async closeTarget(
    contextId: string,
    input: { target_id: string },
  ): Promise<{ ok: true; targetId: string; releasedContext: boolean; activeTargetId?: string }> {
    const context = this.requireContext(contextId);
    const targetId = input.target_id.trim();
    this.requirePage(context, targetId);
    await context.connection.command("Target.closeTarget", { targetId });
    context.pages.delete(targetId);
    this.closeLedgerTarget(context, targetId, "closed");
    this.recordObservation(context, targetId, "browser_target.closed", {
      remainingTargets: context.pages.size,
    });
    if (context.pages.size === 0) {
      await this.releaseContext(contextId);
      return { ok: true, targetId, releasedContext: true };
    }
    if (context.target_id === targetId) {
      const next = context.pages.values().next().value as LiveBrowserPage | undefined;
      if (next) {
        context.target_id = next.target_id;
        context.page_session_id = next.page_session_id;
        context.snapshot_refs = new Map();
      }
    }
    return {
      ok: true,
      targetId,
      releasedContext: false,
      activeTargetId: context.target_id,
    };
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
    if (targetId) {
      this.activateTarget(context, targetId);
    }
    switch (request.kind) {
      case "click":
      case "hover": {
        return await this.withPostActionNavigationGuard(context, request.timeoutMs, async () => {
          const selector = this.resolveActionSelector(context, request);
          const evaluated = await context.connection.command<{ result?: { value?: unknown } }>(
            "Runtime.evaluate",
            {
              expression: buildActionPointExpression(
                request.kind,
                selector,
                resolveCdpActionTimeoutMs(request.timeoutMs),
                request.frameSelector,
              ),
              returnByValue: true,
              awaitPromise: true,
            },
            context.page_session_id,
            resolveCdpCommandTimeoutMs(request.timeoutMs),
          );
          const point = parseCdpActionPoint(evaluated.result?.value);
          await dispatchCdpMouseAction(context, request, point);
          return {
            ok: true,
            targetId: context.target_id,
            kind: request.kind,
            value: evaluated.result?.value,
          };
        });
      }
      case "type":
      case "check":
      case "uncheck":
      case "scrollIntoView":
      case "select": {
        return await this.withPostActionNavigationGuard(context, request.timeoutMs, async () => {
          const selector = this.resolveActionSelector(context, request);
          const evaluated = await context.connection.command<{ result?: { value?: unknown } }>(
            "Runtime.evaluate",
            {
              expression: buildActionExpression(
                request,
                selector,
                resolveCdpActionTimeoutMs(request.timeoutMs),
                request.frameSelector,
              ),
              returnByValue: true,
              awaitPromise: true,
            },
            context.page_session_id,
            resolveCdpCommandTimeoutMs(request.timeoutMs),
          );
          return {
            ok: true,
            targetId: context.target_id,
            kind: request.kind,
            value: evaluated.result?.value,
          };
        });
      }
      case "clickCoords": {
        return await this.withPostActionNavigationGuard(context, undefined, async () => {
          await dispatchCdpMouseAction(context, request, { x: request.x, y: request.y });
          return { ok: true, targetId: context.target_id, kind: request.kind };
        });
      }
      case "press": {
        return await this.withPostActionNavigationGuard(context, undefined, async () => {
          const modifiers = resolveCdpInputModifiers(request.modifiers);
          await context.connection.command(
            "Input.dispatchKeyEvent",
            {
              type: "keyDown",
              key: request.key,
              ...(modifiers ? { modifiers } : {}),
            },
            context.page_session_id,
          );
          if (request.delayMs && request.delayMs > 0) {
            await delay(Math.min(5_000, Math.floor(request.delayMs)));
          }
          await context.connection.command(
            "Input.dispatchKeyEvent",
            {
              type: "keyUp",
              key: request.key,
              ...(modifiers ? { modifiers } : {}),
            },
            context.page_session_id,
          );
          return { ok: true, targetId: context.target_id, kind: request.kind };
        });
      }
      case "drag": {
        return await this.withPostActionNavigationGuard(context, request.timeoutMs, async () => {
          const startSelector = this.resolveActionSelector(context, {
            ref: request.startRef,
            selector: request.startSelector,
          });
          const endSelector = this.resolveActionSelector(context, {
            ref: request.endRef,
            selector: request.endSelector,
          });
          const evaluated = await context.connection.command<{ result?: { value?: unknown } }>(
            "Runtime.evaluate",
            {
              expression: buildDragExpression(
                startSelector,
                endSelector,
                resolveCdpActionTimeoutMs(request.timeoutMs),
              ),
              returnByValue: true,
              awaitPromise: true,
            },
            context.page_session_id,
            resolveCdpCommandTimeoutMs(request.timeoutMs),
          );
          return {
            ok: true,
            targetId: context.target_id,
            kind: request.kind,
            value: evaluated.result?.value,
          };
        });
      }
      case "fill": {
        return await this.withPostActionNavigationGuard(context, request.timeoutMs, async () => {
          const fields = request.fields.map((field) => ({
            ...field,
            selector: this.resolveActionSelector(context, { ref: field.ref }),
          }));
          const evaluated = await context.connection.command<{ result?: { value?: unknown } }>(
            "Runtime.evaluate",
            {
              expression: buildFillExpression(fields, resolveCdpActionTimeoutMs(request.timeoutMs)),
              returnByValue: true,
              awaitPromise: true,
            },
            context.page_session_id,
            resolveCdpCommandTimeoutMs(request.timeoutMs),
          );
          return {
            ok: true,
            targetId: context.target_id,
            kind: request.kind,
            value: evaluated.result?.value,
          };
        });
      }
      case "wait": {
        return {
          ok: true,
          targetId: context.target_id,
          kind: request.kind,
          value: await this.waitForAction(context, request),
        };
      }
      case "evaluate": {
        return await this.withPostActionNavigationGuard(context, request.timeoutMs, async () => {
          const selector =
            request.ref || request.selector
              ? this.resolveActionSelector(context, {
                  ref: request.ref,
                  selector: request.selector,
                })
              : undefined;
          const evaluated = await context.connection.command<{ result?: { value?: unknown } }>(
            "Runtime.evaluate",
            {
              expression: buildEvaluateExpression(request, selector, request.frameSelector),
              returnByValue: true,
              awaitPromise: true,
            },
            context.page_session_id,
            resolveCdpCommandTimeoutMs(request.timeoutMs),
          );
          return {
            ok: true,
            targetId: context.target_id,
            kind: request.kind,
            value: evaluated.result?.value,
          };
        });
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
      case "reload": {
        await this.reloadContext(context, request.timeoutMs);
        return { ok: true, targetId: context.target_id, kind: request.kind };
      }
      case "goBack":
      case "goForward": {
        await this.navigateHistory(context, request.kind === "goBack" ? -1 : 1, request.timeoutMs);
        return { ok: true, targetId: context.target_id, kind: request.kind };
      }
      case "batch": {
        const results: Array<{ ok: boolean; kind: string; error?: string }> = [];
        for (const action of request.actions.slice(0, 100)) {
          try {
            const result = await this.actContext(contextId, {
              ...action,
              targetId:
                "targetId" in action && action.targetId ? action.targetId : request.targetId,
            } as SparseKernelBrowserActRequest);
            results.push({ ok: true, kind: result.kind });
          } catch (error) {
            results.push({
              ok: false,
              kind: "kind" in action ? action.kind : "unknown",
              error: error instanceof Error ? error.message : String(error),
            });
            if (request.stopOnError !== false) {
              break;
            }
          }
        }
        return { ok: true, targetId: context.target_id, kind: request.kind, value: { results } };
      }
      case "close":
        return {
          ok: true,
          targetId: targetId ?? context.target_id,
          kind: request.kind,
          value: await this.closeTarget(context.ledger_context.id, {
            target_id: targetId ?? context.target_id,
          }),
        };
    }
  }

  async releaseContext(contextId: string): Promise<boolean> {
    const context = this.contexts.get(contextId);
    if (!context) {
      return await this.kernel.releaseBrowserContext(contextId);
    }
    this.contexts.delete(contextId);
    try {
      for (const targetId of context.pages.keys()) {
        await context.connection.command("Target.closeTarget", { targetId }).catch(() => {});
        this.closeLedgerTarget(context, targetId, "context_released");
      }
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

  private requirePage(context: LiveBrowserContext, targetId: string): LiveBrowserPage {
    const page = context.pages.get(targetId);
    if (!page) {
      throw new Error(`SparseKernel CDP browser context does not own target: ${targetId}`);
    }
    return page;
  }

  private activePage(context: LiveBrowserContext): LiveBrowserPage {
    return this.requirePage(context, context.target_id);
  }

  private activateTarget(context: LiveBrowserContext, targetId: string): void {
    const page = this.requirePage(context, targetId);
    context.target_id = page.target_id;
    context.page_session_id = page.page_session_id;
  }

  private recordTarget(
    context: LiveBrowserContext,
    targetId: string,
    input: {
      openerTargetId?: string;
      url?: string;
      title?: string;
      status?: string;
      closeReason?: string;
      closedAt?: string;
    } = {},
  ): void {
    const recorded = this.kernel.recordBrowserTarget?.({
      context_id: context.ledger_context.id,
      target_id: targetId,
      opener_target_id: input.openerTargetId,
      url: input.url,
      title: input.title,
      status: input.status,
      close_reason: input.closeReason,
      closed_at: input.closedAt,
      updated_at: new Date().toISOString(),
    });
    void recorded?.catch(() => {});
  }

  private closeLedgerTarget(context: LiveBrowserContext, targetId: string, reason: string): void {
    const closed = this.kernel.closeBrowserTarget?.({
      context_id: context.ledger_context.id,
      target_id: targetId,
      reason,
      closed_at: new Date().toISOString(),
    });
    void closed?.catch(() => {});
  }

  private recordObservation(
    context: LiveBrowserContext,
    targetId: string | undefined,
    observationType: string,
    payload: unknown,
  ): void {
    const observed = this.kernel.recordBrowserObservation?.({
      context_id: context.ledger_context.id,
      target_id: targetId,
      observation_type: observationType,
      payload,
      created_at: new Date().toISOString(),
    });
    void observed?.catch(() => {});
  }

  private async handleFetchRequestPaused(
    context: LiveBrowserContext,
    event: CdpEventMessage,
  ): Promise<void> {
    if (event.method !== "Fetch.requestPaused" || context.allowed_origins.length === 0) {
      return;
    }
    const requestId = readString(event.params.requestId);
    const request = isRecord(event.params.request) ? event.params.request : {};
    const url = readString(request.url);
    if (!requestId || !url) {
      return;
    }
    const page = pageForEvent(context, event);
    if (isUrlAllowedByOrigins(url, context.allowed_origins)) {
      await context.connection.command("Fetch.continueRequest", { requestId }, event.sessionId);
      return;
    }
    await context.connection.command(
      "Fetch.failRequest",
      { requestId, errorReason: "BlockedByClient" },
      event.sessionId,
    );
    this.recordObservation(context, page?.target_id, "browser_network.blocked", {
      requestId,
      url,
      reason: "allowed_origins",
    });
  }

  private async navigate(
    context: LiveBrowserContext,
    url: string,
    timeoutMs = 10_000,
    options: { allowDownloadAbort?: boolean } = {},
  ): Promise<void> {
    assertUrlAllowedByOrigins(url, context.allowed_origins, "navigation");
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
    const tab = await this.describeContextTab(context);
    this.recordTarget(context, context.target_id, {
      url: tab.url ?? url,
      title: tab.title,
      status: "active",
    });
  }

  private async reloadContext(context: LiveBrowserContext, timeoutMs = 10_000): Promise<void> {
    const load = context.connection.waitForEvent(
      "Page.loadEventFired",
      (event) => event.sessionId === context.page_session_id,
      timeoutMs,
    );
    await context.connection.command("Page.reload", {}, context.page_session_id, timeoutMs);
    await load.catch(() => {});
    const tab = await this.describeContextTab(context);
    this.recordTarget(context, context.target_id, {
      url: tab.url,
      title: tab.title,
      status: "active",
    });
  }

  private async navigateHistory(
    context: LiveBrowserContext,
    delta: -1 | 1,
    timeoutMs = 10_000,
  ): Promise<void> {
    const history = await context.connection.command<{
      currentIndex: number;
      entries: Array<{ id: number; url?: string }>;
    }>("Page.getNavigationHistory", {}, context.page_session_id, timeoutMs);
    const nextIndex = history.currentIndex + delta;
    const entry = history.entries[nextIndex];
    if (!entry) {
      throw new Error(
        delta < 0 ? "No previous browser history entry." : "No next browser history entry.",
      );
    }
    if (entry.url) {
      assertUrlAllowedByOrigins(entry.url, context.allowed_origins, "history navigation");
    }
    const load = context.connection.waitForEvent(
      "Page.loadEventFired",
      (event) => event.sessionId === context.page_session_id,
      timeoutMs,
    );
    await context.connection.command(
      "Page.navigateToHistoryEntry",
      { entryId: entry.id },
      context.page_session_id,
      timeoutMs,
    );
    await load.catch(() => {});
    const tab = await this.describeContextTab(context);
    if (tab.url) {
      assertUrlAllowedByOrigins(tab.url, context.allowed_origins, "history navigation");
    }
    this.recordTarget(context, context.target_id, {
      url: tab.url,
      title: tab.title,
      status: "active",
    });
  }

  private async withPostActionNavigationGuard<T extends SparseKernelBrowserActResult>(
    context: LiveBrowserContext,
    timeoutMs: number | undefined,
    action: () => Promise<T>,
  ): Promise<T> {
    const beforeUrl = await this.readCurrentUrl(context);
    const watchTimeoutMs = resolveCdpCommandTimeoutMs(timeoutMs);
    const frameNavigation = this.watchSameTargetNavigation(context, beforeUrl, watchTimeoutMs);
    const newTarget = this.watchNewTarget(context, watchTimeoutMs);
    try {
      const result = await action();
      const observation = await Promise.race([
        frameNavigation.promise,
        newTarget.promise,
        delay(POST_ACTION_NAVIGATION_SETTLE_MS).then(() => undefined),
      ]);
      await this.enforcePostActionNavigation(context, beforeUrl, observation);
      return result;
    } finally {
      frameNavigation.cancel();
      newTarget.cancel();
    }
  }

  private watchSameTargetNavigation(
    context: LiveBrowserContext,
    beforeUrl: string | undefined,
    timeoutMs: number,
  ): { promise: Promise<PostActionNavigationObservation | undefined>; cancel: () => void } {
    const watch = context.connection.watchForNextEvent(
      "Page.frameNavigated",
      (event) => {
        if (event.sessionId && event.sessionId !== context.page_session_id) {
          return false;
        }
        const frame = isRecord(event.params.frame) ? event.params.frame : undefined;
        if (!frame || readString(frame.parentId)) {
          return false;
        }
        const url = readString(frame.url);
        return Boolean(url && url !== beforeUrl);
      },
      timeoutMs,
    );
    return {
      promise: watch.promise
        .then((event) => {
          if (!event) {
            return undefined;
          }
          const frame = isRecord(event.params.frame) ? event.params.frame : undefined;
          return {
            kind: "same-target" as const,
            url: frame ? readString(frame.url) : undefined,
          };
        })
        .catch(() => undefined),
      cancel: watch.cancel,
    };
  }

  private watchNewTarget(
    context: LiveBrowserContext,
    timeoutMs: number,
  ): { promise: Promise<PostActionNavigationObservation | undefined>; cancel: () => void } {
    const watch = context.connection.watchForNextEvent(
      "Target.targetCreated",
      (event) => {
        const targetInfo = isRecord(event.params.targetInfo) ? event.params.targetInfo : undefined;
        if (!targetInfo) {
          return false;
        }
        return (
          readString(targetInfo.browserContextId) === context.cdp_browser_context_id &&
          readString(targetInfo.type) === "page" &&
          Boolean(readString(targetInfo.targetId)) &&
          readString(targetInfo.targetId) !== context.target_id
        );
      },
      timeoutMs,
    );
    return {
      promise: watch.promise
        .then((event) => {
          if (!event) {
            return undefined;
          }
          const targetInfo = isRecord(event.params.targetInfo) ? event.params.targetInfo : {};
          return {
            kind: "new-target" as const,
            targetId: requiredString(
              targetInfo.targetId,
              "Target.targetCreated.targetInfo.targetId",
            ),
            url: readString(targetInfo.url),
          };
        })
        .catch(() => undefined),
      cancel: watch.cancel,
    };
  }

  private async enforcePostActionNavigation(
    context: LiveBrowserContext,
    beforeUrl: string | undefined,
    observation: PostActionNavigationObservation | undefined,
  ): Promise<void> {
    if (observation?.kind === "new-target") {
      try {
        if (observation.url) {
          assertUrlAllowedByOrigins(observation.url, context.allowed_origins, "popup navigation");
        } else if (context.allowed_origins.length > 0) {
          throw new Error(
            `SparseKernel browser popup navigation blocked by allowed origins: target ${observation.targetId} did not report a URL`,
          );
        }
      } catch (error) {
        await context.connection
          .command("Target.closeTarget", { targetId: observation.targetId })
          .catch(() => {});
        throw error;
      }
      await this.attachNewTarget(context, observation.targetId);
      return;
    }
    if (observation?.kind === "same-target") {
      await context.connection
        .waitForEvent(
          "Page.loadEventFired",
          (event) => event.sessionId === context.page_session_id,
          POST_ACTION_LOAD_TIMEOUT_MS,
        )
        .catch(() => {});
    }
    const afterUrl = observation?.url ?? (await this.readCurrentUrl(context));
    if (!afterUrl || afterUrl === beforeUrl) {
      return;
    }
    try {
      assertUrlAllowedByOrigins(afterUrl, context.allowed_origins, "post-action navigation");
    } catch (error) {
      await this.releaseContext(context.ledger_context.id).catch(() => false);
      throw error;
    }
  }

  private async readCurrentUrl(context: LiveBrowserContext): Promise<string | undefined> {
    return (await this.describeContextTab(context)).url;
  }

  private async attachNewTarget(context: LiveBrowserContext, targetId: string): Promise<void> {
    const existing = context.pages.get(targetId);
    if (existing) {
      this.activateTarget(context, targetId);
      return;
    }
    const { sessionId } = await context.connection.command<{ sessionId: string }>(
      "Target.attachToTarget",
      {
        flatten: true,
        targetId,
      },
    );
    await context.connection.command("Page.enable", {}, sessionId);
    await context.connection.command("Runtime.enable", {}, sessionId);
    await context.connection.command("Network.enable", {}, sessionId).catch(() => {});
    if (context.allowed_origins.length > 0) {
      await context.connection
        .command(
          "Fetch.enable",
          { patterns: [{ urlPattern: "*", requestStage: "Request" }] },
          sessionId,
        )
        .catch(() => {});
    }
    await context.connection.command("Log.enable", {}, sessionId).catch(() => {});
    context.pages.set(targetId, createLiveBrowserPage(targetId, sessionId));
    context.target_id = targetId;
    context.page_session_id = sessionId;
    context.snapshot_refs = new Map();
    const tab = await this.describeContextTab(context).catch(() => undefined);
    this.recordTarget(context, targetId, {
      url: tab?.url,
      title: tab?.title,
      status: "active",
    });
    this.recordObservation(context, targetId, "browser_target.attached", {
      totalTargets: context.pages.size,
    });
  }

  private async describeContextTab(context: LiveBrowserContext): Promise<SparseKernelBrowserTab> {
    return await this.describePageTab(context, this.activePage(context));
  }

  private async describePageTab(
    context: LiveBrowserContext,
    page: LiveBrowserPage,
  ): Promise<SparseKernelBrowserTab> {
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
        page.page_session_id,
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
      targetId: page.target_id,
      suggestedTargetId: page.target_id,
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

  private async waitForAction(
    context: LiveBrowserContext,
    request: Extract<SparseKernelBrowserActRequest, { kind: "wait" }>,
  ): Promise<unknown> {
    const timeoutMs = resolveCdpActionTimeoutMs(request.timeoutMs);
    if (request.timeMs && request.timeMs > 0) {
      await delay(Math.max(0, Math.min(60_000, Math.floor(request.timeMs))));
    }
    if (request.text) {
      await this.waitForPagePredicate(
        context,
        buildTextPredicateExpression(request.text, false),
        timeoutMs,
        `text "${request.text}"`,
      );
    }
    if (request.textGone) {
      await this.waitForPagePredicate(
        context,
        buildTextPredicateExpression(request.textGone, true),
        timeoutMs,
        `text "${request.textGone}" to disappear`,
      );
    }
    if (request.selector) {
      await this.waitForPagePredicate(
        context,
        buildSelectorVisiblePredicateExpression(request.selector),
        timeoutMs,
        `selector "${request.selector}"`,
      );
    }
    if (request.url) {
      await this.waitForPagePredicate(
        context,
        buildUrlPredicateExpression(request.url),
        timeoutMs,
        `url "${request.url}"`,
      );
    }
    if (request.loadState) {
      if (request.loadState === "networkidle") {
        await this.waitForPagePredicate(
          context,
          buildReadyStatePredicateExpression("complete"),
          timeoutMs,
          "document load",
        );
        await this.waitForNetworkIdle(context, timeoutMs);
      } else {
        await this.waitForPagePredicate(
          context,
          buildReadyStatePredicateExpression(request.loadState),
          timeoutMs,
          `load state "${request.loadState}"`,
        );
      }
    }
    if (request.fn) {
      await this.waitForPagePredicate(
        context,
        buildFunctionPredicateExpression(request.fn),
        timeoutMs,
        "function predicate",
      );
    }
    return { ok: true };
  }

  private async waitForPagePredicate(
    context: LiveBrowserContext,
    expression: string,
    timeoutMs: number,
    label: string,
  ): Promise<unknown> {
    const deadline = Date.now() + timeoutMs;
    let lastError = "";
    while (Date.now() <= deadline) {
      try {
        const remaining = Math.max(1, deadline - Date.now());
        const evaluated = await context.connection.command<{ result?: { value?: unknown } }>(
          "Runtime.evaluate",
          {
            expression,
            returnByValue: true,
            awaitPromise: true,
          },
          context.page_session_id,
          Math.min(1_000, remaining),
        );
        const value = evaluated.result?.value;
        if (value) {
          return value;
        }
      } catch (error) {
        lastError = error instanceof Error ? error.message : String(error);
      }
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        break;
      }
      await delay(Math.min(100, remaining));
    }
    throw new Error(
      `SparseKernel browser wait timed out waiting for ${label}${lastError ? `: ${lastError}` : ""}`,
    );
  }

  private async waitForNetworkIdle(context: LiveBrowserContext, timeoutMs: number): Promise<void> {
    const page = this.activePage(context);
    const deadline = Date.now() + timeoutMs;
    while (Date.now() <= deadline) {
      const quietForMs = Date.now() - page.last_network_activity_at;
      if (page.network_request_ids.size === 0 && quietForMs >= NETWORK_IDLE_QUIET_MS) {
        return;
      }
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        break;
      }
      await delay(Math.min(100, remaining));
    }
    throw new Error(
      `SparseKernel browser wait timed out waiting for networkidle (${page.network_request_ids.size} request(s) still active)`,
    );
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
    return this.waitForFutureEvent(method, predicate, timeoutMs);
  }

  waitForNextEvent(
    method: string,
    predicate: (message: CdpEventMessage) => boolean,
    timeoutMs = 10_000,
  ): Promise<CdpEventMessage> {
    const watch = this.watchForNextEvent(method, predicate, timeoutMs);
    return watch.promise.then((event) => {
      if (!event) {
        throw new Error(`CDP event cancelled: ${method}`);
      }
      return event;
    });
  }

  watchForNextEvent(
    method: string,
    predicate: (message: CdpEventMessage) => boolean,
    timeoutMs: number,
  ): CdpEventWatch {
    let settled = false;
    let waiter: CdpEventWaiter | undefined;
    let resolveWatch: ((value: CdpEventMessage | undefined) => void) | undefined;
    const promise = new Promise<CdpEventMessage | undefined>((resolve, reject) => {
      resolveWatch = resolve;
      const timeout = setTimeout(() => {
        settled = true;
        if (waiter) {
          this.removeEventWaiter(waiter);
        }
        reject(new Error(`CDP event timed out: ${method}`));
      }, timeoutMs);
      waiter = {
        method,
        predicate,
        resolve: (message) => {
          settled = true;
          resolve(message);
        },
        reject: (error) => {
          settled = true;
          reject(error);
        },
        timeout,
      };
      this.eventWaiters.push(waiter);
    });
    return {
      promise,
      cancel: () => {
        if (settled || !waiter) {
          return;
        }
        settled = true;
        clearTimeout(waiter.timeout);
        this.removeEventWaiter(waiter);
        resolveWatch?.(undefined);
      },
    };
  }

  private waitForFutureEvent(
    method: string,
    predicate: (message: CdpEventMessage) => boolean,
    timeoutMs: number,
  ): Promise<CdpEventMessage> {
    return this.waitForNextEvent(method, predicate, timeoutMs);
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

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createLiveBrowserPage(targetId: string, sessionId: string): LiveBrowserPage {
  return {
    target_id: targetId,
    page_session_id: sessionId,
    console_messages: [],
    network_request_ids: new Set(),
    last_network_activity_at: Date.now(),
  };
}

function normalizeMouseButton(button: string | undefined): "left" | "right" | "middle" {
  const normalized = button?.trim().toLowerCase();
  return normalized === "right" || normalized === "middle" ? normalized : "left";
}

async function dispatchCdpMouseAction(
  context: LiveBrowserContext,
  request: Extract<SparseKernelBrowserActRequest, { kind: "click" | "clickCoords" | "hover" }>,
  point: CdpActionPoint,
): Promise<void> {
  const modifiers =
    request.kind === "click" || request.kind === "clickCoords"
      ? resolveCdpInputModifiers(request.modifiers)
      : 0;
  await context.connection.command(
    "Input.dispatchMouseEvent",
    {
      type: "mouseMoved",
      x: point.x,
      y: point.y,
      button: "none",
      ...(modifiers ? { modifiers } : {}),
    },
    context.page_session_id,
  );
  if (request.kind === "hover") {
    return;
  }
  const button = normalizeMouseButton(request.button);
  const clickCount = request.doubleClick ? 2 : 1;
  await context.connection.command(
    "Input.dispatchMouseEvent",
    {
      type: "mousePressed",
      x: point.x,
      y: point.y,
      button,
      clickCount,
      ...(modifiers ? { modifiers } : {}),
    },
    context.page_session_id,
  );
  if (request.delayMs && request.delayMs > 0) {
    await delay(Math.min(5_000, Math.floor(request.delayMs)));
  }
  await context.connection.command(
    "Input.dispatchMouseEvent",
    {
      type: "mouseReleased",
      x: point.x,
      y: point.y,
      button,
      clickCount,
      ...(modifiers ? { modifiers } : {}),
    },
    context.page_session_id,
  );
}

function resolveCdpInputModifiers(modifiers: string[] | undefined): number {
  let bitmask = 0;
  for (const modifier of modifiers ?? []) {
    switch (modifier.trim().toLowerCase()) {
      case "alt":
        bitmask |= 1;
        break;
      case "control":
      case "ctrl":
        bitmask |= 2;
        break;
      case "meta":
      case "cmd":
      case "command":
        bitmask |= 4;
        break;
      case "shift":
        bitmask |= 8;
        break;
      case "controlormeta":
      case "control_or_meta":
      case "mod":
        bitmask |= process.platform === "darwin" ? 4 : 2;
        break;
    }
  }
  return bitmask;
}

function normalizeAllowedOrigins(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const origins = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string") {
      continue;
    }
    const origin = normalizeAllowedOrigin(item);
    if (origin) {
      origins.add(origin);
    }
  }
  return [...origins].sort();
}

function normalizeAllowedOrigin(raw: string): string | undefined {
  const text = raw.trim();
  if (!text) {
    return undefined;
  }
  if (text === "*") {
    throw new Error("SparseKernel browser allowed origins must not use wildcard '*'.");
  }
  let parsed: URL;
  try {
    parsed = new URL(text);
  } catch {
    throw new Error(`SparseKernel browser allowed origin is not a valid URL: ${text}`);
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error(`SparseKernel browser allowed origin must use http(s): ${text}`);
  }
  return parsed.origin;
}

function assertUrlAllowedByOrigins(url: string, allowedOrigins: string[], label: string): void {
  const allowed = isUrlAllowedByOrigins(url, allowedOrigins);
  if (allowed) {
    return;
  }
  if (allowedOrigins.length === 0) {
    return;
  }
  try {
    new URL(url);
  } catch {
    throw new Error(`SparseKernel browser ${label} URL is not valid: ${url}`);
  }
  throw new Error(
    `SparseKernel browser ${label} blocked by allowed origins: ${url} is not in ${allowedOrigins.join(", ")}`,
  );
}

function isUrlAllowedByOrigins(url: string, allowedOrigins: string[]): boolean {
  if (allowedOrigins.length === 0) {
    return true;
  }
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return false;
  }
  if (parsed.protocol === "about:" && parsed.href === "about:blank") {
    return true;
  }
  if (parsed.protocol === "data:") {
    return true;
  }
  const origin =
    parsed.protocol === "blob:"
      ? normalizeBlobOrigin(parsed.href)
      : parsed.protocol === "http:" || parsed.protocol === "https:"
        ? parsed.origin
        : undefined;
  return Boolean(origin && allowedOrigins.includes(origin));
}

function normalizeBlobOrigin(url: string): string | undefined {
  const withoutScheme = url.slice("blob:".length);
  try {
    const parsed = new URL(withoutScheme);
    return parsed.protocol === "http:" || parsed.protocol === "https:" ? parsed.origin : undefined;
  } catch {
    return undefined;
  }
}

function publicContext(context: LiveBrowserContext): MaterializedBrowserContext {
  return {
    ledger_context: context.ledger_context,
    cdp_endpoint: context.cdp_endpoint,
    cdp_browser_context_id: context.cdp_browser_context_id,
    target_id: context.target_id,
  };
}

function pageForEvent(
  context: LiveBrowserContext,
  event: CdpEventMessage,
): LiveBrowserPage | undefined {
  if (!event.sessionId) {
    return context.pages.get(context.target_id) ?? context.pages.values().next().value;
  }
  for (const page of context.pages.values()) {
    if (page.page_session_id === event.sessionId) {
      return page;
    }
  }
  return undefined;
}

function recordConsoleEvent(
  context: LiveBrowserContext,
  event: CdpEventMessage,
): { targetId: string; message: SparseKernelBrowserConsoleMessage } | undefined {
  const page = pageForEvent(context, event);
  if (!page) {
    return undefined;
  }
  if (event.method === "Runtime.consoleAPICalled") {
    const type = readString(event.params.type) ?? "log";
    const args = Array.isArray(event.params.args) ? event.params.args : [];
    const text = args
      .filter(isRecord)
      .map((arg) => readString(arg.value) ?? readString(arg.description) ?? "")
      .filter(Boolean)
      .join(" ");
    const message = {
      type,
      text,
      timestamp: new Date().toISOString(),
      targetId: page.target_id,
    };
    page.console_messages.push(message);
    if (page.console_messages.length > 500) {
      page.console_messages.splice(0, page.console_messages.length - 500);
    }
    return { targetId: page.target_id, message };
  } else if (event.method === "Log.entryAdded" && isRecord(event.params.entry)) {
    const entry = event.params.entry;
    const level = readString(entry.level) ?? "log";
    const message = {
      type: level,
      level,
      text: readString(entry.text) ?? "",
      timestamp: new Date().toISOString(),
      targetId: page.target_id,
    };
    page.console_messages.push(message);
    if (page.console_messages.length > 500) {
      page.console_messages.splice(0, page.console_messages.length - 500);
    }
    return { targetId: page.target_id, message };
  } else {
    return undefined;
  }
}

function recordNetworkEvent(
  context: LiveBrowserContext,
  event: CdpEventMessage,
): { targetId: string; observationType: string; payload: Record<string, unknown> } | undefined {
  const page = pageForEvent(context, event);
  if (!page) {
    return undefined;
  }
  const requestId = readString(event.params.requestId);
  if (!requestId) {
    return undefined;
  }
  switch (event.method) {
    case "Network.requestWillBeSent": {
      page.network_request_ids.add(requestId);
      page.last_network_activity_at = Date.now();
      const request = isRecord(event.params.request) ? event.params.request : {};
      return {
        targetId: page.target_id,
        observationType: "browser_network.request",
        payload: {
          requestId,
          url: readString(request.url),
          method: readString(request.method),
        },
      };
    }
    case "Network.loadingFinished":
    case "Network.loadingFailed":
      page.network_request_ids.delete(requestId);
      page.last_network_activity_at = Date.now();
      return {
        targetId: page.target_id,
        observationType:
          event.method === "Network.loadingFinished"
            ? "browser_network.finished"
            : "browser_network.failed",
        payload: {
          requestId,
          errorText: readString(event.params.errorText),
        },
      };
    default:
      return undefined;
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

function readNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function sanitizeDownloadName(url: string): string {
  try {
    const parsed = new URL(url);
    const last = parsed.pathname.split("/").filter(Boolean).pop();
    if (last) {
      return last.replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 128) || "download.bin";
    }
  } catch {
    // Fall through to a generic artifact name.
  }
  return "download.bin";
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

function buildActionExpression(
  request: SparseKernelBrowserActRequest,
  selector: string,
  timeoutMs: number,
  frameSelector?: string,
): string {
  const selectorJson = JSON.stringify(selector);
  const timeoutJson = JSON.stringify(timeoutMs);
  const frameSelectorJson = JSON.stringify(frameSelector?.trim() || "");
  if (request.kind === "click" || request.kind === "hover" || request.kind === "scrollIntoView") {
    const eventName = request.kind === "hover" ? "mouseover" : "click";
    const repeat = request.kind === "click" && request.doubleClick ? 2 : 1;
    const button = JSON.stringify(
      normalizeMouseButton(request.kind === "click" ? request.button : undefined),
    );
    return `(async () => {
  ${buildActionTargetHelpers(selectorJson, timeoutJson, request.kind, frameSelectorJson)}
  const node = await waitForActionTarget();
  node.scrollIntoView({ block: "center", inline: "center" });
  if (${JSON.stringify(request.kind)} === "scrollIntoView") return { ok: true };
  for (let i = 0; i < ${repeat}; i += 1) {
    if (${JSON.stringify(request.kind)} === "click" && typeof node.click === "function" && ${button} === "left") {
      node.click();
    } else {
      node.dispatchEvent(new MouseEvent(${JSON.stringify(eventName)}, { bubbles: true, cancelable: true, view: window, button: ${button} === "right" ? 2 : ${button} === "middle" ? 1 : 0 }));
    }
  }
  if (${repeat} > 1) {
    node.dispatchEvent(new MouseEvent("dblclick", { bubbles: true, cancelable: true, view: window }));
  }
  return { ok: true };
})()`;
  }
  if (request.kind === "type") {
    const text = JSON.stringify(request.text);
    const submit = request.submit === true;
    const slowly = request.slowly === true;
    return `(async () => {
  ${buildActionTargetHelpers(selectorJson, timeoutJson, request.kind, frameSelectorJson)}
  const node = await waitForActionTarget();
  node.scrollIntoView({ block: "center", inline: "center" });
  node.focus?.();
  const value = ${text};
  const slowly = ${JSON.stringify(slowly)};
  if ("value" in node) {
    node.value = "";
    if (slowly) {
      for (const char of Array.from(value)) {
        node.value += char;
        node.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText", data: char }));
        await delay(75);
      }
    } else {
      node.value = value;
      node.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText", data: value }));
    }
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
  if (request.kind === "check" || request.kind === "uncheck") {
    const desiredChecked = request.kind === "check";
    return `(async () => {
  ${buildActionTargetHelpers(selectorJson, timeoutJson, request.kind, frameSelectorJson)}
  const node = await waitForActionTarget();
  node.scrollIntoView({ block: "center", inline: "center" });
  const desiredChecked = ${JSON.stringify(desiredChecked)};
  const control = node instanceof HTMLLabelElement && node.control ? node.control : node;
  if (!(control instanceof HTMLInputElement) || !["checkbox", "radio"].includes(String(control.type || "").toLowerCase())) {
    throw new Error("SparseKernel browser ${request.kind} target must be a checkbox, radio, or associated label");
  }
  if (control.checked !== desiredChecked) {
    control.click();
  }
  if (control.checked !== desiredChecked) {
    throw new Error("SparseKernel browser ${request.kind} did not reach requested checked state");
  }
  return { ok: true, checked: control.checked };
})()`;
  }
  if (request.kind === "select") {
    const values = JSON.stringify(request.values);
    return `(async () => {
  ${buildActionTargetHelpers(selectorJson, timeoutJson, request.kind, frameSelectorJson)}
  const node = await waitForActionTarget();
  node.scrollIntoView({ block: "center", inline: "center" });
  const values = ${values};
  if (node instanceof HTMLSelectElement) {
    const wanted = new Set(values.map(String));
    for (const option of Array.from(node.options)) {
      option.selected = wanted.has(option.value) || wanted.has(option.text);
    }
  } else if ("value" in node) {
    node.value = String(values[0] ?? "");
  } else {
    throw new Error("SparseKernel browser select target is not selectable");
  }
  node.dispatchEvent(new Event("input", { bubbles: true }));
  node.dispatchEvent(new Event("change", { bubbles: true }));
  return { ok: true, values };
})()`;
  }
  throw new Error(`SparseKernel CDP browser action does not support ${request.kind} yet.`);
}

function buildActionPointExpression(
  kind: "click" | "hover",
  selector: string,
  timeoutMs: number,
  frameSelector?: string,
): string {
  return `(async () => {
  ${buildActionTargetHelpers(JSON.stringify(selector), JSON.stringify(timeoutMs), kind, JSON.stringify(frameSelector?.trim() || ""))}
  const node = await waitForActionTarget();
  node.scrollIntoView({ block: "center", inline: "center" });
  await delay(0);
  const target = await waitForActionTarget();
  const point = centerPointForNode(target);
  const rect = target.getBoundingClientRect();
  const x = point.x;
  const y = point.y;
  return { ok: true, x, y, rect: { left: rect.left, top: rect.top, width: rect.width, height: rect.height } };
})()`;
}

function parseCdpActionPoint(value: unknown): CdpActionPoint {
  if (!value || typeof value !== "object") {
    throw new Error("SparseKernel browser action target did not return coordinates");
  }
  const record = value as { x?: unknown; y?: unknown };
  const x = Number(record.x);
  const y = Number(record.y);
  if (!Number.isFinite(x) || !Number.isFinite(y)) {
    throw new Error("SparseKernel browser action target returned invalid coordinates");
  }
  return { x, y };
}

function buildDragExpression(
  startSelector: string,
  endSelector: string,
  timeoutMs: number,
): string {
  return `(async () => {
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const isVisible = (node) => {
    if (!node?.isConnected) return false;
    const style = getComputedStyle(node);
    if (style.visibility === "hidden" || style.display === "none") return false;
    return node.getClientRects().length > 0;
  };
  const waitFor = async (selector, label) => {
    const deadline = Date.now() + ${JSON.stringify(timeoutMs)};
    while (Date.now() <= deadline) {
      const node = document.querySelector(selector);
      if (node && isVisible(node)) return node;
      await delay(100);
    }
    throw new Error("SparseKernel browser drag target not found: " + label);
  };
  const start = await waitFor(${JSON.stringify(startSelector)}, "start");
  const end = await waitFor(${JSON.stringify(endSelector)}, "end");
  start.scrollIntoView({ block: "center", inline: "center" });
  end.scrollIntoView({ block: "center", inline: "center" });
  const data = typeof DataTransfer === "function" ? new DataTransfer() : undefined;
  const make = (type) => new DragEvent(type, { bubbles: true, cancelable: true, dataTransfer: data });
  start.dispatchEvent(make("dragstart"));
  end.dispatchEvent(make("dragenter"));
  end.dispatchEvent(make("dragover"));
  end.dispatchEvent(make("drop"));
  start.dispatchEvent(make("dragend"));
  return { ok: true };
})()`;
}

function buildFillExpression(
  fields: Array<SparseKernelBrowserFormField & { selector: string }>,
  timeoutMs: number,
): string {
  return `(async () => {
  const fields = ${JSON.stringify(fields)};
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const waitFor = async (selector, ref) => {
    const deadline = Date.now() + ${JSON.stringify(timeoutMs)};
    while (Date.now() <= deadline) {
      const node = document.querySelector(selector);
      if (node?.isConnected) return node;
      await delay(100);
    }
    throw new Error("SparseKernel browser fill target not found: " + ref);
  };
  let changed = 0;
  for (const field of fields) {
    const node = await waitFor(field.selector, field.ref);
    const type = String(field.type || "").toLowerCase();
    const value = field.value;
    if (type === "checkbox" || type === "radio") {
      node.checked = value === true || value === 1 || value === "1" || value === "true";
    } else if (node instanceof HTMLSelectElement) {
      const wanted = new Set([String(value ?? "")]);
      for (const option of Array.from(node.options)) {
        option.selected = wanted.has(option.value) || wanted.has(option.text);
      }
    } else if ("value" in node) {
      node.value = value == null ? "" : String(value);
    } else {
      node.textContent = value == null ? "" : String(value);
    }
    node.dispatchEvent(new Event("input", { bubbles: true }));
    node.dispatchEvent(new Event("change", { bubbles: true }));
    changed += 1;
  }
  return { ok: true, changed };
})()`;
}

function buildEvaluateExpression(
  request: Extract<SparseKernelBrowserActRequest, { kind: "evaluate" }>,
  selector?: string,
  frameSelector?: string,
): string {
  return `(() => {
  const fnBody = ${JSON.stringify(request.fn)};
  const selector = ${JSON.stringify(selector ?? "")};
  const frameSelector = ${JSON.stringify(frameSelector?.trim() || "")};
  const queryRoot = () => {
    if (!frameSelector) return document;
    const frame = document.querySelector(frameSelector);
    if (!(frame instanceof HTMLIFrameElement || frame instanceof HTMLFrameElement)) {
      throw new Error("SparseKernel browser evaluate frame target not found");
    }
    const frameDocument = frame.contentDocument;
    if (!frameDocument) throw new Error("SparseKernel browser evaluate frame is not accessible");
    return frameDocument;
  };
  const node = selector ? queryRoot().querySelector(selector) : undefined;
  if (selector && !node) throw new Error("SparseKernel browser evaluate target not found");
  const candidate = eval("(" + fnBody + ")");
  return typeof candidate === "function" ? (selector ? candidate(node) : candidate()) : candidate;
})()`;
}

function buildActionTargetHelpers(
  selectorJson: string,
  timeoutJson: string,
  kind: string,
  frameSelectorJson = '""',
): string {
  return `const selector = ${selectorJson};
  const timeoutMs = ${timeoutJson};
  const actionKind = ${JSON.stringify(kind)};
  const frameSelector = ${frameSelectorJson};
  let actionFrameElement = null;
  const queryRoot = () => {
    if (!frameSelector) return document;
    const frame = document.querySelector(frameSelector);
    if (!(frame instanceof HTMLIFrameElement || frame instanceof HTMLFrameElement)) {
      throw new Error("SparseKernel browser action frame target not found");
    }
    const frameDocument = frame.contentDocument;
    if (!frameDocument) throw new Error("SparseKernel browser action frame is not accessible");
    actionFrameElement = frame;
    return frameDocument;
  };
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const rectSnapshot = (node) => {
    const rect = node.getBoundingClientRect();
    return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
  };
  const centerPointForNode = (node) => {
    const rect = node.getBoundingClientRect();
    const frameRect = actionFrameElement ? actionFrameElement.getBoundingClientRect() : { left: 0, top: 0 };
    const x = Math.min(Math.max(frameRect.left + rect.left + rect.width / 2, 0), Math.max(window.innerWidth - 1, 0));
    const y = Math.min(Math.max(frameRect.top + rect.top + rect.height / 2, 0), Math.max(window.innerHeight - 1, 0));
    return { x, y };
  };
  const isVisible = (node) => {
    if (!node?.isConnected) return false;
    if (node.closest?.("[hidden],[aria-hidden='true'],[inert]")) return false;
    const style = getComputedStyle(node);
    if (style.visibility === "hidden" || style.display === "none" || Number(style.opacity) === 0) return false;
    const rect = rectSnapshot(node);
    return rect.width > 0 && rect.height > 0 && node.getClientRects().length > 0;
  };
  const isEnabled = (node) => {
    if (node.disabled === true) return false;
    if (node.getAttribute?.("aria-disabled") === "true") return false;
    const disabledFieldset = node.closest?.("fieldset[disabled]");
    if (disabledFieldset) {
      const firstLegend = disabledFieldset.querySelector("legend");
      if (!firstLegend || !firstLegend.contains(node)) return false;
    }
    return true;
  };
  const isEditable = (node) => {
    if (node.isContentEditable) return true;
    if (node instanceof HTMLTextAreaElement) return !node.readOnly && !node.disabled;
    if (node instanceof HTMLInputElement) {
      const type = String(node.type || "text").toLowerCase();
      return !node.readOnly && !node.disabled && !["button", "checkbox", "color", "file", "hidden", "image", "radio", "range", "reset", "submit"].includes(type);
    }
    return false;
  };
  const receivesCenterHit = (node) => {
    if (getComputedStyle(node).pointerEvents === "none") return false;
    const point = centerPointForNode(node);
    const hit = actionFrameElement
      ? queryRoot().elementFromPoint(point.x - actionFrameElement.getBoundingClientRect().left, point.y - actionFrameElement.getBoundingClientRect().top)
      : document.elementFromPoint(point.x, point.y);
    if (!hit) return false;
    return hit === node || node.contains(hit) || hit.closest?.("label")?.contains(node);
  };
  const isStable = async (node) => {
    const first = rectSnapshot(node);
    await delay(50);
    const second = rectSnapshot(node);
    return Math.abs(first.x - second.x) < 0.5 &&
      Math.abs(first.y - second.y) < 0.5 &&
      Math.abs(first.width - second.width) < 0.5 &&
      Math.abs(first.height - second.height) < 0.5;
  };
  const isActionable = async (node) => {
    if (!isVisible(node)) return false;
    if ((actionKind === "click" || actionKind === "type" || actionKind === "select" || actionKind === "check" || actionKind === "uncheck") && !isEnabled(node)) return false;
    if (actionKind === "type" && !isEditable(node)) return false;
    if (!(await isStable(node))) return false;
    if (actionKind !== "scrollIntoView" && !receivesCenterHit(node)) return false;
    return true;
  };
  const waitForActionTarget = async () => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() <= deadline) {
      const node = queryRoot().querySelector(selector);
      if (node) {
        if (actionKind !== "scrollIntoView") {
          node.scrollIntoView({ block: "center", inline: "center" });
        }
        if (await isActionable(node)) return node;
      }
      await delay(100);
    }
    throw new Error("SparseKernel browser action target not actionable");
  };`;
}

function buildTextPredicateExpression(text: string, gone: boolean): string {
  return `(() => {
  const body = document.body?.innerText || document.documentElement?.textContent || "";
  return ${gone ? "!" : ""}body.includes(${JSON.stringify(text)});
})()`;
}

function buildSelectorVisiblePredicateExpression(selector: string): string {
  return `(() => {
  try {
    const node = document.querySelector(${JSON.stringify(selector)});
    if (!node?.isConnected) return false;
    const style = getComputedStyle(node);
    if (style.visibility === "hidden" || style.display === "none") return false;
    return node.getClientRects().length > 0;
  } catch {
    return false;
  }
})()`;
}

function buildUrlPredicateExpression(pattern: string): string {
  const source = pattern.includes("*") ? globToRegExpSource(pattern) : "";
  return `(() => {
  const href = location.href;
  const pattern = ${JSON.stringify(pattern)};
  const source = ${JSON.stringify(source)};
  return source ? new RegExp(source).test(href) : href.includes(pattern);
})()`;
}

function buildReadyStatePredicateExpression(
  loadState: "load" | "domcontentloaded" | "complete",
): string {
  if (loadState === "domcontentloaded") {
    return `(() => document.readyState === "interactive" || document.readyState === "complete")()`;
  }
  return `(() => document.readyState === "complete")()`;
}

function buildFunctionPredicateExpression(fn: string): string {
  return `(async () => {
  try {
    const candidate = eval("(" + ${JSON.stringify(fn)} + ")");
    const result = typeof candidate === "function" ? candidate() : candidate;
    return Boolean(await result);
  } catch {
    return false;
  }
})()`;
}

function resolveCdpActionTimeoutMs(timeoutMs: number | undefined): number {
  return Math.max(1, Math.min(120_000, Math.floor(timeoutMs ?? 10_000)));
}

function resolveCdpCommandTimeoutMs(timeoutMs: number | undefined): number {
  return resolveCdpActionTimeoutMs(timeoutMs) + 1_000;
}

function globToRegExpSource(pattern: string): string {
  return `^${pattern
    .split("*")
    .map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
    .join(".*")}$`;
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
