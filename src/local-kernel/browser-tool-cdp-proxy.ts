import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createSparseKernelCdpBrowserBroker,
  type MaterializedBrowserContext,
  type SparseKernelBrowserActRequest,
  type SparseKernelCdpBrowserBroker,
  type SparseKernelBrowserTab,
} from "../../packages/browser-broker/src/index.js";
import {
  SparseKernelClient,
  type SparseKernelArtifactSubject,
} from "../../packages/sparsekernel-client/src/index.js";
import type { SparseKernelBrowserProxyRequest } from "./browser-tool-proxy.js";

export type SparseKernelBrowserToolCdpProxyInput = {
  agentId?: string;
  sessionId?: string;
  taskId?: string;
  trustZoneId: string;
  cdpEndpoint: string;
  initialUrl?: string;
  baseUrl?: string;
  maxContexts?: number;
  allowedOrigins?: unknown;
  subject?: SparseKernelArtifactSubject;
};

export type SparseKernelBrowserToolCdpLease = {
  id: string;
  proxyRequest: SparseKernelBrowserProxyRequest;
  release: () => Promise<void>;
};

export async function createSparseKernelBrowserToolCdpProxy(
  input: SparseKernelBrowserToolCdpProxyInput,
): Promise<SparseKernelBrowserToolCdpLease> {
  const broker = createSparseKernelCdpBrowserBroker({
    baseUrl: input.baseUrl,
  });
  const client = new SparseKernelClient({
    baseUrl: input.baseUrl,
  });
  const materialized = await broker.acquireContext({
    agent_id: input.agentId,
    session_id: input.sessionId,
    task_id: input.taskId,
    trust_zone_id: input.trustZoneId,
    cdp_endpoint: input.cdpEndpoint,
    initial_url: input.initialUrl,
    max_contexts: input.maxContexts,
    allowed_origins: input.allowedOrigins,
  });
  const tempDir = await mkdtemp(join(tmpdir(), "openclaw-sparsekernel-browser-"));
  let releasePromise: Promise<void> | undefined;
  const release = async () => {
    releasePromise ??= (async () => {
      try {
        await broker.releaseContext(materialized.ledger_context.id);
      } finally {
        await rm(tempDir, { force: true, recursive: true });
      }
    })();
    await releasePromise;
  };

  const proxyRequest: SparseKernelBrowserProxyRequest = async (request) => {
    const method = request.method.trim().toUpperCase();
    const path = normalizePath(request.path);
    if (releasePromise && path !== "/" && path !== "/doctor") {
      throw new Error("SparseKernel CDP browser context has been released.");
    }

    if (method === "GET" && path === "/") {
      return statusPayload(materialized, !releasePromise);
    }
    if (method === "GET" && path === "/doctor") {
      return {
        ok: true,
        transport: "sparsekernel-cdp",
        checks: [
          {
            name: "sparsekernel_context",
            ok: !releasePromise,
            contextId: materialized.ledger_context.id,
          },
        ],
        status: statusPayload(materialized, !releasePromise),
      };
    }
    if (method === "POST" && path === "/start") {
      return statusPayload(materialized, !releasePromise);
    }
    if (method === "POST" && path === "/stop") {
      return {
        ...statusPayload(materialized, !releasePromise),
        poolManaged: true,
      };
    }
    if (method === "GET" && path === "/profiles") {
      return {
        profiles: [
          {
            name: "sparsekernel",
            driver: "sparsekernel-cdp",
            attachOnly: true,
            trustZoneId: input.trustZoneId,
          },
        ],
      };
    }
    if (method === "GET" && path === "/tabs") {
      return {
        ok: true,
        tabs: (await broker.listTabs(materialized.ledger_context.id)).map(toBrowserTabResult),
      };
    }
    if (method === "GET" && path === "/snapshot") {
      const query = request.query ?? {};
      await focusTargetIfRequested(
        broker,
        materialized.ledger_context.id,
        readString(query.targetId),
      );
      return await broker.snapshotContext(materialized.ledger_context.id, {
        format: query.format === "aria" ? "aria" : "ai",
        max_chars: readNumber(query.maxChars),
        limit: readNumber(query.limit),
        selector: readString(query.selector),
        interactive: query.interactive === true,
        compact: query.compact === true,
        timeout_ms: request.timeoutMs,
      });
    }
    if (method === "GET" && path === "/console") {
      await focusTargetIfRequested(
        broker,
        materialized.ledger_context.id,
        readString(request.query?.targetId),
      );
      return broker.listConsoleMessages(materialized.ledger_context.id, {
        level: readString(request.query?.level),
        limit: readNumber(request.query?.limit),
      });
    }
    if (method === "POST" && path === "/tabs/focus") {
      const body = readRecord(request.body);
      const tab = await broker.focusContext(materialized.ledger_context.id, {
        target_id: requiredString(body.targetId, "targetId"),
      });
      return {
        ok: true,
        ...toBrowserTabResult(tab),
      };
    }
    if (method === "POST" && path === "/tabs/open") {
      const body = readRecord(request.body);
      const url = requiredString(body.url, "url");
      const tab = await broker.navigateContext(materialized.ledger_context.id, {
        url,
        timeout_ms: request.timeoutMs,
      });
      return {
        ok: true,
        ...toBrowserTabResult(tab),
      };
    }
    if (method === "POST" && path === "/navigate") {
      const body = readRecord(request.body);
      await focusTargetIfRequested(
        broker,
        materialized.ledger_context.id,
        readString(body.targetId),
      );
      const tab = await broker.navigateContext(materialized.ledger_context.id, {
        url: requiredString(body.url, "url"),
        timeout_ms: request.timeoutMs,
      });
      return {
        ok: true,
        ...toBrowserTabResult(tab),
      };
    }
    if (method === "POST" && path === "/screenshot") {
      const body = readRecord(request.body);
      await focusTargetIfRequested(
        broker,
        materialized.ledger_context.id,
        readString(body.targetId),
      );
      const format = body.type === "jpeg" ? "jpeg" : "png";
      const result = await broker.captureScreenshotArtifact(materialized.ledger_context.id, {
        format,
        full_page: body.fullPage === true,
        retention_policy: "debug",
        subject: input.subject,
        timeout_ms: request.timeoutMs,
      });
      const artifactBytes = await client.readArtifact({
        id: result.artifact.id,
        subject: input.subject,
      });
      const extension = format === "jpeg" ? "jpg" : "png";
      const path = join(tempDir, `${result.artifact.sha256}.${extension}`);
      await writeFile(path, Buffer.from(artifactBytes.content_base64, "base64"));
      return {
        ok: true,
        path,
        artifact: result.artifact,
        artifactId: result.artifact.id,
        targetId: materialized.target_id,
        sparsekernelContextId: materialized.ledger_context.id,
      };
    }
    if (method === "POST" && path === "/pdf") {
      const body = readRecord(request.body);
      await focusTargetIfRequested(
        broker,
        materialized.ledger_context.id,
        readString(body.targetId),
      );
      const result = await broker.capturePdfArtifact(materialized.ledger_context.id, {
        retention_policy: "debug",
        subject: input.subject,
      });
      const artifactBytes = await client.readArtifact({
        id: result.artifact.id,
        subject: input.subject,
      });
      const path = join(tempDir, `${result.artifact.sha256}.pdf`);
      await writeFile(path, Buffer.from(artifactBytes.content_base64, "base64"));
      return {
        ok: true,
        path,
        artifact: result.artifact,
        artifactId: result.artifact.id,
        targetId: materialized.target_id,
        sparsekernelContextId: materialized.ledger_context.id,
      };
    }
    if (method === "POST" && path === "/hooks/dialog") {
      const body = readRecord(request.body);
      await focusTargetIfRequested(
        broker,
        materialized.ledger_context.id,
        readString(body.targetId),
      );
      return broker.armDialog(materialized.ledger_context.id, {
        accept: body.accept !== false,
        prompt_text: readString(body.promptText),
        timeout_ms: readNumber(body.timeoutMs) ?? request.timeoutMs,
      });
    }
    if (method === "POST" && path === "/hooks/file-chooser") {
      const body = readRecord(request.body);
      return await broker.uploadFiles(materialized.ledger_context.id, {
        paths: Array.isArray(body.paths) ? body.paths.map((path) => String(path)) : [],
        ref: readString(body.ref),
        input_ref: readString(body.inputRef),
        selector: readString(body.selector) ?? readString(body.element),
        target_id: readString(body.targetId),
        timeout_ms: readNumber(body.timeoutMs) ?? request.timeoutMs,
      });
    }
    if (method === "DELETE" && path.startsWith("/tabs/")) {
      await release();
      return {
        ok: true,
        targetId: decodeURIComponent(path.slice("/tabs/".length)),
      };
    }
    if (method === "POST" && path === "/act") {
      const body = readRecord(request.body);
      if (body.kind === "close") {
        await release();
        return {
          ok: true,
          targetId: materialized.target_id,
        };
      }
      return await broker.actContext(materialized.ledger_context.id, readActRequest(body));
    }
    throw new Error(`SparseKernel CDP browser proxy does not support ${method} ${path} yet.`);
  };

  return {
    id: materialized.ledger_context.id,
    proxyRequest,
    release,
  };
}

function statusPayload(context: MaterializedBrowserContext, running: boolean) {
  return {
    ok: true,
    running,
    transport: "sparsekernel-cdp",
    sparsekernel: true,
    contextId: context.ledger_context.id,
    targetId: context.target_id,
    cdpEndpoint: context.cdp_endpoint,
  };
}

function toBrowserTabResult(tab: SparseKernelBrowserTab): Record<string, unknown> {
  return {
    targetId: tab.targetId,
    suggestedTargetId: tab.suggestedTargetId,
    title: tab.title,
    url: tab.url,
    type: tab.type,
    sparsekernelContextId: tab.sparsekernelContextId,
  };
}

async function focusTargetIfRequested(
  broker: SparseKernelCdpBrowserBroker,
  contextId: string,
  targetId: string | undefined,
): Promise<void> {
  if (targetId) {
    await broker.focusContext(contextId, { target_id: targetId });
  }
}

function normalizePath(path: string): string {
  const trimmed = path.trim();
  if (!trimmed) {
    return "/";
  }
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}

function readRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function readNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function readActRequest(value: Record<string, unknown>): SparseKernelBrowserActRequest {
  const kind = readString(value.kind);
  switch (kind) {
    case "click":
      return {
        kind,
        ref: readString(value.ref),
        selector: readString(value.selector),
        targetId: readString(value.targetId),
        doubleClick: value.doubleClick === true,
        button: readString(value.button),
        modifiers: readStringArray(value.modifiers),
        delayMs: readNumber(value.delayMs),
        timeoutMs: readNumber(value.timeoutMs),
      } as const;
    case "clickCoords":
      return {
        kind,
        x: readNumber(value.x) ?? 0,
        y: readNumber(value.y) ?? 0,
        targetId: readString(value.targetId),
        doubleClick: value.doubleClick === true,
        button: readString(value.button),
        delayMs: readNumber(value.delayMs),
        timeoutMs: readNumber(value.timeoutMs),
      } as const;
    case "type":
      return {
        kind,
        ref: readString(value.ref),
        selector: readString(value.selector),
        text: typeof value.text === "string" ? value.text : "",
        targetId: readString(value.targetId),
        submit: value.submit === true,
        slowly: value.slowly === true,
        timeoutMs: readNumber(value.timeoutMs),
      } as const;
    case "press":
      return {
        kind,
        key: readString(value.key) ?? "Enter",
        targetId: readString(value.targetId),
        delayMs: readNumber(value.delayMs),
      } as const;
    case "hover":
    case "scrollIntoView":
      return {
        kind,
        ref: readString(value.ref),
        selector: readString(value.selector),
        targetId: readString(value.targetId),
        timeoutMs: readNumber(value.timeoutMs),
      } as const;
    case "drag":
      return {
        kind,
        startRef: readString(value.startRef),
        startSelector: readString(value.startSelector),
        endRef: readString(value.endRef),
        endSelector: readString(value.endSelector),
        targetId: readString(value.targetId),
        timeoutMs: readNumber(value.timeoutMs),
      } as const;
    case "select":
      return {
        kind,
        ref: readString(value.ref),
        selector: readString(value.selector),
        values: readStringArray(value.values),
        targetId: readString(value.targetId),
        timeoutMs: readNumber(value.timeoutMs),
      } as const;
    case "fill":
      return {
        kind,
        fields: readFormFields(value.fields),
        targetId: readString(value.targetId),
        timeoutMs: readNumber(value.timeoutMs),
      } as const;
    case "wait":
      return {
        kind,
        timeMs: readNumber(value.timeMs),
        text: readString(value.text),
        textGone: readString(value.textGone),
        selector: readString(value.selector),
        url: readString(value.url),
        loadState: readLoadState(value.loadState),
        fn: readString(value.fn),
        targetId: readString(value.targetId),
        timeoutMs: readNumber(value.timeoutMs),
      } as const;
    case "evaluate":
      return {
        kind,
        fn: readString(value.fn) ?? "",
        ref: readString(value.ref),
        targetId: readString(value.targetId),
        timeoutMs: readNumber(value.timeoutMs),
      } as const;
    case "resize":
      return {
        kind,
        width: readNumber(value.width) ?? 1280,
        height: readNumber(value.height) ?? 720,
        targetId: readString(value.targetId),
      } as const;
    case "batch":
      return {
        kind,
        actions: Array.isArray(value.actions)
          ? value.actions.map((action) => readActRequest(readRecord(action)))
          : [],
        targetId: readString(value.targetId),
        stopOnError: value.stopOnError === false ? false : undefined,
      } as const;
    default:
      throw new Error(`SparseKernel CDP browser proxy does not support act kind: ${kind ?? ""}`);
  }
}

function readStringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.map((entry) => String(entry).trim()).filter((entry) => entry.length > 0)
    : [];
}

function readLoadState(value: unknown): "load" | "domcontentloaded" | "networkidle" | undefined {
  const state = readString(value);
  return state === "load" || state === "domcontentloaded" || state === "networkidle"
    ? state
    : undefined;
}

function readFormFields(value: unknown): Array<{
  ref: string;
  type?: string;
  value?: string | number | boolean;
}> {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((entry) => {
    const record = readRecord(entry);
    const rawValue = record.value;
    const type = readString(record.type);
    const parsedValue =
      typeof rawValue === "string" || typeof rawValue === "number" || typeof rawValue === "boolean"
        ? rawValue
        : undefined;
    return {
      ref: readString(record.ref) ?? "",
      ...(type ? { type } : {}),
      ...(parsedValue !== undefined ? { value: parsedValue } : {}),
    };
  });
}

function requiredString(value: unknown, field: string): string {
  const text = readString(value);
  if (!text) {
    throw new Error(`${field} required`);
  }
  return text;
}
