export type ManagedBrowserCdpEndpointInput = {
  env?: NodeJS.ProcessEnv;
  fetchImpl?: typeof fetch;
  profile?: string;
  timeoutMs?: number;
};

export type ManagedBrowserCdpEndpoint = {
  cdpEndpoint: string;
  source: "static" | "managed-control";
  controlUrl?: string;
};

type BrowserControlStatus = {
  running?: boolean;
  cdpReady?: boolean;
  cdpUrl?: string | null;
  transport?: string;
};

const DEFAULT_BROWSER_CONTROL_URL = "http://127.0.0.1:18791";

export async function resolveSparseKernelBrowserCdpEndpoint(
  input: ManagedBrowserCdpEndpointInput = {},
): Promise<ManagedBrowserCdpEndpoint | null> {
  const env = input.env ?? process.env;
  const staticEndpoint = env.OPENCLAW_SPARSEKERNEL_BROWSER_CDP_ENDPOINT?.trim();
  if (staticEndpoint) {
    return {
      cdpEndpoint: staticEndpoint,
      source: "static",
    };
  }

  const mode = env.OPENCLAW_RUNTIME_BROWSER_BROKER?.trim().toLowerCase();
  const configuredControlUrl =
    env.OPENCLAW_SPARSEKERNEL_BROWSER_CONTROL_URL?.trim() ??
    env.OPENCLAW_BROWSER_CONTROL_URL?.trim() ??
    "";
  const shouldUseManaged =
    mode === "managed" ||
    mode === "managed-cdp" ||
    mode === "sparsekernel-managed" ||
    Boolean(configuredControlUrl);
  if (!shouldUseManaged) {
    return null;
  }

  const controlUrl = normalizeLoopbackControlUrl(
    configuredControlUrl || DEFAULT_BROWSER_CONTROL_URL,
  );
  const fetchImpl = input.fetchImpl ?? fetch;
  const query = input.profile?.trim() ? `?profile=${encodeURIComponent(input.profile.trim())}` : "";
  await fetchBrowserControlJson(`${controlUrl}/start${query}`, {
    method: "POST",
    timeoutMs: input.timeoutMs ?? 15_000,
    fetchImpl,
    env,
  });
  const status = await fetchBrowserControlJson<BrowserControlStatus>(`${controlUrl}/${query}`, {
    method: "GET",
    timeoutMs: input.timeoutMs ?? 5_000,
    fetchImpl,
    env,
  });
  if (!status.running) {
    throw new Error("Managed browser control did not report a running browser.");
  }
  if (status.transport && status.transport !== "cdp") {
    throw new Error(
      `Managed browser control reported unsupported transport for SparseKernel CDP: ${status.transport}`,
    );
  }
  const cdpEndpoint = status.cdpUrl?.trim();
  if (!cdpEndpoint) {
    throw new Error("Managed browser control did not report a CDP endpoint.");
  }
  return {
    cdpEndpoint,
    source: "managed-control",
    controlUrl,
  };
}

function normalizeLoopbackControlUrl(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error("SparseKernel browser control URL must be a valid URL.");
  }
  if (url.protocol !== "http:") {
    throw new Error("SparseKernel browser control URL must use http:// in v0.");
  }
  if (!isLoopbackHost(url.hostname)) {
    throw new Error("SparseKernel browser control URL must be loopback in v0.");
  }
  url.hash = "";
  url.search = "";
  return url.toString().replace(/\/$/, "");
}

async function fetchBrowserControlJson<T = unknown>(
  url: string,
  input: {
    method: "GET" | "POST";
    timeoutMs: number;
    fetchImpl: typeof fetch;
    env: NodeJS.ProcessEnv;
  },
): Promise<T> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(new Error("timed out")), input.timeoutMs);
  try {
    const headers = new Headers();
    const token = input.env.OPENCLAW_SPARSEKERNEL_BROWSER_CONTROL_TOKEN?.trim();
    const password = input.env.OPENCLAW_SPARSEKERNEL_BROWSER_CONTROL_PASSWORD?.trim();
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
    if (password) {
      headers.set("x-openclaw-password", password);
    }
    const response = await input.fetchImpl(url, {
      method: input.method,
      headers,
      signal: ctrl.signal,
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  } finally {
    clearTimeout(timer);
  }
}

function isLoopbackHost(host: string): boolean {
  const normalized = host.toLowerCase();
  return normalized === "localhost" || normalized === "127.0.0.1" || normalized === "::1";
}
