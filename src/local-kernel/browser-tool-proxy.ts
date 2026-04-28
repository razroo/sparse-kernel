export const SPARSEKERNEL_BROWSER_PROXY_REQUEST_SYMBOL = Symbol.for(
  "openclaw.sparsekernel.browserProxyRequest",
);

export type SparseKernelBrowserProxyRequest = (opts: {
  method: string;
  path: string;
  query?: Record<string, string | number | boolean | undefined>;
  body?: unknown;
  timeoutMs?: number;
  profile?: string;
}) => Promise<unknown>;

export function attachSparseKernelBrowserProxyRequest(
  params: unknown,
  proxyRequest: SparseKernelBrowserProxyRequest,
): unknown {
  if (!params || typeof params !== "object" || Array.isArray(params)) {
    return params;
  }
  const copy = { ...(params as Record<string, unknown>) };
  Object.defineProperty(copy, SPARSEKERNEL_BROWSER_PROXY_REQUEST_SYMBOL, {
    value: proxyRequest,
    enumerable: false,
  });
  return copy;
}
