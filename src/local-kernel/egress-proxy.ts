import { request as httpRequest, createServer, type IncomingMessage, type Server } from "node:http";
import { request as httpsRequest } from "node:https";
import net from "node:net";
import type { Duplex } from "node:stream";
import { URL } from "node:url";
import type { LocalKernelDatabase } from "./database.js";
import { checkTrustZoneNetworkUrlWithDns } from "./network-policy.js";

export type LoopbackEgressProxyHandle = {
  url: string;
  host: string;
  port: number;
  close: () => Promise<void>;
};

export type LoopbackEgressProxyOptions = {
  db: LocalKernelDatabase;
  trustZoneId: string;
  host?: string;
  port?: number;
  actor?: { type: string; id?: string };
};

const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

export async function startLoopbackEgressProxy(
  options: LoopbackEgressProxyOptions,
): Promise<LoopbackEgressProxyHandle> {
  const host = normalizeLoopbackHost(options.host);
  const server = createServer((request, response) => {
    void handleProxyRequest(options, request, response).catch((error) => {
      response.statusCode = 502;
      response.end(error instanceof Error ? error.message : String(error));
    });
  });
  server.on("connect", (request, clientSocket, head) => {
    void handleConnectRequest(options, request, clientSocket, head).catch((error) => {
      clientSocket.end(
        `HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\n\r\n${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    });
  });
  await listen(server, host, options.port ?? 0);
  const address = server.address();
  if (!address || typeof address === "string") {
    await closeServer(server).catch(() => {});
    throw new Error("SparseKernel egress proxy failed to bind a TCP address.");
  }
  return {
    url: `http://${host}:${address.port}/`,
    host,
    port: address.port,
    close: async () => await closeServer(server),
  };
}

async function handleProxyRequest(
  options: LoopbackEgressProxyOptions,
  request: IncomingMessage,
  response: import("node:http").ServerResponse,
): Promise<void> {
  const target = resolveProxyRequestUrl(request);
  const decision = await checkTrustZoneNetworkUrlWithDns({
    db: options.db,
    trustZoneId: options.trustZoneId,
    url: target.toString(),
    actor: options.actor ?? { type: "egress_proxy", id: options.trustZoneId },
  });
  if (!decision.allowed) {
    options.db.recordAudit({
      actor: options.actor ?? { type: "egress_proxy", id: options.trustZoneId },
      action: "egress_proxy.denied",
      objectType: "network_request",
      objectId: target.origin,
      payload: { url: target.toString(), reason: decision.reason },
    });
    response.writeHead(403, { "content-type": "text/plain" });
    response.end(`SparseKernel egress denied: ${decision.reason}`);
    return;
  }

  const upstream = (target.protocol === "https:" ? httpsRequest : httpRequest)(
    target,
    {
      method: request.method,
      headers: sanitizeProxyHeaders(request.headers),
    },
    (upstreamResponse) => {
      response.writeHead(
        upstreamResponse.statusCode ?? 502,
        sanitizeProxyHeaders(upstreamResponse.headers),
      );
      upstreamResponse.pipe(response);
    },
  );
  upstream.on("error", (error) => {
    response.statusCode = 502;
    response.end(error.message);
  });
  request.pipe(upstream);
}

async function handleConnectRequest(
  options: LoopbackEgressProxyOptions,
  request: IncomingMessage,
  clientSocket: Duplex,
  head: Buffer,
): Promise<void> {
  const target = parseConnectTarget(request.url);
  const policyUrl = `https://${formatHostForUrl(target.hostname)}:${target.port}/`;
  const decision = await checkTrustZoneNetworkUrlWithDns({
    db: options.db,
    trustZoneId: options.trustZoneId,
    url: policyUrl,
    actor: options.actor ?? { type: "egress_proxy", id: options.trustZoneId },
  });
  if (!decision.allowed) {
    options.db.recordAudit({
      actor: options.actor ?? { type: "egress_proxy", id: options.trustZoneId },
      action: "egress_proxy.denied_connect",
      objectType: "network_request",
      objectId: target.hostname,
      payload: { target, reason: decision.reason },
    });
    clientSocket.end(
      `HTTP/1.1 403 Forbidden\r\n\r\nSparseKernel egress denied: ${decision.reason}`,
    );
    return;
  }

  const upstreamSocket = net.connect(target.port, target.hostname, () => {
    clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
    if (head.length > 0) {
      upstreamSocket.write(head);
    }
    clientSocket.pipe(upstreamSocket);
    upstreamSocket.pipe(clientSocket);
  });
  upstreamSocket.on("error", () => {
    clientSocket.end();
  });
}

function resolveProxyRequestUrl(request: IncomingMessage): URL {
  if (request.url?.startsWith("http://") || request.url?.startsWith("https://")) {
    return new URL(request.url);
  }
  const host = request.headers.host;
  if (!host) {
    throw new Error("SparseKernel egress proxy request is missing Host header.");
  }
  return new URL(`http://${host}${request.url ?? "/"}`);
}

function parseConnectTarget(rawTarget: string | undefined): { hostname: string; port: number } {
  const target = rawTarget?.trim();
  if (!target) {
    throw new Error("CONNECT target is required.");
  }
  if (target.startsWith("[")) {
    const match = target.match(/^\[([^\]]+)\](?::(\d+))?$/);
    if (!match) {
      throw new Error("Invalid CONNECT target.");
    }
    return {
      hostname: match[1] ?? "::1",
      port: parsePort(match[2], 443),
    };
  }
  const lastColon = target.lastIndexOf(":");
  if (lastColon <= 0 || lastColon === target.length - 1) {
    return { hostname: target, port: 443 };
  }
  return {
    hostname: target.slice(0, lastColon),
    port: parsePort(target.slice(lastColon + 1), 443),
  };
}

function parsePort(raw: string | undefined, fallback: number): number {
  const port = raw ? Number(raw) : fallback;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("Invalid CONNECT target port.");
  }
  return port;
}

function sanitizeProxyHeaders(
  headers: IncomingMessage["headers"],
): Record<string, string | string[]> {
  const sanitized: Record<string, string | string[]> = {};
  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined || HOP_BY_HOP_HEADERS.has(name.toLowerCase())) {
      continue;
    }
    sanitized[name] = value;
  }
  return sanitized;
}

function normalizeLoopbackHost(host: string | undefined): string {
  const normalized = host?.trim() || "127.0.0.1";
  if (normalized !== "127.0.0.1" && normalized !== "localhost" && normalized !== "::1") {
    throw new Error("SparseKernel egress proxy only binds loopback addresses.");
  }
  return normalized;
}

function formatHostForUrl(host: string): string {
  return net.isIP(host) === 6 ? `[${host}]` : host;
}

async function listen(server: Server, host: string, port: number): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => {
      server.off("error", reject);
      resolve();
    });
  });
}

async function closeServer(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}
