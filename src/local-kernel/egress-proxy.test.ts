import { createServer, request as httpRequest, type Server } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { LocalKernelDatabase } from "./database.js";
import { startLoopbackEgressProxy } from "./egress-proxy.js";

const servers: Server[] = [];

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => resolve());
        }),
    ),
  );
});

describe("SparseKernel loopback egress proxy", () => {
  it("proxies allowed HTTP requests through trust-zone policy", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    const upstream = await startUpstream("proxied");
    const proxy = await startLoopbackEgressProxy({
      db,
      trustZoneId: "public_web",
    });
    try {
      db.upsertNetworkPolicy({
        id: "public_web_default",
        defaultAction: "deny",
        allowPrivateNetwork: true,
        allowedHosts: ["127.0.0.1"],
      });

      await expect(requestViaProxy(proxy.url, `${upstream.url}/demo`)).resolves.toEqual({
        statusCode: 200,
        body: "proxied",
      });
    } finally {
      await proxy.close();
      db.close();
    }
  });

  it("denies proxy requests blocked by trust-zone policy and audits the decision", async () => {
    const db = new LocalKernelDatabase({ dbPath: ":memory:" });
    const upstream = await startUpstream("blocked");
    const proxy = await startLoopbackEgressProxy({
      db,
      trustZoneId: "public_web",
    });
    try {
      db.upsertNetworkPolicy({
        id: "public_web_default",
        defaultAction: "deny",
        allowPrivateNetwork: true,
        allowedHosts: [],
      });

      await expect(requestViaProxy(proxy.url, `${upstream.url}/demo`)).resolves.toMatchObject({
        statusCode: 403,
      });
      expect(db.listAudit({ limit: 20 }).map((entry) => entry.action)).toEqual(
        expect.arrayContaining(["network_policy.denied_default", "egress_proxy.denied"]),
      );
    } finally {
      await proxy.close();
      db.close();
    }
  });
});

async function startUpstream(body: string): Promise<{ url: string }> {
  const server = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "text/plain" });
    response.end(body);
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("upstream did not bind a TCP port");
  }
  return { url: `http://127.0.0.1:${address.port}` };
}

async function requestViaProxy(
  proxyUrl: string,
  targetUrl: string,
): Promise<{ statusCode: number; body: string }> {
  const proxy = new URL(proxyUrl);
  return await new Promise<{ statusCode: number; body: string }>((resolve, reject) => {
    const request = httpRequest(
      {
        host: proxy.hostname,
        port: Number(proxy.port),
        method: "GET",
        path: targetUrl,
        headers: { host: new URL(targetUrl).host },
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        response.on("end", () =>
          resolve({
            statusCode: response.statusCode ?? 0,
            body: Buffer.concat(chunks).toString("utf8"),
          }),
        );
      },
    );
    request.on("error", reject);
    request.end();
  });
}
