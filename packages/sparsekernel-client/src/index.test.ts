import { describe, expect, it, vi } from "vitest";
import {
  isSparseKernelProtocolCompatible,
  SparseKernelClient,
  SPARSEKERNEL_PROTOCOL_VERSION,
} from "./index.js";

describe("SparseKernelClient protocol compatibility", () => {
  it("accepts legacy health responses and matching protocol majors", async () => {
    expect(isSparseKernelProtocolCompatible({})).toBe(true);
    expect(
      isSparseKernelProtocolCompatible({
        protocol_version: "2026-05-01.v1",
      }),
    ).toBe(true);

    const fetchImpl = vi.fn(async () =>
      Response.json({
        ok: true,
        service: "sparsekerneld",
        version: "0.1.0",
        protocol_version: SPARSEKERNEL_PROTOCOL_VERSION,
        schema_version: 2,
      }),
    ) as unknown as typeof fetch;
    const health = await new SparseKernelClient({ fetchImpl }).assertCompatible();
    expect(health.protocol_version).toBe(SPARSEKERNEL_PROTOCOL_VERSION);
  });

  it("rejects incompatible daemon protocol majors", async () => {
    const fetchImpl = vi.fn(async () =>
      Response.json({
        ok: true,
        service: "sparsekerneld",
        version: "0.1.0",
        protocol_version: "2026-04-29.v9",
        schema_version: 2,
      }),
    ) as unknown as typeof fetch;
    await expect(new SparseKernelClient({ fetchImpl }).assertCompatible()).rejects.toThrow(
      /protocol mismatch/,
    );
  });

  it("posts staged artifact import and export requests", async () => {
    const artifact = {
      id: "artifact-a",
      sha256: "0".repeat(64),
      size_bytes: 4,
      storage_ref: "sha256/00/00/hash",
      created_at: "2026-04-29T00:00:00Z",
    };
    const requests: Array<{ path: string; body: unknown }> = [];
    const fetchImpl = vi.fn(async (input, init) => {
      const url = new URL(String(input));
      requests.push({
        path: url.pathname,
        body: JSON.parse(String(init?.body)),
      });
      if (url.pathname === "/artifacts/export-file") {
        return Response.json({ artifact, staged_path: "/tmp/exported.bin" });
      }
      return Response.json(artifact);
    }) as unknown as typeof fetch;
    const client = new SparseKernelClient({ fetchImpl });

    await expect(
      client.importArtifactFile({
        staged_path: "upload.bin",
        mime_type: "application/octet-stream",
      }),
    ).resolves.toMatchObject({ id: "artifact-a" });
    await expect(
      client.exportArtifactFile({
        id: "artifact-a",
        file_name: "download.bin",
      }),
    ).resolves.toMatchObject({ staged_path: "/tmp/exported.bin" });
    expect(requests).toEqual([
      {
        path: "/artifacts/import-file",
        body: {
          staged_path: "upload.bin",
          mime_type: "application/octet-stream",
        },
      },
      {
        path: "/artifacts/export-file",
        body: {
          id: "artifact-a",
          file_name: "download.bin",
        },
      },
    ]);
  });

  it("calls trust-zone proxy and supervised egress APIs", async () => {
    const requests: Array<{ path: string; body?: unknown }> = [];
    const fetchImpl = vi.fn(async (input, init) => {
      const url = new URL(String(input));
      requests.push({
        path: url.pathname,
        ...(init?.body ? { body: JSON.parse(String(init.body)) } : {}),
      });
      if (url.pathname === "/egress-proxies") {
        return Response.json([]);
      }
      return Response.json({
        trust_zone_id: "public_web",
        network_policy_id: "public_web_default",
        proxy_ref: "http://127.0.0.1:18080/",
      });
    }) as unknown as typeof fetch;
    const client = new SparseKernelClient({ fetchImpl });

    await client.attachTrustZoneProxyRef({
      trust_zone_id: "public_web",
      proxy_ref: "http://127.0.0.1:18080/",
    });
    await client.startEgressProxy({ trust_zone_id: "public_web", port: 18080 });
    await client.egressProxies();
    await client.stopEgressProxy({ trust_zone_id: "public_web", clear_proxy_ref: true });

    expect(requests.map((request) => request.path)).toEqual([
      "/trust-zones/proxy-ref",
      "/egress-proxies/start",
      "/egress-proxies",
      "/egress-proxies/stop",
    ]);
  });
});
