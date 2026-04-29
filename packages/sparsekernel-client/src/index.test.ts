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
});
