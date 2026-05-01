import { execFileSync } from "node:child_process";
import { describe, expect, it } from "vitest";

describe("docs-list", () => {
  it("omits scoped agent guides and ignored local docs while reporting docs metadata", () => {
    const output = execFileSync(process.execPath, ["scripts/docs-list.js"], {
      encoding: "utf8",
    });

    expect(output).not.toContain("[missing front matter]");
    expect(output).not.toContain("[summary key missing]");
    expect(output).not.toContain("[unterminated front matter]");
    expect(output).not.toMatch(/^AGENTS\.md -/mu);
    expect(output).not.toContain("superpowers/specs/");
    expect(output).toContain("architecture/sparsekernel.md - SparseKernel architecture");
  });
});
