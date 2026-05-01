import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const directCliScripts = [
  "scripts/check.mjs",
  "scripts/run-oxlint.mjs",
  "scripts/run-vitest.mjs",
  "scripts/prepare-extension-package-boundary-artifacts.mjs",
  "scripts/check-extension-package-tsc-boundary.mjs",
  "scripts/sync-plugin-versions.ts",
  "scripts/changelog-add-unreleased.ts",
];

describe("script entrypoints", () => {
  it.each(directCliScripts)("%s uses a Node 22 compatible entrypoint guard", (scriptPath) => {
    const source = readFileSync(scriptPath, "utf8");

    expect(source).not.toContain("import.meta.main");
    expect(source).toContain('import { pathToFileURL } from "node:url";');
    expect(source).toContain('import.meta.url === pathToFileURL(process.argv[1] ?? "").href');
  });
});
