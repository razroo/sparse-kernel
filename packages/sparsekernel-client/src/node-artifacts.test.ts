import { access, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import type {
  SparseKernelArtifact,
  SparseKernelExportArtifactFileResult,
  SparseKernelImportArtifactFileInput,
} from "./index.js";
import {
  createArtifactFromLocalFile,
  defaultSparseKernelArtifactStagingDir,
  exportArtifactToLocalFile,
} from "./node-artifacts.js";

describe("SparseKernel Node artifact helpers", () => {
  it("stages local file imports without base64 transport and cleans the stage", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "sparsekernel-client-test-"));
    try {
      const source = path.join(root, "source.bin");
      const stagingDir = path.join(root, "staging");
      await writeFile(source, "large artifact");
      let imported: SparseKernelImportArtifactFileInput | undefined;
      let stagedBody = "";
      const client = {
        async importArtifactFile(
          input: SparseKernelImportArtifactFileInput,
        ): Promise<SparseKernelArtifact> {
          imported = input;
          stagedBody = await readFile(input.staged_path, "utf8");
          return {
            id: "artifact-1",
            sha256: "a".repeat(64),
            size_bytes: stagedBody.length,
            storage_ref: "sha256/aa/aa/hash",
            mime_type: input.mime_type,
            retention_policy: input.retention_policy,
            created_at: "2026-04-29T00:00:00Z",
          };
        },
      };

      const artifact = await createArtifactFromLocalFile(client, {
        filePath: source,
        stagingDir,
        stagedName: "unsafe/name?.bin",
        mime_type: "application/octet-stream",
        retention_policy: "durable",
      });

      expect(artifact).toMatchObject({ id: "artifact-1", size_bytes: 14 });
      expect(stagedBody).toBe("large artifact");
      expect(imported?.staged_path.startsWith(stagingDir)).toBe(true);
      expect(path.basename(imported?.staged_path ?? "")).toBe("name_.bin");
      await expect(access(path.dirname(imported?.staged_path ?? ""))).rejects.toThrow();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("copies daemon staged exports to a caller destination and cleans the exported file", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "sparsekernel-client-test-"));
    try {
      const exportDir = path.join(root, "exports");
      const stagedPath = path.join(exportDir, "artifact.bin");
      const destination = path.join(root, "downloads", "artifact.bin");
      await mkdir(exportDir, { recursive: true });
      await writeFile(stagedPath, "exported artifact");
      const client = {
        async exportArtifactFile(input: {
          id: string;
          file_name?: string | null;
        }): Promise<SparseKernelExportArtifactFileResult> {
          expect(input).toMatchObject({ id: "artifact-1", file_name: "artifact.bin" });
          return {
            artifact: {
              id: "artifact-1",
              sha256: "b".repeat(64),
              size_bytes: 17,
              storage_ref: "sha256/bb/bb/hash",
              created_at: "2026-04-29T00:00:00Z",
            },
            staged_path: stagedPath,
          };
        },
      };

      const exported = await exportArtifactToLocalFile(client, {
        id: "artifact-1",
        destinationPath: destination,
      });

      expect(exported.artifact.id).toBe("artifact-1");
      await expect(readFile(destination, "utf8")).resolves.toBe("exported artifact");
      await expect(access(stagedPath)).rejects.toThrow();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("exposes the staging directory used by local Node clients", () => {
    const previousHome = process.env.SPARSEKERNEL_HOME;
    const previousState = process.env.OPENCLAW_STATE_DIR;
    const previousStaging = process.env.SPARSEKERNEL_ARTIFACT_STAGING_DIR;
    try {
      process.env.SPARSEKERNEL_HOME = path.join("tmp", "sparsekernel-home");
      delete process.env.OPENCLAW_STATE_DIR;
      delete process.env.SPARSEKERNEL_ARTIFACT_STAGING_DIR;
      expect(defaultSparseKernelArtifactStagingDir()).toBe(
        path.join("tmp", "sparsekernel-home", "artifacts", ".staging"),
      );

      process.env.SPARSEKERNEL_ARTIFACT_STAGING_DIR = path.join("tmp", "custom-staging");
      expect(defaultSparseKernelArtifactStagingDir()).toBe(path.join("tmp", "custom-staging"));
    } finally {
      restoreEnv("SPARSEKERNEL_HOME", previousHome);
      restoreEnv("OPENCLAW_STATE_DIR", previousState);
      restoreEnv("SPARSEKERNEL_ARTIFACT_STAGING_DIR", previousStaging);
    }
  });
});

function restoreEnv(name: string, value: string | undefined): void {
  if (value === undefined) {
    delete process.env[name];
    return;
  }
  process.env[name] = value;
}
