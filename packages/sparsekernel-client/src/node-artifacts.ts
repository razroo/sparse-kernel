import { copyFile, mkdir, mkdtemp, rm } from "node:fs/promises";
import { homedir } from "node:os";
import path from "node:path";
import type {
  SparseKernelArtifact,
  SparseKernelArtifactRetentionPolicy,
  SparseKernelArtifactSubject,
  SparseKernelClient,
  SparseKernelExportArtifactFileResult,
} from "./index.js";

export type { SparseKernelArtifactRetentionPolicy } from "./index.js";

export type SparseKernelCreateArtifactFromLocalFileInput = {
  filePath: string;
  stagingDir: string;
  stagedName?: string | null;
  mime_type?: string | null;
  retention_policy?: SparseKernelArtifactRetentionPolicy | null;
  subject?: SparseKernelArtifactSubject;
  cleanupStagedFile?: boolean;
};

export type SparseKernelExportArtifactToLocalFileInput = {
  id: string;
  destinationPath: string;
  file_name?: string | null;
  subject?: SparseKernelArtifactSubject;
  cleanupExportedFile?: boolean;
};

export type SparseKernelArtifactImportClient = Pick<SparseKernelClient, "importArtifactFile">;
export type SparseKernelArtifactExportClient = Pick<SparseKernelClient, "exportArtifactFile">;

export async function createArtifactFromLocalFile(
  client: SparseKernelArtifactImportClient,
  input: SparseKernelCreateArtifactFromLocalFileInput,
): Promise<SparseKernelArtifact> {
  const stageRoot = path.resolve(input.stagingDir);
  await mkdir(stageRoot, { recursive: true });
  const stageDir = await mkdtemp(path.join(stageRoot, "client-import-"));
  const stagedPath = path.join(
    stageDir,
    sanitizeFileName((input.stagedName ?? path.basename(input.filePath)) || "artifact.bin"),
  );
  try {
    await copyFile(input.filePath, stagedPath);
    return await client.importArtifactFile({
      staged_path: stagedPath,
      mime_type: input.mime_type,
      retention_policy: input.retention_policy,
      subject: input.subject,
    });
  } finally {
    if (input.cleanupStagedFile !== false) {
      await rm(stageDir, { recursive: true, force: true });
    }
  }
}

export async function exportArtifactToLocalFile(
  client: SparseKernelArtifactExportClient,
  input: SparseKernelExportArtifactToLocalFileInput,
): Promise<SparseKernelExportArtifactFileResult> {
  const result = await client.exportArtifactFile({
    id: input.id,
    file_name: input.file_name ?? path.basename(input.destinationPath),
    subject: input.subject,
  });
  try {
    await mkdir(path.dirname(input.destinationPath), { recursive: true });
    await copyFile(result.staged_path, input.destinationPath);
    return result;
  } finally {
    if (input.cleanupExportedFile !== false) {
      await rm(result.staged_path, { force: true });
    }
  }
}

function sanitizeFileName(name: string): string {
  const basename = path.basename(name).replace(/[^A-Za-z0-9._-]/g, "_");
  return basename || "artifact.bin";
}

export function defaultSparseKernelArtifactStagingDir(): string {
  return (
    process.env.SPARSEKERNEL_ARTIFACT_STAGING_DIR ??
    path.join(defaultSparseKernelArtifactRoot(), ".staging")
  );
}

export function defaultSparseKernelArtifactRoot(): string {
  return path.join(defaultSparseKernelHome(), "artifacts");
}

function defaultSparseKernelHome(): string {
  if (process.env.SPARSEKERNEL_HOME) {
    return process.env.SPARSEKERNEL_HOME;
  }
  if (process.env.OPENCLAW_STATE_DIR) {
    return path.join(process.env.OPENCLAW_STATE_DIR, "sparsekernel");
  }
  const home = homedir();
  return home ? path.join(home, ".sparsekernel") : ".sparsekernel";
}
