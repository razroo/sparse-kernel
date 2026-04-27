import crypto from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import type { LocalKernelDatabase } from "./database.js";
import { resolveArtifactStorageRef, resolveArtifactStoreRoot } from "./paths.js";
import type { ArtifactRecord, RuntimeRetentionPolicy } from "./types.js";

export type ArtifactStoreWriteInput = {
  bytes: Buffer | Uint8Array | string;
  mimeType?: string;
  createdByTaskId?: string;
  createdByToolCallId?: string;
  classification?: string;
  retentionPolicy?: RuntimeRetentionPolicy;
  subject?: {
    subjectType: string;
    subjectId: string;
    permission?: string;
    expiresAt?: string;
  };
};

export type ArtifactStoreFileInput = Omit<ArtifactStoreWriteInput, "bytes"> & {
  filePath: string;
};

function normalizeBytes(bytes: Buffer | Uint8Array | string): Buffer {
  if (typeof bytes === "string") {
    return Buffer.from(bytes);
  }
  return Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
}

function sha256Hex(bytes: Buffer): string {
  return crypto.createHash("sha256").update(bytes).digest("hex");
}

export class ContentAddressedArtifactStore {
  constructor(
    private readonly db: LocalKernelDatabase,
    readonly rootDir: string = resolveArtifactStoreRoot(),
  ) {}

  async write(input: ArtifactStoreWriteInput): Promise<ArtifactRecord> {
    const bytes = normalizeBytes(input.bytes);
    const sha256 = sha256Hex(bytes);
    const storageRef = resolveArtifactStorageRef(sha256);
    const storagePath = path.join(this.rootDir, storageRef);
    await fs.mkdir(path.dirname(storagePath), { recursive: true, mode: 0o700 });
    try {
      await fs.writeFile(storagePath, bytes, { flag: "wx", mode: 0o600 });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST") {
        throw err;
      }
    }
    const artifact = this.db.recordArtifact({
      sha256,
      mimeType: input.mimeType,
      sizeBytes: bytes.byteLength,
      storageRef,
      createdByTaskId: input.createdByTaskId,
      createdByToolCallId: input.createdByToolCallId,
      classification: input.classification,
      retentionPolicy: input.retentionPolicy,
    });
    if (input.subject) {
      this.db.grantArtifactAccess({
        artifactId: artifact.id,
        subjectType: input.subject.subjectType,
        subjectId: input.subject.subjectId,
        permission: input.subject.permission ?? "read",
        expiresAt: input.subject.expiresAt,
      });
    }
    return artifact;
  }

  async importFile(input: ArtifactStoreFileInput): Promise<ArtifactRecord> {
    const bytes = await fs.readFile(input.filePath);
    return await this.write({ ...input, bytes });
  }

  async read(
    artifactId: string,
    subject?: { subjectType: string; subjectId: string; permission?: string },
  ): Promise<Buffer> {
    const artifact = this.db.getArtifact(artifactId);
    if (!artifact) {
      throw new Error(`Artifact not found: ${artifactId}`);
    }
    if (
      subject &&
      !this.db.hasArtifactAccess({
        artifactId,
        subjectType: subject.subjectType,
        subjectId: subject.subjectId,
        permission: subject.permission ?? "read",
      })
    ) {
      this.db.recordAudit({
        actor: { type: subject.subjectType, id: subject.subjectId },
        action: "artifact_access.denied",
        objectType: "artifact",
        objectId: artifactId,
        payload: { permission: subject.permission ?? "read" },
      });
      throw new Error(`Artifact access denied: ${artifactId}`);
    }
    return await fs.readFile(path.join(this.rootDir, artifact.storageRef));
  }

  prune(params: { olderThan: string; retentionPolicies?: RuntimeRetentionPolicy[] }): {
    artifacts: ArtifactRecord[];
    deletedFiles: number;
  } {
    const artifacts = this.db.pruneArtifacts(params);
    let deletedFiles = 0;
    for (const artifact of artifacts) {
      const artifactPath = path.join(this.rootDir, artifact.storageRef);
      try {
        fsSync.unlinkSync(artifactPath);
        deletedFiles += 1;
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
          throw err;
        }
      }
    }
    return { artifacts, deletedFiles };
  }
}
