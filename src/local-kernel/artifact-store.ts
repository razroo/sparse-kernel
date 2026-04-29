import crypto from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import type { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
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

export type ArtifactStoreStreamInput = Omit<ArtifactStoreWriteInput, "bytes"> & {
  stream: Readable;
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

function chunkToBuffer(chunk: Buffer | Uint8Array | string): Buffer {
  return typeof chunk === "string" ? Buffer.from(chunk) : Buffer.from(chunk);
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
    return this.recordStoredArtifact({
      sha256,
      sizeBytes: bytes.byteLength,
      storageRef,
      input,
    });
  }

  async writeStream(input: ArtifactStoreStreamInput): Promise<ArtifactRecord> {
    const tmp = await this.writeStreamToTemp(input.stream);
    const storageRef = resolveArtifactStorageRef(tmp.sha256);
    const storagePath = path.join(this.rootDir, storageRef);
    await fs.mkdir(path.dirname(storagePath), { recursive: true, mode: 0o700 });
    try {
      await fs.rename(tmp.tmpPath, storagePath);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST") {
        await fs.rm(tmp.tmpPath, { force: true }).catch(() => undefined);
        throw err;
      }
      await fs.rm(tmp.tmpPath, { force: true });
    }
    await fs.chmod(storagePath, 0o600).catch(() => undefined);
    return this.recordStoredArtifact({
      sha256: tmp.sha256,
      sizeBytes: tmp.sizeBytes,
      storageRef,
      input,
    });
  }

  private recordStoredArtifact(params: {
    sha256: string;
    sizeBytes: number;
    storageRef: string;
    input: Omit<ArtifactStoreWriteInput, "bytes">;
  }): ArtifactRecord {
    const artifact = this.db.recordArtifact({
      sha256: params.sha256,
      mimeType: params.input.mimeType,
      sizeBytes: params.sizeBytes,
      storageRef: params.storageRef,
      createdByTaskId: params.input.createdByTaskId,
      createdByToolCallId: params.input.createdByToolCallId,
      classification: params.input.classification,
      retentionPolicy: params.input.retentionPolicy,
    });
    if (params.input.subject) {
      this.db.grantArtifactAccess({
        artifactId: artifact.id,
        subjectType: params.input.subject.subjectType,
        subjectId: params.input.subject.subjectId,
        permission: params.input.subject.permission ?? "read",
        expiresAt: params.input.subject.expiresAt,
      });
    }
    return artifact;
  }

  async importFile(input: ArtifactStoreFileInput): Promise<ArtifactRecord> {
    return await this.writeStream({
      ...input,
      stream: fsSync.createReadStream(input.filePath),
    });
  }

  private async writeStreamToTemp(stream: Readable): Promise<{
    tmpPath: string;
    sha256: string;
    sizeBytes: number;
  }> {
    const tmpDir = path.join(this.rootDir, ".tmp");
    await fs.mkdir(tmpDir, { recursive: true, mode: 0o700 });
    const tmpPath = path.join(
      tmpDir,
      `artifact-${process.pid}-${Date.now()}-${crypto.randomUUID()}.tmp`,
    );
    const hash = crypto.createHash("sha256");
    let sizeBytes = 0;
    stream.on("data", (chunk: Buffer | Uint8Array | string) => {
      const bytes = chunkToBuffer(chunk);
      hash.update(bytes);
      sizeBytes += bytes.byteLength;
    });
    try {
      await pipeline(
        stream,
        fsSync.createWriteStream(tmpPath, {
          flags: "wx",
          mode: 0o600,
        }),
      );
      return {
        tmpPath,
        sha256: hash.digest("hex"),
        sizeBytes,
      };
    } catch (err) {
      await fs.rm(tmpPath, { force: true }).catch(() => undefined);
      throw err;
    }
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
