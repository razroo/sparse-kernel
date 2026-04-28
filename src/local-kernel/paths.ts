import path from "node:path";
import { resolveStateDir } from "../config/paths.js";

export function resolveRuntimeKernelDir(env: NodeJS.ProcessEnv = process.env): string {
  return path.join(resolveStateDir(env), "runtime");
}

export function resolveRuntimeKernelDbPath(env: NodeJS.ProcessEnv = process.env): string {
  return path.join(resolveRuntimeKernelDir(env), "openclaw.sqlite");
}

export function resolveArtifactStoreRoot(env: NodeJS.ProcessEnv = process.env): string {
  return path.join(resolveStateDir(env), "artifacts");
}

export function resolveRuntimeBrowserPoolRoot(env: NodeJS.ProcessEnv = process.env): string {
  return path.join(resolveRuntimeKernelDir(env), "browser-pools");
}

export function resolveArtifactStorageRef(sha256: string): string {
  const normalized = sha256.trim().toLowerCase();
  if (!/^[a-f0-9]{64}$/.test(normalized)) {
    throw new Error(`Invalid artifact sha256: ${sha256}`);
  }
  return path.join("sha256", normalized.slice(0, 2), normalized.slice(2, 4), normalized);
}
