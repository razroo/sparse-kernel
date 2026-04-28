import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LocalKernelDatabase } from "../local-kernel/database.js";
import type { OutputRuntimeEnv } from "../runtime.js";
import {
  runtimeRecoverCommand,
  runtimeSessionsCommand,
  runtimeTasksCommand,
  runtimeTranscriptCommand,
} from "./runtime.js";

const roots: string[] = [];

function tempRoot(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-runtime-command-"));
  roots.push(root);
  return root;
}

function makeRuntime(): OutputRuntimeEnv {
  return {
    log: vi.fn(),
    error: vi.fn(),
    exit: vi.fn((code: number) => {
      throw new Error(`exit ${code}`);
    }),
    writeStdout: vi.fn(),
    writeJson: vi.fn(),
  };
}

async function withStateDir<T>(stateDir: string, fn: () => Promise<T>): Promise<T> {
  const previous = process.env.OPENCLAW_STATE_DIR;
  process.env.OPENCLAW_STATE_DIR = stateDir;
  try {
    return await fn();
  } finally {
    if (previous === undefined) {
      delete process.env.OPENCLAW_STATE_DIR;
    } else {
      process.env.OPENCLAW_STATE_DIR = previous;
    }
  }
}

afterEach(() => {
  for (const root of roots.splice(0)) {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

describe("SparseKernel runtime commands", () => {
  it("lists sessions, tasks, transcript events, and recovers stale embedded runs", async () => {
    const stateDir = tempRoot();
    await withStateDir(stateDir, async () => {
      const db = new LocalKernelDatabase();
      try {
        db.upsertSession({ id: "session-a", agentId: "main", sessionKey: "agent:main:main" });
        db.appendTranscriptEvent({
          sessionId: "session-a",
          role: "user",
          eventType: "message",
          content: { text: "hello" },
        });
        db.enqueueTask({
          id: "run-a",
          kind: "openclaw.embedded_run",
          sessionId: "session-a",
        });
        db.claimTask({
          taskId: "run-a",
          workerId: "openclaw:999999:run-a",
          now: "2099-01-01T00:00:00.000Z",
          leaseMs: 60_000,
        });
      } finally {
        db.close();
      }

      const sessionsRuntime = makeRuntime();
      await runtimeSessionsCommand({ json: true }, sessionsRuntime);
      expect(sessionsRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          sessions: [expect.objectContaining({ id: "session-a", agentId: "main" })],
        }),
        2,
      );

      const tasksRuntime = makeRuntime();
      await runtimeTasksCommand({ kind: "openclaw.embedded_run", json: true }, tasksRuntime);
      expect(tasksRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          tasks: [expect.objectContaining({ id: "run-a", status: "running" })],
        }),
        2,
      );

      const transcriptRuntime = makeRuntime();
      await runtimeTranscriptCommand(
        { session: "session-a", limit: "5", json: true },
        transcriptRuntime,
      );
      expect(transcriptRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          events: [expect.objectContaining({ role: "user", eventType: "message" })],
        }),
        2,
      );

      const recoverRuntime = makeRuntime();
      await runtimeRecoverCommand({ task: "run-a", json: true }, recoverRuntime);
      expect(recoverRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          embeddedRuns: expect.objectContaining({ recovered: 1 }),
        }),
        2,
      );
    });
  });
});
