import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ContentAddressedArtifactStore } from "../local-kernel/artifact-store.js";
import { LocalKernelDatabase } from "../local-kernel/database.js";
import type { OutputRuntimeEnv } from "../runtime.js";
import {
  runtimeAcceptanceCommand,
  runtimeArtifactAccessCommand,
  runtimeArtifactSummaryCommand,
  runtimeBrowserPoolsCommand,
  runtimeBudgetSetCommand,
  runtimeCutoverPlanCommand,
  runtimeDoctorCommand,
  runtimeLeasesCommand,
  runtimeMaintainCommand,
  runtimeNetworkProxySetCommand,
  runtimeNetworkProxyShowCommand,
  runtimeRecoverCommand,
  runtimeSessionsCommand,
  runtimeTasksCommand,
  runtimeTranscriptCommand,
  runtimeWorkerIdentitiesCommand,
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

  it("lists artifact access and runs runtime maintenance", async () => {
    const stateDir = tempRoot();
    await withStateDir(stateDir, async () => {
      const db = new LocalKernelDatabase();
      let artifactId = "";
      try {
        const store = new ContentAddressedArtifactStore(db);
        const artifact = await store.write({
          bytes: "debug output",
          mimeType: "text/plain",
          retentionPolicy: "debug",
          subject: { subjectType: "agent", subjectId: "main" },
        });
        artifactId = artifact.id;
        db.db
          .prepare("UPDATE artifacts SET created_at = ? WHERE id = ?")
          .run("2026-01-01T00:00:00.000Z", artifact.id);
        db.ensureBrowserPool({
          id: "browser_pool_public_web",
          trustZoneId: "public_web",
          maxContexts: 2,
          cdpEndpoint: "http://127.0.0.1:9222",
        });
        db.ensureAgent({ id: "main" });
        db.createResourceLease({
          id: "lease-browser-a",
          resourceType: "browser_context",
          resourceId: "browser_context_a",
          trustZoneId: "public_web",
          ownerAgentId: "main",
          metadata: { poolId: "browser_pool_public_web" },
        });
      } finally {
        db.close();
      }

      const accessRuntime = makeRuntime();
      await runtimeArtifactAccessCommand(
        { subjectType: "agent", subject: "main", json: true },
        accessRuntime,
      );
      expect(accessRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          access: [
            expect.objectContaining({
              artifactId,
              subjectType: "agent",
              subjectId: "main",
              permission: "read",
            }),
          ],
        }),
        2,
      );

      const poolsRuntime = makeRuntime();
      await runtimeBrowserPoolsCommand({ trustZone: "public_web", json: true }, poolsRuntime);
      expect(poolsRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          ledgerPools: [
            expect.objectContaining({
              id: "browser_pool_public_web",
              trustZoneId: "public_web",
              maxContexts: 2,
            }),
          ],
        }),
        2,
      );

      const leasesRuntime = makeRuntime();
      await runtimeLeasesCommand(
        { resourceType: "browser_context", status: "active", json: true },
        leasesRuntime,
      );
      expect(leasesRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          leases: [expect.objectContaining({ id: "lease-browser-a" })],
        }),
        2,
      );

      const artifactSummaryRuntime = makeRuntime();
      await runtimeArtifactSummaryCommand({ json: true }, artifactSummaryRuntime);
      expect(artifactSummaryRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          artifacts: [expect.objectContaining({ retentionPolicy: "debug", count: 1 })],
        }),
        2,
      );

      const maintainRuntime = makeRuntime();
      await runtimeMaintainCommand(
        { olderThan: "1d", scheduleEvery: "1h", json: true },
        maintainRuntime,
      );
      expect(maintainRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          prunedArtifacts: 1,
          deletedFiles: 1,
          scheduleEveryMs: 3_600_000,
        }),
        2,
      );

      const skippedRuntime = makeRuntime();
      await runtimeMaintainCommand({ runDue: true, json: true }, skippedRuntime);
      expect(skippedRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          skipped: true,
          reason: "not due",
        }),
        2,
      );
    });
  });

  it("prints broker-managed worker identity plans", async () => {
    const runtime = makeRuntime();
    await runtimeWorkerIdentitiesCommand(
      {
        platform: "linux",
        count: "2",
        prefix: "openclaw-worker",
        uidStart: "63000",
        gid: "63000",
        group: "openclaw-workers",
        json: true,
      },
      runtime,
    );
    expect(runtime.writeJson).toHaveBeenCalledWith(
      expect.objectContaining({
        plan: expect.objectContaining({
          platform: "linux",
          identities: [
            expect.objectContaining({ id: "openclaw-worker-0", uid: 63000 }),
            expect.objectContaining({ id: "openclaw-worker-1", uid: 63001 }),
          ],
          environment: expect.objectContaining({
            OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITY_MODE: "managed",
          }),
        }),
      }),
      2,
    );
  });

  it("attaches and shows trust-zone network proxy refs", async () => {
    const stateDir = tempRoot();
    await withStateDir(stateDir, async () => {
      const setRuntime = makeRuntime();
      await runtimeNetworkProxySetCommand(
        {
          trustZone: "public_web",
          proxyRef: "http://127.0.0.1:18080/",
          json: true,
        },
        setRuntime,
      );
      expect(setRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          ok: true,
          networkPolicy: expect.objectContaining({
            proxyRef: "http://127.0.0.1:18080/",
          }),
        }),
        2,
      );

      const showRuntime = makeRuntime();
      await runtimeNetworkProxyShowCommand({ trustZone: "public_web", json: true }, showRuntime);
      expect(showRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          networkPolicy: expect.objectContaining({
            proxyRef: "http://127.0.0.1:18080/",
          }),
        }),
        2,
      );

      const clearRuntime = makeRuntime();
      await runtimeNetworkProxySetCommand(
        {
          trustZone: "public_web",
          clear: true,
          json: true,
        },
        clearRuntime,
      );
      const clearPayload = vi.mocked(clearRuntime.writeJson).mock.calls[0]?.[0] as {
        networkPolicy?: { proxyRef?: string };
      };
      expect(clearPayload.networkPolicy?.proxyRef).toBeUndefined();
    });
  });

  it("reports runtime doctor checks and acceptance lanes", async () => {
    const stateDir = tempRoot();
    await withStateDir(stateDir, async () => {
      const runtime = makeRuntime();
      await runtimeDoctorCommand({ json: true }, runtime);
      expect(runtime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          schemaVersion: expect.any(Number),
          resourceBudgets: expect.objectContaining({
            activeAgentStepsMax: 100,
            browserContextsMax: 2,
          }),
          checks: expect.arrayContaining([
            expect.objectContaining({ id: "ledger.schema", status: "pass" }),
            expect.objectContaining({ id: "sessions.transcript_compat" }),
            expect.objectContaining({ id: "tools.broker" }),
            expect.objectContaining({ id: "scheduler.resource_budgets", status: "pass" }),
          ]),
          acceptanceLanes: expect.arrayContaining([
            expect.objectContaining({ id: "ledger-and-leases" }),
            expect.objectContaining({ id: "egress-proxy" }),
          ]),
        }),
        2,
      );
    });
  });

  it("updates global SparseKernel resource budgets from the runtime budget command", async () => {
    const stateDir = tempRoot();
    await withStateDir(stateDir, async () => {
      const runtime = makeRuntime();
      await runtimeBudgetSetCommand(
        {
          activeAgentStepsMax: "12",
          browserContextsMax: "3",
          heavySandboxesMax: "2",
          json: true,
        },
        runtime,
      );
      expect(runtime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          ok: true,
          resourceBudgets: expect.objectContaining({
            activeAgentStepsMax: 12,
            browserContextsMax: 3,
            heavySandboxesMax: 2,
          }),
        }),
        2,
      );
    });
  });

  it("reports strict acceptance failures and cutover guidance", async () => {
    const stateDir = tempRoot();
    await withStateDir(stateDir, async () => {
      const runtime = makeRuntime();
      await expect(runtimeAcceptanceCommand({ strict: true, json: true }, runtime)).rejects.toThrow(
        /exit 1/,
      );
      expect(runtime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          ok: false,
          strict: true,
          checks: expect.arrayContaining([
            expect.objectContaining({ id: "sessions.sqlite_strict", status: "fail" }),
            expect.objectContaining({ id: "transcripts.ledger_only", status: "fail" }),
            expect.objectContaining({ id: "plugins.subprocess_default", status: "fail" }),
          ]),
        }),
        2,
      );

      const planRuntime = makeRuntime();
      await runtimeCutoverPlanCommand({ json: true }, planRuntime);
      expect(planRuntime.writeJson).toHaveBeenCalledWith(
        expect.objectContaining({
          environment: expect.objectContaining({
            OPENCLAW_SPARSEKERNEL_STRICT: "1",
            OPENCLAW_RUNTIME_SESSION_STORE: "sqlite-strict",
            OPENCLAW_RUNTIME_TRANSCRIPT_COMPAT: "ledger-only",
          }),
          commands: expect.arrayContaining([
            "openclaw sessions import --from-existing",
            "openclaw runtime acceptance --strict --current-platform --run --include-recommended",
          ]),
        }),
        2,
      );
    });
  });

  it("can execute required SparseKernel acceptance lanes", async () => {
    const stateDir = tempRoot();
    await withStateDir(stateDir, async () => {
      const runtime = makeRuntime();
      await runtimeAcceptanceCommand(
        {
          run: true,
          currentPlatform: true,
          json: true,
          env: { OPENCLAW_RUNTIME_PLUGIN_ALLOW_NO_ISOLATION: "1" } as NodeJS.ProcessEnv,
          runLaneCommand: (lane) => ({
            id: lane.id,
            command: lane.command,
            status: "passed",
            exitCode: 0,
            durationMs: 1,
          }),
        },
        runtime,
      );

      const payload = vi.mocked(runtime.writeJson).mock.calls[0]?.[0] as {
        ok?: boolean;
        ran?: Array<{ id: string; status: string }>;
      };
      expect(payload.ok).toBe(true);
      expect(payload.ran?.map((lane) => lane.id)).toEqual([
        "ledger-and-leases",
        "tool-broker",
        "runtime-cli",
        "egress-proxy",
      ]);
      expect(payload.ran?.every((lane) => lane.status === "passed")).toBe(true);
    });
  });

  it("fails executable acceptance when a selected lane fails", async () => {
    const stateDir = tempRoot();
    await withStateDir(stateDir, async () => {
      const runtime = makeRuntime();
      await expect(
        runtimeAcceptanceCommand(
          {
            run: true,
            includeRecommended: true,
            currentPlatform: true,
            json: true,
            env: { OPENCLAW_RUNTIME_PLUGIN_ALLOW_NO_ISOLATION: "1" } as NodeJS.ProcessEnv,
            runLaneCommand: (lane) => ({
              id: lane.id,
              command: lane.command,
              status: lane.id === "browser-cdp" ? "failed" : "passed",
              exitCode: lane.id === "browser-cdp" ? 1 : 0,
              durationMs: 1,
              stderr: lane.id === "browser-cdp" ? "browser conformance failed" : undefined,
            }),
          },
          runtime,
        ),
      ).rejects.toThrow(/exit 1/);

      const payload = vi.mocked(runtime.writeJson).mock.calls[0]?.[0] as {
        ok?: boolean;
        ran?: Array<{ id: string; status: string; stderr?: string }>;
      };
      expect(payload.ok).toBe(false);
      expect(payload.ran).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            id: "browser-cdp",
            status: "failed",
            stderr: "browser conformance failed",
          }),
        ]),
      );
    });
  });
});
