import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  loadSessionStoreFromRuntimeLedger,
  mirrorSessionStoreToRuntimeLedger,
} from "../config/sessions/runtime-ledger.js";
import {
  accountSandboxForRun,
  checkTrustZoneNetworkUrl,
  ContentAddressedArtifactStore,
  LocalBrowserBroker,
  LocalKernelDatabase,
  LocalSandboxBroker,
} from "./index.js";
import { exportSessionAsJsonl, importLegacySessionStore } from "./session-compat.js";

const roots: string[] = [];
const dbs: LocalKernelDatabase[] = [];

function tempRoot(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-kernel-test-"));
  roots.push(root);
  return root;
}

function openTempDb(root = tempRoot()): LocalKernelDatabase {
  const db = new LocalKernelDatabase({ dbPath: path.join(root, "runtime.sqlite") });
  dbs.push(db);
  return db;
}

afterEach(() => {
  for (const db of dbs.splice(0)) {
    db.close();
  }
  for (const root of roots.splice(0)) {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

describe("local runtime kernel database", () => {
  it("migrates an empty database idempotently", () => {
    const root = tempRoot();
    const db = openTempDb(root);
    expect(db.schemaVersion()).toBe(2);
    db.migrate();
    expect(db.schemaVersion()).toBe(2);
    const migrations = db.db.prepare("SELECT COUNT(*) AS count FROM schema_migrations").get() as {
      count: number;
    };
    expect(migrations.count).toBe(2);
    db.close();

    const reopened = openTempDb(root);
    expect(reopened.schemaVersion()).toBe(2);
    expect(reopened.inspect().counts.audit_log).toBe(0);
  });

  it("mirrors session store entries for SQLite-backed reads", () => {
    const db = openTempDb();
    const storePath = path.join(tempRoot(), "agents", "main", "sessions", "sessions.json");
    db.replaceSessionEntriesForStore({
      storePath,
      entries: [
        {
          sessionKey: "agent:main:main",
          sessionId: "session-a",
          agentId: "main",
          entry: { sessionId: "session-a", updatedAt: 1 },
          updatedAt: "2026-01-01T00:00:00.000Z",
        },
      ],
    });
    expect(db.loadSessionEntriesForStore(storePath)).toEqual({
      "agent:main:main": { sessionId: "session-a", updatedAt: 1 },
    });
    db.replaceSessionEntriesForStore({ storePath, entries: [] });
    expect(db.loadSessionEntriesForStore(storePath)).toBeUndefined();
  });

  it("loads mirrored session stores in strict SQLite session mode", () => {
    const root = tempRoot();
    const stateDir = path.join(root, "state");
    const storePath = path.join(stateDir, "agents", "main", "sessions", "sessions.json");
    const env = {
      ...process.env,
      OPENCLAW_STATE_DIR: stateDir,
      OPENCLAW_RUNTIME_SESSION_STORE: "sqlite",
    };
    const entry = {
      sessionId: "session-a",
      updatedAt: Date.parse("2026-01-01T00:00:00.000Z"),
    };
    mirrorSessionStoreToRuntimeLedger({
      storePath,
      store: { "agent:main:main": entry },
      env,
    });
    expect(loadSessionStoreFromRuntimeLedger(storePath, env)).toEqual({
      "agent:main:main": entry,
    });
  });

  it("appends transcript events with stable per-session ordering", () => {
    const db = openTempDb();
    db.upsertSession({ id: "session-a", agentId: "main", sessionKey: "agent:main:main" });
    const first = db.appendTranscriptEvent({
      sessionId: "session-a",
      role: "user",
      eventType: "message",
      content: { text: "one" },
    });
    const second = db.appendTranscriptEvent({
      sessionId: "session-a",
      parentEventId: first.id,
      role: "assistant",
      eventType: "message",
      content: { text: "two" },
    });
    expect(first.seq).toBe(1);
    expect(second.seq).toBe(2);
    expect(db.listTranscriptEvents("session-a").map((event) => event.seq)).toEqual([1, 2]);
  });

  it("claims queued tasks atomically and reclaims expired leases", () => {
    const db = openTempDb();
    db.enqueueTask({ id: "task-a", kind: "demo", priority: 1 });
    const claimed = db.claimNextTask({
      workerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 1000,
    });
    expect(claimed).toMatchObject({ id: "task-a", status: "running", leaseOwner: "worker-a" });
    expect(db.claimNextTask({ workerId: "worker-b" })).toBeNull();
    expect(db.releaseExpiredLeases("2026-01-01T00:00:02.000Z")).toBe(1);
    const reclaimed = db.claimNextTask({ workerId: "worker-b" });
    expect(reclaimed).toMatchObject({ id: "task-a", status: "running", leaseOwner: "worker-b" });
  });

  it("records artifact metadata, dedupes blobs, and enforces access", async () => {
    const root = tempRoot();
    const db = openTempDb(root);
    const store = new ContentAddressedArtifactStore(db, path.join(root, "artifacts"));
    const first = await store.write({
      bytes: "hello",
      mimeType: "text/plain",
      retentionPolicy: "session",
      subject: { subjectType: "agent", subjectId: "main" },
    });
    const second = await store.write({
      bytes: "hello",
      mimeType: "text/plain",
      retentionPolicy: "session",
    });
    expect(second.id).toBe(first.id);
    await expect(
      store.read(first.id, { subjectType: "agent", subjectId: "main" }),
    ).resolves.toEqual(Buffer.from("hello"));
    await expect(
      store.read(first.id, { subjectType: "agent", subjectId: "other" }),
    ).rejects.toThrow(/denied/);
  });

  it("allows, denies, revokes, and audits capabilities", () => {
    const db = openTempDb();
    expect(
      db.checkCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "tool",
        resourceId: "exec",
        action: "invoke",
      }),
    ).toBe(false);
    const id = db.grantCapability({
      subjectType: "agent",
      subjectId: "main",
      resourceType: "tool",
      resourceId: "exec",
      action: "invoke",
    });
    expect(
      db.checkCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "tool",
        resourceId: "exec",
        action: "invoke",
      }),
    ).toBe(true);
    expect(db.revokeCapability(id)).toBe(true);
    expect(
      db.checkCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "tool",
        resourceId: "exec",
        action: "invoke",
      }),
    ).toBe(false);
    const audit = db.db.prepare("SELECT action FROM audit_log ORDER BY id").all() as Array<{
      action: string;
    }>;
    expect(audit.map((row) => row.action)).toEqual([
      "capability.denied",
      "capability.granted",
      "capability.revoked",
      "capability.denied",
    ]);
  });

  it("brokers browser context leases with max context accounting", () => {
    const db = openTempDb();
    db.ensureAgent({ id: "main" });
    db.grantCapability({
      subjectType: "agent",
      subjectId: "main",
      resourceType: "browser_context",
      resourceId: "public_web",
      action: "allocate",
    });
    const broker = new LocalBrowserBroker(db);
    const context = broker.acquireContext({
      agentId: "main",
      trustZoneId: "public_web",
      maxContexts: 1,
    });
    expect(context.status).toBe("active");
    expect(() =>
      broker.acquireContext({ agentId: "main", trustZoneId: "public_web", maxContexts: 1 }),
    ).toThrow(/no available contexts/);
    expect(broker.releaseContext(context.id)).toBe(true);
  });

  it("applies trust-zone network policy checks to brokered browser origins", () => {
    const db = openTempDb();
    db.ensureAgent({ id: "main" });
    db.grantCapability({
      subjectType: "agent",
      subjectId: "main",
      resourceType: "browser_context",
      resourceId: "public_web",
      action: "allocate",
    });
    expect(
      checkTrustZoneNetworkUrl({
        db,
        trustZoneId: "public_web",
        url: "https://example.com",
      }),
    ).toMatchObject({ allowed: true });
    const broker = new LocalBrowserBroker(db);
    expect(() =>
      broker.acquireContext({
        agentId: "main",
        trustZoneId: "public_web",
        allowedOrigins: ["http://127.0.0.1"],
      }),
    ).toThrow(/network policy/);
  });

  it("records trust-zone budgets and usage summaries", () => {
    const db = openTempDb();
    expect(
      db.updateTrustZoneLimits({
        id: "code_execution",
        maxProcesses: 4,
        maxMemoryMb: 1024,
        maxRuntimeSeconds: 300,
      }),
    ).toBe(true);
    expect(db.listTrustZones().find((zone) => zone.id === "code_execution")).toMatchObject({
      maxProcesses: 4,
      maxMemoryMb: 1024,
      maxRuntimeSeconds: 300,
    });
    db.recordUsage({ resourceType: "tokens", amount: 10, unit: "token" });
    db.recordUsage({ resourceType: "tokens", amount: 15, unit: "token" });
    expect(db.summarizeUsage()).toEqual([{ resourceType: "tokens", amount: 25, unit: "token" }]);
  });

  it("brokers local/no-isolation sandbox allocations without pretending isolation", () => {
    const db = openTempDb();
    db.ensureAgent({ id: "main" });
    db.enqueueTask({ id: "task-a", kind: "demo" });
    db.grantCapability({
      subjectType: "agent",
      subjectId: "main",
      resourceType: "sandbox",
      resourceId: "code_execution",
      action: "allocate",
    });
    const broker = new LocalSandboxBroker(db);
    const allocation = broker.allocateSandbox({
      taskId: "task-a",
      agentId: "main",
      trustZoneId: "code_execution",
    });
    expect(allocation).toMatchObject({ backend: "local/no_isolation", status: "active" });
    expect(broker.releaseSandbox(allocation.id)).toBe(true);
  });

  it("accounts sandboxed runs without requiring a queued task row", () => {
    const root = tempRoot();
    const run = accountSandboxForRun({
      agentId: "main",
      sessionId: "session-a",
      sessionKey: "agent:main:main",
      runId: "run-a",
      backendId: "local",
      dbPath: path.join(root, "runtime.sqlite"),
    });
    try {
      const allocation = run.allocation;
      if (!allocation) {
        throw new Error("expected sandbox accounting allocation");
      }
      expect(allocation).toMatchObject({
        trustZoneId: "code_execution",
        backend: "local/no_isolation",
        status: "active",
      });
      const lease = run.db.db
        .prepare("SELECT status, owner_task_id FROM resource_leases WHERE id = ?")
        .get(allocation.id) as { status: string; owner_task_id: string | null };
      expect(lease).toEqual({ status: "active", owner_task_id: null });
      run.release();
      const released = run.db.db
        .prepare("SELECT status FROM resource_leases WHERE id = ?")
        .get(allocation.id) as { status: string };
      expect(released.status).toBe("released");
    } finally {
      run.close();
    }
  });

  it("imports legacy sessions and exports JSONL compatibility output", async () => {
    const root = tempRoot();
    const sessionsDir = path.join(root, "agents", "main", "sessions");
    fs.mkdirSync(sessionsDir, { recursive: true });
    fs.writeFileSync(
      path.join(sessionsDir, "sessions.json"),
      JSON.stringify({
        "agent:main:main": {
          sessionId: "legacy-session",
          updatedAt: Date.parse("2026-01-01T00:00:00.000Z"),
          totalTokens: 12,
        },
      }),
    );
    fs.writeFileSync(
      path.join(sessionsDir, "legacy-session.jsonl"),
      [
        JSON.stringify({ type: "session", id: "legacy-session" }),
        JSON.stringify({
          type: "message",
          id: "entry-1",
          message: { role: "user", content: "hi", timestamp: 1 },
        }),
        JSON.stringify({
          type: "message",
          id: "entry-2",
          parentId: "entry-1",
          message: { role: "assistant", content: "hello", timestamp: 2 },
        }),
        "",
      ].join("\n"),
    );
    const db = openTempDb(root);
    const result = await importLegacySessionStore({
      db,
      target: { agentId: "main", storePath: path.join(sessionsDir, "sessions.json") },
    });
    expect(result).toMatchObject({ sessions: 1, importedEvents: 2 });
    expect(db.listTranscriptEvents("legacy-session").map((event) => event.role)).toEqual([
      "user",
      "assistant",
    ]);
    const exported = exportSessionAsJsonl({ db, sessionId: "legacy-session" });
    expect(exported).toContain('"type":"session"');
    expect(exported).toContain('"content":"hi"');
    expect(exported).toContain('"content":"hello"');
  });
});
