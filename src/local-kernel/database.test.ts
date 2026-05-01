import { Buffer } from "node:buffer";
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
  accountSandboxForRunEffective,
  buildBuiltinFirewallEgressPlan,
  buildSandboxProcessEnv,
  buildSandboxSpawnPlan,
  buildWorkerIdentityProvisionPlan,
  checkTrustZoneNetworkUrl,
  checkTrustZoneNetworkUrlWithDns,
  ContentAddressedArtifactStore,
  isSandboxBackendAvailable,
  LocalBrowserBroker,
  LocalKernelDatabase,
  LocalSandboxBroker,
  probeSandboxBackends,
  recoverEmbeddedRunTasks,
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
    expect(db.schemaVersion()).toBe(5);
    db.migrate();
    expect(db.schemaVersion()).toBe(5);
    const migrations = db.db.prepare("SELECT COUNT(*) AS count FROM schema_migrations").get() as {
      count: number;
    };
    expect(migrations.count).toBe(5);
    expect(db.getResourceBudgetSnapshot()).toMatchObject({
      logicalAgentsMax: 500,
      activeAgentStepsMax: 100,
      modelCallsInFlightMax: 50,
      filePatchJobsMax: 16,
      testJobsMax: 4,
      browserContextsMax: 2,
      heavySandboxesMax: 1,
    });
    db.close();

    const reopened = openTempDb(root);
    expect(reopened.schemaVersion()).toBe(5);
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
    db.enqueueTask({ id: "task-b", kind: "demo", priority: 10 });
    const claimedById = db.claimTask({
      taskId: "task-a",
      workerId: "worker-id",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 1000,
    });
    expect(claimedById).toMatchObject({
      id: "task-a",
      status: "running",
      leaseOwner: "worker-id",
    });
    expect(db.claimTask({ taskId: "task-a", workerId: "worker-other" })).toBeNull();
    expect(db.completeTask("task-a", "worker-id", {})).toBe(true);

    const claimed = db.claimNextTask({
      workerId: "worker-a",
      now: "2026-01-01T00:00:00.000Z",
      leaseMs: 1000,
    });
    expect(claimed).toMatchObject({ id: "task-b", status: "running", leaseOwner: "worker-a" });
    expect(db.claimNextTask({ workerId: "worker-b" })).toBeNull();
    expect(db.releaseExpiredLeases("2026-01-01T00:00:02.000Z")).toBe(1);
    const reclaimed = db.claimNextTask({ workerId: "worker-b" });
    expect(reclaimed).toMatchObject({ id: "task-b", status: "running", leaseOwner: "worker-b" });
  });

  it("enforces active task budgets through atomic task claiming", () => {
    const db = openTempDb();
    db.setRuntimeInfo("resource_budget.test_jobs_max", "1");
    db.enqueueTask({ id: "test-a", kind: "test.vitest", priority: 3 });
    db.enqueueTask({ id: "test-b", kind: "test.vitest", priority: 2 });
    db.enqueueTask({ id: "demo-c", kind: "demo", priority: 1 });
    expect(db.claimTask({ taskId: "test-a", workerId: "worker-a" })).toMatchObject({
      id: "test-a",
      status: "running",
    });
    expect(db.claimNextTask({ workerId: "worker-b", kinds: ["test.vitest"] })).toBeNull();
    expect(db.claimNextTask({ workerId: "worker-c" })).toMatchObject({
      id: "demo-c",
      status: "running",
    });
    expect(db.listAudit({ limit: 20 })).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          action: "task.claim_denied_resource_budget",
          objectId: "test-b",
          payload: expect.objectContaining({
            budget: "test_jobs",
            active: 1,
            limit: 1,
          }),
        }),
      ]),
    );
  });

  it("enforces the global active-step budget before task-specific budgets", () => {
    const db = openTempDb();
    db.setRuntimeInfo("resource_budget.active_agent_steps_max", "1");
    db.enqueueTask({ id: "step-a", kind: "demo", priority: 2 });
    db.enqueueTask({ id: "step-b", kind: "test.vitest", priority: 1 });
    expect(db.claimTask({ taskId: "step-a", workerId: "worker-a" })).toMatchObject({
      id: "step-a",
      status: "running",
    });
    expect(db.claimNextTask({ workerId: "worker-b" })).toBeNull();
    expect(db.listAudit({ limit: 20 })).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          action: "task.claim_denied_resource_budget",
          objectId: "step-b",
          payload: expect.objectContaining({
            budget: "active_agent_steps",
            active: 1,
            limit: 1,
          }),
        }),
      ]),
    );
  });

  it("recovers dead-owner embedded run tasks for restart claim", () => {
    const db = openTempDb();
    db.upsertSession({ id: "session-a", agentId: "main" });
    db.enqueueTask({
      id: "run-a",
      kind: "openclaw.embedded_run",
      sessionId: "session-a",
    });
    expect(
      db.claimTask({
        taskId: "run-a",
        workerId: "openclaw:424242:run-a",
        now: "2026-01-01T00:00:00.000Z",
        leaseMs: 60_000,
      }),
    ).toMatchObject({ status: "running" });
    const result = recoverEmbeddedRunTasks({
      db,
      taskId: "run-a",
      now: "2026-01-01T00:00:10.000Z",
      isProcessAlive: () => false,
    });
    expect(result).toEqual({ recovered: 1, expired: 0, deadOwners: 1 });
    expect(db.getTask("run-a")).toMatchObject({ status: "queued" });
    expect(db.claimTask({ taskId: "run-a", workerId: "openclaw:1:run-a" })).toMatchObject({
      status: "running",
      leaseOwner: "openclaw:1:run-a",
    });
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
    const sourcePath = path.join(root, "source.txt");
    fs.writeFileSync(sourcePath, "hello");
    const imported = await store.importFile({
      filePath: sourcePath,
      mimeType: "text/plain",
      retentionPolicy: "session",
    });
    expect(second.id).toBe(first.id);
    expect(imported.id).toBe(first.id);
    const tmpDir = path.join(root, "artifacts", ".tmp");
    expect(fs.existsSync(tmpDir) ? fs.readdirSync(tmpDir) : []).toEqual([]);
    await expect(
      store.read(first.id, { subjectType: "agent", subjectId: "main" }),
    ).resolves.toEqual(Buffer.from("hello"));
    await expect(
      store.read(first.id, { subjectType: "agent", subjectId: "other" }),
    ).rejects.toThrow(/denied/);
    expect(db.listArtifactAccess({ subjectType: "agent", subjectId: "main" })).toEqual([
      expect.objectContaining({
        artifactId: first.id,
        permission: "read",
        subjectId: "main",
      }),
    ]);
    expect(db.summarizeArtifactRetention()).toEqual([
      { retentionPolicy: "session", count: 1, sizeBytes: 5 },
    ]);
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
    db.upsertSession({ id: "session-a", agentId: "main" });
    db.enqueueTask({ id: "task-a", kind: "browser", sessionId: "session-a" });
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
    expect(db.listBrowserPools({ trustZoneId: "public_web" })).toEqual([
      expect.objectContaining({
        trustZoneId: "public_web",
        maxContexts: 1,
        activeContexts: 1,
      }),
    ]);
    expect(broker.releaseContext(context.id)).toBe(true);
  });

  it("enforces the global browser context resource budget across pools", () => {
    const db = openTempDb();
    db.ensureAgent({ id: "main" });
    db.updateResourceBudgets({ browserContextsMax: 1 });
    for (const zone of ["public_web", "authenticated_web"]) {
      db.grantCapability({
        subjectType: "agent",
        subjectId: "main",
        resourceType: "browser_context",
        resourceId: zone,
        action: "allocate",
      });
    }
    const broker = new LocalBrowserBroker(db);
    const context = broker.acquireContext({
      agentId: "main",
      trustZoneId: "public_web",
      maxContexts: 10,
    });
    expect(context.status).toBe("active");
    expect(() =>
      broker.acquireContext({
        agentId: "main",
        trustZoneId: "authenticated_web",
        maxContexts: 10,
      }),
    ).toThrow(/Browser context budget exhausted/);
    const audit = db.db
      .prepare("SELECT action, payload_json FROM audit_log ORDER BY id DESC LIMIT 1")
      .get() as { action: string; payload_json: string };
    expect(audit.action).toBe("resource_lease.denied_budget_exhausted");
    expect(JSON.parse(audit.payload_json)).toMatchObject({
      resourceType: "browser_context",
      active: 1,
      limit: 1,
    });
  });

  it("records browser target lifecycle and queryable observations", () => {
    const db = openTempDb();
    db.ensureAgent({ id: "main" });
    db.upsertSession({ id: "session-a", agentId: "main" });
    db.enqueueTask({ id: "task-a", kind: "browser", sessionId: "session-a" });
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
      sessionId: "session-a",
      taskId: "task-a",
      trustZoneId: "public_web",
    });
    db.recordBrowserTarget({
      contextId: context.id,
      targetId: "target-1",
      url: "https://example.com",
      status: "active",
    });
    db.recordBrowserObservation({
      contextId: context.id,
      targetId: "target-1",
      observationType: "browser_console",
      payload: { text: "hello" },
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    db.recordBrowserObservation({
      contextId: context.id,
      targetId: "target-1",
      observationType: "browser_network.request",
      payload: { requestId: "req-1" },
      createdAt: "2026-01-01T00:00:01.000Z",
    });
    expect(db.listBrowserTargets({ contextId: context.id })).toEqual([
      expect.objectContaining({
        targetId: "target-1",
        consoleCount: 1,
        networkCount: 1,
      }),
    ]);
    expect(db.listBrowserObservations({ contextId: context.id, targetId: "target-1" })).toEqual([
      expect.objectContaining({ observationType: "browser_network.request" }),
      expect.objectContaining({ observationType: "browser_console" }),
    ]);
    expect(db.pruneBrowserObservations({ olderThan: "2026-01-01T00:00:00.500Z" })).toBe(1);
    expect(db.closeBrowserTarget({ contextId: context.id, targetId: "target-1" })).toMatchObject({
      status: "closed",
    });
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

  it("denies unsupported schemes, private IPv6, and literal denied CIDRs", () => {
    const db = openTempDb();
    expect(
      checkTrustZoneNetworkUrl({
        db,
        trustZoneId: "public_web",
        url: "file:///tmp/secret",
      }),
    ).toMatchObject({ allowed: false, reason: "unsupported scheme" });
    expect(
      checkTrustZoneNetworkUrl({
        db,
        trustZoneId: "public_web",
        url: "http://[::1]/",
      }),
    ).toMatchObject({ allowed: false, reason: "private network denied" });

    db.db
      .prepare("UPDATE network_policies SET denied_cidrs_json = ? WHERE id = 'public_web_default'")
      .run(JSON.stringify(["203.0.113.0/24", "2001:db8::/32"]));
    expect(
      checkTrustZoneNetworkUrl({
        db,
        trustZoneId: "public_web",
        url: "https://203.0.113.9/resource",
      }),
    ).toMatchObject({ allowed: false, reason: "denied cidr" });
    expect(
      checkTrustZoneNetworkUrl({
        db,
        trustZoneId: "public_web",
        url: "https://[2001:db8::12]/resource",
      }),
    ).toMatchObject({ allowed: false, reason: "denied cidr" });
  });

  it("can resolve DNS before allowing brokered network destinations", async () => {
    const db = openTempDb();
    await expect(
      checkTrustZoneNetworkUrlWithDns({
        db,
        trustZoneId: "public_web",
        url: "https://internal.example.test/dashboard",
        lookup: async () => [{ address: "127.0.0.1", family: 4 }],
      }),
    ).resolves.toMatchObject({
      allowed: false,
      reason: "resolved private network denied",
    });
    db.db
      .prepare("UPDATE network_policies SET denied_cidrs_json = ? WHERE id = 'public_web_default'")
      .run(JSON.stringify(["203.0.113.0/24"]));
    await expect(
      checkTrustZoneNetworkUrlWithDns({
        db,
        trustZoneId: "public_web",
        url: "https://example.test/resource",
        lookup: async () => [{ address: "203.0.113.8", family: 4 }],
      }),
    ).resolves.toMatchObject({
      allowed: false,
      reason: "resolved denied cidr",
    });
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
    db.setRuntimeInfo("maintenance.schedule_every_ms", "3600000");
    expect(db.getRuntimeInfo("maintenance.schedule_every_ms")).toMatchObject({
      key: "maintenance.schedule_every_ms",
      value: "3600000",
    });
  });

  it("enforces trust-zone sandbox budgets and runtime clamps", () => {
    const db = openTempDb();
    expect(
      db.updateTrustZoneLimits({
        id: "code_execution",
        maxProcesses: 1,
        maxRuntimeSeconds: 2,
      }),
    ).toBe(true);
    const first = db.createResourceLease({
      id: "sandbox-budget-a",
      resourceType: "sandbox",
      resourceId: "sandbox-budget-a",
      trustZoneId: "code_execution",
      maxRuntimeMs: 10_000,
      leaseUntil: "2030-01-01T00:00:00.000Z",
      now: "2026-01-01T00:00:00.000Z",
    });
    expect(db.getResourceLease(first)).toMatchObject({
      maxRuntimeMs: 2_000,
      leaseUntil: "2026-01-01T00:00:02.000Z",
    });
    expect(() =>
      db.createResourceLease({
        id: "sandbox-budget-b",
        resourceType: "sandbox",
        resourceId: "sandbox-budget-b",
        trustZoneId: "code_execution",
        now: "2026-01-01T00:00:01.000Z",
      }),
    ).toThrow(/budget exhausted/);
    const audit = db.db
      .prepare("SELECT action, payload_json FROM audit_log ORDER BY id DESC LIMIT 1")
      .get() as { action: string; payload_json: string };
    expect(audit.action).toBe("resource_lease.denied_budget_exhausted");
    expect(JSON.parse(audit.payload_json)).toMatchObject({
      trustZoneId: "code_execution",
      resourceType: "sandbox",
      active: 1,
      limit: 1,
    });
    expect(db.releaseResourceLease(first)).toBe(true);
    expect(
      db.createResourceLease({
        id: "sandbox-budget-c",
        resourceType: "sandbox",
        resourceId: "sandbox-budget-c",
        trustZoneId: "code_execution",
      }),
    ).toBe("sandbox-budget-c");
  });

  it("enforces the global heavy sandbox budget across trust zones", () => {
    const db = openTempDb();
    db.updateResourceBudgets({ heavySandboxesMax: 1 });
    db.createResourceLease({
      id: "heavy-sandbox-a",
      resourceType: "sandbox",
      resourceId: "heavy-sandbox-a",
      trustZoneId: "code_execution",
    });
    expect(() =>
      db.createResourceLease({
        id: "heavy-sandbox-b",
        resourceType: "sandbox",
        resourceId: "heavy-sandbox-b",
        trustZoneId: "plugin_untrusted",
      }),
    ).toThrow(/Heavy sandbox budget exhausted/);
    expect(db.releaseResourceLease("heavy-sandbox-a")).toBe(true);
    expect(
      db.createResourceLease({
        id: "heavy-sandbox-c",
        resourceType: "sandbox",
        resourceId: "heavy-sandbox-c",
        trustZoneId: "plugin_untrusted",
      }),
    ).toBe("heavy-sandbox-c");
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

  it("probes sandbox backend availability and reports honest boundaries", () => {
    const probes = probeSandboxBackends({
      commandAvailable: (command) => command === "bwrap",
    });
    expect(probes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          backend: "local/no_isolation",
          available: true,
          hardBoundary: false,
        }),
        expect.objectContaining({
          backend: "bwrap",
          available: true,
          command: "bwrap",
          hardBoundary: true,
        }),
        expect.objectContaining({
          backend: "docker",
          available: false,
        }),
      ]),
    );
  });

  it("fails closed when untrusted plugin work requests local/no-isolation", () => {
    const previousAllow = process.env.OPENCLAW_RUNTIME_SANDBOX_ALLOW_LOCAL_UNTRUSTED;
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_ALLOW_LOCAL_UNTRUSTED;
    try {
      const db = openTempDb();
      const broker = new LocalSandboxBroker(db);
      expect(() =>
        broker.allocateSandbox({
          taskId: "task-a",
          trustZoneId: "plugin_untrusted",
          requirements: { backend: "local/no_isolation" },
        }),
      ).toThrow(/requires an isolated backend/);
      const audit = db.db
        .prepare("SELECT action, payload_json FROM audit_log ORDER BY id DESC LIMIT 1")
        .get() as { action: string; payload_json: string };
      expect(audit.action).toBe("sandbox.allocation_denied_isolation_required");
      expect(JSON.parse(audit.payload_json)).toMatchObject({
        requestedBackend: "local/no_isolation",
        isolationProfile: "plugin_untrusted",
      });
    } finally {
      if (previousAllow === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_ALLOW_LOCAL_UNTRUSTED;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_ALLOW_LOCAL_UNTRUSTED = previousAllow;
      }
    }
  });

  it("runs trusted local commands behind an active sandbox lease", async () => {
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
      requirements: { maxRuntimeMs: 5_000, maxBytesOut: 1024 },
    });
    await expect(
      broker.runCommand({
        allocationId: allocation.id,
        command: process.execPath,
        args: ["-e", "process.stdout.write('ok')"],
      }),
    ).resolves.toMatchObject({ exitCode: 0, stdout: "ok", timedOut: false });
    expect(db.summarizeUsage().map((row) => row.resourceType)).toEqual(
      expect.arrayContaining(["sandbox_runtime", "sandbox_output"]),
    );
    expect(broker.releaseSandbox(allocation.id)).toBe(true);
    await expect(
      broker.runCommand({ allocationId: allocation.id, command: process.execPath }),
    ).rejects.toThrow(/not active/);
  });

  it("passes stdin through sandbox command leases", async () => {
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
      requirements: { maxRuntimeMs: 5_000, maxBytesOut: 1024 },
    });
    await expect(
      broker.runCommand({
        allocationId: allocation.id,
        command: process.execPath,
        args: ["-e", "process.stdin.pipe(process.stdout)"],
        stdin: "worker-input",
      }),
    ).resolves.toMatchObject({ exitCode: 0, stdout: "worker-input", timedOut: false });
  });

  it("does not leak host environment into isolated sandbox command backends", () => {
    const previous = process.env.OPENCLAW_TEST_SECRET_TOKEN;
    process.env.OPENCLAW_TEST_SECRET_TOKEN = "should-not-leak";
    try {
      expect(buildSandboxProcessEnv({ backend: "bwrap" })).not.toHaveProperty(
        "OPENCLAW_TEST_SECRET_TOKEN",
      );
      expect(
        buildSandboxProcessEnv({
          backend: "minijail",
          env: { EXPLICIT_WORKER_ENV: "ok" },
        }),
      ).toMatchObject({
        HOME: "/tmp",
        TMPDIR: "/tmp",
        EXPLICIT_WORKER_ENV: "ok",
      });
      expect(buildSandboxProcessEnv({ backend: "local/no_isolation" })).toHaveProperty(
        "OPENCLAW_TEST_SECRET_TOKEN",
        "should-not-leak",
      );
    } finally {
      if (previous === undefined) {
        delete process.env.OPENCLAW_TEST_SECRET_TOKEN;
      } else {
        process.env.OPENCLAW_TEST_SECRET_TOKEN = previous;
      }
    }
  });

  it("fails closed for proxy-required sandbox egress without a proxy policy", () => {
    const previous = process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_PROXY;
    process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_PROXY = "1";
    try {
      const db = openTempDb();
      const broker = new LocalSandboxBroker(db);
      expect(() =>
        broker.allocateSandbox({
          taskId: "task-a",
          trustZoneId: "public_web",
          requirements: { backend: "local/no_isolation" },
        }),
      ).toThrow(/proxy-backed network policy/);
      const audit = db.db
        .prepare("SELECT action, payload_json FROM audit_log ORDER BY id DESC LIMIT 1")
        .get() as { action: string; payload_json: string };
      expect(audit.action).toBe("network_policy.proxy_required_missing");
      expect(JSON.parse(audit.payload_json)).toMatchObject({ reason: "missing proxy_ref" });

      expect(
        broker.allocateSandbox({
          taskId: "task-a",
          trustZoneId: "code_execution",
          requirements: { backend: "local/no_isolation" },
        }),
      ).toMatchObject({ backend: "local/no_isolation", status: "active" });
    } finally {
      if (previous === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_PROXY;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_PROXY = previous;
      }
    }
  });

  it("fails closed when hard sandbox egress enforcement is required for network-allowing zones", () => {
    const previousHard = process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS;
    const previousProxy = process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_PROXY;
    const previousHelper = process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER;
    const previousHelperArgs = process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
    process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS = "1";
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_PROXY;
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER;
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
    try {
      const db = openTempDb();
      db.db
        .prepare("UPDATE network_policies SET proxy_ref = ? WHERE id = 'public_web_default'")
        .run("http://127.0.0.1:8080");
      const broker = new LocalSandboxBroker(db);
      expect(() =>
        broker.allocateSandbox({
          taskId: "task-a",
          trustZoneId: "public_web",
          requirements: { backend: "local/no_isolation" },
        }),
      ).toThrow(/host-level egress enforcement/);
      const audit = db.db
        .prepare("SELECT action, payload_json FROM audit_log ORDER BY id DESC LIMIT 1")
        .get() as { action: string; payload_json: string };
      expect(audit.action).toBe("network_policy.hard_egress_unavailable");
      expect(JSON.parse(audit.payload_json)).toMatchObject({
        backend: "local/no_isolation",
        reason: "missing hard egress helper",
      });

      expect(
        broker.allocateSandbox({
          taskId: "task-a",
          trustZoneId: "code_execution",
          requirements: { backend: "local/no_isolation" },
        }),
      ).toMatchObject({ backend: "local/no_isolation", status: "active" });
    } finally {
      if (previousHard === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS = previousHard;
      }
      if (previousProxy === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_PROXY;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_PROXY = previousProxy;
      }
      if (previousHelper === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER = previousHelper;
      }
      if (previousHelperArgs === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS = previousHelperArgs;
      }
    }
  });

  it("uses a hard egress helper for network-allowing sandbox leases", () => {
    const previousHard = process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS;
    const previousHelper = process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER;
    const previousHelperArgs = process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
    process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER = process.execPath;
    process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS = JSON.stringify([
      "-e",
      [
        "let input='';",
        "process.stdin.setEncoding('utf8');",
        "process.stdin.on('data', chunk => input += chunk);",
        "process.stdin.on('end', () => {",
        "  const request = JSON.parse(input);",
        "  if (request.action === 'release') process.exit(0);",
        "  process.stdout.write(JSON.stringify({",
        "    ok: true,",
        "    enforcementId: `fw-${request.allocationId}`,",
        "    boundary: 'host_firewall',",
        "    description: 'unit-test-helper'",
        "  }));",
        "});",
      ].join("\n"),
    ]);
    try {
      const db = openTempDb();
      const broker = new LocalSandboxBroker(db);
      const allocation = broker.allocateSandbox({
        taskId: "task-a",
        trustZoneId: "public_web",
        requirements: { backend: "local/no_isolation" },
      });
      expect(allocation).toMatchObject({ backend: "local/no_isolation", status: "active" });
      const lease = db.getResourceLease(allocation.id);
      expect(lease?.metadata).toMatchObject({
        hardEgress: {
          enforcementId: `fw-${allocation.id}`,
          boundary: "host_firewall",
        },
      });
      expect(broker.releaseSandbox(allocation.id)).toBe(true);
      expect(db.listAudit({ limit: 20 }).map((entry) => entry.action)).toEqual(
        expect.arrayContaining([
          "network_policy.hard_egress_enforced",
          "sandbox.allocated",
          "sandbox.released",
        ]),
      );
    } finally {
      if (previousHard === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS = previousHard;
      }
      if (previousHelper === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER = previousHelper;
      }
      if (previousHelperArgs === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS = previousHelperArgs;
      }
    }
  });

  it("supports builtin hard egress only for backends that carry a real boundary", () => {
    const previousHard = process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS;
    const previousHelper = process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER;
    const previousHelperArgs = process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
    process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER = "builtin";
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
    try {
      const db = openTempDb();
      const broker = new LocalSandboxBroker(db);
      expect(() =>
        broker.allocateSandbox({
          taskId: "task-a",
          trustZoneId: "public_web",
          requirements: { backend: "local/no_isolation" },
        }),
      ).toThrow(/builtin hard egress only supports/);

      if (isSandboxBackendAvailable("bwrap")) {
        const allocation = broker.allocateSandbox({
          taskId: "task-b",
          trustZoneId: "public_web",
          requirements: { backend: "bwrap" },
        });
        expect(db.getResourceLease(allocation.id)?.metadata).toMatchObject({
          hardEgress: {
            helper: "builtin",
            boundary: "platform_enforcer",
          },
        });
        expect(broker.releaseSandbox(allocation.id)).toBe(true);
      }
    } finally {
      if (previousHard === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS = previousHard;
      }
      if (previousHelper === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER = previousHelper;
      }
      if (previousHelperArgs === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS = previousHelperArgs;
      }
    }
  });

  it("attaches loopback proxy refs to trust-zone network policies", () => {
    const root = tempRoot();
    const db = openTempDb(root);
    try {
      const attached = db.attachNetworkPolicyProxyToTrustZone({
        trustZoneId: "public_web",
        proxyRef: "http://127.0.0.1:18080/",
      });
      expect(attached.networkPolicy.proxyRef).toBe("http://127.0.0.1:18080/");
      expect(db.getNetworkPolicyForTrustZone("public_web")?.proxyRef).toBe(
        "http://127.0.0.1:18080/",
      );
      db.attachNetworkPolicyProxyToTrustZone({
        trustZoneId: "public_web",
        proxyRef: null,
      });
      expect(db.getNetworkPolicyForTrustZone("public_web")?.proxyRef).toBeUndefined();
      const audit = db.listAudit({ limit: 10 }).map((entry) => entry.action);
      expect(audit).toEqual(
        expect.arrayContaining([
          "network_policy.proxy_ref_attached",
          "network_policy.proxy_ref_cleared",
        ]),
      );
    } finally {
      db.close();
    }
  });

  it("builds scoped linux firewall plans from IP allowlists and loopback proxies", () => {
    const plan = buildBuiltinFirewallEgressPlan({
      allocationId: "sandbox_test",
      backend: "local/no_isolation",
      policy: {
        trustZoneId: "public_web",
        backend: "local/no_isolation",
        networkPolicy: {
          id: "public_web_default",
          defaultAction: "allow",
          allowPrivateNetwork: false,
          allowedHosts: ["203.0.113.10", "2001:db8::/32", "example.com"],
          proxyRef: "http://127.0.0.1:8080",
        },
      },
      env: {
        OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM: "linux_iptables",
        OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE: "uid:4242",
        OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED: "1",
      } as NodeJS.ProcessEnv,
    });
    expect(plan.platform).toBe("linux_iptables");
    expect(plan.scope).toEqual({ kind: "uid", value: "4242" });
    expect(plan.allowedCidrs).toEqual([
      "127.0.0.0/8",
      "2001:db8::/32",
      "203.0.113.10/32",
      "::1/128",
    ]);
    expect(plan.proxyDelegatedHosts).toEqual(["example.com"]);
    expect(plan.protocolCoverage).toBe("all_ip");
    expect(plan.commands.map((entry) => entry.command)).toEqual(
      expect.arrayContaining(["iptables", "ip6tables"]),
    );
    expect(plan.commands.some((entry) => entry.args.includes("--uid-owner"))).toBe(true);
    expect(plan.releaseCommands.length).toBeGreaterThan(0);
  });

  it("builds scoped macOS pf firewall plans", () => {
    const plan = buildBuiltinFirewallEgressPlan({
      allocationId: "sandbox_test",
      backend: "local/no_isolation",
      policy: {
        trustZoneId: "public_web",
        backend: "local/no_isolation",
        networkPolicy: {
          id: "public_web_default",
          defaultAction: "allow",
          allowPrivateNetwork: false,
          allowedHosts: ["203.0.113.10"],
          proxyRef: "http://127.0.0.1:8080",
        },
      },
      env: {
        OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM: "darwin_pf",
        OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE: "uid:4242",
        OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED: "1",
      } as NodeJS.ProcessEnv,
    });
    expect(plan.platform).toBe("darwin_pf");
    expect(plan.protocolCoverage).toBe("tcp_udp");
    expect(plan.commands[0]?.command).toBe("/bin/sh");
    expect(plan.commands[0]?.args.join(" ")).toContain("pfctl");
    expect(plan.commands[0]?.args.join(" ")).toContain("user 4242");
    expect(plan.releaseCommands[0]?.args).toEqual(
      expect.arrayContaining(["-a", "com.apple/openclaw_sparsekernel/sandbox_test"]),
    );
  });

  it("builds scoped Windows default-block allowlist plans", () => {
    const sid = "S-1-5-21-1000-1000-1000-1001";
    const plan = buildBuiltinFirewallEgressPlan({
      allocationId: "sandbox_test",
      backend: "local/no_isolation",
      policy: {
        trustZoneId: "public_web",
        backend: "local/no_isolation",
        networkPolicy: {
          id: "public_web_default",
          defaultAction: "allow",
          allowPrivateNetwork: false,
          allowedHosts: ["203.0.113.10"],
        },
      },
      env: {
        OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM: "windows_advfirewall",
        OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE: `sid:${sid}`,
        OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED: "1",
        OPENCLAW_RUNTIME_SANDBOX_FIREWALL_WINDOWS_DEFAULT_OUTBOUND_BLOCK_CONFIRMED: "1",
      } as NodeJS.ProcessEnv,
    });
    expect(plan.platform).toBe("windows_advfirewall");
    expect(plan.commands[0]?.command).toBe("powershell.exe");
    expect(plan.commands[0]?.args.join(" ")).toContain("Get-NetFirewallProfile");
    expect(plan.commands.some((entry) => entry.args.includes("-LocalUser"))).toBe(true);
    expect(plan.commands.flatMap((entry) => entry.args)).toEqual(
      expect.arrayContaining(["-LocalUser", `D:(A;;CC;;;${sid})`]),
    );
    expect(plan.releaseCommands[0]?.args).toEqual(
      expect.arrayContaining(["Remove-NetFirewallRule"]),
    );
  });

  it("rejects firewall plans that cannot be expressed as a host firewall allowlist", () => {
    expect(() =>
      buildBuiltinFirewallEgressPlan({
        allocationId: "sandbox_test",
        backend: "local/no_isolation",
        policy: {
          trustZoneId: "public_web",
          backend: "local/no_isolation",
          networkPolicy: {
            id: "public_web_default",
            defaultAction: "allow",
            allowPrivateNetwork: false,
            allowedHosts: ["example.com"],
          },
        },
        env: {
          OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM: "linux_iptables",
          OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE: "uid:4242",
          OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED: "1",
        } as NodeJS.ProcessEnv,
      }),
    ).toThrow(/IP\/CIDR allowed_hosts/);
  });

  it("allocates builtin firewall leases with broker-managed worker identities", async () => {
    if (typeof process.getuid !== "function" || typeof process.getgid !== "function") {
      return;
    }
    const trueCommand = fs.existsSync("/usr/bin/true") ? "/usr/bin/true" : "true";
    const uid = process.getuid();
    const gid = process.getgid();
    const envNames = [
      "OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS",
      "OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER",
      "OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_IPTABLES",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_IP6TABLES",
      "OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITIES",
      "OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITY_MODE",
    ];
    const previous = new Map(envNames.map((name) => [name, process.env[name]]));
    process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER = "builtin-firewall";
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM = "linux_iptables";
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE;
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED;
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_IPTABLES = trueCommand;
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_IP6TABLES = trueCommand;
    process.env.OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITY_MODE = "managed";
    process.env.OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITIES = JSON.stringify([
      { id: "current", uid, gid },
    ]);
    try {
      const db = openTempDb();
      db.db
        .prepare("UPDATE network_policies SET proxy_ref = ? WHERE id = 'public_web_default'")
        .run("http://127.0.0.1:8080");
      const broker = new LocalSandboxBroker(db);
      const allocation = broker.allocateSandbox({
        taskId: "task-a",
        trustZoneId: "public_web",
        requirements: { backend: "local/no_isolation" },
      });
      const metadata = db.getResourceLease(allocation.id)?.metadata;
      expect(metadata).toMatchObject({
        workerIdentity: {
          id: "current",
          uid,
          gid,
        },
        hardEgress: {
          helper: "builtin-firewall",
          boundary: "host_firewall",
          workerIdentity: {
            id: "current",
          },
        },
      });
      const result = await broker.runCommand({
        allocationId: allocation.id,
        command: process.execPath,
        args: [
          "-e",
          "console.log(`${process.getuid?.()}:${process.getgid?.()}:${process.env.HTTP_PROXY}`)",
        ],
      });
      expect(result.exitCode).toBe(0);
      expect(result.stdout.trim()).toBe(`${uid}:${gid}:http://127.0.0.1:8080/`);
      expect(broker.releaseSandbox(allocation.id)).toBe(true);
    } finally {
      for (const name of envNames) {
        const value = previous.get(name);
        if (value === undefined) {
          delete process.env[name];
        } else {
          process.env[name] = value;
        }
      }
    }
  });

  it("enforces program-scoped Windows firewall leases at command execution", async () => {
    const trueCommand = fs.existsSync("/usr/bin/true") ? "/usr/bin/true" : "true";
    const envNames = [
      "OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS",
      "OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER",
      "OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_POWERSHELL",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_WINDOWS_DEFAULT_OUTBOUND_BLOCK_CONFIRMED",
    ];
    const previous = new Map(envNames.map((name) => [name, process.env[name]]));
    process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER = "builtin-firewall";
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM = "windows_advfirewall";
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE = `program:${process.execPath}`;
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_POWERSHELL = trueCommand;
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_WINDOWS_DEFAULT_OUTBOUND_BLOCK_CONFIRMED = "1";
    try {
      const db = openTempDb();
      db.db
        .prepare("UPDATE network_policies SET proxy_ref = ? WHERE id = 'public_web_default'")
        .run("http://127.0.0.1:8080");
      const broker = new LocalSandboxBroker(db);
      const allocation = broker.allocateSandbox({
        taskId: "task-a",
        trustZoneId: "public_web",
        requirements: { backend: "local/no_isolation" },
      });
      await expect(
        broker.runCommand({
          allocationId: allocation.id,
          command: "/bin/echo",
          args: ["blocked"],
        }),
      ).rejects.toThrow(/scoped firewall program/);
      const result = await broker.runCommand({
        allocationId: allocation.id,
        command: process.execPath,
        args: ["-e", "console.log('ok')"],
      });
      expect(result.exitCode).toBe(0);
      expect(result.stdout.trim()).toBe("ok");
      expect(broker.releaseSandbox(allocation.id)).toBe(true);
    } finally {
      for (const name of envNames) {
        const value = previous.get(name);
        if (value === undefined) {
          delete process.env[name];
        } else {
          process.env[name] = value;
        }
      }
    }
  });

  it("fails closed for builtin firewall hard egress without explicit host mutation", () => {
    const envNames = [
      "OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS",
      "OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER",
      "OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY",
    ];
    const previous = new Map(envNames.map((name) => [name, process.env[name]]));
    process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER = "builtin-firewall";
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM = "linux_iptables";
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE = "uid:4242";
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED = "1";
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY;
    try {
      const db = openTempDb();
      db.db
        .prepare("UPDATE network_policies SET proxy_ref = ? WHERE id = 'public_web_default'")
        .run("http://127.0.0.1:8080");
      const broker = new LocalSandboxBroker(db);
      expect(() =>
        broker.allocateSandbox({
          taskId: "task-a",
          trustZoneId: "public_web",
          requirements: { backend: "local/no_isolation" },
        }),
      ).toThrow(/will not mutate the host firewall/);
      const audit = db.db
        .prepare("SELECT action, payload_json FROM audit_log ORDER BY id DESC LIMIT 1")
        .get() as { action: string; payload_json: string };
      expect(audit.action).toBe("network_policy.hard_egress_unavailable");
      expect(JSON.parse(audit.payload_json).reason).toMatch(/will not mutate the host firewall/);
    } finally {
      for (const name of envNames) {
        const value = previous.get(name);
        if (value === undefined) {
          delete process.env[name];
        } else {
          process.env[name] = value;
        }
      }
    }
  });

  it("fails closed when full-protocol firewall coverage is required but macOS pf is selected", () => {
    if (typeof process.getuid !== "function") {
      return;
    }
    const trueCommand = fs.existsSync("/usr/bin/true") ? "/usr/bin/true" : "true";
    const envNames = [
      "OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS",
      "OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER",
      "OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_REQUIRE_FULL_PROTOCOL",
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PFCTL",
    ];
    const previous = new Map(envNames.map((name) => [name, process.env[name]]));
    process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER = "builtin-firewall";
    delete process.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS;
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM = "darwin_pf";
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE = `uid:${process.getuid()}`;
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_REQUIRE_FULL_PROTOCOL = "1";
    process.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PFCTL = trueCommand;
    try {
      const db = openTempDb();
      db.db
        .prepare("UPDATE network_policies SET proxy_ref = ? WHERE id = 'public_web_default'")
        .run("http://127.0.0.1:8080");
      const broker = new LocalSandboxBroker(db);
      expect(() =>
        broker.allocateSandbox({
          taskId: "task-a",
          trustZoneId: "public_web",
          requirements: { backend: "local/no_isolation" },
        }),
      ).toThrow(/full-protocol/);
    } finally {
      for (const name of envNames) {
        const value = previous.get(name);
        if (value === undefined) {
          delete process.env[name];
        } else {
          process.env[name] = value;
        }
      }
    }
  });

  it("builds broker-managed worker identity provisioning plans", () => {
    const plan = buildWorkerIdentityProvisionPlan({
      platform: "linux",
      count: 2,
      prefix: "openclaw-worker",
      uidStart: 63000,
      gid: 63000,
      group: "openclaw-workers",
    });
    expect(plan.identities).toEqual([
      expect.objectContaining({ id: "openclaw-worker-0", uid: 63000, gid: 63000 }),
      expect.objectContaining({ id: "openclaw-worker-1", uid: 63001, gid: 63000 }),
    ]);
    expect(plan.environment.OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITY_MODE).toBe("managed");
    expect(plan.commands.some((entry) => entry.args.join(" ").includes("useradd"))).toBe(true);
  });

  it("persists sandbox backend metadata across broker instances", async () => {
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
    const allocation = new LocalSandboxBroker(db).allocateSandbox({
      taskId: "task-a",
      agentId: "main",
      trustZoneId: "code_execution",
      requirements: { backend: "local/no_isolation", maxRuntimeMs: 5_000 },
    });
    expect(db.getResourceLease(allocation.id)).toMatchObject({
      metadata: expect.objectContaining({
        backend: "local/no_isolation",
        policy: expect.objectContaining({
          trustZoneId: "code_execution",
          backend: "local/no_isolation",
        }),
      }),
    });
    await expect(
      new LocalSandboxBroker(db).runCommand({
        allocationId: allocation.id,
        command: process.execPath,
        args: ["-e", "process.stdout.write('persisted')"],
      }),
    ).resolves.toMatchObject({ exitCode: 0, stdout: "persisted" });
    expect(db.listResourceLeases({ resourceType: "sandbox", status: "active" })).toEqual([
      expect.objectContaining({ id: allocation.id }),
    ]);
  });

  it("does not execute sandbox commands when allocation backend state is missing", async () => {
    const db = openTempDb();
    db.createResourceLease({
      id: "sandbox-missing-backend",
      resourceType: "sandbox",
      resourceId: "sandbox-missing-backend",
      trustZoneId: "code_execution",
    });
    const broker = new LocalSandboxBroker(db);
    await expect(
      broker.runCommand({
        allocationId: "sandbox-missing-backend",
        command: process.execPath,
      }),
    ).rejects.toThrow(/backend is not available/);
  });

  it("does not silently allocate unconfigured external sandbox backends", () => {
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
    expect(() =>
      broker.allocateSandbox({
        taskId: "task-a",
        agentId: "main",
        trustZoneId: "code_execution",
        requirements: { backend: "other" },
      }),
    ).toThrow(/Sandbox backend unavailable: other/);
    const audit = db.db
      .prepare(
        "SELECT action FROM audit_log WHERE action = 'sandbox.allocation_denied_backend_unavailable'",
      )
      .get() as { action: string } | undefined;
    expect(audit?.action).toBe("sandbox.allocation_denied_backend_unavailable");
  });

  it("builds brokered VM sandbox wrapper command plans", () => {
    const previousCommand = process.env.OPENCLAW_RUNTIME_SANDBOX_VM_COMMAND;
    const previousArgs = process.env.OPENCLAW_RUNTIME_SANDBOX_VM_ARGS;
    const previousBoundary = process.env.OPENCLAW_RUNTIME_SANDBOX_VM_BOUNDARY;
    process.env.OPENCLAW_RUNTIME_SANDBOX_VM_COMMAND = process.execPath;
    process.env.OPENCLAW_RUNTIME_SANDBOX_VM_ARGS = JSON.stringify(["vm-wrapper.mjs"]);
    process.env.OPENCLAW_RUNTIME_SANDBOX_VM_BOUNDARY = "vm_firewall";
    try {
      const probes = probeSandboxBackends({ env: process.env });
      expect(probes.find((probe) => probe.backend === "vm")).toMatchObject({
        available: true,
        hardBoundary: true,
        command: process.execPath,
      });
      expect(isSandboxBackendAvailable("vm")).toBe(true);

      const plan = buildSandboxSpawnPlan({
        backend: "vm",
        command: "node",
        args: ["worker.mjs"],
        cwd: "/workspace",
        env: { SAFE_ENV: "1" },
        stdin: true,
        isolationProfile: "plugin_untrusted",
      });
      const requestIndex = plan.args.indexOf("--sparsekernel-request-base64");
      expect(plan).toMatchObject({
        command: process.execPath,
        args: expect.arrayContaining(["vm-wrapper.mjs", "--", "node", "worker.mjs"]),
      });
      expect(requestIndex).toBeGreaterThanOrEqual(0);
      const encoded = plan.args[requestIndex + 1];
      if (!encoded) {
        throw new Error("expected encoded sandbox command request");
      }
      const request = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8")) as {
        protocol: string;
        backend: string;
        command: string;
        args: string[];
        env: Record<string, string>;
        stdin: boolean;
      };
      expect(request).toMatchObject({
        protocol: "openclaw.sparsekernel.sandbox-command.v1",
        backend: "vm",
        command: "node",
        args: ["worker.mjs"],
        env: { SAFE_ENV: "1" },
        stdin: true,
      });
      expect(plan.isolation).toContain("declaredBoundary=vm_firewall");
    } finally {
      if (previousCommand === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_VM_COMMAND;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_VM_COMMAND = previousCommand;
      }
      if (previousArgs === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_VM_ARGS;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_VM_ARGS = previousArgs;
      }
      if (previousBoundary === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SANDBOX_VM_BOUNDARY;
      } else {
        process.env.OPENCLAW_RUNTIME_SANDBOX_VM_BOUNDARY = previousBoundary;
      }
    }
  });

  it("builds explicit Docker sandbox command plans without host fallback", () => {
    const bwrapPlan = buildSandboxSpawnPlan({
      backend: "bwrap",
      command: "node",
      args: ["-v"],
      cwd: "/work",
      env: { EXPLICIT_WORKER_ENV: "ok" },
      isolationProfile: "readonly_workspace",
      resolveCommand: () => "bwrap",
    });
    expect(bwrapPlan.args).toEqual(
      expect.arrayContaining(["--clearenv", "--setenv", "EXPLICIT_WORKER_ENV", "ok"]),
    );
    expect(bwrapPlan.args).toEqual(expect.arrayContaining(["--tmpfs", "/home"]));
    expect(bwrapPlan.args).toEqual(expect.arrayContaining(["--ro-bind", "/work", "/work"]));
    expect(bwrapPlan.isolation).toContain("profile=readonly_workspace");

    const minijailPlan = buildSandboxSpawnPlan({
      backend: "minijail",
      command: "node",
      args: ["-v"],
      isolationProfile: "plugin_untrusted",
      resolveCommand: () => "minijail0",
    });
    expect(minijailPlan.args.slice(0, 2)).toEqual(["-p", "-v"]);
    expect(minijailPlan.isolation).toContain("profile=plugin_untrusted");

    expect(() =>
      buildSandboxSpawnPlan({
        backend: "docker",
        command: "node",
        args: ["-v"],
        resolveCommand: () => "docker",
      }),
    ).toThrow(/dockerImage/);
    expect(
      buildSandboxSpawnPlan({
        backend: "docker",
        command: "node",
        args: ["-v"],
        cwd: "/work",
        dockerImage: "openclaw-runtime:local",
        dockerPolicy: {
          networkMode: "bridge",
          proxyServer: "http://127.0.0.1:18080/",
          memoryMb: 512,
          pidsLimit: 16,
          readOnlyRoot: true,
        },
        env: { SAFE_ENV: "1", "BAD-NAME": "skip" },
        resolveCommand: () => "docker",
      }),
    ).toMatchObject({
      command: "docker",
      args: expect.arrayContaining([
        "run",
        "--rm",
        "--pull",
        "never",
        "--network",
        "bridge",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--read-only",
        "--memory",
        "512m",
        "--pids-limit",
        "16",
        "--add-host",
        "host.docker.internal:host-gateway",
        "--env",
        "SAFE_ENV=1",
        "--env",
        "HTTP_PROXY=http://host.docker.internal:18080/",
        "-v",
        "/work:/workspace:rw",
        "openclaw-runtime:local",
        "node",
        "-v",
      ]),
    });
    expect(
      buildSandboxSpawnPlan({
        backend: "docker",
        command: "node",
        args: ["worker.mjs"],
        dockerImage: "openclaw-runtime:local",
        stdin: true,
        resolveCommand: () => "docker",
      }).args.slice(0, 4),
    ).toEqual(["run", "--rm", "-i", "--pull"]);
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
      const db = run.db;
      if (!db) {
        throw new Error("expected local sandbox accounting DB");
      }
      const lease = db.db
        .prepare("SELECT status, owner_task_id FROM resource_leases WHERE id = ?")
        .get(allocation.id) as { status: string; owner_task_id: string | null };
      expect(lease).toEqual({ status: "active", owner_task_id: null });
      void run.release();
      const released = db.db
        .prepare("SELECT status FROM resource_leases WHERE id = ?")
        .get(allocation.id) as { status: string };
      expect(released.status).toBe("released");
    } finally {
      run.close();
    }
  });

  it("can account sandbox allocations through the SparseKernel daemon API", async () => {
    const calls: string[] = [];
    const run = await accountSandboxForRunEffective({
      agentId: "main",
      sessionId: "session-a",
      runId: "run-a",
      taskId: "task-a",
      backendId: "docker",
      env: { OPENCLAW_RUNTIME_TOOL_BROKER: "daemon" } as NodeJS.ProcessEnv,
      daemonKernel: {
        async grantCapability(input) {
          calls.push(`grant:${input.resource_type}:${input.resource_id}`);
          return {
            id: "cap-a",
            subject_type: input.subject_type,
            subject_id: input.subject_id,
            resource_type: input.resource_type,
            resource_id: input.resource_id,
            action: input.action,
            created_at: "2026-01-01T00:00:00.000Z",
          };
        },
        async allocateSandbox(input) {
          calls.push(`allocate:${input.backend}`);
          return {
            id: "sandbox-a",
            task_id: input.task_id,
            trust_zone_id: input.trust_zone_id,
            backend: input.backend ?? "local/no_isolation",
            status: "active",
            created_at: "2026-01-01T00:00:00.000Z",
          };
        },
        async releaseSandbox(id) {
          calls.push(`release:${id}`);
          return true;
        },
      },
    });
    expect(run.allocation).toMatchObject({ id: "sandbox-a", backend: "docker" });
    await run.release();
    expect(calls).toEqual(["grant:sandbox:code_execution", "allocate:docker", "release:sandbox-a"]);
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
