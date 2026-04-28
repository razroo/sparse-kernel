import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type {
  SparseKernelAppendTranscriptEventInput,
  SparseKernelSession,
  SparseKernelTask,
  SparseKernelTranscriptEvent,
  SparseKernelUpsertSessionInput,
} from "../../packages/sparsekernel-client/src/index.js";
import { LocalKernelDatabase } from "./database.js";
import type { EmbeddedRunKernelLedgerClient } from "./run-ledger-runtime.js";
import { materializeEmbeddedRunInKernel } from "./run-ledger-runtime.js";

const roots: string[] = [];
const dbs: LocalKernelDatabase[] = [];

function tempRoot(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-run-ledger-test-"));
  roots.push(root);
  return root;
}

afterEach(() => {
  for (const db of dbs.splice(0)) {
    db.close();
  }
  for (const root of roots.splice(0)) {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

class FakeRunKernel implements EmbeddedRunKernelLedgerClient {
  readonly sessions: SparseKernelUpsertSessionInput[] = [];
  readonly tasks = new Map<string, SparseKernelTask>();
  readonly events: SparseKernelAppendTranscriptEventInput[] = [];
  readonly completed: string[] = [];
  readonly failed: Array<{ taskId: string; error: string }> = [];

  async upsertSession(input: SparseKernelUpsertSessionInput): Promise<SparseKernelSession> {
    this.sessions.push(input);
    return {
      id: input.id,
      agent_id: input.agent_id,
      session_key: input.session_key,
      channel: input.channel,
      status: input.status ?? "active",
      current_token_count: input.current_token_count ?? 0,
      last_activity_at: input.last_activity_at,
      created_at: "2026-04-27T00:00:00Z",
      updated_at: "2026-04-27T00:00:00Z",
    };
  }

  async enqueueTask(input: {
    id?: string;
    agent_id?: string | null;
    session_id?: string | null;
    kind: string;
    priority?: number;
  }): Promise<SparseKernelTask> {
    const id = input.id ?? `task-${this.tasks.size + 1}`;
    const task: SparseKernelTask = {
      id,
      agent_id: input.agent_id,
      session_id: input.session_id,
      kind: input.kind,
      priority: input.priority ?? 0,
      status: "queued",
      attempts: 0,
      created_at: "2026-04-27T00:00:00Z",
      updated_at: "2026-04-27T00:00:00Z",
    };
    this.tasks.set(id, task);
    return task;
  }

  async claimTask(input: { task_id: string; worker_id: string }): Promise<SparseKernelTask | null> {
    const task = this.tasks.get(input.task_id);
    if (!task || task.status !== "queued") {
      return null;
    }
    task.status = "running";
    task.lease_owner = input.worker_id;
    task.attempts += 1;
    return task;
  }

  async heartbeatTask(): Promise<boolean> {
    return true;
  }

  async completeTask(input: { task_id: string }): Promise<boolean> {
    this.completed.push(input.task_id);
    const task = this.tasks.get(input.task_id);
    if (task) {
      task.status = "completed";
    }
    return true;
  }

  async failTask(input: { task_id: string; error: string }): Promise<boolean> {
    this.failed.push({ taskId: input.task_id, error: input.error });
    const task = this.tasks.get(input.task_id);
    if (task) {
      task.status = "failed";
    }
    return true;
  }

  async appendTranscriptEvent(
    input: SparseKernelAppendTranscriptEventInput,
  ): Promise<SparseKernelTranscriptEvent> {
    this.events.push(input);
    return {
      id: this.events.length,
      session_id: input.session_id,
      parent_event_id: input.parent_event_id,
      seq: this.events.length,
      role: input.role,
      event_type: input.event_type,
      content: input.content,
      tool_call_id: input.tool_call_id,
      token_count: input.token_count,
      created_at: "2026-04-27T00:00:00Z",
    };
  }
}

describe("embedded run SparseKernel ledger", () => {
  it("claims a local task and writes transcript events through completion", async () => {
    const root = tempRoot();
    const dbPath = path.join(root, "runtime.sqlite");
    const ledger = await materializeEmbeddedRunInKernel({
      agentId: "agent-a",
      sessionId: "session-a",
      sessionKey: "agent:agent-a:main",
      runId: "run-a",
      provider: "openai",
      modelId: "gpt-5.4",
      dbPath,
      env: { OPENCLAW_RUNTIME_TOOL_BROKER: "local" } as NodeJS.ProcessEnv,
    });
    expect(ledger).toBeDefined();
    await ledger?.appendTranscriptEvent({
      role: "user",
      eventType: "prompt.submitted",
      content: { prompt: "hello" },
    });
    await ledger?.complete({ output: { ok: true } });
    ledger?.close();

    const db = new LocalKernelDatabase({ dbPath });
    dbs.push(db);
    expect(db.getTask("run-a")).toMatchObject({ status: "succeeded", attempts: 1 });
    expect(db.listTranscriptEvents("session-a").map((event) => event.eventType)).toEqual([
      "run.started",
      "prompt.submitted",
      "run.completed",
    ]);
  });

  it("routes run lifecycle through the daemon client when daemon mode is enabled", async () => {
    const kernel = new FakeRunKernel();
    const ledger = await materializeEmbeddedRunInKernel({
      agentId: "agent-a",
      sessionId: "session-a",
      runId: "run-daemon",
      daemonKernel: kernel,
      env: { OPENCLAW_RUNTIME_TOOL_BROKER: "daemon" } as NodeJS.ProcessEnv,
    });
    expect(ledger?.mode).toBe("daemon");
    await ledger?.appendTranscriptEvent({
      role: "assistant",
      eventType: "message",
      content: { text: "done" },
    });
    await ledger?.complete();
    ledger?.close();

    expect(kernel.sessions).toHaveLength(1);
    expect(kernel.tasks.get("run-daemon")).toMatchObject({ status: "completed", attempts: 1 });
    expect(kernel.events.map((event) => event.event_type)).toEqual([
      "run.started",
      "message",
      "run.completed",
    ]);
    expect(kernel.completed).toEqual(["run-daemon"]);
  });
});
