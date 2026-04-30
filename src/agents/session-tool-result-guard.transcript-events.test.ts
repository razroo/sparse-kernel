import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AgentMessage } from "@mariozechner/pi-agent-core";
import { SessionManager } from "@mariozechner/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { openLocalKernelDatabase } from "../local-kernel/database.js";
import {
  onSessionTranscriptUpdate,
  type SessionTranscriptUpdate,
} from "../sessions/transcript-events.js";
import { guardSessionManager } from "./session-tool-result-guard-wrapper.js";

const listeners: Array<() => void> = [];
const roots: string[] = [];

afterEach(() => {
  while (listeners.length > 0) {
    listeners.pop()?.();
  }
  for (const root of roots.splice(0)) {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

describe("guardSessionManager transcript updates", () => {
  it("includes the session key when broadcasting appended non-tool-result messages", () => {
    const updates: SessionTranscriptUpdate[] = [];
    listeners.push(onSessionTranscriptUpdate((update) => updates.push(update)));

    const sm = SessionManager.inMemory();
    const sessionFile = "/tmp/openclaw-session-message-events.jsonl";
    Object.assign(sm, {
      getSessionFile: () => sessionFile,
    });

    const guarded = guardSessionManager(sm, {
      agentId: "main",
      sessionKey: "agent:main:worker",
    });
    const appendMessage = guarded.appendMessage.bind(guarded) as unknown as (
      message: AgentMessage,
    ) => void;

    appendMessage({
      role: "assistant",
      content: [{ type: "text", text: "hello from subagent" }],
      timestamp: Date.now(),
    } as AgentMessage);

    expect(updates).toHaveLength(1);
    expect(updates[0]).toMatchObject({
      sessionFile,
      sessionKey: "agent:main:worker",
      message: {
        role: "assistant",
      },
    });
  });

  it("mirrors guarded append methods through the runtime transcript ledger in primary mode", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-transcript-ledger-"));
    roots.push(root);
    const previousStateDir = process.env.OPENCLAW_STATE_DIR;
    const previousMode = process.env.OPENCLAW_RUNTIME_SESSION_STORE;
    process.env.OPENCLAW_STATE_DIR = root;
    process.env.OPENCLAW_RUNTIME_SESSION_STORE = "sqlite";
    try {
      const sessionDir = path.join(root, "sessions");
      fs.mkdirSync(sessionDir, { recursive: true });
      const sessionFile = path.join(sessionDir, "session-a.jsonl");
      const storePath = path.join(sessionDir, "sessions.json");
      fs.writeFileSync(
        storePath,
        JSON.stringify({
          "agent:main:worker": {
            sessionId: "session-a",
            status: "idle",
          },
        }),
      );

      const sm = SessionManager.inMemory();
      Object.assign(sm, {
        getSessionFile: () => sessionFile,
      });
      const guarded = guardSessionManager(sm, {
        agentId: "main",
        sessionKey: "agent:main:worker",
      });
      guarded.appendThinkingLevelChange("high");

      const db = openLocalKernelDatabase();
      try {
        const events = db.listTranscriptEvents("session-a");
        expect(events).toHaveLength(1);
        expect(events[0]).toMatchObject({
          role: "system",
          eventType: expect.any(String),
        });
        expect(events[0]?.content).toMatchObject({
          id: expect.any(String),
          type: expect.any(String),
        });
      } finally {
        db.close();
      }
    } finally {
      if (previousStateDir === undefined) {
        delete process.env.OPENCLAW_STATE_DIR;
      } else {
        process.env.OPENCLAW_STATE_DIR = previousStateDir;
      }
      if (previousMode === undefined) {
        delete process.env.OPENCLAW_RUNTIME_SESSION_STORE;
      } else {
        process.env.OPENCLAW_RUNTIME_SESSION_STORE = previousMode;
      }
    }
  });
});
