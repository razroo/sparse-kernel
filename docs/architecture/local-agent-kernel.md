---
summary: "Local runtime kernel ledger, artifact store, broker interfaces, and trust-zone resource accounting"
read_when:
  - Changing session, task, tool-call, browser, sandbox, artifact, or audit persistence
  - Adding runtime DB migrations or brokered resource operations
  - Reviewing local agent kernel security boundaries and non-boundaries
title: "Local Agent Kernel"
sidebarTitle: "Local Agent Kernel"
---

OpenClaw is moving toward SparseKernel: the Gateway owns runtime state transitions, brokers access to expensive or sensitive resources, and records the transitions that matter. See [SparseKernel](/architecture/sparsekernel) for the Rust daemon and SDK direction.

This is not just replacing JSON with SQLite. The runtime ledger is for structured state, coordination, leases, permissions, resource accounting, and audit trails. Large byte payloads stay out of SQLite.

## Current storage model

Existing sessions remain file-backed:

- `sessions.json` stores session metadata per agent.
- `<sessionId>.jsonl` stores append-only transcript entries.
- Memory uses plugin-owned SQLite indexes such as `~/.openclaw/memory/{agentId}.sqlite`.
- Existing task tracking already has SQLite-backed registries under OpenClaw state.

The v0 kernel does not remove those paths. It adds an import/export compatibility layer, a SQLite session-entry mirror, and a strict SQLite read mode so existing sessions can be copied into the runtime ledger and exported back to legacy JSONL while the legacy files remain available.

## Runtime ledger

The runtime DB lives at:

```bash
~/.openclaw/runtime/openclaw.sqlite
```

Use:

```bash
openclaw runtime migrate
openclaw runtime inspect
openclaw sparsekernel sessions
openclaw sparsekernel tasks --kind openclaw.embedded_run
openclaw sparsekernel transcript --session <session-id>
openclaw sparsekernel recover
openclaw runtime budget
openclaw runtime budget set --trust-zone code_execution --max-runtime-seconds 600
openclaw runtime vacuum
openclaw runtime prune --older-than 7d
```

The DB uses WAL mode, foreign keys, a busy timeout, migrations, and typed runtime APIs. Agents and plugins should not open this database directly.

The initial schema includes agents, sessions, legacy session-entry mirrors, transcript events, messages, tasks, task events, tool calls, trust zones, network policies, resource leases, browser pools and contexts, artifacts, artifact access, capabilities, audit log, and usage records.

## Artifact store

Large outputs are content-addressed files under:

```bash
~/.openclaw/artifacts/sha256/ab/cd/<sha256>
```

SQLite stores metadata, access records, and retention policy only. Screenshots, downloads, PDFs, browser traces, videos, large HTML dumps, workspace snapshots, and exported transcripts should be artifact files, not SQLite blobs.

Brokered tool calls also keep oversized JSON outputs out of `tool_calls.output_json`. The ToolBroker returns the original tool result to the agent, stores large serialized outputs as `debug` artifacts, and records an artifact reference plus audit event in the ledger.

Retention policies are:

- `ephemeral`
- `session`
- `durable`
- `debug`

`openclaw runtime prune` currently prunes old `ephemeral` and `debug` artifacts by default. Pass `--retention ephemeral,debug,session` only when session-scoped artifacts should also be removed.

## Trust zones

Trust zones describe the policy boundary for resource pools:

- `public_web`
- `authenticated_web`
- `local_files_readonly`
- `local_files_rw`
- `code_execution`
- `plugin_untrusted`
- `user_browser_profile`

Expensive resources are pooled by trust zone, not by logical agent. A task or session gets a lease from the pool.

## Browser broker

The browser broker model is:

- browser pools are keyed by trust zone;
- browser contexts are leased per task/session/agent;
- screenshots, traces, HAR, video, and downloads become artifacts only when requested or needed for debugging;
- raw CDP access is not exposed to agents by default.

Important boundary: BrowserContext isolation is session isolation, not host isolation. Playwright route blocking and SSRF guards are useful controls, but they are not hard security boundaries.

The broker applies configured trust-zone network policy to explicit allowed origins before allocating a context. This is an egress guard for brokered contexts, not a kernel or VM boundary. Set `OPENCLAW_RUNTIME_BROWSER_BROKER=cdp` and `OPENCLAW_SPARSEKERNEL_BROWSER_CDP_ENDPOINT=<loopback endpoint>` to make the OpenClaw browser tool acquire a real SparseKernel CDP context around the tool call.

## Sandbox broker

The sandbox broker records allocations and leases behind a backend abstraction. Current v0 includes a local/no-isolation accounting backend, detects Docker/bwrap/minijail availability when those backends are requested, and preserves existing Docker/SSH/OpenShell sandbox systems for later wrapping. In daemon broker mode, embedded runs grant sandbox allocation capability and allocate/release the `code_execution` sandbox lease through the SparseKernel daemon API before falling back to local accounting.

Important boundary: `local/no_isolation` means accounting only. It does not provide process, filesystem, network, kernel, or VM isolation. Docker, bwrap, minijail, gVisor, or VM backends must be described by their actual guarantees when implemented.

## Capability flow

Sensitive operations should check capabilities before they mutate state or allocate resources:

- artifact reads and writes;
- browser context allocation;
- sandbox allocation;
- tool invocation.

Capability grants, revokes, and denied checks are audited. Embedded agent runs now materialize the active step as a task lease and transcript events, then wrap the effective tool set with the local ToolBroker outside tests unless explicitly disabled with `OPENCLAW_RUNTIME_TOOL_BROKER=off`. Set `OPENCLAW_RUNTIME_TOOL_BROKER=daemon` to use the `sparsekerneld` run-ledger and ToolBroker path; daemon setup failures fall back to the local runtime broker. Set `OPENCLAW_RUNTIME_TOOL_CAPABILITY_MODE=strict` to stop auto-granting sensitive tools such as `exec`, `read`, `write`, and `browser`; those tools then fail closed unless a capability already exists or `OPENCLAW_RUNTIME_TOOL_ALLOW_SENSITIVE=1` is set for compatibility. Native in-process plugins remain trusted in v0; the broker wrapper establishes the contract for moving plugin/tool invocation behind capability checks and then out of process.

The SparseKernel daemon now exposes the v0 run and ToolBroker lifecycle over local JSON: session upsert/list, transcript append/list, task enqueue/claim-by-id/claim-next/heartbeat/complete/fail, tool-call create/start/complete/fail/list, browser acquire/release, sandbox allocate/release, and artifact access. Tool-call create checks `tool` / `<tool-name>` / `invoke`; completion records small structured output plus `artifact_ids`; and every transition writes audit records. See [Tool Broker](/architecture/tool-broker).

## Session compatibility

Import existing sessions:

```bash
openclaw sessions import --from-existing
```

Export one runtime-ledger session as legacy JSONL:

```bash
openclaw sessions export --session <session-id> --format jsonl
```

Import is additive and idempotent for already-imported transcript events. It does not delete or rewrite `sessions.json` or transcript files.

Session metadata writes are mirrored into the runtime ledger by default. Set `OPENCLAW_RUNTIME_SESSION_STORE=sqlite` to make the runtime ledger the primary session metadata store; set `OPENCLAW_RUNTIME_SESSION_STORE=off` to disable mirroring. Strict SQLite mode writes the ledger first and still attempts legacy JSON output for compatibility. Failed ledger writes are hard failures; failed legacy JSON writes are warnings because SQLite is authoritative in this mode.

## Current limitations

- Full transcript ownership is still staged: session metadata can run with SQLite as primary, assistant transcript mirrors and import/export are ledger-backed, and the pi session manager still appends JSONL.
- Browser process pooling is still accounting-first; the current browser control backend is not fully replaced by pooled Playwright process management.
- Sandbox allocation records requested bwrap/minijail/Docker backends and checks availability, but local/no-isolation still does not harden execution.
- Native plugins still execute in process; tool invocation is brokered and audited but untrusted plugin isolation is a later process-boundary change.
- Network policy enforcement currently exists where brokers call it; host-level egress proxying is still future work.

## Contributor rules

- Add runtime DB migrations instead of ad hoc JSON files for coordination state.
- Keep large blobs in the artifact store and DB metadata in SQLite.
- Do not expose raw kernel DB handles to agents or plugins.
- Use broker APIs for browser, sandbox, artifact, and sensitive tool operations.
- Add tests for every migration and broker operation.
