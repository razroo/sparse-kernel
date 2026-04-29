---
summary: "SparseKernel architecture for sparse local multi-agent execution"
read_when:
  - Introducing SparseKernel runtime APIs, ledgers, brokers, or adapters
  - Reviewing OpenClaw SparseKernel design decisions
title: "SparseKernel"
sidebarTitle: "SparseKernel"
---

SparseKernel is a SQLite-backed local agent runtime that keeps hundreds of agents as compact durable state and materializes only the active slice through leased tools, files, browsers, and sandboxes.

In OpenClaw, this direction is called OpenClaw SparseKernel. The existing TypeScript local-kernel layer remains the compatibility path while the Rust SparseKernel workspace grows into the daemon, ledger, scheduler, brokers, and client SDK surface.

## Why it exists

Small machines can keep many logical agents parked in durable state, but they cannot keep hundreds of full coding harnesses, browsers, sandboxes, and model calls live at once. SparseKernel separates logical agent state from materialized execution:

- agents sleep as compact rows and transcript events;
- active work claims bounded tasks and leases;
- browsers, sandboxes, files, and tools are brokered;
- large outputs are artifacts, not database blobs;
- capability checks mediate information flow.

## Components

- Ledger: SQLite runtime database for structured state, leases, permissions, and audit records.
- Artifact Store: content-addressed files for large blobs.
- Task and Lease Scheduler: bounded active execution.
- Capability Engine: allow, deny, revoke, and audit sensitive actions.
- Tool Broker: mediated tool invocation.
- Browser Broker: process pools by trust zone and contexts by task/session.
- Sandbox Broker: backend abstraction for local, bwrap, Docker, minijail, smolvm, or VM backends.
- Client SDKs and Adapters: TypeScript client plus OpenClaw adapter work.

## V0 layout

- `crates/sparsekernel-core`: Rust ledger, artifacts, task leases, capabilities, audit, and mock brokers.
- `crates/sparsekernel-cli`: `sparsekernel` CLI and `sparsekerneld` local daemon.
- `migrations/0001_initial.sql`: initial SQLite schema.
- `packages/browser-broker`: TypeScript CDP adapter that materializes leased browser contexts and artifactizes screenshots/downloads.
- `packages/openclaw-sparsekernel-adapter`: TypeScript adapter that wraps OpenClaw-shaped tool execution with daemon-backed session upsert, capability grants, tool-call lifecycle transitions, and oversized-output artifactization.
- `packages/sparsekernel-client`: small TypeScript daemon client.
- `schemas/`: API and event schema definitions.

## Local API

The v0 daemon exposes localhost JSON endpoints for health/status, session upsert/list, transcript event append/list, task enqueue/claim-by-id/claim-next/heartbeat/complete/fail, expired lease release, tool-call create/start/complete/fail/list, artifact create/read/metadata, browser context acquire/release/list, loopback CDP endpoint probing, sandbox allocate/release, capability grant/check/list/revoke, task listing, and audit listing. `/health` advertises `protocol_version`, `schema_version`, and feature strings so clients can reject incompatible daemon protocols before making state-changing calls. The TypeScript client uses those endpoints instead of opening the SQLite file directly.

The API is intentionally narrow. Agents and adapters should call the daemon or typed core APIs; they should not read or mutate the ledger with raw SQL.

OpenClaw embedded runs now materialize the active step as a SparseKernel task lease and append transcript events for run start, prompt submission, assistant output, and run completion/failure. The default path writes to the local runtime ledger for compatibility. Restart recovery requeues expired embedded-run leases and leases owned by dead OpenClaw worker pids. Set `OPENCLAW_RUNTIME_TOOL_BROKER=daemon` or `OPENCLAW_SPARSEKERNEL_TOOL_BROKER=1` to route the run ledger, tool broker, and sandbox accounting through `sparsekerneld`; daemon setup failures fall back to the local broker.

The browser broker can now register an existing local CDP endpoint on a trust-zone pool and probe `/json/version` to verify the browser process is reachable. The TypeScript CDP adapter can then create a real browser context and target for the leased task/session, capture screenshots, and store screenshots/downloads as SparseKernel artifacts. OpenClaw can materialize that CDP context around the browser tool behind `OPENCLAW_RUNTIME_BROWSER_BROKER=cdp` plus `OPENCLAW_SPARSEKERNEL_BROWSER_CDP_ENDPOINT=<loopback endpoint>`. That is a local integration seam, not agent authority: agents receive leases, context ids, and artifact ids, not raw CDP endpoints.

The tool broker records tool-call lifecycle transitions, capability-checks agent invocation, and links output artifacts. See [Tool Broker](/architecture/tool-broker).

## Current limitations

V0 proves the foundation. It does not implement production sandbox backends, provide bypass-proof host-level egress blocking, make subprocess plugin workers safe when no real isolation backend is available, stream large artifact transfers, or rewrite OpenClaw around SparseKernel.
