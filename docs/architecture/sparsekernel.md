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
- `packages/sparsekernel-client`: small TypeScript daemon client.
- `schemas/`: API and event schema definitions.

## Local API

The v0 daemon exposes localhost JSON endpoints for health/status, task enqueue/claim/heartbeat/complete/fail, expired lease release, artifact create/read/metadata, browser context acquire/release/list, loopback CDP endpoint probing, capability grant/check/list/revoke, task listing, and audit listing. The TypeScript client uses those endpoints instead of opening the SQLite file directly.

The API is intentionally narrow. Agents and adapters should call the daemon or typed core APIs; they should not read or mutate the ledger with raw SQL.

The browser broker can now register an existing local CDP endpoint on a trust-zone pool and probe `/json/version` to verify the browser process is reachable. That is a local integration seam, not agent authority: agents receive leases and context ids, not raw CDP endpoints.

## Current limitations

V0 proves the foundation. It does not launch or supervise Playwright browser process pools, create real Playwright contexts, implement production sandbox backends, enforce an egress proxy, isolate plugins in subprocesses, stream large artifact transfers, or rewrite OpenClaw around SparseKernel.
