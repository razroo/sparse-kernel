---
summary: "SparseKernel tool broker lifecycle, capability checks, artifacts, and audit records"
read_when:
  - Implementing or reviewing SparseKernel tool-call mediation
  - Adding tool-call lifecycle, artifact output, or capability checks
title: "Tool Broker"
sidebarTitle: "Tool Broker"
---

SparseKernel treats tool invocation as a brokered lifecycle, not an ambient function call.

The v0 daemon exposes local API endpoints to create, start, complete, fail, and list tool calls. Creating a tool call for an agent checks `tool` / `<tool-name>` / `invoke` capability before a ledger row is inserted. Denied checks write capability audit records, and the broker also records a `tool_call.denied` audit event.

Tool call statuses are:

- `created`
- `running`
- `completed`
- `failed`

Completion records small structured results in `tool_calls.output_json`. Large outputs, screenshots, downloads, traces, and files must be stored through the artifact store first; completion records their artifact ids in `output_json.artifact_ids`.

The broker emits audit events for:

- `tool_call.created`
- `tool_call.denied`
- `tool_call.started`
- `tool_call.completed`
- `tool_call.failed`

V0 does not move native plugins out of process. It establishes the ledger and API contract so OpenClaw adapters can route invocation through SparseKernel now, then move untrusted or community plugins behind stronger process or sandbox boundaries later.
