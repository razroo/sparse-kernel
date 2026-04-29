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

The `@openclaw/openclaw-sparsekernel-adapter` package wraps OpenClaw-shaped tool objects around this daemon lifecycle. It upserts the session, grants bounded per-tool invoke capabilities for the active agent, records create/start/complete/fail transitions, and stores oversized serialized tool outputs as `debug` artifacts while returning the original tool result to the running agent.

Embedded OpenClaw runs keep the local SQLite broker as the default compatibility path. Set `OPENCLAW_RUNTIME_TOOL_BROKER=daemon` or `OPENCLAW_SPARSEKERNEL_TOOL_BROKER=1` to route the embedded run through `sparsekerneld`; set `OPENCLAW_SPARSEKERNEL_BASE_URL` or `SPARSEKERNEL_BASE_URL` when the daemon is not on the default localhost URL. If daemon setup fails, OpenClaw falls back to the local runtime broker and logs the reason.

The compatibility path auto-grants per-run tool invoke capabilities by default so existing behavior keeps working. Set `OPENCLAW_RUNTIME_TOOL_CAPABILITY_MODE=strict` to fail closed for sensitive tools (`exec`, `read`, `write`, `browser`, and filesystem/sandbox-shaped tool names) unless capabilities are granted by another policy path. `OPENCLAW_RUNTIME_TOOL_ALLOW_SENSITIVE=1` temporarily restores auto-grants for those tools.

Before tools are wrapped, the embedded runner now creates a SparseKernel task for the active run, claims it with a worker lease, and appends transcript events for run start, prompt submission, assistant output, and completion or failure. Browser and sandbox allocations can attach to the same task id, so the ledger can answer which active step owned each expensive resource.

For browser tools, the local and daemon brokers can attach a hidden SparseKernel browser proxy to the tool parameters when CDP browser brokering is enabled. The tool still sees the existing OpenClaw browser action contract, but supported browser actions are routed through the leased SparseKernel context. Snapshot reads stay brokered and bounded, basic actions use refs from the latest brokered snapshot, and screenshot bytes are artifactized. Browser leases are cached for the active run and released from broker cleanup.

For exec-shaped tools, set `OPENCLAW_RUNTIME_TOOL_SANDBOX_EXEC=1` to route `exec`, `bash`, and `shell` invocations through the sandbox broker command runner. The broker still checks `tool` / `<tool-name>` / `invoke`, then checks sandbox allocation capability for the active agent, allocates a `code_execution` lease, runs the bounded command through the selected sandbox backend, records usage and audit events, and releases the lease. This is opt-in because the compatibility path still supports existing ambient tool implementations.

Sandbox allocations persist the selected backend and trust-zone policy snapshot in resource-lease metadata. A restarted broker object can recover whether an allocation was local/no-isolation, bwrap, minijail, or Docker without relying on an in-memory map. Docker command execution is explicit: callers must provide `dockerImage` or `OPENCLAW_SPARSEKERNEL_DOCKER_IMAGE`, the wrapper uses `--pull never`, drops Linux capabilities, sets `no-new-privileges`, applies read-only root/tmpfs defaults, maps trust-zone memory/process limits to Docker flags, and keeps container networking disabled unless a policy proxy is configured.

V0 does not move native plugins out of process by default. It establishes the ledger and API contract so OpenClaw adapters can route invocation through SparseKernel now, then move untrusted or community plugins behind stronger process or sandbox boundaries later. Set `OPENCLAW_RUNTIME_PLUGIN_PROCESS_BOUNDARY=subprocess` or `OPENCLAW_RUNTIME_PLUGIN_PROCESS=strict` to require plugin tools to provide subprocess metadata. Opt-in workers receive a JSON request on stdin with `protocol`, `pluginId`, `toolName`, `toolCallId`, `params`, and broker context, and must return an `AgentToolResult` JSON object on stdout. Plugin subprocess workers now execute through the sandbox broker as `plugin_untrusted` leases by default, so allocation capability, command audit events, usage records, and release accounting apply to plugin workers too. The broker fails closed when a worker would use `local/no_isolation` unless the plugin marks the subprocess sandbox `requireIsolated: false` or `OPENCLAW_RUNTIME_PLUGIN_ALLOW_NO_ISOLATION=1` is set for trusted local workers. Plugin tools without subprocess metadata fail closed with `plugin_tool.subprocess_required` audit records.
