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
openclaw sparsekernel artifact-access --subject main --subject-type agent
openclaw sparsekernel artifacts summary
openclaw sparsekernel leases --status active
openclaw sparsekernel browser-pools --json
openclaw sparsekernel browser-targets --json
openclaw sparsekernel browser-observations --context <browser-context-id>
openclaw sparsekernel recover
openclaw runtime budget
openclaw runtime budget set --active-agent-steps-max 100 --browser-contexts-max 2
openclaw runtime budget set --trust-zone code_execution --max-runtime-seconds 600
openclaw runtime cutover-plan
openclaw runtime acceptance --strict --current-platform
openclaw runtime network-proxy set --trust-zone public_web --proxy-ref http://127.0.0.1:8888/
openclaw runtime network-proxy show --trust-zone public_web
openclaw runtime egress-proxy --trust-zone public_web --attach
openclaw runtime egress-proxies list
openclaw runtime egress-proxies stop --trust-zone public_web --clear
openclaw runtime maintain --older-than 7d
openclaw runtime maintain --run-due --schedule-every 1h
openclaw runtime vacuum
openclaw runtime prune --older-than 7d
```

The DB uses WAL mode, foreign keys, a busy timeout, migrations, and typed runtime APIs. Agents and plugins should not open this database directly.

The initial schema includes agents, sessions, legacy session-entry mirrors, transcript events, messages, tasks, task events, tool calls, trust zones, network policies, resource leases, browser pools and contexts, browser targets, browser observations, artifacts, artifact access, capabilities, audit log, and usage records.

## Artifact store

Large outputs are content-addressed files under:

```bash
~/.openclaw/artifacts/sha256/ab/cd/<sha256>
```

SQLite stores metadata, access records, and retention policy only. Screenshots, downloads, PDFs, browser traces, videos, large HTML dumps, workspace snapshots, and exported transcripts should be artifact files, not SQLite blobs.

The local artifact store streams file imports through a temporary blob while hashing them, then moves the finished file into the content-addressed path. This keeps large downloads and workspace snapshots out of Node heap while preserving sha256 dedupe. The daemon keeps JSON base64 artifact create/read for small compatibility payloads, capped by `SPARSEKERNEL_ARTIFACT_BASE64_MAX_BYTES` and defaulting to 1 MiB, but also exposes local staged file import/export endpoints so large artifacts can move through daemon-owned transfer directories instead of through JSON bodies.

Brokered tool calls also keep oversized JSON outputs out of `tool_calls.output_json`. The ToolBroker returns the original tool result to the agent, stores large serialized outputs as `debug` artifacts, and records an artifact reference plus audit event in the ledger.

Retention policies are:

- `ephemeral`
- `session`
- `durable`
- `debug`

`openclaw runtime prune` currently prunes old `ephemeral` and `debug` artifacts by default. Pass `--retention ephemeral,debug,session` only when session-scoped artifacts should also be removed. `openclaw runtime maintain` combines expired lease recovery, embedded-run recovery, artifact pruning, browser-observation pruning, and idle native browser pool sweeps for local maintenance jobs. Lease recovery treats `lease_until <= now` as due and records a `leases.expired_released` audit event when it recovers task or resource leases. `openclaw runtime maintain --schedule-every <duration>` stores a local maintenance cadence in `runtime_info`; `--run-due` lets a cron, launchd, or systemd timer invoke maintenance frequently while the ledger skips work until the cadence is due.

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

The broker applies configured trust-zone network policy to explicit allowed origins before allocating a context. It also denies unsupported URL schemes, private-network destinations when the policy disallows them, literal IPs matching denied CIDRs, and, when `OPENCLAW_RUNTIME_BROWSER_POLICY_ENFORCE=1` is set, hostnames that resolve to denied/private addresses. Set `OPENCLAW_RUNTIME_BROWSER_POLICY_DNS=0` only when a caller intentionally wants literal-host checks without DNS resolution. Use `openclaw runtime network-proxy set --trust-zone <id> --proxy-ref <loopback-url>` to attach an existing loopback proxy to `network_policies.proxy_ref`, or `openclaw runtime egress-proxy --trust-zone <id> --attach` to start the built-in HTTP/CONNECT proxy and attach it. The Rust daemon also exposes `/trust-zones/network-policy`, `/trust-zones/proxy-ref`, and `/egress-proxies/*` APIs; daemon-supervised proxy mode now starts the built-in policy-checking loopback proxy by default and only launches an operator proxy command when explicitly configured. The proxy checks the same network policy before opening upstream sockets and records denied proxy requests in the audit log. Set `OPENCLAW_RUNTIME_BROWSER_REQUIRE_PROXY=1` to require a valid loopback `network_policies.proxy_ref`; native browser pools then launch Chromium with `--proxy-server=<proxy_ref>` and reject static or externally managed CDP endpoints unless `OPENCLAW_RUNTIME_BROWSER_EXTERNAL_PROXY_OK=1` accepts that external process as already proxy-controlled. This is a concrete proxy-backed browser egress path, but it is still process configuration, not a kernel or VM boundary. Set `OPENCLAW_RUNTIME_BROWSER_BROKER=cdp` and `OPENCLAW_SPARSEKERNEL_BROWSER_CDP_ENDPOINT=<loopback endpoint>` to make the OpenClaw browser tool acquire a real SparseKernel CDP context for the active run. Set `OPENCLAW_RUNTIME_BROWSER_BROKER=managed` to use the existing OpenClaw browser control service as the managed process owner and let SparseKernel lease CDP contexts from its reported endpoint. Set `OPENCLAW_RUNTIME_BROWSER_BROKER=native` to let SparseKernel launch and supervise a local Chromium-compatible process pool keyed by trust zone/profile, with process lifetime tied to brokered context leases and idle timeout. `OPENCLAW_SPARSEKERNEL_BROWSER_MAX_CONTEXTS` caps active contexts per native pool. Use `openclaw sparsekernel browser-pools` to inspect ledger pools plus in-process native pool refcounts, limits, start counts, clean stops, and crash counts. The runtime injects an internal browser proxy for supported navigation, history navigation, tab, snapshot, console, screenshot, PDF, direct file-input upload, dialog, download, and action routes instead of exposing raw CDP to the agent. Brokered actions cover the OpenClaw action contract with CDP input events, bounded DOM evaluation, selector retry with stronger actionability checks, click/coordinate-click/press modifiers, check/uncheck, reload, back/forward history navigation, per-target CDP-backed network-idle waiting, and post-action navigation checks. Same-target action navigations must stay inside the context's allowed origins when a policy is configured; same-policy popups are attached as broker-owned targets and disallowed popups are closed. Broker-owned targets and per-target console/network/artifact observations are persisted in first-class ledger tables and mirrored to audit events. CDP Fetch interception blocks out-of-policy requests when an allowed-origin policy is configured, but this remains request control rather than a hard security boundary. Closing a target releases the whole context only when no broker-owned targets remain. Screenshot, download, and PDF outputs go through the artifact store, preferring staged local imports over base64 JSON transport when the daemon client supports it and staged exports when the OpenClaw browser proxy needs a local file path. Download observations include broker-selected filename, CDP guid, received/total byte counts when available, artifact size, MIME type, source URL, and sha256.

## Sandbox broker

The sandbox broker records allocations and leases behind a backend abstraction. Current v0 includes a local/no-isolation backend that can run trusted local commands behind an active sandbox lease for scheduling, timeout, output, usage, and audit accounting. Lease metadata persists the selected backend, isolation description, selected isolation profile, and trust-zone policy snapshot in SQLite so a restarted broker instance can recover the allocation backend without relying on an in-memory map. The broker can also build hardened command spawn plans for requested bwrap and minijail backends when those binaries are available. Isolation profiles are `trusted_local`, `web_brokered`, `readonly_workspace`, `rw_workspace`, `code_execution`, and `plugin_untrusted`; trust zones select a default profile, and callers can request a stricter profile for a specific allocation. bwrap plans now use a tmpfs home, a sanitized environment, `/tmp` tmpfs, readonly workspace binds for readonly/web/plugin profiles, and writable binds only for `rw_workspace` and `code_execution`. minijail plans use a hardened profile argument set by default and can be extended with `OPENCLAW_RUNTIME_MINIJAIL_HARDENED_FLAGS` or `OPENCLAW_RUNTIME_MINIJAIL_<PROFILE>_FLAGS`. Docker is now a policy-backed command backend: it requires a locally available `docker` CLI plus an explicit `dockerImage` or `OPENCLAW_SPARSEKERNEL_DOCKER_IMAGE`, uses `--pull never`, drops capabilities, sets `no-new-privileges`, applies read-only root/tmpfs defaults, maps trust-zone memory/process limits to Docker flags, and keeps networking disabled unless a policy proxy is configured. Trust-zone `max_processes` now caps active sandbox leases, and `max_runtime_seconds` clamps resource lease runtime and expiry. Set `OPENCLAW_RUNTIME_SANDBOX_REQUIRE_PROXY=1` to fail closed when a network-allowing trust zone lacks a valid loopback `network_policies.proxy_ref`; Docker command plans translate that host loopback proxy to `host.docker.internal` for container workers, and local command workers receive `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` in their brokered environment. This is proxy configuration and allocation gating, not a kernel firewall. Set `OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS=1` when a deployment requires host-level egress enforcement. Network-allowing allocations then fail closed unless `OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER` confirms a host firewall, egress proxy, VM firewall, or platform enforcer for the allocation; set the helper to `builtin` to accept only selected backends whose current spawn plan already carries a no-network/process boundary, currently bwrap and Docker no-network plans in the local command runner. Set the helper to `builtin-firewall` to use the built-in scoped firewall manager. It can claim a broker-managed local UID/GID worker from `OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITIES` or `OPENCLAW_RUNTIME_SANDBOX_WORKER_UIDS`, apply Linux iptables or macOS pf rules for that identity, and spawn the command with Node's `uid`/`gid` options. Use `openclaw runtime worker-identities` to generate the platform-specific account provisioning plan and environment JSON for these workers. Without a managed identity pool it requires an explicit dedicated scope and `OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED=1`. Windows support now runs a PowerShell preflight that verifies Firewall profiles are enabled with `DefaultOutboundAction Block` before installing scoped allow rules; SparseKernel does not silently trust an environment assertion or change global Windows defaults. All built-in host mutation requires `OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY=1`. Hostname allowlists are accepted only when a loopback proxy is configured; the firewall admits loopback proxy traffic while the proxy path enforces hostnames. The result is recorded in lease metadata, including protocol coverage (`all_ip` or `tcp_udp`) and any proxy-delegated hosts, and release calls notify external helpers or remove built-in firewall rules before marking the lease released. Set `OPENCLAW_RUNTIME_SANDBOX_FIREWALL_REQUIRE_FULL_PROTOCOL=1` to reject macOS pf TCP/UDP-only plans and require a full-protocol backend, VM, or external helper. SSH, OpenShell, and VM-backed execution remain future wrappers and are not silently executed on the host. In daemon broker mode, embedded runs grant sandbox allocation capability and allocate/release the `code_execution` sandbox lease through the SparseKernel daemon API before falling back to local accounting.

Daemon and local sandbox allocations persist requested Docker images, runtime caps, and output caps on the resource lease so restarted brokers and audit views can recover the intended command boundary.

Important boundary: `local/no_isolation` means accounting only. It does not provide process, filesystem, network, kernel, or VM isolation. Docker, bwrap, minijail, gVisor, or VM backends must be described by their actual guarantees when implemented.

## Capability flow

Sensitive operations should check capabilities before they mutate state or allocate resources:

- artifact reads and writes;
- browser context allocation;
- sandbox allocation;
- tool invocation.

Capability grants, revokes, and denied checks are audited. Embedded agent runs now materialize the active step as a task lease and transcript events, then wrap the effective tool set with the local ToolBroker outside tests unless explicitly disabled with `OPENCLAW_RUNTIME_TOOL_BROKER=off`. Set `OPENCLAW_RUNTIME_TOOL_BROKER=daemon` to use the `sparsekerneld` run-ledger and ToolBroker path; daemon setup failures fall back to the local runtime broker. Set `OPENCLAW_RUNTIME_TOOL_CAPABILITY_MODE=strict` to stop auto-granting sensitive tools such as `exec`, `read`, `write`, and `browser`; those tools then fail closed unless a capability already exists or `OPENCLAW_RUNTIME_TOOL_ALLOW_SENSITIVE=1` is set for compatibility. `exec`, `bash`, and `shell` shaped tools route through the sandbox broker command runner by default when the active agent has sandbox allocation capability; set `OPENCLAW_RUNTIME_TOOL_SANDBOX_EXEC=off` only for compatibility. The command broker auto-selects an available isolated backend before falling back to `local/no_isolation` for trusted local compatibility. Bundled/native in-process plugins remain trusted by default, while workspace/global/config plugins are subprocess-first unless `OPENCLAW_RUNTIME_PLUGIN_TRUST_DEFAULT=trusted` is set for compatibility. Individual plugin tools can also declare `processBoundary: "subprocess_required"`. Tools without per-tool metadata can use an operator default worker from `OPENCLAW_RUNTIME_PLUGIN_SUBPROCESS_COMMAND` and `OPENCLAW_RUNTIME_PLUGIN_SUBPROCESS_ARGS`; when that default worker is configured, plugin tools from any origin route through it automatically unless they explicitly opt into trusted in-process execution. Subprocess workers can use the standard `createSparseKernelPluginWorkerHandler` / `runSparseKernelPluginWorker` helper to implement the JSON protocol. Subprocess workers emit plugin and sandbox audit events and fail closed if they would use `local/no_isolation` without an explicit trusted-worker opt-out; tools without either per-tool or operator-default worker metadata fail closed when subprocess policy requires them.

The SparseKernel daemon now exposes the v0 run and ToolBroker lifecycle over local JSON: session upsert/list, transcript append/list, task enqueue/claim-by-id/claim-next/heartbeat/complete/fail, tool-call create/start/complete/fail/list, browser acquire/release, sandbox allocate/release, sandbox backend probes, and artifact access. Tool-call create checks `tool` / `<tool-name>` / `invoke`; completion records small structured output plus `artifact_ids`; and every transition writes audit records. See [Tool Broker](/architecture/tool-broker).

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

Session metadata writes are mirrored into the runtime ledger by default. Set `OPENCLAW_RUNTIME_SESSION_STORE=sqlite` to make the runtime ledger the primary session metadata store while retaining legacy file fallback for stores that have not been imported yet. Set `OPENCLAW_RUNTIME_SESSION_STORE=sqlite-strict` to make the runtime ledger authoritative for reads as well; missing ledger rows return an empty store instead of reading `sessions.json`. Set `OPENCLAW_SPARSEKERNEL_STRICT=1` as the short cutover switch for SparseKernel runs; when `OPENCLAW_RUNTIME_SESSION_STORE` is not set it selects `sqlite-strict`, which also defaults transcript compatibility to `ledger-only`. Set `OPENCLAW_RUNTIME_SESSION_STORE=off` to disable mirroring. SQLite primary modes write assistant transcript appends to the ledger first. `sqlite` keeps writing the legacy JSONL compatibility line by default; `sqlite-strict` defaults to ledger-only transcript appends. Set `OPENCLAW_RUNTIME_TRANSCRIPT_COMPAT=jsonl` to keep JSONL appends in strict mode, or `OPENCLAW_RUNTIME_TRANSCRIPT_COMPAT=ledger-only` to disable compatibility writes in either primary mode. The guarded session manager also mirrors remaining pi-session append methods through the runtime ledger append API and fails hard in SQLite primary modes if the session cannot be resolved. Failed ledger writes are hard failures in both primary modes.

## Runtime doctor and acceptance

Run:

```bash
openclaw runtime doctor
```

The doctor reports schema status, session-store mode, transcript compatibility mode, ToolBroker mode, sandbox backend availability, small-VM resource budgets, hard-egress configuration, loopback proxy configuration, managed worker identity status, plugin subprocess readiness, browser broker status, and the SparseKernel acceptance lanes. `openclaw runtime acceptance --strict --current-platform` turns those readiness checks into a fail-closed operator gate for ledger-only transcripts, brokered tools, plugin subprocess policy, explicit browser brokering, egress proxy setup, and resource-budget availability. Add `--run` to execute the required lanes from the same command, and add `--include-recommended` to include browser and platform hard-egress conformance lanes. `openclaw runtime cutover-plan` prints the import, environment, executable acceptance, and JSONL export commands for a staged strict cutover. `pnpm sparsekernel:acceptance --run --include-recommended` runs the same lane map without opening the runtime ledger.

## Current limitations

- Full transcript ownership is still staged for default compatibility runs: session metadata can run with SQLite as primary, assistant delivery transcript appends can run ledger-only in strict mode, import/export is ledger-backed, and remaining pi session manager append paths now mirror through the runtime ledger append API before returning in primary modes. `OPENCLAW_SPARSEKERNEL_STRICT=1` selects the ledger-authoritative path for SparseKernel runs unless more specific session-store environment overrides are set. Legacy JSONL remains supported for compatibility and export, but strict ledger-only transcript appends no longer create a JSONL line unless compatibility mode is requested.
- Native browser process pooling exists for loopback CDP contexts, but it is still a broker-owned Chromium supervisor rather than an exposed Playwright handle. Brokered actions now cover selector retry, same-origin frame-scoped actions through the proxy, stronger actionability checks, CDP-backed network-idle waits, history back/forward, post-action navigation checks, click/coordinate-click/press modifiers, check/uncheck, console/screenshot/PDF/upload/dialog/download routes, staged artifact import/export, richer download observations, and per-pool context caps. Browser conformance is an executable recommended acceptance lane; host isolation still comes from the browser process/sandbox boundary, not BrowserContext.
- Sandbox allocation can execute trusted local commands, hardened bwrap/minijail profile-wrapped commands when available, policy-backed explicitly imaged Docker commands, and operator-supplied `vm`, `ssh`, `openshell`, or `other` command wrappers. VM/remote wrappers receive a `openclaw.sparsekernel.sandbox-command.v1` request plus the original command and stdin. `plugin_untrusted` rejects explicit `local/no_isolation` by default and auto-selects an available isolated backend, including a configured VM wrapper, when no backend is requested; `local/no_isolation` still does not harden trusted execution; Docker isolation depends on the host Docker daemon and selected image; remote wrapper isolation is only as strong as the operator-provided backend.
- `OPENCLAW_SPARSEKERNEL_STRICT=1` now implies strict subprocess policy for all plugin tools, including bundled/native plugin tools. Tools with subprocess metadata or an operator default worker run through the sandbox broker; tools without either fail closed instead of silently falling back in process. Automatically extracting arbitrary existing plugin tool functions into a worker is still future work.
- Network policy enforcement currently exists where brokers call it. DNS-aware denial is implemented for brokered browser navigation checks, proxy-required sandbox mode gates allocations and configures Docker/local proxy environment, and hard-egress sandbox mode can use the built-in no-network backend verifier for bwrap/Docker, the built-in scoped firewall manager for broker-managed UID/GID workers on Linux/macOS, Windows scoped allow rules after active default-block profile verification, or an external host firewall, egress proxy, VM firewall, or platform enforcer helper. SparseKernel still rejects unscoped arbitrary host-process firewall claims instead of treating them as a portable security boundary.

## Contributor rules

- Add runtime DB migrations instead of ad hoc JSON files for coordination state.
- Keep large blobs in the artifact store and DB metadata in SQLite.
- Do not expose raw kernel DB handles to agents or plugins.
- Use broker APIs for browser, sandbox, artifact, and sensitive tool operations.
- Add tests for every migration and broker operation.
