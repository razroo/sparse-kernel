---
summary: "SparseKernel security boundaries and non-boundaries"
read_when:
  - Reviewing SparseKernel sandbox, browser, capability, or artifact security claims
  - Writing docs or UI text about isolation guarantees
title: "Security Boundaries"
sidebarTitle: "Security Boundaries"
---

SparseKernel security claims must be precise.

- SQLite is not a security boundary if agents can access the DB file directly.
- Agents must use SparseKernel APIs, not raw DB access.
- BrowserContext is session isolation, not host isolation.
- A loopback CDP probe only proves that a local browser endpoint responds; it is not authorization to expose CDP to agents.
- Playwright route blocking is not a hard security boundary.
- Docker is one sandbox backend, not the default mental model.
- `local/no_isolation` is only for trusted operations and accounting.
- The local/no-isolation sandbox command runner schedules and audits trusted commands behind a lease, but it does not confine process, filesystem, or network access.
- The bwrap and minijail command runners use the installed backend binaries when available, but their exact isolation depends on host kernel support and the broker-selected bind policy. Isolated command backends receive a sanitized environment by default; plugin or sandbox callers must pass required worker environment explicitly.
- SparseKernel network policy checks, including DNS-aware hostname resolution, are broker-side request guards unless backed by a real proxy, firewall, sandbox, or VM boundary.
- Sandbox proxy-required mode fails closed for network-allowing trust zones without a valid loopback proxy reference and configures Docker command workers to use that proxy. It is not a bypass-proof host egress boundary for arbitrary code.
- `OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS=1` fails closed for network-allowing sandbox trust zones unless `OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER` confirms an allocation with a supported boundary: `host_firewall`, `egress_proxy`, `vm_firewall`, or `platform_enforcer`. The helper receives a JSON request on stdin for allocate/release and is an operator-supplied privileged boundary. SparseKernel records the helper result on the sandbox lease and audits enforcement, but the security property is only as strong as that helper and the host policy it installs.
- Browser proxy-required mode configures SparseKernel-owned native browser processes with a loopback proxy and denies unverified external CDP endpoints; it does not control arbitrary host processes.
- Docker command execution requires an explicit image and disables container networking by default, but its isolation is only the host Docker daemon's isolation; it is not a per-agent security model.
- Persisted lease metadata records the selected backend and policy intent; it is audit/accounting state, not isolation by itself.
- Untrusted plugins must not get ambient host authority.
- The subprocess-required plugin mode runs explicit JSON workers through the sandbox broker. It auto-selects an available isolated backend when possible, but it still only becomes suitable for untrusted code when the selected backend is a real isolation backend; `local/no_isolation` is blocked by default for plugin subprocess workers and should only be enabled for trusted local workers.
- Plugin tool `processBoundary: "subprocess_required"` and `OPENCLAW_RUNTIME_PLUGIN_TRUST_DEFAULT=untrusted` are fail-closed controls for brokered tool execution; they do not make existing in-process plugin initialization safe.
- Secrets should be referenced, not stored as plaintext in SQLite.

Capabilities are the v0 policy primitive. They are intentionally simple: subject, resource, action, optional constraints, optional expiry. Denied sensitive checks are audit-logged.
