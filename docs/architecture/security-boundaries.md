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
- Browser proxy-required mode configures SparseKernel-owned native browser processes with a loopback proxy and denies unverified external CDP endpoints; it does not control arbitrary host processes.
- Docker command execution requires an explicit image and disables container networking by default, but its isolation is only the host Docker daemon's isolation; it is not a per-agent security model.
- Persisted lease metadata records the selected backend and policy intent; it is audit/accounting state, not isolation by itself.
- Untrusted plugins must not get ambient host authority.
- The subprocess-required plugin mode runs explicit JSON workers through the sandbox broker. It auto-selects an available isolated backend when possible, but it still only becomes suitable for untrusted code when the selected backend is a real isolation backend; `local/no_isolation` is blocked by default for plugin subprocess workers and should only be enabled for trusted local workers.
- Secrets should be referenced, not stored as plaintext in SQLite.

Capabilities are the v0 policy primitive. They are intentionally simple: subject, resource, action, optional constraints, optional expiry. Denied sensitive checks are audit-logged.
