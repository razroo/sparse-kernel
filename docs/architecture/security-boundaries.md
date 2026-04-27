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
- Playwright route blocking is not a hard security boundary.
- Docker is one sandbox backend, not the default mental model.
- `local/no_isolation` is only for trusted operations and accounting.
- Untrusted plugins must not get ambient host authority.
- Secrets should be referenced, not stored as plaintext in SQLite.

Capabilities are the v0 policy primitive. They are intentionally simple: subject, resource, action, optional constraints, optional expiry. Denied sensitive checks are audit-logged.
