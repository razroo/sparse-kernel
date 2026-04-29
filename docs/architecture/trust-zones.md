---
summary: "SparseKernel trust zones and resource lease boundaries"
read_when:
  - Changing browser, sandbox, filesystem, or network trust policy
  - Adding SparseKernel broker backends
title: "Trust Zones"
sidebarTitle: "Trust Zones"
---

SparseKernel leases expensive and sensitive resources by trust zone, not by logical agent.

Default zones:

- `public_web`
- `authenticated_web`
- `local_files_readonly`
- `local_files_rw`
- `code_execution`
- `plugin_untrusted`
- `user_browser_profile`

Trust zones are policy labels. A trust zone can select a sandbox backend, network policy, filesystem policy, and resource budget. Logical agents request capabilities against a zone and then receive a lease for one bounded step.

## Why zones instead of per-agent sandboxes

Hundreds of logical agents on a 4 GB VM cannot each own a browser process or sandbox. SparseKernel parks most agents in SQLite and materializes only active work. Expensive resources are pooled by trust boundary and leased to the active task or session.

## Security notes

`local/no_isolation` is accounting and trusted local execution only. Its command runner can enforce lease bookkeeping, timeouts, output capture, usage records, and audit events, but it does not provide process, filesystem, network, kernel, or VM isolation. Plugin subprocess workers in the `plugin_untrusted` zone prefer available isolated command backends automatically: bwrap, minijail, then Docker when a plugin image is configured. The bwrap and minijail runners wrap commands only when those host binaries are present and their isolation is backend-defined, not a universal SparseKernel guarantee. Docker, smolvm, and VM backends must be described by their real properties when implemented.
