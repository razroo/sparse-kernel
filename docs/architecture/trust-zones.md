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

Network policy proxy references are part of the trust-zone policy. Use `openclaw runtime network-proxy set --trust-zone <id> --proxy-ref <loopback-url>` to attach an operator-managed proxy, or `openclaw runtime egress-proxy --trust-zone <id> --attach` to start OpenClaw's policy-checking loopback proxy and store its URL in `network_policies.proxy_ref`. The SparseKernel daemon exposes the same operation over its trust-zone and supervised egress proxy APIs so long-running daemon deployments can own the built-in proxy lifecycle; operator proxy child processes remain an explicit compatibility mode.

Sandbox command allocations resolve a trust-zone isolation profile before selecting a backend:

- `public_web`, `authenticated_web`, and `user_browser_profile` use `web_brokered`.
- `local_files_readonly` uses `readonly_workspace`.
- `local_files_rw` uses `rw_workspace`.
- `code_execution` uses `code_execution`.
- `plugin_untrusted` uses `plugin_untrusted`.
- Unknown zones fall back to `rw_workspace` only when their filesystem policy is explicitly readwrite; otherwise they use `trusted_local`.

## Why zones instead of per-agent sandboxes

Hundreds of logical agents on a 4 GB VM cannot each own a browser process or sandbox. SparseKernel parks most agents in SQLite and materializes only active work. Expensive resources are pooled by trust boundary and leased to the active task or session.

## Security notes

`local/no_isolation` is accounting and trusted local execution only. Its command runner can enforce lease bookkeeping, timeouts, output capture, usage records, and audit events, but it does not provide process, filesystem, network, kernel, or VM isolation. Plugin subprocess workers in the `plugin_untrusted` zone prefer available isolated command backends automatically: bwrap, minijail, then Docker/VM when available. Explicit `local/no_isolation` for `plugin_untrusted` fails closed by default and requires `OPENCLAW_RUNTIME_SANDBOX_ALLOW_LOCAL_UNTRUSTED=1` for trusted local testing. The bwrap runner uses a tmpfs home, tmpfs `/tmp`, sanitized environment, and readonly workspace binds for readonly/web/plugin profiles; it only grants writable workspace binds to `rw_workspace` and `code_execution`. The minijail runner uses a hardened profile flag set and supports operator profile extensions through environment configuration. Both wrap commands only when those host binaries are present, and their isolation remains backend-defined rather than a universal SparseKernel guarantee. Isolated command backends use a minimal process environment by default instead of inheriting host secrets. Docker, smolvm, and VM backends must be described by their real properties when implemented. When hard sandbox egress is required, network-allowing trust zones must be backed by a verified no-network backend, the built-in scoped firewall manager for broker-managed local workers, or an operator-supplied helper that confirms a real host firewall, egress proxy, VM firewall, or platform enforcer for each lease; otherwise allocation fails closed. Use `openclaw runtime worker-identities` to generate the managed local worker provisioning plan, and use `OPENCLAW_RUNTIME_SANDBOX_FIREWALL_REQUIRE_FULL_PROTOCOL=1` for trust zones that must reject TCP/UDP-only macOS pf coverage.
