---
summary: "SparseKernel resource model for small 4 GB VMs"
read_when:
  - Tuning local agent worker pools or broker limits
  - Explaining how many logical agents can run on small machines
title: "Four GB VM Design"
sidebarTitle: "Four GB VM Design"
---

SparseKernel is designed for small machines where logical agent count is much higher than active harness count.

Default assumptions:

- logical agents max: `500`
- active agent steps max: `100`
- model calls in flight max: `50`
- file patch jobs max: `16`
- test jobs max: `4`
- browser contexts max: `2`
- heavy sandboxes max: `1`

Five hundred logical agents are feasible because most are parked in SQLite as compact state. Five hundred materialized coding harnesses are not feasible on a 4 GB VM.

Book-writing and file-writing agents can run at higher active counts than coding agents because they do not all need browsers, sandboxes, test runners, or heavy model contexts. Expensive work should be scarce, leased, and scheduled.

Resource leases let SparseKernel answer which task owned which expensive resource and when it was released or expired. Trust-zone budgets are enforced at lease creation for sandbox work: `max_processes` caps active sandbox leases, and `max_runtime_seconds` clamps lease runtime and expiry. This keeps a 4 GB machine from materializing more heavy execution work than its configured trust zone allows.

Browser targets and observations are compact ledger rows, not retained screenshots or traces. This lets small machines keep enough browser provenance to answer which target made a request, emitted console output, or produced an artifact while still pruning old observations with `openclaw runtime prune`.
