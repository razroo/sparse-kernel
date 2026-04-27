---
summary: "SparseKernel browser broker model"
read_when:
  - Implementing or reviewing browser context pooling
  - Capturing screenshots, downloads, traces, or browser observations
title: "Browser Broker"
sidebarTitle: "Browser Broker"
---

SparseKernel browser pools are keyed by trust zone. Browser contexts are leased per task, session, or agent.

The v0 daemon exposes broker endpoints to acquire, release, and list browser contexts and pools. Acquisition is capability-checked with `browser_context` / `<trust-zone-id>` / `allocate`, and every acquire/release writes audit and lease records.

V0 also has a concrete local-browser attachment point: callers may register and probe an existing loopback CDP endpoint such as `http://127.0.0.1:9222`. The broker validates that CDP endpoints are loopback-only, probes `/json/version`, stores the endpoint on the trust-zone pool, and records audit events. It still does not return raw CDP or Playwright handles to agents.

The `@openclaw/sparsekernel-browser-broker` adapter materializes the ledger lease into a real CDP browser context with `Target.createBrowserContext`, creates a target for the task/session, captures screenshots through `Page.captureScreenshot`, and writes screenshots/downloads through the SparseKernel artifact API. Callers can construct it directly with a kernel-shaped client or use `createSparseKernelCdpBrowserBroker` to wire it to `sparsekerneld`. Download capture uses CDP download events and the content-addressed artifact store rather than exposing arbitrary download paths to agents.

The current daemon does not launch or supervise a Playwright browser process yet. The next backend should:

- keep browser processes scarce and pooled by trust zone;
- reuse the existing OpenClaw browser launcher/profile code without crossing plugin internals;
- disable screenshots, traces, HAR, and video by default;
- avoid exposing raw CDP access to agents by default;
- enforce max context counts through leases.

BrowserContext isolation is session isolation, not host isolation. Playwright route blocking is useful request control, not a hard security boundary.
