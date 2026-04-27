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

The current daemon does not launch a Playwright browser process or create real Playwright `BrowserContext` objects yet. The next backend should:

- keep browser processes scarce and pooled by trust zone;
- create cheap contexts per task/session;
- disable screenshots, traces, HAR, and video by default;
- store downloads and captured media as artifacts;
- avoid exposing raw CDP access to agents by default;
- enforce max context counts through leases.

BrowserContext isolation is session isolation, not host isolation. Playwright route blocking is useful request control, not a hard security boundary.
