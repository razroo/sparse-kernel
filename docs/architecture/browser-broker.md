---
summary: "SparseKernel browser broker model"
read_when:
  - Implementing or reviewing browser context pooling
  - Capturing screenshots, downloads, traces, or browser observations
title: "Browser Broker"
sidebarTitle: "Browser Broker"
---

SparseKernel browser pools are keyed by trust zone. Browser contexts are leased per task, session, or agent.

V0 provides the broker interface and mock accounting. A later Playwright broker should:

- keep browser processes scarce and pooled by trust zone;
- create cheap contexts per task/session;
- disable screenshots, traces, HAR, and video by default;
- store downloads and captured media as artifacts;
- avoid exposing raw CDP access to agents by default;
- enforce max context counts through leases.

BrowserContext isolation is session isolation, not host isolation. Playwright route blocking is useful request control, not a hard security boundary.
