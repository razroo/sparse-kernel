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

The `@openclaw/sparsekernel-browser-broker` adapter materializes the ledger lease into a real CDP browser context with `Target.createBrowserContext`, creates a target for the task/session, captures screenshots through `Page.captureScreenshot`, captures PDFs through `Page.printToPDF`, records per-target console and network observations through the SparseKernel audit API, and writes screenshots/downloads/PDFs through the SparseKernel artifact API. Callers can construct it directly with a kernel-shaped client or use `createSparseKernelCdpBrowserBroker` to wire it to `sparsekerneld`. Download capture uses CDP download events and the content-addressed artifact store rather than exposing arbitrary download paths to agents.

When `OPENCLAW_RUNTIME_BROWSER_BROKER=cdp` and `OPENCLAW_SPARSEKERNEL_BROWSER_CDP_ENDPOINT=<loopback endpoint>` are set, the embedded OpenClaw browser tool receives an internal SparseKernel proxy instead of raw CDP access. Set `OPENCLAW_RUNTIME_BROWSER_BROKER=managed` to let SparseKernel ask the existing OpenClaw browser control service to start the managed browser and return its loopback CDP endpoint; `OPENCLAW_SPARSEKERNEL_BROWSER_CONTROL_URL` overrides the default `http://127.0.0.1:18791` control URL.

Set `OPENCLAW_RUNTIME_BROWSER_BROKER=native` to let SparseKernel launch and supervise a local Chromium-compatible process pool by trust zone and profile. The native pool uses a loopback-only remote debugging endpoint, a runtime-owned browser profile directory, and pooled process refcounts; the leased CDP context is released first, then the browser process is stopped after the pool idle timeout. Use `OPENCLAW_SPARSEKERNEL_BROWSER_EXECUTABLE` when Chrome/Chromium is not discoverable on `PATH` or a common platform path. Headless mode is on by default. `OPENCLAW_SPARSEKERNEL_BROWSER_NO_SANDBOX=1` is an explicit opt-out and should only be used when the host environment cannot run Chromium's sandbox.

Supported v0 actions (`status`, `doctor`, `profiles`, `tabs`, `open`, `navigate`, `focus`, `close`, `snapshot`, `console`, `screenshot`, `pdf`, direct file-input `upload`, `dialog`, and brokered `act`) operate against broker-owned targets inside the leased CDP context. Brokered `act` covers the OpenClaw action contract for click, coordinate click, type, press, hover, scroll, drag, select, fill, resize, wait, evaluate, close, and batch using CDP input events plus bounded DOM evaluation. Selector-backed actions retry inside the leased page until their action timeout, and `wait --load networkidle` uses per-target CDP Network events plus a quiet window rather than only checking `document.readyState`. Actions that can change page state are followed by a broker-side navigation check: same-target navigations are accepted only when the resulting URL stays inside the context's allowed-origin policy, same-policy popups are attached as broker-owned targets, and disallowed popups are closed. Snapshots use a bounded CDP `Runtime.evaluate` DOM read, actions resolve refs from the latest brokered snapshot where needed, console output is captured from CDP runtime/log events per target, and screenshot/PDF output is captured as SparseKernel artifacts, read back through artifact access, and converted to existing tool result formats for compatibility. Closing a broker-owned target now closes that target; the full browser context is released only when the last target closes or broker cleanup runs.

BrowserContext isolation is session isolation, not host isolation. Playwright route blocking is useful request control, not a hard security boundary.

The brokered CDP action engine is intentionally implemented inside the broker boundary, but it is not a byte-for-byte Playwright clone. Behaviors that depend on Playwright's full actionability checks, locator retry model, popup ownership, or network-idle semantics should still be covered by targeted tests before relying on them for critical automations. Set `OPENCLAW_SPARSEKERNEL_BROWSER_LIVE=1` when running `src/local-kernel/browser-cdp-live.test.ts` to exercise the native pool and brokered actions against a real local Chromium-compatible browser.
