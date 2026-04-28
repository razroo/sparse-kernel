import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  SparseKernelCdpBrowserBroker,
  type SparseKernelBrowserKernelClient,
} from "../../packages/browser-broker/src/index.js";
import type {
  SparseKernelArtifact,
  SparseKernelBrowserContext,
  SparseKernelBrowserObservationInput,
  SparseKernelBrowserTarget,
  SparseKernelCreateArtifactInput,
  SparseKernelRecordBrowserTargetInput,
} from "../../packages/sparsekernel-client/src/index.js";
import {
  acquireNativeBrowserProcess,
  resolveNativeBrowserExecutable,
  stopAllNativeBrowserProcesses,
} from "./browser-process-pool.js";

const LIVE = process.env.OPENCLAW_SPARSEKERNEL_BROWSER_LIVE === "1";
const describeLive = LIVE ? describe : describe.skip;

class LiveSmokeKernel implements SparseKernelBrowserKernelClient {
  readonly observations: SparseKernelBrowserObservationInput[] = [];
  readonly targets: SparseKernelRecordBrowserTargetInput[] = [];
  private nextContext = 1;

  async probeBrowserPool(input: { cdp_endpoint: string }) {
    return {
      endpoint: input.cdp_endpoint,
      reachable: true,
      status_code: 200,
    };
  }

  async acquireBrowserContext(): Promise<SparseKernelBrowserContext> {
    return {
      id: `browser_ctx_live_${this.nextContext++}`,
      pool_id: "browser_pool_public_web",
      profile_mode: "ephemeral",
      status: "active",
      created_at: new Date().toISOString(),
    };
  }

  async releaseBrowserContext(): Promise<boolean> {
    return true;
  }

  async createArtifact(input: SparseKernelCreateArtifactInput): Promise<SparseKernelArtifact> {
    const content =
      input.content_base64 ?? Buffer.from(input.content_text ?? "").toString("base64");
    return {
      id: "artifact_live",
      sha256: "live",
      size_bytes: Buffer.from(content, "base64").length,
      storage_ref: "sha256/live",
      mime_type: input.mime_type,
      retention_policy: input.retention_policy,
      created_at: new Date().toISOString(),
    };
  }

  async recordBrowserObservation(input: SparseKernelBrowserObservationInput): Promise<void> {
    this.observations.push(input);
  }

  async recordBrowserTarget(
    input: SparseKernelRecordBrowserTargetInput,
  ): Promise<SparseKernelBrowserTarget> {
    this.targets.push(input);
    return {
      id: `${input.context_id}:${input.target_id}`,
      context_id: input.context_id,
      target_id: input.target_id,
      status: input.status ?? "active",
      console_count: 0,
      network_count: 0,
      artifact_count: 0,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
  }
}

describeLive("SparseKernel CDP browser broker live smoke", () => {
  afterEach(async () => {
    await stopAllNativeBrowserProcesses();
  });

  it(
    "drives a real native Chromium context through brokered actions",
    { timeout: 60_000 },
    async () => {
      const executable = resolveNativeBrowserExecutable();
      if (!executable) {
        throw new Error(
          "OPENCLAW_SPARSEKERNEL_BROWSER_LIVE=1 requires a Chromium-compatible executable.",
        );
      }
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-sparsekernel-live-"));
      let broker: SparseKernelCdpBrowserBroker | undefined;
      let contextId: string | undefined;
      try {
        const browser = await acquireNativeBrowserProcess({
          trustZoneId: "public_web",
          env: {
            ...process.env,
            OPENCLAW_STATE_DIR: root,
          } as NodeJS.ProcessEnv,
          executablePath: executable,
          readyTimeoutMs: 20_000,
          idleTimeoutMs: 0,
        });
        const kernel = new LiveSmokeKernel();
        broker = new SparseKernelCdpBrowserBroker({ kernel });
        const context = await broker.acquireContext({
          trust_zone_id: "public_web",
          cdp_endpoint: browser.cdpEndpoint,
          initial_url: liveSmokePageUrl(),
        });
        contextId = context.ledger_context.id;

        await broker.actContext(contextId, {
          kind: "wait",
          selector: "#late",
          timeoutMs: 5_000,
        });
        await broker.actContext(contextId, {
          kind: "click",
          selector: "#late",
          timeoutMs: 5_000,
        });
        await expect(
          broker.actContext(contextId, {
            kind: "wait",
            text: "Clicked",
            timeoutMs: 5_000,
          }),
        ).resolves.toMatchObject({ ok: true });

        await broker.actContext(contextId, {
          kind: "type",
          selector: "#name",
          text: "SparseKernel",
          slowly: true,
          timeoutMs: 10_000,
        });
        await expect(
          broker.actContext(contextId, {
            kind: "evaluate",
            fn: '() => document.querySelector("#name")?.value',
            timeoutMs: 5_000,
          }),
        ).resolves.toMatchObject({ value: "SparseKernel" });

        await expect(
          broker.actContext(contextId, {
            kind: "wait",
            loadState: "networkidle",
            timeoutMs: 5_000,
          }),
        ).resolves.toMatchObject({ ok: true });
        await expect(broker.captureScreenshotArtifact(contextId)).resolves.toMatchObject({
          artifact_type: "screenshot",
        });
        expect(kernel.targets.some((target) => target.target_id === context.target_id)).toBe(true);
        expect(
          kernel.observations.some(
            (observation) => observation.observation_type === "browser_artifact.created",
          ),
        ).toBe(true);
      } finally {
        if (broker && contextId) {
          await broker.releaseContext(contextId).catch(() => false);
        }
        fs.rmSync(root, { recursive: true, force: true });
      }
    },
  );
});

function liveSmokePageUrl(): string {
  const html = `<!doctype html>
<html>
  <body>
    <input id="name" aria-label="Name" />
    <div id="out">Waiting</div>
    <script>
      setTimeout(() => {
        const button = document.createElement("button");
        button.id = "late";
        button.textContent = "Late action";
        button.addEventListener("click", () => {
          document.body.dataset.clicked = "yes";
          document.querySelector("#out").textContent = "Clicked";
        });
        document.body.append(button);
      }, 200);
    </script>
  </body>
</html>`;
  return `data:text/html,${encodeURIComponent(html)}`;
}
