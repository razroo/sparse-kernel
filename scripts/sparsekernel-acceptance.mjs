#!/usr/bin/env node

import { spawnSync } from "node:child_process";

const lanes = [
  {
    id: "ledger-and-leases",
    platform: "all",
    status: "required",
    command: "pnpm test src/local-kernel/database.test.ts",
    purpose: "SQLite ledger, leases, artifacts, capabilities, browser and sandbox lifecycle.",
  },
  {
    id: "tool-broker",
    platform: "all",
    status: "required",
    command: "pnpm test src/local-kernel/tool-broker.test.ts",
    purpose: "Capability-mediated tools, plugin subprocesses, sandboxed exec, browser proxying.",
  },
  {
    id: "runtime-cli",
    platform: "all",
    status: "required",
    command: "pnpm test src/commands/runtime.sparsekernel.test.ts",
    purpose: "Runtime operator commands, strict acceptance, cutover plan, and doctor checks.",
  },
  {
    id: "egress-proxy",
    platform: "all",
    status: "required",
    command: "pnpm test src/local-kernel/egress-proxy.test.ts",
    purpose: "Loopback egress proxy policy enforcement.",
  },
  {
    id: "browser-cdp",
    platform: "all",
    status: "recommended",
    command: "pnpm test packages/browser-broker/src/index.test.ts",
    purpose:
      "Brokered CDP browser parity for lifecycle, artifacts, actions, tabs, console, PDF, upload.",
  },
  {
    id: "linux-hard-egress",
    platform: "linux",
    status: "recommended",
    command:
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY=1 pnpm test src/local-kernel/hard-egress-firewall.test.ts",
    purpose: "Linux firewall helper planning and fail-closed hard-egress behavior.",
  },
  {
    id: "darwin-hard-egress",
    platform: "darwin",
    status: "recommended",
    command:
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY=1 pnpm test src/local-kernel/hard-egress-firewall.test.ts",
    purpose: "macOS pf helper planning and documented limits.",
  },
  {
    id: "windows-hard-egress",
    platform: "windows",
    status: "recommended",
    command: "pnpm test src/local-kernel/hard-egress-firewall.test.ts",
    purpose: "Windows firewall allow-rule planning and default-outbound-block preflight.",
  },
];

const json = process.argv.includes("--json");
const currentOnly = process.argv.includes("--current-platform");
const run = process.argv.includes("--run");
const includeRecommended = process.argv.includes("--include-recommended");
const platform =
  process.platform === "win32" ? "windows" : process.platform === "darwin" ? "darwin" : "linux";
const selected = currentOnly
  ? lanes.filter((lane) => lane.platform === "all" || lane.platform === platform)
  : lanes;
const runnable = selected.filter(
  (lane) => lane.status === "required" || (includeRecommended && lane.status === "recommended"),
);

function tail(value) {
  if (!value) return undefined;
  return value.length <= 4000 ? value : value.slice(value.length - 4000);
}

function runLane(lane) {
  const started = Date.now();
  const result = spawnSync(lane.command, {
    shell: true,
    encoding: "utf8",
    stdio: json ? "pipe" : "inherit",
    maxBuffer: 8 * 1024 * 1024,
    env: process.env,
  });
  return {
    id: lane.id,
    command: lane.command,
    status: result.status === 0 ? "passed" : "failed",
    exitCode: result.status,
    signal: result.signal,
    durationMs: Date.now() - started,
    ...(json ? { stdout: tail(result.stdout), stderr: tail(result.stderr) } : {}),
  };
}

if (json) {
  const results = run ? runnable.map(runLane) : undefined;
  process.stdout.write(
    `${JSON.stringify(
      {
        ok: results ? results.every((result) => result.status === "passed") : true,
        platform,
        lanes: selected,
        ...(run ? { ran: results, includeRecommended } : {}),
      },
      null,
      2,
    )}\n`,
  );
  if (results?.some((result) => result.status !== "passed")) {
    process.exitCode = 1;
  }
} else {
  process.stdout.write(`SparseKernel acceptance lanes (${platform})\n`);
  for (const lane of selected) {
    process.stdout.write(`- ${lane.id} [${lane.platform}/${lane.status}]: ${lane.command}\n`);
    process.stdout.write(`  ${lane.purpose}\n`);
  }
  if (run) {
    process.stdout.write(
      `\nRunning ${runnable.length} ${includeRecommended ? "required/recommended" : "required"} lane(s)\n`,
    );
    const results = runnable.map(runLane);
    for (const result of results) {
      process.stdout.write(
        `- ${result.status.toUpperCase()} ${result.id} (${result.durationMs}ms)\n`,
      );
    }
    if (results.some((result) => result.status !== "passed")) {
      process.exitCode = 1;
    }
  }
}
