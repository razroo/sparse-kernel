#!/usr/bin/env node

const lanes = [
  {
    id: "ledger-and-leases",
    platform: "all",
    command: "pnpm test src/local-kernel/database.test.ts",
    purpose: "SQLite ledger, leases, artifacts, capabilities, browser and sandbox lifecycle.",
  },
  {
    id: "tool-broker",
    platform: "all",
    command: "pnpm test src/local-kernel/tool-broker.test.ts",
    purpose: "Capability-mediated tools, plugin subprocesses, sandboxed exec, browser proxying.",
  },
  {
    id: "runtime-cli",
    platform: "all",
    command: "pnpm test src/commands/runtime.sparsekernel.test.ts",
    purpose: "Runtime operator commands, strict acceptance, cutover plan, and doctor checks.",
  },
  {
    id: "egress-proxy",
    platform: "all",
    command: "pnpm test src/local-kernel/egress-proxy.test.ts",
    purpose: "Loopback egress proxy policy enforcement.",
  },
  {
    id: "browser-cdp",
    platform: "all",
    command: "pnpm test packages/browser-broker/src/index.test.ts",
    purpose:
      "Brokered CDP browser parity for lifecycle, artifacts, actions, tabs, console, PDF, upload.",
  },
  {
    id: "linux-hard-egress",
    platform: "linux",
    command:
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY=1 pnpm test src/local-kernel/hard-egress-firewall.test.ts",
    purpose: "Linux firewall helper planning and fail-closed hard-egress behavior.",
  },
  {
    id: "darwin-hard-egress",
    platform: "darwin",
    command:
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY=1 pnpm test src/local-kernel/hard-egress-firewall.test.ts",
    purpose: "macOS pf helper planning and documented limits.",
  },
  {
    id: "windows-hard-egress",
    platform: "win32",
    command: "pnpm test src/local-kernel/hard-egress-firewall.test.ts",
    purpose: "Windows firewall allow-rule planning and default-outbound-block preflight.",
  },
];

const json = process.argv.includes("--json");
const currentOnly = process.argv.includes("--current-platform");
const platform = process.platform;
const selected = currentOnly
  ? lanes.filter((lane) => lane.platform === "all" || lane.platform === platform)
  : lanes;

if (json) {
  process.stdout.write(`${JSON.stringify({ platform, lanes: selected }, null, 2)}\n`);
} else {
  process.stdout.write(`SparseKernel acceptance lanes (${platform})\n`);
  for (const lane of selected) {
    process.stdout.write(`- ${lane.id} [${lane.platform}]: ${lane.command}\n`);
    process.stdout.write(`  ${lane.purpose}\n`);
  }
}
