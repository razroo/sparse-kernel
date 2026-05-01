#!/usr/bin/env node
// Runs local workflow sanity checks.
// Uses an installed actionlint when present, otherwise falls back to `go run`
// for the pinned version used by CI, then runs repo-specific composite guards.
import { spawnSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { parse as parseYaml } from "yaml";

const ACTIONLINT_VERSION = "1.7.11";
const WORKFLOW_DIR = ".github/workflows";
const PERMISSION_LEVELS = new Map([
  ["none", 0],
  ["read", 1],
  ["write", 2],
]);
const PERMISSION_SCOPES = [
  "actions",
  "attestations",
  "checks",
  "contents",
  "deployments",
  "discussions",
  "id-token",
  "issues",
  "models",
  "packages",
  "pages",
  "pull-requests",
  "security-events",
  "statuses",
];

function commandExists(command) {
  return spawnSync("bash", ["-lc", `command -v ${command}`], { stdio: "ignore" }).status === 0;
}

function run(command, args) {
  const result = spawnSync(command, args, { stdio: "inherit" });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function parseWorkflow(path) {
  return parseYaml(readFileSync(path, "utf8"));
}

function workflowPaths() {
  return readdirSync(WORKFLOW_DIR)
    .filter((entry) => entry.endsWith(".yml") || entry.endsWith(".yaml"))
    .map((entry) => join(WORKFLOW_DIR, entry));
}

function permissionLevel(value) {
  if (typeof value !== "string") {
    return undefined;
  }
  return PERMISSION_LEVELS.get(value);
}

function normalizePermissions(raw) {
  if (raw === undefined) {
    return undefined;
  }
  if (raw === null) {
    return { all: undefined, scopes: new Map() };
  }
  if (typeof raw === "string") {
    if (raw === "read-all") {
      return { all: 1, scopes: new Map() };
    }
    if (raw === "write-all") {
      return { all: 2, scopes: new Map() };
    }
    return undefined;
  }
  if (typeof raw !== "object") {
    return undefined;
  }
  return {
    all: undefined,
    scopes: new Map(
      Object.entries(raw).map(([scope, value]) => [scope, permissionLevel(value) ?? 0]),
    ),
  };
}

function allowedLevel(grant, scope) {
  if (grant.all !== undefined) {
    return grant.all;
  }
  return grant.scopes.get(scope) ?? 0;
}

function explicitJobPermissions(workflow, job) {
  return normalizePermissions(job.permissions ?? workflow.permissions);
}

function requestedJobPermissions(workflow, job) {
  return normalizePermissions(job.permissions ?? workflow.permissions);
}

function localReusableWorkflowPath(uses) {
  if (typeof uses !== "string" || !uses.startsWith("./.github/workflows/")) {
    return undefined;
  }
  return uses.slice(2);
}

function requestedScopes(grant) {
  if (grant.all !== undefined) {
    return PERMISSION_SCOPES;
  }
  return [...grant.scopes.keys()];
}

function checkReusableWorkflowPermissions() {
  const workflows = new Map(workflowPaths().map((path) => [path, parseWorkflow(path)]));
  const errors = [];

  for (const [callerPath, caller] of workflows) {
    for (const [callerJobName, callerJob] of Object.entries(caller.jobs ?? {})) {
      const reusablePath = localReusableWorkflowPath(callerJob?.uses);
      if (!reusablePath) {
        continue;
      }
      const callee = workflows.get(reusablePath);
      if (!callee) {
        continue;
      }
      const callerPermissions = explicitJobPermissions(caller, callerJob);
      if (!callerPermissions) {
        continue;
      }
      for (const [calleeJobName, calleeJob] of Object.entries(callee.jobs ?? {})) {
        const calleePermissions = requestedJobPermissions(callee, calleeJob);
        if (!calleePermissions) {
          continue;
        }
        for (const scope of requestedScopes(calleePermissions)) {
          const requested = allowedLevel(calleePermissions, scope);
          const granted = allowedLevel(callerPermissions, scope);
          if (requested > granted) {
            errors.push(
              `${callerPath}#${callerJobName} calls ${reusablePath}, but nested job ${calleeJobName} requests ${scope}: ${[...PERMISSION_LEVELS.entries()].find(([, level]) => level === requested)?.[0] ?? requested} and the caller grants ${scope}: ${[...PERMISSION_LEVELS.entries()].find(([, level]) => level === granted)?.[0] ?? granted}.`,
            );
          }
        }
      }
    }
  }

  if (errors.length > 0) {
    console.error("Reusable workflow permission check failed:");
    for (const error of errors) {
      console.error(`- ${error}`);
    }
    process.exit(1);
  }
}

if (commandExists("actionlint")) {
  run("actionlint", []);
} else {
  run("go", ["run", `github.com/rhysd/actionlint/cmd/actionlint@v${ACTIONLINT_VERSION}`]);
}

checkReusableWorkflowPermissions();
run("python3", ["scripts/check-composite-action-input-interpolation.py"]);
run("node", ["scripts/check-no-conflict-markers.mjs"]);
