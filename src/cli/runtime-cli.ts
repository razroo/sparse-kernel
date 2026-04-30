import type { Command } from "commander";
import {
  runtimeArtifactAccessCommand,
  runtimeArtifactSummaryCommand,
  runtimeAcceptanceCommand,
  runtimeBrowserObservationsCommand,
  runtimeBrowserPoolsCommand,
  runtimeBrowserTargetsCommand,
  runtimeBudgetCommand,
  runtimeBudgetSetCommand,
  runtimeCutoverPlanCommand,
  runtimeDoctorCommand,
  runtimeEgressProxyCommand,
  runtimeEgressProxyListCommand,
  runtimeEgressProxyStopCommand,
  runtimeInspectCommand,
  runtimeLeasesCommand,
  runtimeMaintainCommand,
  runtimeMigrateCommand,
  runtimeNetworkProxySetCommand,
  runtimeNetworkProxyShowCommand,
  runtimePruneCommand,
  runtimeRecoverCommand,
  runtimeSessionsCommand,
  runtimeTasksCommand,
  runtimeTranscriptCommand,
  runtimeVacuumCommand,
  runtimeWorkerIdentitiesCommand,
} from "../commands/runtime.js";
import { defaultRuntime } from "../runtime.js";
import { formatDocsLink } from "../terminal/links.js";
import { theme } from "../terminal/theme.js";
import { formatHelpExamples } from "./help-format.js";

function createRunner(
  commandFn: (opts: Record<string, unknown>, runtime: typeof defaultRuntime) => Promise<void>,
) {
  return async (opts: Record<string, unknown>) => {
    try {
      await commandFn(opts, defaultRuntime);
    } catch (err) {
      defaultRuntime.error(String(err));
      defaultRuntime.exit(1);
    }
  };
}

export function registerRuntimeCli(program: Command) {
  const runtime = program
    .command("runtime")
    .alias("sparsekernel")
    .description("Manage the local runtime kernel ledger")
    .addHelpText(
      "after",
      () =>
        `\n${theme.heading("Examples:")}\n${formatHelpExamples([
          ["openclaw runtime migrate", "Create or update the runtime SQLite DB."],
          ["openclaw runtime inspect --json", "Inspect runtime ledger counts."],
          ["openclaw sparsekernel sessions --json", "List SparseKernel sessions."],
          ["openclaw sparsekernel tasks --kind openclaw.embedded_run", "List runtime tasks."],
          ["openclaw sparsekernel transcript --session <id>", "Show transcript events."],
          [
            "openclaw sparsekernel artifact-access --subject main --subject-type agent",
            "List artifact access grants.",
          ],
          ["openclaw sparsekernel browser-targets --json", "List brokered browser targets."],
          ["openclaw sparsekernel browser-pools --json", "List brokered browser pools."],
          [
            "openclaw sparsekernel browser-observations --context <id>",
            "List browser observations.",
          ],
          ["openclaw sparsekernel leases --status active", "List active resource leases."],
          ["openclaw sparsekernel artifacts summary", "Summarize artifact retention."],
          ["openclaw sparsekernel recover", "Recover expired or dead embedded-run leases."],
          ["openclaw runtime budget", "List trust-zone budgets and usage."],
          [
            "openclaw runtime budget set --trust-zone code_execution --max-runtime-seconds 600",
            "Update a trust-zone budget.",
          ],
          [
            "openclaw runtime worker-identities --count 4 --json",
            "Plan broker-managed SparseKernel worker identities.",
          ],
          ["openclaw runtime doctor", "Check SparseKernel runtime readiness."],
          [
            "openclaw runtime acceptance --strict --run",
            "Check strict SparseKernel readiness and run required lanes.",
          ],
          ["openclaw runtime cutover-plan", "Print a guided SparseKernel strict cutover plan."],
          [
            "openclaw runtime egress-proxy --trust-zone public_web",
            "Start a loopback policy-enforcing egress proxy.",
          ],
          [
            "openclaw runtime network-proxy set --trust-zone public_web --proxy-ref http://127.0.0.1:8888/",
            "Attach a loopback proxy reference to a trust-zone network policy.",
          ],
          [
            "openclaw runtime maintain --run-due --schedule-every 1h",
            "Run scheduled runtime maintenance when due.",
          ],
          ["openclaw runtime prune --older-than 7d", "Prune ephemeral/debug artifacts."],
          ["openclaw runtime vacuum", "Vacuum the runtime DB."],
        ])}`,
    )
    .addHelpText(
      "after",
      () =>
        `\n${theme.muted("Docs:")} ${formatDocsLink(
          "/architecture/local-agent-kernel",
          "docs.openclaw.ai/architecture/local-agent-kernel",
        )}\n`,
    )
    .action(() => {
      runtime.help({ error: true });
    });

  runtime
    .command("migrate")
    .description("Create or update the runtime SQLite DB")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) => runtimeMigrateCommand({ json: Boolean(opts.json) }, defaultRuntime)),
    );

  runtime
    .command("inspect")
    .description("Inspect runtime DB schema and table counts")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) => runtimeInspectCommand({ json: Boolean(opts.json) }, defaultRuntime)),
    );

  runtime
    .command("doctor")
    .description("Inspect SparseKernel runtime readiness and acceptance lanes")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) => runtimeDoctorCommand({ json: Boolean(opts.json) }, defaultRuntime)),
    );

  runtime
    .command("acceptance")
    .description("Check SparseKernel acceptance readiness and test lanes")
    .option("--strict", "Require strict SparseKernel cutover settings", false)
    .option("--current-platform", "Show only lanes for the current platform", false)
    .option("--run", "Run required acceptance lane commands", false)
    .option("--include-recommended", "Also run recommended acceptance lanes with --run", false)
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeAcceptanceCommand(
          {
            strict: Boolean(opts.strict),
            currentPlatform: Boolean(opts.currentPlatform),
            run: Boolean(opts.run),
            includeRecommended: Boolean(opts.includeRecommended),
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("cutover-plan")
    .description("Print a guided plan for switching SparseKernel to strict ledger-primary mode")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeCutoverPlanCommand({ json: Boolean(opts.json) }, defaultRuntime),
      ),
    );

  runtime
    .command("egress-proxy")
    .description("Start a loopback trust-zone egress proxy")
    .option("--trust-zone <id>", "Trust zone id", "public_web")
    .option("--host <host>", "Loopback bind host", "127.0.0.1")
    .option("--port <port>", "Bind port (0 chooses a free port)", "0")
    .option("--attach", "Attach the started proxy_ref to the trust zone", false)
    .option("--supervised", "Start through the in-process SparseKernel proxy supervisor", false)
    .option("--json", "Output JSON startup payload", false)
    .action(
      createRunner((opts) =>
        runtimeEgressProxyCommand(
          {
            trustZone: opts.trustZone as string | undefined,
            host: opts.host as string | undefined,
            port: opts.port as string | undefined,
            attach: Boolean(opts.attach),
            supervised: Boolean(opts.supervised),
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  const egressProxies = runtime
    .command("egress-proxies")
    .description("Inspect or stop supervised SparseKernel egress proxies");

  egressProxies
    .command("list")
    .description("List supervised egress proxies in this process")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeEgressProxyListCommand({ json: Boolean(opts.json) }, defaultRuntime),
      ),
    );

  egressProxies
    .command("stop")
    .description("Stop a supervised egress proxy in this process")
    .option("--trust-zone <id>", "Trust zone id", "public_web")
    .option("--clear", "Clear trust-zone proxy_ref after stopping", false)
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeEgressProxyStopCommand(
          {
            trustZone: opts.trustZone as string | undefined,
            clear: Boolean(opts.clear),
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  const networkProxy = runtime
    .command("network-proxy")
    .description("Attach or inspect trust-zone network proxy references");

  networkProxy
    .command("set")
    .description("Attach proxy_ref to a trust-zone network policy")
    .option("--trust-zone <id>", "Trust zone id", "public_web")
    .requiredOption("--proxy-ref <url>", "Loopback proxy URL")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeNetworkProxySetCommand(
          {
            trustZone: opts.trustZone as string | undefined,
            proxyRef: opts.proxyRef as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  networkProxy
    .command("clear")
    .description("Clear proxy_ref from a trust-zone network policy")
    .option("--trust-zone <id>", "Trust zone id", "public_web")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeNetworkProxySetCommand(
          {
            trustZone: opts.trustZone as string | undefined,
            clear: true,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  networkProxy
    .command("show")
    .description("Show a trust-zone network proxy reference")
    .option("--trust-zone <id>", "Trust zone id", "public_web")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeNetworkProxyShowCommand(
          {
            trustZone: opts.trustZone as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("vacuum")
    .description("Vacuum the runtime SQLite DB")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) => runtimeVacuumCommand({ json: Boolean(opts.json) }, defaultRuntime)),
    );

  runtime
    .command("sessions")
    .description("List SparseKernel ledger sessions")
    .option("--agent <id>", "Filter by agent id")
    .option("--limit <n>", "Maximum rows to return", "50")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeSessionsCommand(
          {
            agent: opts.agent as string | undefined,
            limit: opts.limit as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("tasks")
    .description("List SparseKernel ledger tasks")
    .option("--status <status>", "Filter by task status")
    .option("--kind <kind>", "Filter by task kind")
    .option("--limit <n>", "Maximum rows to return", "50")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeTasksCommand(
          {
            status: opts.status as string | undefined,
            kind: opts.kind as string | undefined,
            limit: opts.limit as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("transcript")
    .description("Show SparseKernel transcript events")
    .requiredOption("--session <id>", "Runtime session id")
    .option("--limit <n>", "Maximum events to return", "100")
    .option("--format <format>", "events or jsonl", "events")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeTranscriptCommand(
          {
            session: opts.session as string | undefined,
            limit: opts.limit as string | undefined,
            format: opts.format as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("browser-targets")
    .description("List SparseKernel brokered browser targets")
    .option("--context <id>", "Filter by browser context id")
    .option("--session <id>", "Filter by session id")
    .option("--task <id>", "Filter by task id")
    .option("--status <status>", "Filter by target status")
    .option("--limit <n>", "Maximum targets to return", "100")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeBrowserTargetsCommand(
          {
            context: opts.context as string | undefined,
            session: opts.session as string | undefined,
            task: opts.task as string | undefined,
            status: opts.status as string | undefined,
            limit: opts.limit as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("browser-pools")
    .description("List SparseKernel browser pools")
    .option("--trust-zone <id>", "Filter by trust zone id")
    .option("--status <status>", "Filter by pool status")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeBrowserPoolsCommand(
          {
            trustZone: opts.trustZone as string | undefined,
            status: opts.status as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("browser-observations")
    .description("List SparseKernel browser observations")
    .option("--context <id>", "Filter by browser context id")
    .option("--target <id>", "Filter by target id")
    .option("--type <type>", "Filter by observation type")
    .option("--since <duration>", "Only include observations newer than this duration")
    .option("--limit <n>", "Maximum observations to return", "100")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeBrowserObservationsCommand(
          {
            context: opts.context as string | undefined,
            target: opts.target as string | undefined,
            type: opts.type as string | undefined,
            since: opts.since as string | undefined,
            limit: opts.limit as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("leases")
    .description("List SparseKernel resource leases")
    .option("--resource-type <type>", "Filter by resource type")
    .option("--status <status>", "Filter by lease status")
    .option("--trust-zone <id>", "Filter by trust zone id")
    .option("--agent <id>", "Filter by owning agent id")
    .option("--limit <n>", "Maximum leases to return", "100")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeLeasesCommand(
          {
            resourceType: opts.resourceType as string | undefined,
            status: opts.status as string | undefined,
            trustZone: opts.trustZone as string | undefined,
            agent: opts.agent as string | undefined,
            limit: opts.limit as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("artifacts")
    .description("Inspect SparseKernel artifacts")
    .command("summary")
    .description("Summarize artifacts by retention policy")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeArtifactSummaryCommand({ json: Boolean(opts.json) }, defaultRuntime),
      ),
    );

  runtime
    .command("artifact-access")
    .description("List SparseKernel artifact access grants")
    .option("--artifact <id>", "Filter by artifact id")
    .option("--subject-type <type>", "Filter by subject type")
    .option("--subject <id>", "Filter by subject id")
    .option("--permission <permission>", "Filter by permission")
    .option("--limit <n>", "Maximum access rows to return", "100")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeArtifactAccessCommand(
          {
            artifact: opts.artifact as string | undefined,
            subjectType: opts.subjectType as string | undefined,
            subject: opts.subject as string | undefined,
            permission: opts.permission as string | undefined,
            limit: opts.limit as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("recover")
    .description("Recover expired SparseKernel leases and dead embedded-run tasks")
    .option("--task <id>", "Only recover one embedded-run task")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeRecoverCommand(
          {
            task: opts.task as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("maintain")
    .description("Recover expired leases and prune old runtime records")
    .option("--older-than <duration>", "Artifact and observation age cutoff (default: 7d)", "7d")
    .option(
      "--retention <policies>",
      "Comma-separated retention policies (default: ephemeral,debug)",
    )
    .option("--task <id>", "Only recover one embedded-run task")
    .option("--schedule-every <duration>", "Persist a maintenance cadence such as 1h or 30m")
    .option("--run-due", "Skip maintenance when the persisted cadence is not due", false)
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeMaintainCommand(
          {
            olderThan: opts.olderThan as string | undefined,
            retention: opts.retention as string | undefined,
            task: opts.task as string | undefined,
            scheduleEvery: opts.scheduleEvery as string | undefined,
            runDue: Boolean(opts.runDue),
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("budget")
    .description("Inspect runtime trust-zone budgets and usage")
    .option("--since <duration>", "Only summarize usage newer than this duration")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeBudgetCommand(
          {
            since: opts.since as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    )
    .command("set")
    .description("Update trust-zone or global SparseKernel budget limits")
    .option("--trust-zone <id>", "Trust zone id")
    .option("--max-processes <count>", "Maximum process count")
    .option("--max-memory-mb <mb>", "Maximum memory in MiB")
    .option("--max-runtime-seconds <seconds>", "Maximum runtime in seconds")
    .option("--logical-agents-max <count>", "Maximum parked logical agents")
    .option("--active-agent-steps-max <count>", "Maximum active agent steps")
    .option("--model-calls-in-flight-max <count>", "Maximum concurrent model-call tasks")
    .option("--file-patch-jobs-max <count>", "Maximum concurrent file patch jobs")
    .option("--test-jobs-max <count>", "Maximum concurrent test jobs")
    .option("--browser-contexts-max <count>", "Maximum active browser context leases")
    .option("--heavy-sandboxes-max <count>", "Maximum active sandbox leases")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeBudgetSetCommand(
          {
            trustZone: opts.trustZone as string | undefined,
            maxProcesses: opts.maxProcesses as string | undefined,
            maxMemoryMb: opts.maxMemoryMb as string | undefined,
            maxRuntimeSeconds: opts.maxRuntimeSeconds as string | undefined,
            logicalAgentsMax: opts.logicalAgentsMax as string | undefined,
            activeAgentStepsMax: opts.activeAgentStepsMax as string | undefined,
            modelCallsInFlightMax: opts.modelCallsInFlightMax as string | undefined,
            filePatchJobsMax: opts.filePatchJobsMax as string | undefined,
            testJobsMax: opts.testJobsMax as string | undefined,
            browserContextsMax: opts.browserContextsMax as string | undefined,
            heavySandboxesMax: opts.heavySandboxesMax as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("worker-identities")
    .description("Plan or provision broker-managed SparseKernel worker identities")
    .option("--count <n>", "Number of worker identities", "2")
    .option("--prefix <name>", "Worker account name prefix")
    .option("--uid-start <uid>", "First UID for Unix worker accounts")
    .option("--gid <gid>", "Unix worker group id")
    .option("--group <name>", "Worker group name")
    .option("--platform <platform>", "linux, darwin, or windows")
    .option("--apply", "Run the generated provisioning commands", false)
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeWorkerIdentitiesCommand(
          {
            count: opts.count as string | undefined,
            prefix: opts.prefix as string | undefined,
            uidStart: opts.uidStart as string | undefined,
            gid: opts.gid as string | undefined,
            group: opts.group as string | undefined,
            platform: opts.platform as string | undefined,
            apply: Boolean(opts.apply),
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );

  runtime
    .command("prune")
    .description("Prune old ephemeral/debug runtime artifacts")
    .option("--older-than <duration>", "Artifact age cutoff (default: 7d)", "7d")
    .option(
      "--retention <policies>",
      "Comma-separated retention policies (default: ephemeral,debug)",
    )
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimePruneCommand(
          {
            olderThan: opts.olderThan as string | undefined,
            retention: opts.retention as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );
}
