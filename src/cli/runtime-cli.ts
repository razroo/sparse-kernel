import type { Command } from "commander";
import {
  runtimeBudgetCommand,
  runtimeBudgetSetCommand,
  runtimeInspectCommand,
  runtimeMigrateCommand,
  runtimePruneCommand,
  runtimeVacuumCommand,
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
    .description("Manage the local runtime kernel ledger")
    .addHelpText(
      "after",
      () =>
        `\n${theme.heading("Examples:")}\n${formatHelpExamples([
          ["openclaw runtime migrate", "Create or update the runtime SQLite DB."],
          ["openclaw runtime inspect --json", "Inspect runtime ledger counts."],
          ["openclaw runtime budget", "List trust-zone budgets and usage."],
          [
            "openclaw runtime budget set --trust-zone code_execution --max-runtime-seconds 600",
            "Update a trust-zone budget.",
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
    .command("vacuum")
    .description("Vacuum the runtime SQLite DB")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) => runtimeVacuumCommand({ json: Boolean(opts.json) }, defaultRuntime)),
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
    .description("Update trust-zone budget limits")
    .requiredOption("--trust-zone <id>", "Trust zone id")
    .option("--max-processes <count>", "Maximum process count")
    .option("--max-memory-mb <mb>", "Maximum memory in MiB")
    .option("--max-runtime-seconds <seconds>", "Maximum runtime in seconds")
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimeBudgetSetCommand(
          {
            trustZone: opts.trustZone as string | undefined,
            maxProcesses: opts.maxProcesses as string | undefined,
            maxMemoryMb: opts.maxMemoryMb as string | undefined,
            maxRuntimeSeconds: opts.maxRuntimeSeconds as string | undefined,
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
    .option("--json", "Output JSON", false)
    .action(
      createRunner((opts) =>
        runtimePruneCommand(
          {
            olderThan: opts.olderThan as string | undefined,
            json: Boolean(opts.json),
          },
          defaultRuntime,
        ),
      ),
    );
}
