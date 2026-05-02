import { readFileSync } from "node:fs";
import process from "node:process";
import { parse } from "yaml";

const OPENAPI_PATH = "schemas/sparsekernel.openapi.yaml";
const DAEMON_PATH = "crates/sparsekernel-cli/src/lib.rs";
const CLIENT_PATH = "packages/sparsekernel-client/src/index.ts";

export function collectDaemonRoutes(source) {
  return new Set(
    [...source.matchAll(/\("(?:GET|POST|PUT|PATCH|DELETE)",\s*"([^"]+)"\)/gu)].map(
      (match) => match[1],
    ),
  );
}

export function collectClientPaths(source) {
  return new Set([...source.matchAll(/["'](\/[a-zA-Z0-9][^"']*)["']/gu)].map((match) => match[1]));
}

export function collectSchemaRefs(value, refs = new Set()) {
  if (!value || typeof value !== "object") {
    return refs;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      collectSchemaRefs(item, refs);
    }
    return refs;
  }
  for (const [key, child] of Object.entries(value)) {
    if (key === "$ref" && typeof child === "string") {
      refs.add(child);
      continue;
    }
    collectSchemaRefs(child, refs);
  }
  return refs;
}

export function checkSparseKernelOpenApi({ openapiText, daemonSource, clientSource }) {
  const openapi = parse(openapiText);
  const errors = [];
  const paths = openapi?.paths;
  if (!paths || typeof paths !== "object" || Array.isArray(paths)) {
    errors.push(`${OPENAPI_PATH}: missing object-valued paths`);
    return { errors };
  }

  const openapiPaths = new Set(Object.keys(paths));
  const daemonRoutes = collectDaemonRoutes(daemonSource);
  const clientPaths = collectClientPaths(clientSource);

  pushSetDiff(
    errors,
    "Daemon routes missing from SparseKernel OpenAPI",
    daemonRoutes,
    openapiPaths,
  );
  pushSetDiff(
    errors,
    "SparseKernel OpenAPI paths without daemon route literals",
    openapiPaths,
    daemonRoutes,
  );
  pushSetDiff(errors, "Client paths missing from SparseKernel OpenAPI", clientPaths, openapiPaths);

  const schemas = openapi.components?.schemas;
  const schemaNames = new Set(
    schemas && typeof schemas === "object" && !Array.isArray(schemas) ? Object.keys(schemas) : [],
  );
  const unresolvedSchemaRefs = [...collectSchemaRefs(openapi)]
    .filter((ref) => ref.startsWith("#/components/schemas/"))
    .map((ref) => ref.slice("#/components/schemas/".length))
    .filter((schemaName) => !schemaNames.has(schemaName))
    .toSorted(compareStrings);
  if (unresolvedSchemaRefs.length > 0) {
    errors.push(formatList("Unresolved SparseKernel OpenAPI schema refs", unresolvedSchemaRefs));
  }

  return {
    errors,
    counts: {
      daemonRoutes: daemonRoutes.size,
      clientPaths: clientPaths.size,
      openapiPaths: openapiPaths.size,
      schemaRefs: collectSchemaRefs(openapi).size,
    },
  };
}

function pushSetDiff(errors, title, left, right) {
  const diff = [...left].filter((value) => !right.has(value)).toSorted(compareStrings);
  if (diff.length > 0) {
    errors.push(formatList(title, diff));
  }
}

function compareStrings(left, right) {
  return left.localeCompare(right);
}

function formatList(title, values) {
  return `${title}:\n${values.map((value) => `  - ${value}`).join("\n")}`;
}

function main() {
  const result = checkSparseKernelOpenApi({
    openapiText: readFileSync(OPENAPI_PATH, "utf8"),
    daemonSource: readFileSync(DAEMON_PATH, "utf8"),
    clientSource: readFileSync(CLIENT_PATH, "utf8"),
  });
  if (result.errors.length > 0) {
    console.error(result.errors.join("\n\n"));
    return 1;
  }
  console.log(
    `SparseKernel OpenAPI parity ok (${result.counts.openapiPaths} paths, ${result.counts.schemaRefs} schema refs).`,
  );
  return 0;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  process.exitCode = main();
}
