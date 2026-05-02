import { readFileSync } from "node:fs";
import process from "node:process";
import { parse } from "yaml";

const OPENAPI_PATH = "schemas/sparsekernel.openapi.yaml";
const DAEMON_PATH = "crates/sparsekernel-cli/src/lib.rs";
const CLIENT_PATH = "packages/sparsekernel-client/src/index.ts";
const CLIENT_SCHEMA_MAPPINGS = [
  mapping("SparseKernelResourceBudgets", "RuntimeResourceBudgets"),
  mapping("SparseKernelNetworkPolicy", "NetworkPolicy"),
  mapping("SparseKernelTrustZoneProxyAttachment", "TrustZoneProxyAttachment"),
  mapping("SparseKernelSupervisedEgressProxy", "SupervisedEgressProxy"),
  mapping("SparseKernelTask", "Task"),
  mapping("SparseKernelSession", "Session"),
  mapping("SparseKernelTranscriptEvent", "TranscriptEvent"),
  mapping("SparseKernelToolCall", "ToolCall"),
  mapping("SparseKernelAuditEvent", "AuditEvent"),
  mapping("SparseKernelArtifact", "Artifact"),
  mapping("SparseKernelArtifactSubject", "ArtifactSubject"),
  mapping("SparseKernelCreateArtifactInput", "CreateArtifactInput"),
  mapping("SparseKernelImportArtifactFileInput", "ImportArtifactFileInput"),
  mapping("SparseKernelReadArtifactResult", "ReadArtifactResult"),
  mapping("SparseKernelExportArtifactFileResult", "ExportArtifactFileResult"),
  mapping("SparseKernelBrowserContext", "BrowserContext", {
    ignoreClientProperties: ["allowedOrigins"],
  }),
  mapping("SparseKernelBrowserPool", "BrowserPool"),
  mapping("SparseKernelBrowserEndpointProbe", "BrowserEndpointProbe"),
  mapping("SparseKernelBrowserTarget", "BrowserTarget"),
  mapping("SparseKernelBrowserObservation", "BrowserObservation"),
  mapping("SparseKernelAcquireBrowserContextInput", "AcquireBrowserContextInput"),
  mapping("SparseKernelBrowserObservationInput", "BrowserObservationInput"),
  mapping("SparseKernelRecordBrowserTargetInput", "RecordBrowserTargetInput"),
  mapping("SparseKernelCloseBrowserTargetInput", "CloseBrowserTargetInput"),
  mapping("SparseKernelListBrowserTargetsInput", "ListBrowserTargetsInput"),
  mapping("SparseKernelListBrowserObservationsInput", "ListBrowserObservationsInput"),
  mapping("SparseKernelEnqueueTaskInput", "EnqueueTaskInput"),
  mapping("SparseKernelClaimTaskInput", "ClaimTaskInput"),
  mapping("SparseKernelClaimTaskByIdInput", "ClaimTaskByIdInput"),
  mapping("SparseKernelHeartbeatTaskInput", "HeartbeatTaskInput"),
  mapping("SparseKernelCompleteTaskInput", "CompleteTaskInput"),
  mapping("SparseKernelFailTaskInput", "FailTaskInput"),
  mapping("SparseKernelReleaseExpiredLeasesResult", "ReleaseExpiredLeasesResult"),
  mapping("SparseKernelSandboxAllocation", "SandboxAllocation"),
  mapping("SparseKernelSandboxBackendProbe", "SandboxBackendProbe"),
  mapping("SparseKernelAllocateSandboxInput", "AllocateSandboxInput"),
  mapping("SparseKernelCreateToolCallInput", "CreateToolCallInput"),
  mapping("SparseKernelCompleteToolCallInput", "CompleteToolCallInput"),
  mapping("SparseKernelCapability", "Capability"),
  mapping("SparseKernelGrantCapabilityInput", "GrantCapabilityInput"),
  mapping("SparseKernelCapabilityCheckInput", "CapabilityCheckInput"),
];

function mapping(clientType, schemaName, options = {}) {
  return { clientType, schemaName, ...options };
}

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

export function collectClientTypeProperties(source, typeName) {
  const typeMatch = new RegExp(`export type ${typeName} = \\{([\\s\\S]*?)\\n\\};`, "u").exec(
    source,
  );
  if (!typeMatch) {
    return undefined;
  }
  return new Set(
    typeMatch[1]
      .split("\n")
      .map((line) => /^\s*([A-Za-z0-9_]+)\??:/u.exec(line)?.[1])
      .filter(Boolean),
  );
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

  checkClientSchemaProperties(errors, clientSource, schemas);

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

function checkClientSchemaProperties(errors, clientSource, schemas) {
  for (const item of CLIENT_SCHEMA_MAPPINGS) {
    const clientProperties = collectClientTypeProperties(clientSource, item.clientType);
    const schemaProperties = schemaPropertiesFor(schemas, item.schemaName);
    if (!clientProperties) {
      errors.push(`Client type missing for SparseKernel OpenAPI parity: ${item.clientType}`);
      continue;
    }
    if (!schemaProperties) {
      errors.push(`OpenAPI schema missing for SparseKernel client parity: ${item.schemaName}`);
      continue;
    }

    const ignoredClientProperties = new Set(item.ignoreClientProperties ?? []);
    const filteredClientProperties = new Set(
      [...clientProperties].filter((property) => !ignoredClientProperties.has(property)),
    );
    pushSetDiff(
      errors,
      `${item.clientType} properties missing from ${item.schemaName}`,
      filteredClientProperties,
      schemaProperties,
    );
    pushSetDiff(
      errors,
      `${item.schemaName} properties missing from ${item.clientType}`,
      schemaProperties,
      filteredClientProperties,
    );
  }
}

function schemaPropertiesFor(schemas, schemaName) {
  const properties = schemas?.[schemaName]?.properties;
  if (!properties || typeof properties !== "object" || Array.isArray(properties)) {
    return undefined;
  }
  return new Set(Object.keys(properties));
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
