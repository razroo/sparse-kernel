import type { LocalKernelDatabase } from "./database.js";
import { startLoopbackEgressProxy, type LoopbackEgressProxyHandle } from "./egress-proxy.js";
import { resolveNetworkPolicyProxyRef } from "./network-policy.js";

export type SupervisedEgressProxyRecord = {
  trustZoneId: string;
  proxyRef: string;
  host: string;
  port: number;
  startedAt: string;
};

type SupervisedProxyHandle = SupervisedEgressProxyRecord & {
  handle: LoopbackEgressProxyHandle;
};

const supervised = new Map<string, SupervisedProxyHandle>();

export async function ensureSupervisedEgressProxy(params: {
  db: LocalKernelDatabase;
  trustZoneId: string;
  host?: string;
  port?: number;
}): Promise<SupervisedEgressProxyRecord> {
  const existing = supervised.get(params.trustZoneId);
  if (existing) {
    return publicRecord(existing);
  }
  const handle = await startLoopbackEgressProxy({
    db: params.db,
    trustZoneId: params.trustZoneId,
    host: params.host,
    port: params.port,
    actor: { type: "runtime" },
  });
  const proxyDecision = resolveNetworkPolicyProxyRef(handle.url);
  if (!proxyDecision.ok) {
    await handle.close().catch(() => {});
    throw new Error(
      `Supervised egress proxy produced an invalid proxy_ref: ${proxyDecision.reason}`,
    );
  }
  params.db.attachNetworkPolicyProxyToTrustZone({
    trustZoneId: params.trustZoneId,
    proxyRef: proxyDecision.proxyServer,
    actor: { type: "runtime" },
  });
  const record: SupervisedProxyHandle = {
    trustZoneId: params.trustZoneId,
    proxyRef: proxyDecision.proxyServer,
    host: handle.host,
    port: handle.port,
    startedAt: new Date().toISOString(),
    handle,
  };
  supervised.set(params.trustZoneId, record);
  params.db.recordAudit({
    actor: { type: "runtime" },
    action: "egress_proxy.supervised_started",
    objectType: "trust_zone",
    objectId: params.trustZoneId,
    payload: { proxyRef: record.proxyRef, host: record.host, port: record.port },
  });
  return publicRecord(record);
}

export async function stopSupervisedEgressProxy(params: {
  db: LocalKernelDatabase;
  trustZoneId: string;
  clearProxyRef?: boolean;
}): Promise<boolean> {
  const existing = supervised.get(params.trustZoneId);
  if (!existing) {
    return false;
  }
  supervised.delete(params.trustZoneId);
  await existing.handle.close();
  if (params.clearProxyRef) {
    params.db.attachNetworkPolicyProxyToTrustZone({
      trustZoneId: params.trustZoneId,
      proxyRef: null,
      actor: { type: "runtime" },
    });
  }
  params.db.recordAudit({
    actor: { type: "runtime" },
    action: "egress_proxy.supervised_stopped",
    objectType: "trust_zone",
    objectId: params.trustZoneId,
    payload: { clearProxyRef: params.clearProxyRef === true },
  });
  return true;
}

export function listSupervisedEgressProxies(): SupervisedEgressProxyRecord[] {
  return [...supervised.values()].map(publicRecord);
}

function publicRecord(record: SupervisedProxyHandle): SupervisedEgressProxyRecord {
  return {
    trustZoneId: record.trustZoneId,
    proxyRef: record.proxyRef,
    host: record.host,
    port: record.port,
    startedAt: record.startedAt,
  };
}
