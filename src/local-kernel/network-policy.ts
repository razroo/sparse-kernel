import net from "node:net";
import type { LocalKernelDatabase } from "./database.js";

export type NetworkPolicyDecision = {
  allowed: boolean;
  reason: string;
  proxyRef?: string;
};

function hostMatchesPattern(host: string, pattern: string): boolean {
  const normalizedHost = host.toLowerCase();
  const normalizedPattern = pattern.trim().toLowerCase();
  if (!normalizedPattern) {
    return false;
  }
  if (normalizedPattern.startsWith("*.")) {
    const suffix = normalizedPattern.slice(1);
    return normalizedHost.endsWith(suffix);
  }
  return normalizedHost === normalizedPattern;
}

function isPrivateIpv4(host: string): boolean {
  if (net.isIP(host) !== 4) {
    return false;
  }
  const [a = 0, b = 0] = host.split(".").map((part) => Number.parseInt(part, 10));
  return (
    a === 10 ||
    a === 127 ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && b === 168) ||
    (a === 169 && b === 254)
  );
}

function isLocalHost(host: string): boolean {
  const normalized = host.toLowerCase();
  return normalized === "localhost" || normalized.endsWith(".localhost") || isPrivateIpv4(host);
}

export function checkTrustZoneNetworkUrl(params: {
  db: LocalKernelDatabase;
  trustZoneId: string;
  url: string;
  actor?: { type: string; id?: string };
}): NetworkPolicyDecision {
  const policy = params.db.getNetworkPolicyForTrustZone(params.trustZoneId);
  if (!policy) {
    params.db.recordAudit({
      actor: params.actor ?? { type: "runtime" },
      action: "network_policy.denied_missing_policy",
      objectType: "trust_zone",
      objectId: params.trustZoneId,
      payload: { url: params.url },
    });
    return { allowed: false, reason: "missing policy" };
  }

  let parsed: URL;
  try {
    parsed = new URL(params.url);
  } catch {
    return { allowed: false, reason: "invalid url" };
  }

  const host = parsed.hostname;
  if (!policy.allowPrivateNetwork && isLocalHost(host)) {
    params.db.recordAudit({
      actor: params.actor ?? { type: "runtime" },
      action: "network_policy.denied_private_network",
      objectType: "network_policy",
      objectId: policy.id,
      payload: { trustZoneId: params.trustZoneId, host },
    });
    return { allowed: false, reason: "private network denied", proxyRef: policy.proxyRef };
  }

  if (policy.allowedHosts?.some((pattern) => hostMatchesPattern(host, pattern))) {
    return { allowed: true, reason: "allowed host", proxyRef: policy.proxyRef };
  }

  const allowed = policy.defaultAction === "allow";
  if (!allowed) {
    params.db.recordAudit({
      actor: params.actor ?? { type: "runtime" },
      action: "network_policy.denied_default",
      objectType: "network_policy",
      objectId: policy.id,
      payload: { trustZoneId: params.trustZoneId, host },
    });
  }
  return {
    allowed,
    reason: allowed ? "default allow" : "default deny",
    proxyRef: policy.proxyRef,
  };
}
