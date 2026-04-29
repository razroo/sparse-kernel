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

function normalizeUrlHostname(host: string): string {
  if (host.startsWith("[") && host.endsWith("]")) {
    return host.slice(1, -1);
  }
  return host;
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

function parseIpv4(host: string): number | undefined {
  if (net.isIP(host) !== 4) {
    return undefined;
  }
  const parts = host.split(".").map((part) => Number.parseInt(part, 10));
  if (
    parts.length !== 4 ||
    parts.some((part) => !Number.isFinite(part) || part < 0 || part > 255)
  ) {
    return undefined;
  }
  return parts.reduce((value, part) => (value << 8) + part, 0) >>> 0;
}

function parseIpv6(host: string): bigint | undefined {
  if (net.isIP(host) !== 6) {
    return undefined;
  }
  const normalized = host.toLowerCase();
  const [headRaw = "", tailRaw = ""] = normalized.split("::");
  const head = headRaw ? headRaw.split(":") : [];
  const tail = tailRaw ? tailRaw.split(":") : [];
  const missing = 8 - head.length - tail.length;
  if (missing < 0 || (normalized.includes("::") ? false : missing !== 0)) {
    return undefined;
  }
  const groups = [
    ...head,
    ...Array.from({ length: normalized.includes("::") ? missing : 0 }, () => "0"),
    ...tail,
  ];
  if (groups.length !== 8) {
    return undefined;
  }
  let value = 0n;
  for (const group of groups) {
    const parsed = Number.parseInt(group || "0", 16);
    if (!Number.isFinite(parsed) || parsed < 0 || parsed > 0xffff) {
      return undefined;
    }
    value = (value << 16n) + BigInt(parsed);
  }
  return value;
}

function isPrivateIpv6(host: string): boolean {
  const value = parseIpv6(host);
  if (value === undefined) {
    return false;
  }
  if (value === 1n) {
    return true;
  }
  const uniqueLocal = 0xfc00n << 112n;
  const uniqueLocalMask = 0xfe00n << 112n;
  if ((value & uniqueLocalMask) === uniqueLocal) {
    return true;
  }
  const linkLocal = 0xfe80n << 112n;
  const linkLocalMask = 0xffc0n << 112n;
  return (value & linkLocalMask) === linkLocal;
}

function cidrContainsHost(cidr: string, host: string): boolean {
  const [rawAddress, rawPrefix] = cidr.trim().split("/");
  if (!rawAddress || rawPrefix === undefined) {
    return false;
  }
  const prefix = Number.parseInt(rawPrefix, 10);
  if (!Number.isFinite(prefix)) {
    return false;
  }
  const cidrVersion = net.isIP(rawAddress);
  const hostVersion = net.isIP(host);
  if (cidrVersion === 4 && hostVersion === 4) {
    if (prefix < 0 || prefix > 32) {
      return false;
    }
    const cidrValue = parseIpv4(rawAddress);
    const hostValue = parseIpv4(host);
    if (cidrValue === undefined || hostValue === undefined) {
      return false;
    }
    const mask = prefix === 0 ? 0 : (0xffffffff << (32 - prefix)) >>> 0;
    return (hostValue & mask) === (cidrValue & mask);
  }
  if (cidrVersion === 6 && hostVersion === 6) {
    if (prefix < 0 || prefix > 128) {
      return false;
    }
    const cidrValue = parseIpv6(rawAddress);
    const hostValue = parseIpv6(host);
    if (cidrValue === undefined || hostValue === undefined) {
      return false;
    }
    const mask = prefix === 0 ? 0n : ((1n << BigInt(prefix)) - 1n) << BigInt(128 - prefix);
    return (hostValue & mask) === (cidrValue & mask);
  }
  return false;
}

function isLocalHost(host: string): boolean {
  const normalized = host.toLowerCase();
  return (
    normalized === "localhost" ||
    normalized.endsWith(".localhost") ||
    isPrivateIpv4(host) ||
    isPrivateIpv6(host)
  );
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
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    params.db.recordAudit({
      actor: params.actor ?? { type: "runtime" },
      action: "network_policy.denied_scheme",
      objectType: "network_policy",
      objectId: policy.id,
      payload: { trustZoneId: params.trustZoneId, protocol: parsed.protocol },
    });
    return { allowed: false, reason: "unsupported scheme", proxyRef: policy.proxyRef };
  }

  const host = normalizeUrlHostname(parsed.hostname);
  const deniedCidr = policy.deniedCidrs?.find((cidr) => cidrContainsHost(cidr, host));
  if (deniedCidr) {
    params.db.recordAudit({
      actor: params.actor ?? { type: "runtime" },
      action: "network_policy.denied_cidr",
      objectType: "network_policy",
      objectId: policy.id,
      payload: { trustZoneId: params.trustZoneId, host, cidr: deniedCidr },
    });
    return { allowed: false, reason: "denied cidr", proxyRef: policy.proxyRef };
  }

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
