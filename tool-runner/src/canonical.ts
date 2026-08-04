import { createHash } from "node:crypto";

// Deterministic JSON projection used by the reviewed bundle contract. Manifests
// accept JSON values only; object keys are recursively sorted and unsupported
// JS values are rejected rather than silently omitted.
export function canonicalJSON(value: unknown): string {
  if (value === null || typeof value === "boolean" || typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("manifest contains a non-finite number");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJSON).join(",")}]`;
  if (typeof value === "object") {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${canonicalJSON(object[key])}`).join(",")}}`;
  }
  throw new Error(`manifest contains unsupported ${typeof value} value`);
}

export function toolDigest(manifestWithoutDigest: unknown, bundle: Uint8Array): string {
  const hash = createHash("sha256");
  hash.update(canonicalJSON(manifestWithoutDigest));
  hash.update(new Uint8Array([0]));
  hash.update(bundle);
  return `sha256:${hash.digest("hex")}`;
}
