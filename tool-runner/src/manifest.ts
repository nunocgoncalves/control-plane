import type { Dirent } from "node:fs";
import { readFile, readdir, stat } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { basename, join, resolve } from "node:path";
import { init, parse } from "es-module-lexer";
import { toolDigest } from "./canonical.js";
import type { ToolModule } from "./types.js";

export type EffectClassName = "read_only" | "idempotent_write" | "non_idempotent_write";
export type CredentialSchemeName = "bearer" | "oauth_client_credentials";

export interface ToolManifest {
  apiVersion: "iterabase.io/tool/v1";
  name: string;
  version: string;
  digest: string;
  description: string;
  bundle: "index.mjs";
  inputSchema: Record<string, unknown>;
  effectClass: EffectClassName;
  credentialSlots?: Array<{ name: string; scheme: CredentialSchemeName; required?: boolean; bindingSchema?: Record<string, unknown> }>;
  artifactCapabilities?: { readsArtifacts?: boolean; writesArtifacts?: boolean; acceptedMimeTypes?: string[] };
  timeoutMs: number;
  idempotencyProof?: { strategy: "upstream_key"; description?: string; upstreamKeyHeader: string };
  consequenceSummaryTemplate?: { localizedTemplates: Record<string, string>; argumentPaths: Record<string, string> };
}

export interface LoadedTool {
  manifest: ToolManifest;
  module: ToolModule;
  directory: string;
  sizeBytes: number;
  layer: "product" | "client";
}

export interface LoadedGeneration {
  revision: string;
  artifactDigest: string;
  directory: string;
  tools: LoadedTool[];
  sizeBytes: number;
}

const namePattern = /^[a-z][a-z0-9_-]*(?:\.[a-z][a-z0-9_-]*)+$/;
const digestPattern = /^sha256:[a-f0-9]{64}$/;

function record(value: unknown, field: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${field} must be an object`);
  return value as Record<string, unknown>;
}

function exactKeys(value: Record<string, unknown>, allowed: readonly string[], field: string): void {
  const unknown = Object.keys(value).filter((key) => !allowed.includes(key));
  if (unknown.length) throw new Error(`${field} has unknown field ${unknown[0]}`);
}

function stringRecord(value: unknown, field: string): Record<string, string> {
  const out = record(value, field);
  for (const [key, item] of Object.entries(out)) if (typeof item !== "string") throw new Error(`${field}.${key} must be a string`);
  return out as Record<string, string>;
}

export function parseManifest(value: unknown): ToolManifest {
  const m = record(value, "manifest");
  exactKeys(m, ["apiVersion", "name", "version", "digest", "description", "bundle", "inputSchema", "effectClass", "credentialSlots", "artifactCapabilities", "timeoutMs", "idempotencyProof", "consequenceSummaryTemplate"], "manifest");
  if (m.apiVersion !== "iterabase.io/tool/v1") throw new Error("manifest apiVersion must be iterabase.io/tool/v1");
  if (typeof m.name !== "string" || !namePattern.test(m.name)) throw new Error("manifest name must be a dotted tool namespace/name");
  if (typeof m.version !== "string" || !m.version.trim() || m.version.length > 128) throw new Error("manifest version must be a non-empty string <=128 bytes");
  if (typeof m.digest !== "string" || !digestPattern.test(m.digest)) throw new Error("manifest digest must be canonical sha256:<hex>");
  if (typeof m.description !== "string" || !m.description.trim()) throw new Error("manifest description is required");
  if (m.bundle !== "index.mjs") throw new Error("manifest bundle must be index.mjs");
  record(m.inputSchema, "inputSchema");
  if (!(["read_only", "idempotent_write", "non_idempotent_write"] as unknown[]).includes(m.effectClass)) throw new Error("manifest effectClass is invalid");
  if (!Number.isSafeInteger(m.timeoutMs) || (m.timeoutMs as number) <= 0 || (m.timeoutMs as number) > 3_600_000) throw new Error("manifest timeoutMs must be 1..3600000");

  if (m.credentialSlots !== undefined) {
    if (!Array.isArray(m.credentialSlots)) throw new Error("credentialSlots must be an array");
    const names = new Set<string>();
    for (const [index, value] of m.credentialSlots.entries()) {
      const slot = record(value, `credentialSlots[${index}]`);
      exactKeys(slot, ["name", "scheme", "required", "bindingSchema"], `credentialSlots[${index}]`);
      if (typeof slot.name !== "string" || !slot.name) throw new Error(`credentialSlots[${index}].name is required`);
      if (names.has(slot.name)) throw new Error(`duplicate credential slot ${slot.name}`);
      names.add(slot.name);
      if (slot.scheme !== "bearer" && slot.scheme !== "oauth_client_credentials") throw new Error(`credentialSlots[${index}].scheme is invalid`);
      if (slot.required !== undefined && typeof slot.required !== "boolean") throw new Error(`credentialSlots[${index}].required must be boolean`);
      if (slot.bindingSchema !== undefined) record(slot.bindingSchema, `credentialSlots[${index}].bindingSchema`);
    }
  }

  if (m.artifactCapabilities !== undefined) {
    const capabilities = record(m.artifactCapabilities, "artifactCapabilities");
    exactKeys(capabilities, ["readsArtifacts", "writesArtifacts", "acceptedMimeTypes"], "artifactCapabilities");
    for (const field of ["readsArtifacts", "writesArtifacts"] as const) if (capabilities[field] !== undefined && typeof capabilities[field] !== "boolean") throw new Error(`artifactCapabilities.${field} must be boolean`);
    if (capabilities.acceptedMimeTypes !== undefined && (!Array.isArray(capabilities.acceptedMimeTypes) || capabilities.acceptedMimeTypes.some((mime) => typeof mime !== "string" || !mime))) throw new Error("artifactCapabilities.acceptedMimeTypes must contain non-empty strings");
  }

  if (m.idempotencyProof !== undefined || m.effectClass === "idempotent_write") {
    const proof = record(m.idempotencyProof, "idempotencyProof");
    exactKeys(proof, ["strategy", "description", "upstreamKeyHeader"], "idempotencyProof");
    if (proof.strategy !== "upstream_key" || typeof proof.upstreamKeyHeader !== "string" || !proof.upstreamKeyHeader) throw new Error("idempotencyProof requires upstream_key strategy and upstreamKeyHeader");
    if (proof.description !== undefined && typeof proof.description !== "string") throw new Error("idempotencyProof.description must be a string");
  }

  if (m.consequenceSummaryTemplate !== undefined || m.effectClass !== "read_only") {
    const summary = record(m.consequenceSummaryTemplate, "consequenceSummaryTemplate");
    exactKeys(summary, ["localizedTemplates", "argumentPaths"], "consequenceSummaryTemplate");
    const localized = stringRecord(summary.localizedTemplates, "consequenceSummaryTemplate.localizedTemplates");
    stringRecord(summary.argumentPaths, "consequenceSummaryTemplate.argumentPaths");
    if (typeof localized.en !== "string" || typeof localized.pt !== "string") throw new Error("consequenceSummaryTemplate requires en and pt templates");
  }
  return m as unknown as ToolManifest;
}

async function loadTool(directory: string, layer: LoadedTool["layer"], maxBundleBytes: number): Promise<LoadedTool> {
  const manifestPath = join(directory, "manifest.json");
  const raw = await readFile(manifestPath, "utf8");
  const parsed = JSON.parse(raw) as Record<string, unknown>;
  const manifest = parseManifest(parsed);
  const bundlePath = resolve(directory, manifest.bundle);
  if (basename(bundlePath) !== "index.mjs" || resolve(directory, "index.mjs") !== bundlePath) throw new Error(`${manifest.name}: bundle escapes tool directory`);
  const bundle = await readFile(bundlePath);
  if (bundle.byteLength > maxBundleBytes) throw new Error(`${manifest.name}: bundle exceeds ${maxBundleBytes} bytes`);
  await init;
  const source = bundle.toString("utf8");
  for (const specifier of parse(source)[0]) {
    if (specifier.d >= 0) throw new Error(`${manifest.name}: dynamic import is not allowed in a self-contained bundle`);
    if (specifier.d === -1 && (!specifier.n || !specifier.n.startsWith("node:"))) throw new Error(`${manifest.name}: bundle imports must use explicit node: built-ins; bundle local/package dependencies into index.mjs`);
  }
  const projection = { ...parsed };
  delete projection.digest;
  const actual = toolDigest(projection, bundle);
  if (actual !== manifest.digest) throw new Error(`${manifest.name}: digest mismatch: claimed ${manifest.digest}, actual ${actual}`);
  const imported = await import(`${pathToFileURL(bundlePath).href}?digest=${encodeURIComponent(actual)}`) as Partial<ToolModule>;
  if (!imported.identity || imported.identity.name !== manifest.name || imported.identity.version !== manifest.version) throw new Error(`${manifest.name}: bundle identity name/version mismatch`);
  if (typeof imported.invoke !== "function") throw new Error(`${manifest.name}: bundle must export invoke(context, arguments)`);
  return { manifest, module: imported as ToolModule, directory, sizeBytes: bundle.byteLength + Buffer.byteLength(raw), layer };
}

async function loadLayer(root: string, layer: LoadedTool["layer"], maxBundleBytes: number): Promise<Map<string, LoadedTool>> {
  const out = new Map<string, LoadedTool>();
  let entries: Dirent[];
  try { entries = await readdir(root, { withFileTypes: true }); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return out;
    throw error;
  }
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    if (entry.name === ".gitkeep" && entry.isFile()) continue;
    if (!entry.isDirectory()) throw new Error(`${layer}: only tool directories are allowed (${entry.name})`);
    const tool = await loadTool(join(root, entry.name), layer, maxBundleBytes);
    if (out.has(tool.manifest.name)) throw new Error(`${layer}: duplicate logical tool name ${tool.manifest.name}`);
    out.set(tool.manifest.name, tool);
  }
  return out;
}

export async function loadGeneration(directory: string, metadata: { revision: string; artifactDigest: string }, limits = { maxBundleBytes: 16 * 1024 * 1024, maxTools: 128 }): Promise<LoadedGeneration> {
  const product = await loadLayer(join(directory, "tools", "product"), "product", limits.maxBundleBytes);
  const client = await loadLayer(join(directory, "tools", "client"), "client", limits.maxBundleBytes);
  for (const [name, override] of client) {
    const base = product.get(name);
    if (base && base.manifest.version === override.manifest.version && base.manifest.digest !== override.manifest.digest) {
      throw new Error(`${name}: client override reuses immutable version ${override.manifest.version} with a different digest`);
    }
    product.set(name, override);
  }
  const tools = [...product.values()].sort((a, b) => a.manifest.name.localeCompare(b.manifest.name));
  if (tools.length > limits.maxTools) throw new Error(`generation has ${tools.length} tools; maximum is ${limits.maxTools}`);
  const info = await stat(directory);
  if (!info.isDirectory()) throw new Error("generation path is not a directory");
  return { ...metadata, directory, tools, sizeBytes: tools.reduce((sum, tool) => sum + tool.sizeBytes, 0) };
}
