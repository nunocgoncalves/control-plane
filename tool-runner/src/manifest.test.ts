import { mkdtemp, mkdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { rmSync } from "node:fs";
import { toolDigest } from "./canonical.js";
import { loadGeneration } from "./manifest.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });

async function fixture(layer: "product" | "client", options: { name?: string; version?: string; exportVersion?: string; body?: string } = {}) {
  const root = roots[0] ?? await mkdtemp(join(tmpdir(), "tool-generation-"));
  if (!roots.length) roots.push(root);
  const name = options.name ?? "test.echo";
  const version = options.version ?? "1.0.0";
  const dir = join(root, "tools", layer, `${name.replace(".", "-")}-${version}`);
  await mkdir(dir, { recursive: true });
  const bundle = Buffer.from(options.body ?? `export const identity={name:${JSON.stringify(name)},version:${JSON.stringify(options.exportVersion ?? version)}};export async function invoke(_c,args){return {result:args}};`);
  const projection = { apiVersion: "iterabase.io/tool/v1", name, version, description: "Echo", bundle: "index.mjs", inputSchema: { type: "object" }, effectClass: "read_only", timeoutMs: 1000 };
  const manifest = { ...projection, digest: toolDigest(projection, bundle) };
  await writeFile(join(dir, "index.mjs"), bundle);
  await writeFile(join(dir, "manifest.json"), JSON.stringify(manifest));
  return { root, dir, manifest };
}

describe("immutable tool generation loading", () => {
  it("loads a validated self-contained ESM bundle", async () => {
    const { root, manifest } = await fixture("product");
    const generation = await loadGeneration(root, { revision: "main@sha1:abc", artifactDigest: "sha256:artifact" });
    expect(generation.tools.map((tool) => [tool.manifest.name, tool.manifest.digest])).toEqual([["test.echo", manifest.digest]]);
    await expect(generation.tools[0].module.invoke({} as never, { value: 7 })).resolves.toEqual({ result: { value: 7 } });
  });

  it("rejects digest mismatch before loading", async () => {
    const { root, dir } = await fixture("product");
    await writeFile(join(dir, "index.mjs"), "export const identity={name:'test.echo',version:'1.0.0'};export async function invoke(){};");
    await expect(loadGeneration(root, { revision: "r", artifactDigest: "d" })).rejects.toThrow("digest mismatch");
  });

  it("rejects bundle version mismatch before registration", async () => {
    const { root } = await fixture("product", { exportVersion: "2.0.0" });
    await expect(loadGeneration(root, { revision: "r", artifactDigest: "d" })).rejects.toThrow("identity name/version mismatch");
  });

  it("rejects unknown manifest fields", async () => {
    const { root, dir } = await fixture("product");
    const manifest = JSON.parse(await readFile(join(dir, "manifest.json"), "utf8")) as Record<string, unknown>;
    manifest.unreviewed = true;
    await writeFile(join(dir, "manifest.json"), JSON.stringify(manifest));
    await expect(loadGeneration(root, { revision: "r", artifactDigest: "d" })).rejects.toThrow("unknown field unreviewed");
  });

  it("rejects local imports outside the digested bundle", async () => {
    const local = await fixture("product", { body: "import './helper.mjs';export const identity={name:'test.echo',version:'1.0.0'};export async function invoke(){};" });
    await expect(loadGeneration(local.root, { revision: "r", artifactDigest: "d" })).rejects.toThrow("bundle local/package dependencies");
  });

  it("uses client logical-name precedence with a new immutable version", async () => {
    const { root } = await fixture("product", { version: "1.0.0" });
    await fixture("client", { version: "2.0.0", body: "export const identity={name:'test.echo',version:'2.0.0'};export async function invoke(){return {result:'client'}};" });
    const generation = await loadGeneration(root, { revision: "r", artifactDigest: "d" });
    expect(generation.tools).toHaveLength(1);
    expect(generation.tools[0].manifest.version).toBe("2.0.0");
    expect(generation.tools[0].layer).toBe("client");
  });

  it("rejects client reuse of a product version with changed content", async () => {
    const { root } = await fixture("product", { version: "1.0.0" });
    await fixture("client", { version: "1.0.0", body: "export const identity={name:'test.echo',version:'1.0.0'};export async function invoke(){return {result:'changed'}};" });
    await expect(loadGeneration(root, { revision: "r", artifactDigest: "d" })).rejects.toThrow("reuses immutable version");
  });
});
