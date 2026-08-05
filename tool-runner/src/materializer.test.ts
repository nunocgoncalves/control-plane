import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as tar from "tar";
import { FluxMaterializer, validateArchive, type FluxArtifact, type MaterializerConfig } from "./materializer.js";

const roots: string[] = [];
const originalFetch = globalThis.fetch;
afterEach(async () => {
  globalThis.fetch = originalFetch;
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

async function root(): Promise<string> {
  const value = await mkdtemp(join(tmpdir(), "flux-artifact-"));
  roots.push(value);
  return value;
}

function config(directory: string): MaterializerConfig {
  return {
    namespace: "flux-system", sourceName: "overlay",
    artifactsRoot: join(directory, "artifacts"), controlRoot: join(directory, "control"),
    pollMs: 10, maxArchiveBytes: 1024 * 1024, maxExtractedBytes: 1024 * 1024, maxGenerations: 8,
  };
}

function materializer(cfg: MaterializerConfig, artifact: FluxArtifact): FluxMaterializer {
  const kubeConfig = {
    makeApiClient: () => ({ getNamespacedCustomObject: async () => ({ status: { artifact } }) }),
  };
  return new FluxMaterializer(cfg, kubeConfig as never);
}

async function archiveFixture(directory: string, version: string): Promise<{ archive: string; bytes: Buffer; artifact: FluxArtifact }> {
  const source = join(directory, `source-${version}`);
  const tool = join(source, "tools", "product", "echo");
  await mkdir(tool, { recursive: true });
  await writeFile(join(tool, "index.mjs"), `export const version=${JSON.stringify(version)};`);
  await writeFile(join(tool, "manifest.json"), JSON.stringify({ version }));
  const archive = join(directory, `artifact-${version}.tgz`);
  await tar.c({ gzip: true, cwd: source, file: archive }, ["tools"]);
  const bytes = await readFile(archive);
  const digest = `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
  return {
    archive, bytes,
    artifact: { revision: `master@sha1:${version}`, digest, url: `http://source-controller.flux-system.svc./gitrepository/overlay/${digest}.tar.gz` },
  };
}

function serve(bytes: Buffer): void {
  globalThis.fetch = vi.fn(async () => new Response(new Uint8Array(bytes), {
    status: 200,
    headers: { "content-length": String(bytes.byteLength) },
  })) as typeof fetch;
}

describe("Flux archive validation", () => {
  it("counts only regular tool files", async () => {
    const directory = await root();
    await mkdir(join(directory, "tools", "product", "echo"), { recursive: true });
    await writeFile(join(directory, "tools", "product", "echo", "index.mjs"), "1234");
    await writeFile(join(directory, "README.md"), "ignored");
    const archive = join(directory, "artifact.tgz");
    await tar.c({ gzip: true, cwd: directory, file: archive }, ["tools", "README.md"]);
    await expect(validateArchive(archive, 1024)).resolves.toBe(4);
  });

  it("rejects links even outside the selected tool tree", async () => {
    const directory = await root();
    await mkdir(join(directory, "tools", "product"), { recursive: true });
    await writeFile(join(directory, "target"), "secret");
    const { symlink } = await import("node:fs/promises");
    await symlink("target", join(directory, "escape"));
    const archive = join(directory, "artifact.tgz");
    await tar.c({ gzip: true, cwd: directory, file: archive }, ["tools", "escape"]);
    await expect(validateArchive(archive, 1024)).rejects.toThrow("unsupported archive entry");
  });
});

describe("Flux materialization", () => {
  it("reads the exact Ready artifact, verifies it, and publishes the generation atomically", async () => {
    const directory = await root();
    const fixture = await archiveFixture(directory, "v1");
    const cfg = config(directory);
    await mkdir(join(cfg.artifactsRoot, "generations"), { recursive: true });
    await mkdir(cfg.controlRoot, { recursive: true });
    const instance = materializer(cfg, fixture.artifact);
    serve(fixture.bytes);

    const current = await instance.currentArtifact();
    expect(current).toEqual(fixture.artifact);
    await instance.materialize(current!);

    const published = JSON.parse(await readFile(join(cfg.controlRoot, "current.json"), "utf8")) as {
      revision: string; artifactDigest: string; directory: string;
    };
    expect(published.revision).toBe(fixture.artifact.revision);
    expect(published.artifactDigest).toBe(fixture.artifact.digest);
    expect(await readFile(join(published.directory, "tools", "product", "echo", "index.mjs"), "utf8")).toContain("v1");
    expect((await readdir(cfg.artifactsRoot)).filter((name) => name.startsWith(".artifact-") || name.startsWith(".staging-"))).toEqual([]);
    expect(globalThis.fetch).toHaveBeenCalledWith(fixture.artifact.url, { redirect: "error" });
  });

  it("preserves the last published generation when the next artifact fails digest verification", async () => {
    const directory = await root();
    const first = await archiveFixture(directory, "v1");
    const second = await archiveFixture(directory, "v2");
    const cfg = config(directory);
    await mkdir(join(cfg.artifactsRoot, "generations"), { recursive: true });
    await mkdir(cfg.controlRoot, { recursive: true });
    const instance = materializer(cfg, first.artifact);
    serve(first.bytes);
    await instance.materialize(first.artifact);
    const before = await readFile(join(cfg.controlRoot, "current.json"), "utf8");

    serve(first.bytes);
    await expect(instance.materialize(second.artifact)).rejects.toThrow("Flux artifact digest mismatch");

    expect(await readFile(join(cfg.controlRoot, "current.json"), "utf8")).toBe(before);
    const published = JSON.parse(before) as { directory: string };
    expect(await readFile(join(published.directory, "tools", "product", "echo", "index.mjs"), "utf8")).toContain("v1");
    await expect(readFile(join(cfg.artifactsRoot, "generations", second.artifact.digest.slice(7), "tools", "product", "echo", "index.mjs"))).rejects.toThrow();
  });
});
