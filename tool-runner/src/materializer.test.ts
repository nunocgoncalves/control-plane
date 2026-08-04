import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import * as tar from "tar";
import { validateArchive } from "./materializer.js";

const roots: string[] = [];
afterEach(async () => { for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true }); });

describe("Flux archive validation", () => {
  it("counts only regular tool files", async () => {
    const root = await mkdtemp(join(tmpdir(), "flux-artifact-")); roots.push(root);
    await mkdir(join(root, "tools", "product", "echo"), { recursive: true });
    await writeFile(join(root, "tools", "product", "echo", "index.mjs"), "1234");
    await writeFile(join(root, "README.md"), "ignored");
    const archive = join(root, "artifact.tgz");
    await tar.c({ gzip: true, cwd: root, file: archive }, ["tools", "README.md"]);
    await expect(validateArchive(archive, 1024)).resolves.toBe(4);
  });

  it("rejects links even outside the selected tool tree", async () => {
    const root = await mkdtemp(join(tmpdir(), "flux-artifact-")); roots.push(root);
    await mkdir(join(root, "tools", "product"), { recursive: true });
    await writeFile(join(root, "target"), "secret");
    const { symlink } = await import("node:fs/promises");
    await symlink("target", join(root, "escape"));
    const archive = join(root, "artifact.tgz");
    await tar.c({ gzip: true, cwd: root, file: archive }, ["tools", "escape"]);
    await expect(validateArchive(archive, 1024)).rejects.toThrow("unsupported archive entry");
  });
});
