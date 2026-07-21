import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdirSync, rmSync, symlinkSync, chmodSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  resolveSandboxRoot,
  sandboxSubpaths,
  validateSandbox,
  resolveWorkingDir,
  SandboxError,
} from "./sandbox.js";

const UID = process.getuid();
const GID = process.getgid();
let base: string;

beforeEach(() => {
  base = mkdtemp();
});
afterEach(() => {
  rmSync(base, { recursive: true, force: true });
});

function mkdtemp(): string {
  return mkdirSync(join(tmpdir(), `harness-sb-${Math.random().toString(36).slice(2)}`), { recursive: true });
}

describe("resolveSandboxRoot", () => {
  it("joins the mount root + sandbox id", () => {
    expect(resolveSandboxRoot("/data/sandboxes", "sess-a")).toBe("/data/sandboxes/sess-a");
  });
  it("rejects a sandbox id with a path separator / traversal", () => {
    expect(() => resolveSandboxRoot("/data/sandboxes", "a/b")).toThrow(SandboxError);
    expect(() => resolveSandboxRoot("/data/sandboxes", "..")).toThrow(SandboxError);
    expect(() => resolveSandboxRoot("/data/sandboxes", ".")).toThrow(SandboxError);
    expect(() => resolveSandboxRoot("/data/sandboxes", "")).toThrow(SandboxError);
  });
});

describe("sandboxSubpaths", () => {
  it("builds the canonical layout", () => {
    expect(sandboxSubpaths("/data/sandboxes/sess-a")).toEqual({
      root: "/data/sandboxes/sess-a",
      home: "/data/sandboxes/sess-a/home",
      tmp: "/data/sandboxes/sess-a/tmp",
      session: "/data/sandboxes/sess-a/session",
      workspace: "/data/sandboxes/sess-a/workspace",
    });
  });
});

describe("validateSandbox", () => {
  it("validates a provisioned 0700 sandbox owned by the session UID/GID", () => {
    const root = join(base, "sess-a");
    mkdirSync(root, { mode: 0o700 });
    chmodSync(root, 0o700);
    const paths = validateSandbox(root, UID, GID);
    expect(paths.root).toBe(root);
    expect(paths.session).toBe(join(root, "session"));
  });

  it("rejects a missing sandbox (never auto-creates in v1)", () => {
    expect(() => validateSandbox(join(base, "nope"), UID, GID)).toThrow(SandboxError);
  });

  it("rejects a symlinked root", () => {
    const real = join(base, "real");
    const link = join(base, "link");
    mkdirSync(real, { mode: 0o700 });
    chmodSync(real, 0o700);
    symlinkSync(real, link);
    expect(() => validateSandbox(link, UID, GID)).toThrow(SandboxError);
  });

  it("rejects a non-directory root", () => {
    const file = join(base, "file");
    writeFileSync(file, "x");
    chmodSync(file, 0o700);
    expect(() => validateSandbox(file, UID, GID)).toThrow(SandboxError);
  });

  it("rejects an ownership mismatch", () => {
    const root = join(base, "sess-a");
    mkdirSync(root, { mode: 0o700 });
    chmodSync(root, 0o700);
    expect(() => validateSandbox(root, UID + 1, GID)).toThrow(SandboxError); // uid mismatch
    expect(() => validateSandbox(root, UID, GID + 1)).toThrow(SandboxError); // gid mismatch
  });

  it("rejects a wrong mode (not 0700)", () => {
    const root = join(base, "sess-a");
    mkdirSync(root, { mode: 0o755 });
    chmodSync(root, 0o755);
    expect(() => validateSandbox(root, UID, GID)).toThrow(SandboxError);
  });
});

describe("resolveWorkingDir", () => {
  const root = "/data/sandboxes/sess-a";
  it("resolves a relative working dir inside the sandbox", () => {
    expect(resolveWorkingDir(root, "workspace/repo")).toBe("/data/sandboxes/sess-a/workspace/repo");
    expect(resolveWorkingDir(root, "home")).toBe("/data/sandboxes/sess-a/home");
  });
  it("accepts the sandbox root itself", () => {
    expect(resolveWorkingDir(root, ".")).toBe("/data/sandboxes/sess-a");
  });
  it("rejects an absolute working dir", () => {
    expect(() => resolveWorkingDir(root, "/etc")).toThrow(SandboxError);
  });
  it("rejects a working dir that escapes the sandbox", () => {
    expect(() => resolveWorkingDir(root, "../sibling")).toThrow(SandboxError);
    expect(() => resolveWorkingDir(root, "sub/../../..")).toThrow(SandboxError);
  });
});
