import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdirSync, rmSync, symlinkSync, chmodSync, writeFileSync, lstatSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  resolveSandboxRoot,
  sandboxSubpaths,
  validateSandbox,
  provisionSandbox,
  ensureSandboxMountRoot,
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

describe("provisionSandbox", () => {
  it("creates a missing sandbox root + subdirs at 0700 owned by the session UID/GID", () => {
    const root = join(base, "sess-a");
    const paths = provisionSandbox(root, UID, GID);
    expect(paths.root).toBe(root);
    expect(paths.workspace).toBe(join(root, "workspace"));
    for (const p of [paths.root, paths.home, paths.tmp, paths.session, paths.workspace]) {
      const st = lstatSync(p);
      expect(st.isDirectory()).toBe(true);
      expect(st.isSymbolicLink()).toBe(false);
      expect(st.uid).toBe(UID);
      expect(st.gid).toBe(GID);
      expect(st.mode & 0o777).toBe(0o700);
    }
  });

  it("is idempotent: re-provisioning a correctly-provisioned sandbox is a no-op", () => {
    const root = join(base, "sess-a");
    provisionSandbox(root, UID, GID);
    // Second call must not throw or mutate; same paths returned.
    const paths = provisionSandbox(root, UID, GID);
    expect(paths.root).toBe(root);
    const st = lstatSync(root);
    expect(st.mode & 0o777).toBe(0o700);
    expect(st.uid).toBe(UID);
  });

  it("refuses an existing mismatched root (never chowns): wrong mode -> FAILED", () => {
    const root = join(base, "sess-a");
    mkdirSync(root, { mode: 0o755 });
    chmodSync(root, 0o755);
    expect(() => provisionSandbox(root, UID, GID)).toThrow(SandboxError);
    // Not chowned; subdirs not created.
    expect(lstatSync(root).mode & 0o777).toBe(0o755);
    expect(() => lstatSync(join(root, "home"))).toThrow();
  });

  it("refuses an existing root with missing subdirs (never completes partial)", () => {
    const root = join(base, "sess-a");
    mkdirSync(root, { mode: 0o700 });
    chmodSync(root, 0o700); // root correct but subdirs missing
    expect(() => provisionSandbox(root, UID, GID)).toThrow(SandboxError);
    expect(() => lstatSync(join(root, "home"))).toThrow();
  });

  it("refuses a symlinked root (never chowns)", () => {
    const real = join(base, "real");
    const link = join(base, "link");
    mkdirSync(real, { mode: 0o700 });
    chmodSync(real, 0o700);
    symlinkSync(real, link);
    expect(() => provisionSandbox(link, UID, GID)).toThrow(SandboxError);
  });
});

describe("ensureSandboxMountRoot", () => {
  it("creates the mount root at 0711 if missing", () => {
    const mr = join(base, "mountroot");
    ensureSandboxMountRoot(mr);
    const st = lstatSync(mr);
    expect(st.isDirectory()).toBe(true);
    expect(st.mode & 0o777).toBe(0o711);
  });

  it("chmods an existing permissive mount root to 0711 and verifies ownership", () => {
    const mr = join(base, "mountroot");
    mkdirSync(mr, { mode: 0o777 });
    ensureSandboxMountRoot(mr);
    const st = lstatSync(mr);
    expect(st.mode & 0o777).toBe(0o711);
    expect(st.uid).toBe(process.getuid());
    expect(st.gid).toBe(process.getgid());
  });

  it("is idempotent on an already-correct 0711 root", () => {
    const mr = join(base, "mountroot");
    mkdirSync(mr, { mode: 0o711 });
    chmodSync(mr, 0o711);
    expect(() => ensureSandboxMountRoot(mr)).not.toThrow();
    expect(lstatSync(mr).mode & 0o777).toBe(0o711);
  });

  it("refuses a symlink mount root (TOCTOU-safe)", () => {
    const real = join(base, "realroot");
    mkdirSync(real, { mode: 0o711 });
    const link = join(base, "mountroot-link");
    symlinkSync(real, link);
    expect(() => ensureSandboxMountRoot(link)).toThrow(SandboxError);
  });

  it("refuses a non-directory mount root", () => {
    const mr = join(base, "mountroot-file");
    writeFileSync(mr, "not a dir");
    expect(() => ensureSandboxMountRoot(mr)).toThrow(SandboxError);
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
