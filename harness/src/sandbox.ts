// Sandbox validation (HOR-381). The supervisor validates the per-session
// sandbox on the shared RWX PVC before launching the child: resolve the root
// beneath the boot-configured mount root, verify it exists / is a directory /
// is NOT a symlink / is owned by the session UID/GID / is mode 0700, and
// resolve the relative working directory within it.
//
// The supervisor NEVER chowns and (v1) NEVER auto-creates a sandbox root — a
// missing or mismatched sandbox yields a typed SandboxError the supervisor
// turns into a FAILED outcome. Provisioning (create+chown, UID/GID assignment,
// repo CoW checkouts) is HOR-245's job.

import { lstatSync } from "node:fs";
import { isAbsolute, join, resolve, sep } from "node:path";

export class SandboxError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "SandboxError";
  }
}

export interface SandboxPaths {
  root: string;
  home: string;
  tmp: string;
  session: string; // pi JSONL transcript + metadata
  workspace: string; // zero or more task repos/dirs (provisioned by HOR-245)
}

/** Build canonical subpaths under a sandbox root. */
export function sandboxSubpaths(sandboxRoot: string): SandboxPaths {
  return {
    root: sandboxRoot,
    home: join(sandboxRoot, "home"),
    tmp: join(sandboxRoot, "tmp"),
    session: join(sandboxRoot, "session"),
    workspace: join(sandboxRoot, "workspace"),
  };
}

/**
 * Resolve the sandbox root beneath the mount root. The sandbox id must be a
 * single path component (no separators, no traversal) so an AssignTurn can
 * never direct the supervisor outside the mount root.
 */
export function resolveSandboxRoot(sandboxMountRoot: string, sandboxId: string): string {
  if (!sandboxId) throw new SandboxError("sandboxId is required");
  if (sandboxId.includes("/") || sandboxId.includes(sep) || sandboxId === "." || sandboxId === "..")
    throw new SandboxError(`sandboxId must be a single path component (got ${JSON.stringify(sandboxId)})`);
  return join(sandboxMountRoot, sandboxId);
}

/**
 * Validate a sandbox root and return its canonical subpaths. Checks: exists,
 * is a directory, is NOT a symlink, owned by (uid, gid), mode 0700. Does not
 * chown and does not auto-create — a missing/mismatched root is a typed error.
 */
export function validateSandbox(sandboxRoot: string, uid: number, gid: number): SandboxPaths {
  let st;
  try {
    st = lstatSync(sandboxRoot);
  } catch {
    throw new SandboxError(`sandbox root missing (not provisioned): ${sandboxRoot}`);
  }
  if (st.isSymbolicLink()) throw new SandboxError(`sandbox root is a symlink (refused): ${sandboxRoot}`);
  if (!st.isDirectory()) throw new SandboxError(`sandbox root is not a directory: ${sandboxRoot}`);
  if (st.uid !== uid) throw new SandboxError(`sandbox root uid ${st.uid} != expected ${uid}: ${sandboxRoot}`);
  if (st.gid !== gid) throw new SandboxError(`sandbox root gid ${st.gid} != expected ${gid}: ${sandboxRoot}`);
  const mode = st.mode & 0o777;
  if (mode !== 0o700) throw new SandboxError(`sandbox root mode ${mode.toString(8)} != 0700: ${sandboxRoot}`);
  return sandboxSubpaths(sandboxRoot);
}

/**
 * Resolve a relative working directory within the sandbox root. Rejects
 * absolute paths and any traversal that escapes the root. Returns the absolute
 * cwd the child runs under.
 */
export function resolveWorkingDir(sandboxRoot: string, workingDir: string): string {
  if (!workingDir) throw new SandboxError("workingDir is required");
  if (isAbsolute(workingDir)) throw new SandboxError("workingDir must be relative to the sandbox root");
  const root = resolve(sandboxRoot);
  const target = resolve(root, workingDir);
  if (target !== root && !target.startsWith(root + sep))
    throw new SandboxError(`workingDir escapes the sandbox root: ${workingDir}`);
  return target;
}
