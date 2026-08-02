// Isolated mount-root safety tests for ensureSandboxMountRoot (HOR-245).
//
// The real-filesystem tests in sandbox.test.ts cover the establish/verify happy
// paths and the symlink/non-directory refusals. Two failure modes cannot be
// produced by a non-root test process against the real filesystem:
//   - chmod EPERM (root-squash / read-only RWX volume ignoring chmod)
//   - a foreign-owned mount root (a non-root process cannot create a dir owned
//     by another UID)
// These are the exact attack scenarios the HOR-381 isolation contract defends
// against, so they are covered here by mocking node:fs's lstatSync/chmodSync.
import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Stats } from "node:fs";

const { lstatMock, chmodMock, mkdirMock } = vi.hoisted(() => ({
  lstatMock: vi.fn<(path: string) => Stats>(),
  chmodMock: vi.fn<(path: string, mode: number) => void>(),
  // mkdir is a no-op: the mocked lstat/chmod are the source of truth, so we
  // never need a real on-disk path (the attack paths use /data/sandboxes).
  mkdirMock: vi.fn<(path: string, options: object) => void>(),
}));

vi.mock("node:fs", async (importActual) => {
  const actual = await importActual<typeof import("node:fs")>();
  return {
    ...actual,
    lstatSync: lstatMock,
    chmodSync: chmodMock,
    mkdirSync: mkdirMock,
  };
});

// Import AFTER the mock is registered.
import { ensureSandboxMountRoot, SandboxError } from "./sandbox.js";

/** Minimal Stats stub: a non-symlink directory with the given owner/mode. */
function dirStat(uid: number, gid: number, mode: number = 0o040711): Stats {
  return {
    isSymbolicLink: () => false,
    isDirectory: () => true,
    isFile: () => false,
    uid,
    gid,
    mode,
  } as unknown as Stats;
}

const ME = process.getuid();
const MY_GID = process.getgid();
const PATH = "/data/sandboxes"; // mocked fs; path need not exist on disk

describe("ensureSandboxMountRoot — establish+verify failure modes", () => {
  beforeEach(() => {
    lstatMock.mockReset();
    chmodMock.mockReset();
    mkdirMock.mockReset();
    mkdirMock.mockReturnValue(undefined);
  });

  it("fails startup when chmod is ignored with EPERM (root-squash)", () => {
    // First (and only) lstat: a dir owned by the supervisor, so we reach chmod.
    lstatMock.mockReturnValue(dirStat(ME!, MY_GID!, 0o040755));
    chmodMock.mockImplementation(() => {
      const err = new Error("operation not permitted") as NodeJS.ErrnoException;
      err.code = "EPERM";
      throw err;
    });

    expect(() => ensureSandboxMountRoot(PATH)).toThrow(SandboxError);
    expect(chmodMock).toHaveBeenCalledWith(PATH, 0o711);
  });

  it("fails startup when the root is owned by a foreign (session) UID", () => {
    // A non-root supervisor cannot reclaim a foreign-owned root.
    lstatMock.mockReturnValue(dirStat(4242, 4242, 0o040711));
    chmodMock.mockReturnValue(undefined);

    expect(() => ensureSandboxMountRoot(PATH)).toThrow(SandboxError);
    // chmod must never run for a foreign-owned root under a non-root supervisor.
    expect(chmodMock).not.toHaveBeenCalled();
  });

  it("fails startup when chmod silently no-ops (verify catches the wrong mode)", () => {
    // Simulate a volume that accepts chmod but keeps the old mode (root-squash
    // lie): the post-chmod re-stat must catch the un-fixed mode.
    const permissive = dirStat(ME!, MY_GID!, 0o040777);
    lstatMock
      .mockReturnValueOnce(permissive) // pre-chmod stat (owner ok → reach chmod)
      .mockReturnValueOnce(permissive); // post-chmod VERIFY stat (still 0777)
    chmodMock.mockReturnValue(undefined);

    expect(() => ensureSandboxMountRoot(PATH)).toThrow(SandboxError);
  });
});
