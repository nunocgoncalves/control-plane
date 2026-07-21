import { describe, it, expect } from "vitest";
import { join } from "node:path";
import {
  buildSetprivArgv,
  buildSpawnOptions,
  validateLaunchOptions,
  LauncherError,
  SETPRIV_BIN,
  type LaunchOptions,
} from "./launcher.js";

const VALID: LaunchOptions = {
  script: "/app/dist/child.js",
  uid: 1000,
  gid: 1000,
  sandboxRoot: "/data/sandboxes/sess-a",
  workingDir: "workspace/repo",
  stdio: ["pipe", "pipe", "pipe"],
  env: { HOME: "/data/sandboxes/sess-a/home" },
};

describe("buildSetprivArgv", () => {
  it("builds the full-drop setpriv argv execing process.execPath <script>", () => {
    expect(buildSetprivArgv(VALID)).toEqual([
      "--reuid",
      "1000",
      "--regid",
      "1000",
      "--clear-groups",
      "--no-new-privs",
      "--bounding-set",
      "-all",
      "--inh-caps",
      "-all",
      "--ambient-caps",
      "-all",
      process.execPath,
      "/app/dist/child.js",
    ]);
  });

  it("stringifies uid/gid", () => {
    const argv = buildSetprivArgv({ ...VALID, uid: 2000, gid: 3000 });
    expect(argv[1]).toBe("2000");
    expect(argv[3]).toBe("3000");
  });
});

describe("buildSpawnOptions", () => {
  it("sets cwd inside the sandbox and passes stdio + env", () => {
    const opts = buildSpawnOptions(VALID);
    expect(opts.cwd).toBe(join(VALID.sandboxRoot, VALID.workingDir));
    expect(opts.stdio).toBe(VALID.stdio);
    expect(opts.env).toBe(VALID.env);
  });
});

describe("validateLaunchOptions", () => {
  it("rejects a non-positive or non-integer uid/gid", () => {
    expect(() => validateLaunchOptions({ ...VALID, uid: 0 })).toThrow(LauncherError);
    expect(() => validateLaunchOptions({ ...VALID, uid: -1 })).toThrow(LauncherError);
    expect(() => validateLaunchOptions({ ...VALID, uid: 1.5 })).toThrow(LauncherError);
    expect(() => validateLaunchOptions({ ...VALID, gid: 0 })).toThrow(LauncherError);
  });

  it("rejects a relative or missing script path", () => {
    expect(() => validateLaunchOptions({ ...VALID, script: "dist/child.js" })).toThrow(LauncherError);
    expect(() => validateLaunchOptions({ ...VALID, script: "" })).toThrow(LauncherError);
  });

  it("rejects a non-absolute sandboxRoot", () => {
    expect(() => validateLaunchOptions({ ...VALID, sandboxRoot: "data/sess-a" })).toThrow(LauncherError);
  });

  it("rejects an absolute workingDir", () => {
    expect(() => validateLaunchOptions({ ...VALID, workingDir: "/etc" })).toThrow(LauncherError);
  });

  it("rejects a workingDir that escapes sandboxRoot", () => {
    expect(() => validateLaunchOptions({ ...VALID, workingDir: "../sibling" })).toThrow(LauncherError);
    expect(() => validateLaunchOptions({ ...VALID, workingDir: "sub/../../.." })).toThrow(LauncherError);
  });

  it("accepts a workingDir at the sandbox root", () => {
    expect(() => validateLaunchOptions({ ...VALID, workingDir: "." })).not.toThrow();
  });
});

describe("SETPRIV_BIN", () => {
  it("defaults to 'setpriv'", () => {
    // SETPRIV_BIN is resolved at module load from HARNESS_SETPRIV_BIN; the
    // default (no env) is "setpriv". (The isolation test overrides it in Linux.)
    expect(SETPRIV_BIN).toBe("setpriv");
  });
});
