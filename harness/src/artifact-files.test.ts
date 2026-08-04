import { createHash } from "node:crypto";
import { closeSync, mkdirSync, mkdtempSync, openSync, readFileSync, rmSync, symlinkSync, writeFileSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { materializeArtifacts, publishWorkspaceArtifact, writeAllSync } from "./artifact-files.js";
import type { GatewayClient } from "./gateway-client.js";
import { SandboxError } from "./sandbox.js";

const scope = { turnId: "turn", runId: "attempt", fencingGeneration: 1n };
const uid = process.getuid?.() ?? 0;
const gid = process.getgid?.() ?? 0;
let root: string;
let workspace: string;

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), "artifact-files-"));
  workspace = join(root, "workspace");
  mkdirSync(workspace, { mode: 0o700 });
});
afterEach(() => rmSync(root, { recursive: true, force: true }));

function digest(body: Uint8Array): string {
  return `sha256:${createHash("sha256").update(body).digest("hex")}`;
}

function downloadClient(body: Uint8Array): GatewayClient {
  return {
    discover: async () => [], invokeTool: async () => { throw new Error("unused"); }, cancelInvocation: async () => ({ state: 0 }), resetTransport: () => {},
    getArtifact: async function* (_scope, artifactId) {
      yield { $typeName: "iterabase.gateway.v1.GetArtifactResponse", kind: { case: "metadata", value: { $typeName: "iterabase.gateway.v1.ArtifactMetadata", ref: { $typeName: "iterabase.gateway.v1.ArtifactRef", artifactId, mimeType: "text/plain", sizeBytes: BigInt(body.length), digest: digest(body) }, source: "user_upload", state: "available", createdAtUnixMs: 0n } } };
      yield { $typeName: "iterabase.gateway.v1.GetArtifactResponse", kind: { case: "chunk", value: body } };
    },
  };
}

describe("artifact materialization", () => {
  it("streams and verifies canonical bytes before exposing the session-owned file", async () => {
    const body = new TextEncoder().encode("immutable input");
    await materializeArtifacts(downloadClient(body), scope, [{
      $typeName: "iterabase.harness.v1.ArtifactMaterialization",
      ref: { $typeName: "iterabase.harness.v1.ArtifactRef", artifactId: "a1", mimeType: "text/plain", sizeBytes: BigInt(body.length), digest: digest(body) },
      relativePath: "inputs/a1",
    }], workspace, uid, gid);
    expect(readFileSync(join(workspace, "inputs/a1"), "utf8")).toBe("immutable input");
  });

  it("persists a complete chunk when the filesystem performs short writes", () => {
    const path = join(workspace, "short-write");
    const fd = openSync(path, "w");
    try {
      const body = new TextEncoder().encode("complete chunk");
      writeAllSync(fd, body, (target, buffer, offset, length) =>
        writeSync(target, buffer, offset, Math.min(length, 2)),
      );
    } finally {
      closeSync(fd);
    }
    expect(readFileSync(path, "utf8")).toBe("complete chunk");
  });

  it("fails closed on digest/size mismatch", async () => {
    const body = new TextEncoder().encode("corrupt");
    const client = downloadClient(body);
    // Keep service metadata equal to the assignment but send different bytes.
    client.getArtifact = async function* (_scope, artifactId) {
      const claimed = new TextEncoder().encode("expected");
      yield { $typeName: "iterabase.gateway.v1.GetArtifactResponse", kind: { case: "metadata", value: { $typeName: "iterabase.gateway.v1.ArtifactMetadata", ref: { $typeName: "iterabase.gateway.v1.ArtifactRef", artifactId, mimeType: "text/plain", sizeBytes: BigInt(claimed.length), digest: digest(claimed) }, source: "user_upload", state: "available", createdAtUnixMs: 0n } } };
      yield { $typeName: "iterabase.gateway.v1.GetArtifactResponse", kind: { case: "chunk", value: body } };
    };
    const claimed = new TextEncoder().encode("expected");
    await expect(materializeArtifacts(client, scope, [{
      $typeName: "iterabase.harness.v1.ArtifactMaterialization",
      ref: { $typeName: "iterabase.harness.v1.ArtifactRef", artifactId: "a1", mimeType: "text/plain", sizeBytes: BigInt(claimed.length), digest: digest(claimed) },
      relativePath: "inputs/a1",
    }], workspace, uid, gid)).rejects.toBeInstanceOf(SandboxError);
  });
});

describe("artifact publication", () => {
  it("rejects traversal and symlink escape", async () => {
    const client = downloadClient(new Uint8Array());
    await expect(publishWorkspaceArtifact(client, scope, workspace, "../outside", "text/plain", uid, gid)).rejects.toBeInstanceOf(SandboxError);

    const outside = join(root, "outside");
    mkdirSync(outside);
    writeFileSync(join(outside, "secret"), "secret");
    symlinkSync(outside, join(workspace, "escape"));
    await expect(publishWorkspaceArtifact(client, scope, workspace, "escape/secret", "text/plain", uid, gid)).rejects.toBeInstanceOf(SandboxError);
  });

  it("streams a regular in-workspace file and returns the canonical reference", async () => {
    const body = new TextEncoder().encode("result");
    writeFileSync(join(workspace, "result.txt"), body);
    const client = downloadClient(new Uint8Array());
    client.putArtifact = async (_scope, input) => {
      const chunks: Uint8Array[] = [];
      for await (const chunk of input.chunks) chunks.push(chunk);
      const got = Buffer.concat(chunks.map((c) => Buffer.from(c)));
      expect(got.toString()).toBe("result");
      return { $typeName: "iterabase.gateway.v1.ArtifactMetadata", ref: { $typeName: "iterabase.gateway.v1.ArtifactRef", artifactId: "new", mimeType: input.mimeType, sizeBytes: input.expectedSizeBytes, digest: input.expectedDigest }, source: "sandbox_publish", state: "available", createdAtUnixMs: 0n };
    };
    const metadata = await publishWorkspaceArtifact(client, scope, workspace, "result.txt", "text/plain", uid, gid);
    expect(metadata.ref?.artifactId).toBe("new");
  });
});
