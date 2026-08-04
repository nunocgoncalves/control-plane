// HOR-399 supervisor-owned artifact materialization/publication. The child sees
// only session-owned files and opaque references; it never receives a MinIO
// endpoint, URL, or credential.

import { createHash } from "node:crypto";
import {
  closeSync,
  constants,
  createReadStream,
  existsSync,
  fchmodSync,
  fchownSync,
  fstatSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readlinkSync,
  readSync,
  realpathSync,
  renameSync,
  statSync,
  unlinkSync,
  writeSync,
} from "node:fs";
import { dirname, isAbsolute, join, normalize, relative, resolve, sep } from "node:path";
import type { ArtifactMaterialization } from "./gen/iterabase/harness/v1/harness_pb.js";
import type { ArtifactMetadata } from "./gen/iterabase/gateway/v1/gateway_pb.js";
import type { AssignmentScope, GatewayClient } from "./gateway-client.js";
import { SandboxError } from "./sandbox.js";

export async function materializeArtifacts(
  client: GatewayClient,
  scope: AssignmentScope,
  materializations: ArtifactMaterialization[],
  workspace: string,
  uid: number,
  gid: number,
  signal?: AbortSignal,
): Promise<void> {
  if (materializations.length > 0 && !client.getArtifact) throw new SandboxError("artifact client is unavailable");
  for (const materialization of materializations) {
    const ref = materialization.ref;
    if (!ref?.artifactId || !materialization.relativePath) throw new SandboxError("invalid artifact materialization assignment");
    const destination = resolveArtifactPath(workspace, materialization.relativePath);
    ensureParentDirectories(workspace, dirname(destination), uid, gid);
    if (existsSync(destination)) {
      verifyExisting(destination, ref.sizeBytes, ref.digest, uid, gid);
      continue;
    }
    const temp = `${destination}.tmp-${process.pid}-${Date.now()}`;
    let fd: number | undefined;
    try {
      fd = openSync(temp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | noFollowFlag(), 0o600);
      const hash = createHash("sha256");
      let count = 0n;
      let canonicalSeen = false;
      for await (const response of client.getArtifact!(scope, ref.artifactId, signal)) {
        if (response.kind.case === "metadata") {
          if (canonicalSeen) throw new SandboxError("artifact service sent duplicate metadata");
          canonicalSeen = true;
          const canonical = response.kind.value.ref;
          if (!canonical || canonical.artifactId !== ref.artifactId || canonical.sizeBytes !== ref.sizeBytes || canonical.digest !== ref.digest) {
            throw new SandboxError("artifact metadata differs from assigned canonical reference");
          }
          continue;
        }
        if (response.kind.case !== "chunk" || !canonicalSeen) throw new SandboxError("artifact stream did not begin with metadata");
        const chunk = response.kind.value;
        writeAllSync(fd, chunk);
        hash.update(chunk);
        count += BigInt(chunk.byteLength);
      }
      if (!canonicalSeen) throw new SandboxError("artifact stream returned no metadata");
      const digest = `sha256:${hash.digest("hex")}`;
      if (count !== ref.sizeBytes || digest !== ref.digest) {
        throw new SandboxError(`artifact digest/size mismatch: expected ${ref.digest}/${ref.sizeBytes}, got ${digest}/${count}`);
      }
      fsyncSync(fd);
      if (fstatSync(fd, { bigint: true }).size !== count) {
        throw new SandboxError("materialized file size differs from verified stream size");
      }
      fchmodSync(fd, 0o600);
      fchownSync(fd, uid, gid);
      closeSync(fd);
      fd = undefined;
      if (existsSync(destination)) throw new SandboxError("artifact destination appeared during materialization");
      renameSync(temp, destination);
    } catch (err) {
      if (fd !== undefined) closeSync(fd);
      try { unlinkSync(temp); } catch { /* absent */ }
      throw err instanceof SandboxError ? err : new SandboxError(`materialize artifact ${ref.artifactId}: ${String(err)}`);
    }
  }
}

type SyncWriter = (fd: number, buffer: Uint8Array, offset: number, length: number) => number;

/** Node may legally perform a short synchronous write. Keep writing until the
 * complete verified chunk is persisted or fail closed on no progress. */
export function writeAllSync(fd: number, chunk: Uint8Array, writer: SyncWriter = writeSync): void {
  let offset = 0;
  while (offset < chunk.byteLength) {
    const written = writer(fd, chunk, offset, chunk.byteLength - offset);
    if (!Number.isInteger(written) || written <= 0) throw new SandboxError("artifact materialization write made no progress");
    offset += written;
  }
}

export async function publishWorkspaceArtifact(
  client: GatewayClient,
  scope: AssignmentScope,
  workspace: string,
  relativePath: string,
  mimeType: string,
  uid: number,
  gid: number,
  signal?: AbortSignal,
): Promise<ArtifactMetadata> {
  if (!client.putArtifact) throw new SandboxError("artifact client is unavailable");
  const path = resolveArtifactPath(workspace, relativePath);
  let fd: number | undefined;
  try {
    fd = openSync(path, constants.O_RDONLY | noFollowFlag());
    const stat = fstatSync(fd, { bigint: true });
    if (!stat.isFile()) throw new SandboxError("published path must be a regular file");
    if (Number(stat.uid) !== uid || Number(stat.gid) !== gid) throw new SandboxError("published file ownership does not match the active session");
    assertOpenedFileInside(fd, path, workspace);

    const hash = createHash("sha256");
    const buf = Buffer.allocUnsafe(64 * 1024);
    let offset = 0;
    for (;;) {
      const n = readSync(fd, buf, 0, buf.length, offset);
      if (n === 0) break;
      hash.update(buf.subarray(0, n));
      offset += n;
    }
    const digest = `sha256:${hash.digest("hex")}`;
    const stream = createReadStream(path, { fd, autoClose: false, start: 0 });
    const metadata = await client.putArtifact!(scope, {
      mimeType,
      expectedSizeBytes: stat.size,
      expectedDigest: digest,
      chunks: stream as AsyncIterable<Uint8Array>,
    }, signal);
    return metadata;
  } catch (err) {
    throw err instanceof SandboxError ? err : new SandboxError(`publish artifact: ${String(err)}`);
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

export function resolveArtifactPath(workspace: string, relativePath: string): string {
  if (!relativePath || isAbsolute(relativePath) || relativePath.includes("\0")) throw new SandboxError("artifact path must be a non-empty relative path");
  const clean = normalize(relativePath);
  if (clean === "." || clean === ".." || clean.startsWith(`..${sep}`)) throw new SandboxError("artifact path escapes workspace");
  const root = resolve(workspace);
  const path = resolve(root, clean);
  if (path === root || !path.startsWith(root + sep)) throw new SandboxError("artifact path escapes workspace");
  return path;
}

function ensureParentDirectories(workspace: string, target: string, uid: number, gid: number): void {
  const root = resolve(workspace);
  const rel = relative(root, target);
  let current = root;
  for (const segment of rel.split(sep).filter(Boolean)) {
    current = join(current, segment);
    if (existsSync(current)) {
      const st = lstatSync(current);
      if (st.isSymbolicLink() || !st.isDirectory()) throw new SandboxError("artifact destination parent is not a real directory");
      continue;
    }
    mkdirSync(current, { mode: 0o700 });
    const fd = openSync(current, constants.O_RDONLY | noFollowFlag());
    try {
      fchmodSync(fd, 0o700);
      fchownSync(fd, uid, gid);
    } finally {
      closeSync(fd);
    }
  }
}

function verifyExisting(path: string, expectedSize: bigint, expectedDigest: string, uid: number, gid: number): void {
  const st = lstatSync(path);
  if (st.isSymbolicLink() || !st.isFile() || st.uid !== uid || st.gid !== gid) throw new SandboxError("existing materialized artifact is unsafe");
  if (BigInt(st.size) !== expectedSize) throw new SandboxError("existing materialized artifact size mismatch");
  const digest = `sha256:${createHash("sha256").update(readFileSync(path)).digest("hex")}`;
  if (digest !== expectedDigest) throw new SandboxError("existing materialized artifact digest mismatch");
}

function assertOpenedFileInside(fd: number, originalPath: string, workspace: string): void {
  let opened: string;
  try {
    if (process.platform !== "linux") throw new Error("proc fd validation is Linux-only");
    opened = readlinkSync(`/proc/self/fd/${fd}`);
  } catch {
    // Non-Linux tests: resolve the already-opened path's canonical target. The
    // production harness uses /proc/self/fd, which closes the symlink TOCTOU.
    opened = realpathSync(originalPath);
  }
  const root = realpathSync(workspace);
  const canonical = resolve(opened.replace(/ \(deleted\)$/, ""));
  if (!canonical.startsWith(root + sep)) throw new SandboxError("published path resolves outside the active workspace");
  const st = statSync(originalPath);
  const fst = fstatSync(fd);
  if (st.dev !== fst.dev || st.ino !== fst.ino) throw new SandboxError("published path changed while opening");
}

function noFollowFlag(): number {
  return typeof constants.O_NOFOLLOW === "number" ? constants.O_NOFOLLOW : 0;
}
