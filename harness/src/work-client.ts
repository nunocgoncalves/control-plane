// The Work bidi-stream gRPC client (HOR-381). The worker is the mTLS gRPC
// client; the control-plane (HOR-249) is the server. One long-lived Work stream
// per worker: open -> Hello -> Welcome -> (Ready/AssignTurn/TurnEvent/TokenDelta/
// Heartbeat/EventAck/AbortTurn) -> close.
//
// Transport: createGrpcTransport (HTTP/2 + mTLS). Cert material is re-read on
// each transport creation so an idle reconnect consumes rotated files. No RPC
// deadline on the long-lived stream; HTTP/2 PING (pingIntervalMs) provides
// liveness. Reconnect/backoff + the protocol state machine live in the
// supervisor; this module is the transport + the Hello/Welcome handshake.

import { readFileSync } from "node:fs";
import { createClient, type Transport } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import {
  Harness,
  type ControlMessage,
  type WorkerMessage,
} from "./gen/iterabase/harness/v1/harness_pb.js";
import type { HarnessConfig } from "./config.js";
import { AsyncQueue } from "./async-queue.js";

export class WorkClientError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "WorkClientError";
  }
}

export interface Welcome {
  protocolVersion: string;
  fencingGeneration: bigint;
  heartbeatIntervalMs: number;
  leaseTimeoutMs: number;
}

export interface WorkStream {
  /** Push a worker message (Ready, TurnEvent, TokenDelta, Heartbeat). No-op after close(). */
  send(msg: WorkerMessage): void;
  /** The control-plane message stream — Welcome already consumed; yields AssignTurn/AbortTurn/EventAck. */
  control: AsyncIterable<ControlMessage>;
  /** Close the input side (sends end-of-stream to the CP). */
  close(): void;
}

export interface WorkConnection {
  stream: WorkStream;
  welcome: Welcome;
}

/** Create the mTLS gRPC HTTP/2 transport. Certs re-read each call (rotation). */
export function createWorkTransport(cfg: HarnessConfig): Transport {
  return createGrpcTransport({
    baseUrl: cfg.controlPlane.url,
    nodeOptions: {
      ca: readFileSync(cfg.tls.ca),
      key: readFileSync(cfg.tls.key),
      cert: readFileSync(cfg.tls.cert),
      rejectUnauthorized: true,
      servername: cfg.controlPlane.serverName, // SNI + expected server cert name
    },
    pingIntervalMs: cfg.transport.http2PingIntervalMs,
    pingTimeoutMs: cfg.transport.http2PingTimeoutMs,
  });
}

/**
 * Open the Work bidi stream, send `hello`, await + validate the Welcome.
 * Returns the stream (for the supervisor to push worker messages + consume
 * control messages) and the parsed Welcome. Throws WorkClientError if the
 * stream closes before Welcome or the first message is not a Welcome.
 */
export async function openWorkStream(hello: WorkerMessage, transport: Transport): Promise<WorkConnection> {
  const client = createClient(Harness, transport);
  const input = new AsyncQueue<WorkerMessage>();
  input.push(hello);
  const iterator = client.work(input)[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done)
    throw new WorkClientError("stream closed before Welcome");
  const msg = first.value;
  if (msg.kind.case !== "welcome")
    throw new WorkClientError(`expected Welcome as first control message, got ${msg.kind.case ?? "empty"}`);
  const w = msg.kind.value;
  return {
    welcome: {
      protocolVersion: w.protocolVersion,
      fencingGeneration: w.fencingGeneration,
      heartbeatIntervalMs: w.heartbeatIntervalMs,
      leaseTimeoutMs: w.leaseTimeoutMs,
    },
    stream: {
      send: (m) => input.push(m),
      control: { [Symbol.asyncIterator]: () => iterator },
      close: () => input.close(),
    },
  };
}

// The bidi client consumes the shared AsyncQueue (async-queue.ts) as the
// input AsyncIterable<WorkerMessage>; throw()/return() from call cancel close
// it so cancellation semantics live in one place.
