// The supervisor's inference-gateway model bridge (HOR-395, ARCH-010/011). The
// trusted supervisor opens an HTTP/2 mTLS stream to the inference gateway's
// workload listener (HOR-398) and forwards raw OpenAI SSE `data:` payloads to
// the child over fd 5; the child parses them into pi stream events. The
// supervisor stays transport-oriented (ARCH-010): it does not own the model
// catalogue, backend selection, or delta semantics.
//
// Auth: the worker's SPIFFE-bound mTLS cert (held by the supervisor only). The
// gateway validates the active durable turn + assigned model from
// `X-Iterabase-Run-Id` / `X-Iterabase-Turn-Id` + the request `model` field
// (HOR-398). The supervisor validates the requested model equals the active
// assignment's model BEFORE opening the stream (ARCH-004 fail-closed).
//
// Cancellation: a child `cancel` aborts the upstream HTTP/2 request (closes the
// stream); the bridge emits a terminal `modelEnd{status:"aborted"}`. Slow
// upstream behavior is bounded by HTTP/2 flow control + the child's bounded
// queue (backpressure: the child stops reading fd 5 → the supervisor stops
// reading the SSE stream). Prompt/response bodies are never logged (ARCH-011).

import { readFileSync } from "node:fs";
import * as http2 from "node:http2";
import type { HarnessConfig } from "./config.js";

export interface ModelBridgeDeps {
  cfg: HarnessConfig;
}

export interface ModelRequest {
  /** The OpenAI chat-completions request body (model/messages/tools/stream:true). */
  body: unknown;
}

export type ModelEndStatus = "ok" | "error" | "aborted";

export interface ModelStreamCallbacks {
  /** One SSE `data:` payload (a JSON object string, or "[DONE]"). Returns false
   * to apply backpressure (the producer pauses its upstream reader until the
   * `onDrain` callback fires). */
  onChunk: (data: string) => boolean;
  /** Authoritative terminal signal. */
  onEnd: (status: ModelEndStatus, httpStatus: number | undefined, errorMessage: string | undefined) => void;
  /** Subscribe to the downstream drain signal (resume a paused upstream reader). */
  onDrain: (listener: () => void) => () => void;
}

export class ModelBridgeError extends Error {}

/** Validate that the requested model matches the active assignment (ARCH-004). */
export function assertAssignedModel(requestedModelId: string, assignedModelId: string): void {
  if (requestedModelId !== assignedModelId) {
    throw new ModelBridgeError(`model mismatch: requested ${requestedModelId} != assigned ${assignedModelId}`);
  }
}

/**
 * Open the authenticated inference-gateway stream and forward SSE chunks until
 * the stream ends, errors, or the signal aborts. Returns when the terminal
 * callback has been invoked. The body is NOT logged.
 */
export function streamModel(
  deps: ModelBridgeDeps,
  req: ModelRequest,
  assignedModelId: string,
  headers: { runId: string; turnId: string },
  signal: AbortSignal | undefined,
  cb: ModelStreamCallbacks,
): Promise<void> {
  const body = req.body as { model?: string } | null;
  const requestedModel = body?.model;
  if (typeof requestedModel !== "string") {
    cb.onEnd("error", undefined, "model request missing model field");
    return Promise.resolve();
  }
  try {
    assertAssignedModel(requestedModel, assignedModelId);
  } catch (err) {
    cb.onEnd("error", undefined, err instanceof Error ? err.message : String(err));
    return Promise.resolve();
  }

  return new Promise<void>((resolve) => {
    const { cfg } = deps;
    const tls = cfg.tls;
    let ca: Buffer, cert: Buffer, key: Buffer;
    try {
      ca = readFileSync(tls.ca);
      cert = readFileSync(tls.cert);
      key = readFileSync(tls.key);
    } catch (err) {
      cb.onEnd("error", undefined, `tls read failed: ${err instanceof Error ? err.message : String(err)}`);
      resolve();
      return;
    }

    const url = new URL(cfg.inferenceGateway.url);
    const bodyJson = JSON.stringify(req.body);
    let settled = false;
    let session: http2.ClientHttp2Session | null = null;
    let stream: http2.ClientHttp2Stream | null = null;

    const finish = (status: ModelEndStatus, httpStatus: number | undefined, errorMessage: string | undefined): void => {
      if (settled) return;
      settled = true;
      cb.onEnd(status, httpStatus, errorMessage);
      try {
        stream?.close(http2.constants.NGHTTP2_NO_ERROR);
      } catch {
        /* already closed */
      }
      try {
        session?.close();
      } catch {
        /* already closed */
      }
      resolve();
    };

    const onAbort = (): void => {
      finish("aborted", undefined, "aborted");
    };
    if (signal) {
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener("abort", onAbort, { once: true });
    }

    try {
      session = http2.connect(`https://${url.host}`, {
        ca,
        cert,
        key,
        rejectUnauthorized: true,
        servername: cfg.inferenceGateway.serverName,
      } as http2.SecureClientSessionOptions);
    } catch (err) {
      cb.onEnd("error", undefined, `http2 connect failed: ${err instanceof Error ? err.message : String(err)}`);
      resolve();
      return;
    }

    session.on("error", (err) => {
      if (!settled) finish("error", undefined, `session error: ${err.message}`);
    });
    session.on("close", () => {
      if (!settled) finish("error", undefined, "session closed before completion");
    });

    const path = `${url.pathname === "/" ? "" : url.pathname}/v1/chat/completions`;
    const reqHeaders: http2.OutgoingHttpHeaders = {
      ":method": "POST",
      ":path": path,
      "content-type": "application/json",
      "content-length": Buffer.byteLength(bodyJson),
      "x-iterabase-run-id": headers.runId,
      "x-iterabase-turn-id": headers.turnId,
      accept: "text/event-stream",
    };

    try {
      stream = session.request(reqHeaders);
    } catch (err) {
      finish("error", undefined, `request failed: ${err instanceof Error ? err.message : String(err)}`);
      return;
    }

    stream.setEncoding("utf8");
    let buf = "";
    let httpStatus: number | undefined;
    let paused = false;

    /** Drain complete SSE frames from `buf`. Returns false if backpressure
     * paused the upstream reader (the caller must stop and wait for drain). */
    const processBuffer = (): boolean => {
      let idx: number;
      while ((idx = buf.indexOf("\n\n")) >= 0) {
        const frame = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        for (const line of frame.split("\n")) {
          if (line.startsWith("data:")) {
            const data = line.slice(5).trimStart();
            // Backpressure: if the fd-5 write did not drain, pause the HTTP/2
            // reader until the child drains fd 5 (HOR-395 bounded buffering —
            // a slow child cannot grow an unbounded queue in supervisor memory).
            if (!cb.onChunk(data)) {
              paused = true;
              try { stream?.pause(); } catch { /* stream gone */ }
              return false;
            }
          }
        }
      }
      return true;
    };

    const resumeOnDrain = (): void => {
      const off = cb.onDrain(() => {
        off();
        if (paused) {
          paused = false;
          // Re-drain anything buffered while paused before resuming the reader.
          if (processBuffer()) {
            try { stream?.resume(); } catch { /* stream gone */ }
          } else {
            resumeOnDrain();
          }
        }
      });
    };

    stream.on("response", (respHeaders) => {
      const status = respHeaders[":status"];
      httpStatus = typeof status === "number" ? status : undefined;
      if (httpStatus && httpStatus >= 400) {
        // Drain the error body, then signal an error terminal.
      }
    });

    stream.on("data", (chunk: string) => {
      if (settled) return;
      buf += chunk;
      if (!processBuffer()) resumeOnDrain();
    });

    stream.on("end", () => {
      if (settled) return;
      // Flush any trailing frame without a blank-line terminator.
      if (buf.trim()) {
        for (const line of buf.split("\n")) {
          if (line.startsWith("data:")) {
            const data = line.slice(5).trimStart();
            cb.onChunk(data);
          }
        }
      }
      finish(httpStatus && httpStatus >= 400 ? "error" : "ok", httpStatus, httpStatus && httpStatus >= 400 ? `inference gateway HTTP ${httpStatus}` : undefined);
    });

    stream.on("error", (err) => {
      if (!settled) finish("error", httpStatus, `stream error: ${err.message}`);
    });

    stream.write(bodyJson);
    stream.end();
  });
}
