// The harness Connect server (HOR-351). Embeds pi via the SDK (the harness IS
// the agent) and exposes the Harness RPC over mTLS to the control-plane
// (HOR-249). Also serves plain-HTTP /readyz + /healthz for the kubelet.
//
// Lifecycle:
//   boot: loadConfig -> createSession (resume-or-create by session.id) ->
//         start mTLS Connect server (8443) + plain-HTTP probes (8081) ->
//         /readyz flips True.
//   Prompt: session.prompt(message) -> stream mapEvent(session.subscribe())
//           until Settled{completed}; Abort -> session.abort() -> Settled{aborted}.
//   SIGTERM: abort in-flight prompt (emit Settled{aborted}) -> dispose session
//            -> close servers -> exit.

import { createServer as createHttpsServer } from "node:https";
import { createServer as createHttpServer } from "node:http";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { create } from "@bufbuild/protobuf";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import type { AgentSession } from "@earendil-works/pi-coding-agent";
import { loadConfig } from "./config.js";
import { createSession } from "./session.js";
import { EventMapper } from "./events.js";
import {
  AbortResponseSchema,
  EventSchema,
  Harness,
  SettledSchema,
  Settled_Reason,
  type Event,
  type PromptRequest,
} from "./gen/iterabase/harness/v1/harness_pb.js";

interface ActiveTurn {
  aborted: boolean;
  failed: boolean;
}

// The Harness service implementation. Extracted so the Connect contract
// (Prompt streaming + Abort) is unit-testable via an in-memory transport with
// a fake session — no mTLS/HTTP/real model required.
export function createHarnessService(s: AgentSession) {
  let active: ActiveTurn | null = null;

  return {
    prompt: async function* (req: PromptRequest) {
      const q = new AsyncQueue<Event>();
      const flags: ActiveTurn = { aborted: false, failed: false };
      active = flags;
      const mapper = new EventMapper(s);

      const unsub = s.subscribe((ev) => {
        if (ev.type === "auto_retry_end" && !ev.success) flags.failed = true;
        const mapped = ev.type === "agent_settled" ? buildSettled(flags, s) : mapper.map(ev);
        if (mapped) q.push(mapped);
        if (ev.type === "agent_settled") q.close();
      });

      const run = s.prompt(req.message).then(
        () => undefined,
        () => {
          flags.failed = true;
          q.push(buildSettled(flags, s));
          q.close();
        },
      );

      try {
        for await (const e of q) yield e;
      } finally {
        unsub();
        active = null;
        await run;
      }
    },

    abort: async () => {
      if (active) active.aborted = true;
      await s.abort();
      return create(AbortResponseSchema, {});
    },
  };
}

let session: AgentSession | null = null;
let ready = false;

async function main(): Promise<void> {
  const cfg = loadConfig();
  session = (await createSession(cfg)).session;

  const handler = connectNodeAdapter({ routes: (router) => router.service(Harness, createHarnessService(session!)) });

  const rpcServer = createHttpsServer(
    {
      cert: readFileSync(cfg.tls.cert),
      key: readFileSync(cfg.tls.key),
      ca: readFileSync(cfg.tls.ca),
      requestCert: true, // mTLS: require the control-plane client cert
      rejectUnauthorized: true,
    },
    handler,
  );

  const probeServer = createHttpServer((req, res) => {
    if (req.url === "/readyz") {
      res.writeHead(ready ? 200 : 503).end(ready ? "ok" : "not ready");
    } else if (req.url === "/healthz") {
      res.writeHead(200).end("ok");
    } else {
      res.writeHead(404).end();
    }
  });

  rpcServer.listen(cfg.server.port);
  probeServer.listen(cfg.server.healthPort);
  ready = true;
  console.log(
    `harness ready (session.id=${cfg.session.id}, rpc=:${cfg.server.port}, probes=:${cfg.server.healthPort})`,
  );

  const shutdown = (sig: string): void => {
    console.log(`harness received ${sig}; shutting down`);
    ready = false;
    if (session) void session.abort().catch(() => {});
    setTimeout(() => {
      session?.dispose();
      rpcServer.close();
      probeServer.close();
      process.exit(0);
    }, 500);
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

function buildSettled(flags: ActiveTurn, s: AgentSession): Event {
  return create(EventSchema, {
    kind: {
      case: "settled",
      value: create(SettledSchema, {
        reason: flags.aborted
          ? Settled_Reason.ABORTED
          : flags.failed
            ? Settled_Reason.FAILED
            : Settled_Reason.COMPLETED,
        messageCount: s.messages.length,
      }),
    },
  });
}

// Minimal async queue: subscribe pushes events; the Prompt generator drains
// until close() (after Settled). push() is a no-op once closed.
class AsyncQueue<T> {
  private readonly buf: T[] = [];
  private closed = false;
  private readonly waiters: Array<(r: IteratorResult<T>) => void> = [];

  push(v: T): void {
    if (this.closed) return;
    const w = this.waiters.shift();
    if (w) w({ value: v, done: false });
    else this.buf.push(v);
  }
  close(): void {
    this.closed = true;
    for (const w of this.waiters) w({ value: undefined as never, done: true });
    this.waiters.length = 0;
  }
  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: (): Promise<IteratorResult<T>> => {
        if (this.buf.length) return Promise.resolve({ value: this.buf.shift() as T, done: false });
        if (this.closed) return Promise.resolve({ value: undefined as never, done: true });
        return new Promise((resolve) => this.waiters.push(resolve));
      },
    };
  }
}

// Run only when this module is the entry point (not when imported by tests).
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
