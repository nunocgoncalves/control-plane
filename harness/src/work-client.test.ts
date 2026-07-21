import { describe, it, expect } from "vitest";
import { createRouterTransport } from "@connectrpc/connect";
import { create } from "@bufbuild/protobuf";
import {
  Harness,
  WorkerMessageSchema,
  HelloSchema,
  ControlMessageSchema,
  WelcomeSchema,
  AssignTurnSchema,
  type WorkerMessage,
  type ControlMessage,
} from "./gen/iterabase/harness/v1/harness_pb.js";
import { openWorkStream, WorkClientError } from "./work-client.js";

function hello(): WorkerMessage {
  return create(WorkerMessageSchema, {
    kind: { case: "hello", value: create(HelloSchema, { workerId: "pod-1", poolId: "pool-1", protocolVersion: "1" }) },
  });
}

function welcome(gen = 7n): ControlMessage {
  return create(ControlMessageSchema, {
    kind: {
      case: "welcome",
      value: create(WelcomeSchema, {
        protocolVersion: "1",
        fencingGeneration: gen,
        heartbeatIntervalMs: 5000,
        leaseTimeoutMs: 30_000,
      }),
    },
  });
}

describe("openWorkStream", () => {
  it("sends Hello and validates the Welcome", async () => {
    const transport = createRouterTransport((router) => {
      router.service(Harness, {
        async *work(req) {
          yield welcome(7n);
          for await (const _ of req) {
            /* drain worker messages */
          }
        },
      });
    });
    const conn = await openWorkStream(hello(), transport);
    expect(conn.welcome.fencingGeneration).toBe(7n);
    expect(conn.welcome.heartbeatIntervalMs).toBe(5000);
    expect(conn.welcome.leaseTimeoutMs).toBe(30_000);
    conn.stream.close();
  });

  it("throws if the first control message is not a Welcome", async () => {
    const transport = createRouterTransport((router) => {
      router.service(Harness, {
        async *work(req) {
          yield create(ControlMessageSchema, {
            kind: { case: "assignTurn", value: create(AssignTurnSchema, { turnId: "t1" }) },
          });
          for await (const _ of req) {
          }
        },
      });
    });
    await expect(openWorkStream(hello(), transport)).rejects.toThrow(WorkClientError);
  });

  it("throws if the stream closes before Welcome", async () => {
    const transport = createRouterTransport((router) => {
      router.service(Harness, {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        async *work(_req) {
          // yield nothing — stream closes immediately
        },
      });
    });
    await expect(openWorkStream(hello(), transport)).rejects.toThrow(WorkClientError);
  });

  it("forwards control messages past Welcome to the supervisor", async () => {
    const transport = createRouterTransport((router) => {
      router.service(Harness, {
        async *work(req) {
          yield welcome();
          yield create(ControlMessageSchema, {
            kind: { case: "assignTurn", value: create(AssignTurnSchema, { turnId: "t1" }) },
          });
          for await (const _ of req) {
          }
        },
      });
    });
    const conn = await openWorkStream(hello(), transport);
    const seen: ControlMessage[] = [];
    for await (const m of conn.stream.control) {
      seen.push(m);
      if (seen.length === 1) break;
    }
    expect(seen[0]?.kind.case).toBe("assignTurn");
    conn.stream.close();
  });
});
