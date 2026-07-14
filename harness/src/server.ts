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
//   SIGTERM: abort in-flight prompt (emit Settled{aborted}) -> flush+close
//            session -> close servers -> exit.
//
// SKELETON: requires `make proto` (Connect stubs: ./gen/harness/v1/*) +
// `npm install` to compile. The structure below is the intended wiring.

import { loadConfig } from "./config.js";
import { createSession } from "./session.js";
import { mapEvent } from "./events.js";

// TODO after `make proto`:
// import { createServer } from "node:https";
// import { ConnectRouter } from "@connectrpc/connect-node";
// import { HarnessService } from "./gen/iterabase/harness/v1/harness_connect.js";
// import * as pb from "./gen/harness/v1/harness_pb.js";

async function main(): Promise<void> {
  const cfg = loadConfig();
  const { session } = await createSession(cfg);

  // TODO: mTLS Connect server on cfg.server.port (8443):
  //   - Prompt(req): subscribe to session; call session.prompt(req.message);
  //     yield mapEvent(e) for each event until Settled; then close the stream.
  //   - Abort(_): session.abort(); the active Prompt stream emits Settled{aborted}.
  //   Require + validate the control-plane client cert against cfg.tls.ca.

  // TODO: plain-HTTP probe server on cfg.server.healthPort (8081):
  //   /readyz  -> 200 once config+session+extensions+model+server are ready (stays True)
  //   /healthz -> 200 if the process is alive
  //   (loopback only; not behind mTLS — the kubelet can't speak Connect/mTLS)

  // TODO: SIGTERM handler: abort in-flight prompt, flush+close session, close
  // servers, exit. terminationGracePeriodSeconds is set by HOR-245.

  void session;
  void mapEvent;
  console.log(`harness booting (session.id=${cfg.session.id}) — TODO: wire Connect + probe servers`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
