import { Hono } from "hono";
import { upgradeWebSocket } from "hono/bun";
import { verifyToken } from "../../shared/auth/jwt";
import * as liveService from "./live.service";
import { WsMessageEnvelope } from "./live.dto";
import { LiveRateLimitServiceError, LiveValidationServiceError } from "./live.error";

export const liveRouter = new Hono();

async function authenticateWs(rawToken: string | undefined): Promise<number | null> {
  if (!rawToken) return null;
  try {
    const payload = await verifyToken(rawToken);
    return payload.sub;
  } catch {
    return null;
  }
}

function sendError(ws: { send: (data: string) => void }, message: string) {
  ws.send(JSON.stringify({ type: "error", payload: { message }, timestamp: new Date().toISOString() }));
}

liveRouter.get(
  "/ws/live",
  upgradeWebSocket(async (c) => {
    const header = c.req.header("Authorization");
    const queryToken = c.req.query("token");
    const rawToken = header?.startsWith("Bearer ") ? header.slice(7) : header ?? queryToken;
    const userId = await authenticateWs(rawToken);

    return {
      async onOpen(_evt, ws) {
        if (userId === null) {
          ws.close(4001, "Unauthorized");
          return;
        }
        await liveService.registerConnection(userId, ws);
      },
      async onMessage(evt, ws) {
        if (userId === null) return;
        try {
          const raw = JSON.parse(typeof evt.data === "string" ? evt.data : evt.data.toString());
          const envelope = WsMessageEnvelope.safeParse(raw);
          if (!envelope.success) {
            sendError(ws, "Invalid message envelope");
            return;
          }
          const { type, payload } = envelope.data;
          if (type === "ping") {
            liveService.handlePing(ws);
          } else if (type === "update_presence") {
            await liveService.handleUpdatePresence(userId, payload);
          }
        } catch (e) {
          const message = e instanceof LiveRateLimitServiceError || e instanceof LiveValidationServiceError ? e.message : "Invalid message";
          sendError(ws, message);
        }
      },
      onClose(_evt, ws) {
        if (userId !== null) liveService.removeConnection(userId, ws);
      },
    };
  })
);

liveRouter.get(
  "/ws/livestream",
  upgradeWebSocket(async (c) => {
    const header = c.req.header("Authorization");
    const queryToken = c.req.query("token");
    const rawToken = header?.startsWith("Bearer ") ? header.slice(7) : header ?? queryToken;
    const userId = await authenticateWs(rawToken);

    return {
      async onOpen(_evt, ws) {
        if (userId === null) {
          ws.close(4001, "Unauthorized");
          return;
        }
        await liveService.registerLivestreamConnection(userId, ws);
      },
      async onMessage(evt, ws) {
        if (userId === null) return;
        try {
          const raw = JSON.parse(typeof evt.data === "string" ? evt.data : evt.data.toString());
          const envelope = WsMessageEnvelope.safeParse(raw);
          if (!envelope.success) {
            sendError(ws, "Invalid message envelope");
            return;
          }

          const { type, payload } = envelope.data;
          if (type === "ping") {
            liveService.handlePing(ws);
          } else if (type === "hello") {
            await liveService.handleLivestreamHello(userId, payload);
          } else if (type === "livestream_start") {
            liveService.handleLivestreamStart(userId, payload);
          } else if (type === "livestream_end") {
            liveService.handleLivestreamEnd(userId);
          } else if (type === "livestream_watch") {
            liveService.handleLivestreamWatch(userId, payload);
          } else if (type === "livestream_offer") {
            liveService.handleLivestreamOffer(userId, payload);
          } else if (type === "livestream_answer") {
            liveService.handleLivestreamAnswer(userId, payload);
          } else if (type === "livestream_ice") {
            liveService.handleLivestreamIce(userId, payload);
          }
        } catch (e) {
          const message = e instanceof LiveValidationServiceError ? e.message : "Invalid message";
          sendError(ws, message);
        }
      },
      onClose(_evt, ws) {
        if (userId !== null) liveService.removeLivestreamConnection(userId, ws);
      },
    };
  })
);