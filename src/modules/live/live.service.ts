import type { WSContext } from "hono/ws";
import { listFamilyIdsByUserId, listSharedPeerUserIds } from "./live.repository";
import {
  HelloPayloadDto,
  LivestreamAnswerPayloadDto,
  LivestreamIcePayloadDto,
  LivestreamOfferPayloadDto,
  LivestreamStartPayloadDto,
  LivestreamWatchPayloadDto,
  parseUserId,
  UpdatePresencePayloadDto,
} from "./live.dto";
import { LiveRateLimitServiceError, LiveValidationServiceError } from "./live.error";
import { db } from "../../db";
import { users } from "../../db/schema";
import { eq } from "drizzle-orm";

interface PresenceConnection {
  ws: WSContext;
  presence: Record<string, unknown> | null;
  fullName: string;
  email: string;
}

interface LivestreamConnection {
  ws: WSContext;
  fullName: string;
  familyIds: Set<number>;
  broadcasting: boolean;
}

const presenceConnections = new Map<number, PresenceConnection>();
const livestreamConnections = new Map<number, LivestreamConnection>();

// Rate limiter: sliding window token bucket
const rateBuckets = new Map<number, number[]>();
const RATE_LIMIT = 10; // per second

function checkRateLimit(userId: number) {
  const now = Date.now();
  const bucket = rateBuckets.get(userId) ?? [];
  const filtered = bucket.filter((t) => now - t < 1000);
  if (filtered.length >= RATE_LIMIT) throw new LiveRateLimitServiceError();
  filtered.push(now);
  rateBuckets.set(userId, filtered);
}

function sendJson(ws: WSContext, data: unknown) {
  ws.send(JSON.stringify(data));
}

function nowIso() {
  return new Date().toISOString();
}

function shareAnyFamily(a: Set<number>, b: Set<number>) {
  for (const familyId of a) {
    if (b.has(familyId)) return true;
  }
  return false;
}

function broadcastToLivestreamFamilies(fromUserId: number, familyIds: Set<number>, data: unknown) {
  for (const [userId, peer] of livestreamConnections) {
    if (userId === fromUserId) continue;
    if (shareAnyFamily(peer.familyIds, familyIds)) sendJson(peer.ws, data);
  }
}

function sendToLivestreamUser(userId: number, data: unknown) {
  const peer = livestreamConnections.get(userId);
  if (!peer) return;
  sendJson(peer.ws, data);
}

export async function registerConnection(userId: number, ws: WSContext) {
  const existing = presenceConnections.get(userId);
  if (existing && existing.ws !== ws) {
    try {
      existing.ws.close(4000, "Replaced");
    } catch {
    }
  }

  // Fetch user info for broadcasts
  const [user] = await db
    .select({ fullName: users.fullName, email: users.email })
    .from(users)
    .where(eq(users.id, userId))
    .limit(1);

  presenceConnections.set(userId, { ws, presence: null, fullName: user?.fullName ?? "", email: user?.email ?? "" });

  // Send current presence of online peers
  const peerIds = await listSharedPeerUserIds(userId);
  for (const peerId of peerIds) {
    const peer = presenceConnections.get(peerId);
    if (peer?.presence) {
      sendJson(ws, {
        type: "member_presence_updated",
        payload: { userId: peerId, email: peer.email, fullName: peer.fullName, ...peer.presence },
        timestamp: nowIso(),
      });
    }
  }
}

export function removeConnection(userId: number, ws: WSContext) {
  const conn = presenceConnections.get(userId);
  if (conn?.ws === ws) {
    presenceConnections.delete(userId);
    rateBuckets.delete(userId);
  }
}

export async function registerLivestreamConnection(userId: number, ws: WSContext) {
  const existing = livestreamConnections.get(userId);
  if (existing && existing.ws !== ws) {
    try {
      existing.ws.close(4000, "Replaced");
    } catch {
    }
  }

  const [user] = await db.select({ fullName: users.fullName }).from(users).where(eq(users.id, userId)).limit(1);
  const familyIds = new Set(await listFamilyIdsByUserId(userId));

  livestreamConnections.set(userId, {
    ws,
    fullName: user?.fullName ?? "",
    familyIds,
    broadcasting: false,
  });
}

export function removeLivestreamConnection(userId: number, ws: WSContext) {
  const conn = livestreamConnections.get(userId);
  if (!conn || conn.ws !== ws) return;

  livestreamConnections.delete(userId);

  if (conn.broadcasting) {
    broadcastToLivestreamFamilies(userId, conn.familyIds, {
      type: "livestream_ended",
      payload: { broadcasterId: String(userId) },
      timestamp: nowIso(),
    });
  }
}

export async function handleLivestreamHello(userId: number, rawPayload: unknown) {
  const result = HelloPayloadDto.safeParse(rawPayload);
  if (!result.success) throw new LiveValidationServiceError(result.error.issues.map((i) => i.message).join(", "));

  const conn = livestreamConnections.get(userId);
  if (!conn) return;

  const payload = result.data;
  const allowedFamilyIds = await listFamilyIdsByUserId(userId);
  const allowedSet = new Set(allowedFamilyIds);

  if (payload.name.length > 0) {
    conn.fullName = payload.name;
  }

  if (payload.familyIds.length > 0) {
    const requested = payload.familyIds.map(parseUserId).filter((id) => Number.isInteger(id) && allowedSet.has(id));
    conn.familyIds = new Set(requested);
  } else {
    conn.familyIds = allowedSet;
  }

  const active = Array.from(livestreamConnections.entries())
    .filter(([peerId, peer]) => peerId !== userId && peer.broadcasting && shareAnyFamily(peer.familyIds, conn.familyIds))
    .map(([peerId, peer]) => ({ broadcasterId: String(peerId), broadcasterName: peer.fullName }));

  if (active.length > 0) {
    sendJson(conn.ws, {
      type: "livestream_active_list",
      payload: { active },
      timestamp: nowIso(),
    });
  }
}

export function handleLivestreamStart(userId: number, rawPayload: unknown) {
  const result = LivestreamStartPayloadDto.safeParse(rawPayload);
  if (!result.success) throw new LiveValidationServiceError(result.error.issues.map((i) => i.message).join(", "));

  const conn = livestreamConnections.get(userId);
  if (!conn) return;

  if (result.data.broadcasterName.length > 0) {
    conn.fullName = result.data.broadcasterName;
  }

  conn.broadcasting = true;
  broadcastToLivestreamFamilies(userId, conn.familyIds, {
    type: "livestream_started",
    payload: { broadcasterId: String(userId), broadcasterName: conn.fullName },
    timestamp: nowIso(),
  });
}

export function handleLivestreamEnd(userId: number) {
  const conn = livestreamConnections.get(userId);
  if (!conn || !conn.broadcasting) return;

  conn.broadcasting = false;
  broadcastToLivestreamFamilies(userId, conn.familyIds, {
    type: "livestream_ended",
    payload: { broadcasterId: String(userId) },
    timestamp: nowIso(),
  });
}

export function handleLivestreamWatch(userId: number, rawPayload: unknown) {
  const result = LivestreamWatchPayloadDto.safeParse(rawPayload);
  if (!result.success) throw new LiveValidationServiceError(result.error.issues.map((i) => i.message).join(", "));

  const broadcasterId = parseUserId(result.data.broadcasterId);
  sendToLivestreamUser(broadcasterId, {
    type: "livestream_watch",
    payload: { viewerId: String(userId) },
    timestamp: nowIso(),
  });
}

export function handleLivestreamOffer(userId: number, rawPayload: unknown) {
  const result = LivestreamOfferPayloadDto.safeParse(rawPayload);
  if (!result.success) throw new LiveValidationServiceError(result.error.issues.map((i) => i.message).join(", "));

  const viewerId = parseUserId(result.data.viewerId);
  sendToLivestreamUser(viewerId, {
    type: "livestream_offer",
    payload: { fromId: String(userId), sdp: result.data.sdp },
    timestamp: nowIso(),
  });
}

export function handleLivestreamAnswer(userId: number, rawPayload: unknown) {
  const result = LivestreamAnswerPayloadDto.safeParse(rawPayload);
  if (!result.success) throw new LiveValidationServiceError(result.error.issues.map((i) => i.message).join(", "));

  const broadcasterId = parseUserId(result.data.broadcasterId);
  sendToLivestreamUser(broadcasterId, {
    type: "livestream_answer",
    payload: {
      fromId: String(userId),
      broadcasterId: String(userId),
      sdp: result.data.sdp,
    },
    timestamp: nowIso(),
  });
}

export function handleLivestreamIce(userId: number, rawPayload: unknown) {
  const result = LivestreamIcePayloadDto.safeParse(rawPayload);
  if (!result.success) throw new LiveValidationServiceError(result.error.issues.map((i) => i.message).join(", "));

  const targetId = parseUserId(result.data.targetId);
  sendToLivestreamUser(targetId, {
    type: "livestream_ice",
    payload: {
      fromId: String(userId),
      candidate: result.data.candidate,
      sdpMid: result.data.sdpMid ?? null,
      sdpMLineIndex: result.data.sdpMLineIndex ?? null,
    },
    timestamp: nowIso(),
  });
}

export async function handleUpdatePresence(userId: number, rawPayload: unknown) {
  checkRateLimit(userId);

  const result = UpdatePresencePayloadDto.safeParse(rawPayload);
  if (!result.success) throw new LiveValidationServiceError(result.error.issues.map((i) => i.message).join(", "));

  const conn = presenceConnections.get(userId);
  if (!conn) return;

  const payload = result.data;
  conn.presence = payload;

  const peerIds = await listSharedPeerUserIds(userId);
  const broadcast = {
    type: "member_presence_updated",
    payload: {
      userId,
      email: conn.email,
      fullName: conn.fullName,
      ...payload,
    },
    timestamp: nowIso(),
  };

  for (const peerId of peerIds) {
    const peer = presenceConnections.get(peerId);
    if (peer) sendJson(peer.ws, broadcast);
  }
}

export function handlePing(ws: WSContext) {
  sendJson(ws, { type: "pong", payload: {}, timestamp: nowIso() });
}
