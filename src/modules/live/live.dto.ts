import { z } from "zod";

export const UpdatePresencePayloadDto = z.object({
  name: z.string(),
  latitude: z.number(),
  longitude: z.number(),
  rotation: z.number(),
  batteryLevel: z.number().int().min(0).max(100),
  isCharging: z.boolean(),
  internetStatus: z.enum(["wifi", "mobile"]),
  metadata: z.record(z.string(), z.unknown()).optional().default({}),
});

export const WsMessageEnvelope = z.object({
  type: z.string(),
  payload: z.record(z.string(), z.unknown()).optional().default({}),
  timestamp: z.string().optional(),
});

const NumericString = z.string().regex(/^\d+$/);

export const HelloPayloadDto = z.object({
  name: z.string().trim().max(255).optional().default(""),
  familyIds: z.array(NumericString).optional().default([]),
});

export const LivestreamStartPayloadDto = z.object({
  broadcasterName: z.string().trim().max(255).optional().default(""),
});

export const LivestreamWatchPayloadDto = z.object({
  broadcasterId: NumericString,
});

export const LivestreamOfferPayloadDto = z.object({
  viewerId: NumericString,
  sdp: z.string().min(1),
});

export const LivestreamAnswerPayloadDto = z.object({
  broadcasterId: NumericString,
  sdp: z.string().min(1),
});

export const LivestreamIcePayloadDto = z.object({
  targetId: NumericString,
  candidate: z.string().min(1),
  sdpMid: z.string().nullable().optional(),
  sdpMLineIndex: z.number().int().nullable().optional(),
});

export function parseUserId(value: string): number {
  return Number.parseInt(value, 10);
}
