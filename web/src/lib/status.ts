import type { Client, ClientStats, ServerSource, Status } from "../types";
import { lastCounterRate } from "./series";

// Status is derived from telemetry: while connected, an advancing audio counter
// means "Playing" and a stalled one means silence ("No Audio").

/** Playing / No Audio / Error / Offline for a client. `sourceSending` says whether
 *  the client's selected source is actively pushing packets: if it is but the
 *  client isn't appending samples, it *should* be playing but isn't — Error. */
export function clientStatus(
  c: Client,
  st: ClientStats | undefined,
  sourceSending?: boolean,
): Status {
  if (!c.connected) return "offline";
  if (!c.selected_source_id) return "no-audio";
  const flowing = lastCounterRate(st?.samples, "samples_appended") > 0;
  if (flowing) return "playing";
  return sourceSending ? "error" : "no-audio";
}

/** Playing / No Audio / No Listeners for a source. No-audio (a silent source)
 *  takes priority over no-listeners, so a silent idle source reads as No Audio
 *  even with nobody listening. */
export function sourceStatus(src: ServerSource, clients: Client[]): Status {
  const last = src.samples[src.samples.length - 1] as { has_audio?: boolean } | undefined;
  if (last && last.has_audio === false) return "no-audio";
  const sending = lastCounterRate(src.samples, "packets_sent") > 0;
  if (sending) return "playing";
  const listeners = clients.some(
    (c) => c.connected && c.selected_source_id === src.source_id,
  );
  return listeners ? "no-audio" : "no-listeners";
}
