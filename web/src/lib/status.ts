import type { Client, ClientStats, ServerSource, Status } from "../types";
import { lastCounterRate } from "./series";

// Live status is derived from telemetry: a device that's connected but whose
// audio counter isn't advancing reads as "No Audio", one whose counter is
// advancing reads as "Playing". Telemetry flows continuously while connected,
// so a stalled counter genuinely means silence (source paused / no source).

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

/** Playing / No Audio / No Listeners for a source. */
export function sourceStatus(src: ServerSource, clients: Client[]): Status {
  const sending = lastCounterRate(src.samples, "packets_sent") > 0;
  if (sending) return "playing";
  const listeners = clients.some(
    (c) => c.connected && c.selected_source_id === src.source_id,
  );
  return listeners ? "no-audio" : "no-listeners";
}
