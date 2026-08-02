// Data model mirrored from the Rust API (src/api.rs, src/metrics.rs).

/** One telemetry sample. Fields are cumulative counters or gauges; `t` is the
 *  server-clock timestamp in ms. Indexed access keeps the many numeric keys
 *  ergonomic for the graph helpers. */
export interface Sample {
  t: number;
  packet_ms?: number;
  [key: string]: number | undefined;
}

/** A connected playback client (from the SSE `clients` list). */
export interface Client {
  id: string;
  ip: string;
  name: string;
  seconds_ago: number;
  connected: boolean;
  volume: number;
  delay_ms: number;
  /** Selected source id as a string; "" means off. */
  selected_source_id: string;
  output_channels: number;
  /** One source-channel index per output channel (-1 = silence); [] = identity. */
  channel_map: number[];
  /** Group this client belongs to (null = ungrouped). */
  group_id: string | null;
}

/** A client group (from the SSE `groups` list). Members follow its source. */
export interface Group {
  id: string;
  name: string | null;
  /** Selected source id string ("" = none). */
  source_id: string;
}

/** A catalog entry for the source dropdown. */
export interface CatalogSource {
  source_id: string;
  name: string;
  source_type: string;
  sample_rate: number;
  channels: number;
  format: string;
  /** Send lead (ms) — the cap for a client's delay slider on this source. */
  lead_ms: number;
}

/** A source's send-path stats + meta (local or remote), with rolling samples. */
export interface ServerSource {
  source_id: string;
  name: string;
  sample_rate: number;
  channels: number;
  lead_ms: number;
  redundancy: number;
  last_lead_ms: number;
  unicast: boolean;
  remote: boolean;
  samples: Sample[];
}

/** A client's telemetry history, keyed by id. */
export interface ClientStats {
  id: string;
  samples: Sample[];
}

/** Live send-timing knobs, optimistically tracked while editing. */
export interface SendParams {
  lead_ms: number;
  redundancy: number;
  last_lead_ms: number;
  unicast: boolean;
}

export type Status = "playing" | "no-audio" | "offline" | "no-listeners" | "error";

export type TabId = "clients" | "buffers" | "sync" | "settings";
