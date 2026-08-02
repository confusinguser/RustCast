import type { Sample } from "../types";

export interface Point {
  x: number;
  y: number;
}
export interface Marker {
  x: number;
  color: string;
}

/** Per-second rate of a cumulative counter, sample-to-sample. */
export function rateSeries(samples: Sample[], key: string): Point[] {
  const out: Point[] = [];
  for (let i = 1; i < samples.length; i++) {
    const dt = (samples[i].t - samples[i - 1].t) / 1000;
    if (dt <= 0) continue;
    out.push({
      x: samples[i].t,
      y: Math.max(0, ((samples[i][key] as number) - (samples[i - 1][key] as number)) / dt),
    });
  }
  return out;
}

/** A plain gauge series {x,y} for a numeric key. */
export function gaugeSeries(samples: Sample[], key: string): Point[] {
  return samples.map((s) => ({ x: s.t, y: (s[key] as number) ?? 0 }));
}

/** A buffer-depth series converted to ms via each sample's own packet duration. */
export function msSeries(samples: Sample[], key: string): Point[] {
  return samples.map((s) => ({ x: s.t, y: ((s[key] as number) || 0) * (s.packet_ms || 0) }));
}

/** Vertical event markers where the summed counters increased between samples. */
export function eventMarkers(samples: Sample[], keys: string[], color: string): Marker[] {
  const out: Marker[] = [];
  for (let i = 1; i < samples.length; i++) {
    let d = 0;
    for (const k of keys) d += (samples[i][k] as number) - (samples[i - 1][k] as number);
    if (d > 0) out.push({ x: samples[i].t, color });
  }
  return out;
}

/** Rate of a cumulative counter over just the last two samples (0 if stalled). */
export function lastCounterRate(samples: Sample[] | undefined, key: string): number {
  if (!samples || samples.length < 2) return 0;
  const a = samples[samples.length - 2];
  const b = samples[samples.length - 1];
  const dt = (b.t - a.t) / 1000;
  if (dt <= 0) return 0;
  return Math.max(0, ((b[key] as number) - (a[key] as number)) / dt);
}
