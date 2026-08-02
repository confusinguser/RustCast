import { Graph } from "./Graph";
import { Readouts, Stat } from "./ui";
import { C } from "../theme";
import { gaugeSeries } from "../lib/series";
import type { ClientStats } from "../types";

// Live time-sync (NTP) telemetry: clock offset vs the client and round-trip
// time. Shown in the Sync tab.
export function SyncStats({ st, clockOffset }: { st?: ClientStats; clockOffset: number }) {
  if (!st || !(st.samples || []).length) {
    return <div className="text-xs text-slate-400 py-2">no telemetry yet from this device</div>;
  }
  const samples = st.samples;
  const latest = samples[samples.length - 1];
  const n = (v: number | undefined) => v || 0;
  return (
    <div>
      <Readouts>
        <Stat k="Clock offset" v={`${n(latest.clock_offset_ms).toFixed(2)} ms`} />
        <Stat k="Target offset" v={`${n(latest.clock_target_offset_ms).toFixed(2)} ms`} />
        <Stat k="RTT" v={`${n(latest.rtt_ms).toFixed(1)} ms`} />
        <Stat k="Sync samples" v={n(latest.sync_samples)} />
        <Stat
          k="Last sample"
          v={`${n(latest.last_offset_ms).toFixed(2)} ms (${n(latest.last_rtt_ms).toFixed(1)} ms)`}
        />
      </Readouts>
      <Graph
        title="Clock offset vs client"
        unit="server − client, ms"
        clockOffset={clockOffset}
        signed
        height={120}
        series={[
          { name: "applied", color: C.rate, points: gaugeSeries(samples, "clock_offset_ms") },
          { name: "target", color: C.jitter, points: gaugeSeries(samples, "clock_target_offset_ms") },
        ]}
      />
      <Graph
        title="Sync round-trip time"
        unit="ms"
        clockOffset={clockOffset}
        height={110}
        series={[{ name: "rtt", color: C.send, points: gaugeSeries(samples, "rtt_ms") }]}
      />
    </div>
  );
}
