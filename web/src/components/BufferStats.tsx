import { Graph } from "./Graph";
import { Readouts, Stat } from "./ui";
import { C } from "../theme";
import { eventMarkers, gaugeSeries, msSeries, rateSeries } from "../lib/series";
import type { ClientStats } from "../types";

// Live buffer + delay telemetry: jitter/output depth, throughput, and the
// estimated delay from the source. Shown in the Buffers tab and the modal.
export function BufferStats({ st, clockOffset }: { st?: ClientStats; clockOffset: number }) {
  if (!st || !(st.samples || []).length) {
    return <div className="text-xs text-slate-400 py-2">no telemetry yet from this device</div>;
  }
  const samples = st.samples;
  const latest = samples[samples.length - 1];
  const rate = rateSeries(samples, "samples_appended");
  const lastRate = rate.length ? rate[rate.length - 1].y : 0;
  const dropMarkers = eventMarkers(samples, ["overrun_drops", "late_drops", "lost_packets"], C.drop);
  const underMarkers = eventMarkers(samples, ["underruns"], C.under);
  const n = (v: number | undefined) => v || 0;

  return (
    <div>
      <Readouts>
        <Stat
          k="Jitter buffer"
          v={`${latest.jitter_buffer_len} pkt · ${(n(latest.jitter_buffer_len) * n(latest.packet_ms)).toFixed(0)} ms`}
        />
        <Stat
          k="Output queue"
          v={`${latest.output_queue_len} buf · ${(n(latest.output_queue_len) * n(latest.packet_ms)).toFixed(0)} ms`}
          tone={latest.output_queue_len === 0 ? "warn" : ""}
        />
        <Stat k="Samples/s" v={lastRate.toFixed(0)} />
        <Stat k="Source delay" v={`${n(latest.source_delay_ms).toFixed(0)} ms`} />
        <Stat k="Underruns" v={latest.underruns} tone={n(latest.underruns) > 0 ? "warn" : ""} />
        <Stat k="Overrun" v={latest.overrun_drops} tone={n(latest.overrun_drops) > 0 ? "bad" : ""} />
        <Stat k="Late" v={latest.late_drops} tone={n(latest.late_drops) > 0 ? "bad" : ""} />
        <Stat k="Lost" v={latest.lost_packets} tone={n(latest.lost_packets) > 0 ? "bad" : ""} />
      </Readouts>
      <Graph
        title="Buffer depth"
        unit="ms buffered"
        clockOffset={clockOffset}
        height={110}
        series={[
          { name: "jitter buffer", color: C.jitter, points: msSeries(samples, "jitter_buffer_len") },
          { name: "output queue", color: C.output, points: msSeries(samples, "output_queue_len") },
        ]}
        markers={[...dropMarkers, ...underMarkers]}
      />
      <Graph
        title="Samples handed to player"
        unit="samples/s"
        clockOffset={clockOffset}
        series={[{ name: "rate", color: C.rate, points: rate }]}
        markers={[...dropMarkers, ...underMarkers]}
      />
      <Graph
        title="Estimated delay from source"
        unit="ms · ≈ delay setting"
        clockOffset={clockOffset}
        signed
        height={110}
        series={[{ name: "delay", color: C.delay, points: gaugeSeries(samples, "source_delay_ms") }]}
      />
      <div className="flex flex-wrap gap-3 mt-1.5 text-[11px] text-slate-500">
        <span className="inline-flex items-center gap-1.5">
          <span className="w-2.5 h-[3px] rounded-sm inline-block" style={{ background: C.drop }} />
          drop (overrun/late/lost)
        </span>
        <span className="inline-flex items-center gap-1.5">
          <span className="w-2.5 h-[3px] rounded-sm inline-block" style={{ background: C.under }} />
          underrun
        </span>
      </div>
    </div>
  );
}
