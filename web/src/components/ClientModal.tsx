import { useEffect } from "react";
import { Icon } from "../icons";
import { Readouts, SliderRow, Stat, StatusBadge } from "./ui";
import { BufferStats } from "./BufferStats";
import { ChannelRouting } from "./ChannelRouting";
import { clientStatus } from "../lib/status";
import type { CatalogSource, Client, ClientStats } from "../types";

// Opened from a client card's three-dot button. Shows live buffer/delay
// telemetry, an inline delay slider, a shortcut to the Buffers tab, and the
// drag-and-drop channel routing.
export function ClientModal({
  c,
  st,
  catalog,
  clockOffset,
  sourceSending,
  delay,
  delayMax,
  onClose,
  onChannelMap,
  onDelay,
  onJumpToBuffers,
}: {
  c: Client;
  st?: ClientStats;
  catalog: CatalogSource[];
  clockOffset: number;
  sourceSending?: boolean;
  delay: number;
  delayMax: number | undefined;
  onClose: () => void;
  onChannelMap: (id: string, map: number[]) => void;
  onDelay: (id: string, ms: number) => void;
  onJumpToBuffers: (id: string) => void;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const src = catalog.find((s) => s.source_id === c.selected_source_id);
  const latest = st && st.samples && st.samples.length ? st.samples[st.samples.length - 1] : null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/40"
      onClick={onClose}
    >
      <div
        className="bg-white rounded-2xl w-full max-w-2xl max-h-[86vh] overflow-y-auto thin-scroll p-5"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-2.5">
            <h2 className="text-lg font-semibold text-slate-800">{c.name}</h2>
            <StatusBadge status={clientStatus(c, st, sourceSending)} />
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700 rounded-md p-1 hover:bg-slate-100"
          >
            <Icon name="x" size={20} />
          </button>
        </div>
        <Readouts>
          <Stat k="IP" v={<span className="font-mono">{c.ip}</span>} />
          <Stat k="Status" v={c.connected ? "connected" : `last seen ${c.seconds_ago}s ago`} />
          <Stat k="Source" v={src ? src.name : "Off"} />
          <Stat k="Output" v={c.output_channels ? `${c.output_channels} ch` : "—"} />
        </Readouts>

        {/* Delay: slider + measured value + a shortcut over to the Buffers tab. */}
        <div className="mt-4 bg-slate-50 rounded-lg p-3">
          <div className="flex items-center justify-between">
            <span className="text-[10px] uppercase tracking-wide text-slate-400">Delay</span>
            <button
              onClick={() => onJumpToBuffers(c.id)}
              className="inline-flex items-center gap-1.5 text-xs font-medium text-emerald-700 bg-emerald-50 hover:bg-emerald-100 rounded-lg px-2.5 py-1"
            >
              Open in Buffers <Icon name="arrow" size={14} />
            </button>
          </div>
          <SliderRow
            label="Delay"
            min={0}
            max={delayMax || 0}
            value={Math.min(delay, delayMax || 0)}
            suffix="ms"
            disabled={!delayMax}
            onChange={(v) => onDelay(c.id, v)}
            title="Playback delay behind the source's send lead"
          />
          <div className="text-xs text-slate-400 mt-2">
            {delay} ms set
            {latest ? ` · ${(latest.source_delay_ms || 0).toFixed(0)} ms measured` : ""}
            {delayMax ? ` · max ${delayMax} ms` : " · select a source to set delay"}
          </div>
        </div>

        <ChannelRouting c={c} catalog={catalog} onChannelMap={onChannelMap} />

        <div className="mt-4 border-t border-slate-100 pt-3">
          <div className="text-xs font-medium text-slate-500 mb-2">Live buffer &amp; delay</div>
          <BufferStats st={st} clockOffset={clockOffset} />
        </div>
      </div>
    </div>
  );
}
