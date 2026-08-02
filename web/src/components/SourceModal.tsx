import { useEffect } from "react";
import { Icon } from "../icons";
import { Readouts, SliderRow, Stat, StatusBadge } from "./ui";
import { Graph } from "./Graph";
import { tip } from "../tooltip";
import { C } from "../theme";
import { eventMarkers, gaugeSeries, rateSeries } from "../lib/series";
import { sourceStatus } from "../lib/status";
import type { Client, SendParams, ServerSource } from "../types";

// Opened by clicking a source card on the Clients board: the source's live
// send-path telemetry and (for local sources) its send-timing controls.
export function SourceModal({
  src,
  clients,
  sp,
  clockOffset,
  onClose,
  onSend,
  onReanchor,
}: {
  src: ServerSource;
  clients: Client[];
  sp?: SendParams;
  clockOffset: number;
  onClose: () => void;
  onSend: (id: string, patch: Record<string, unknown>) => void;
  onReanchor: (id: string) => void;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const s = src.samples || [];
  const latest = s[s.length - 1];
  const sendRate = rateSeries(s, "packets_sent");
  const lastRate = sendRate.length ? sendRate[sendRate.length - 1].y : 0;
  const reanchors = latest ? latest.reanchors : 0;
  const listeners = clients.filter(
    (c) => c.connected && c.selected_source_id === src.source_id,
  ).length;

  const lead = sp?.lead_ms ?? src.lead_ms;
  const redundancy = sp?.redundancy ?? src.redundancy ?? 1;
  const lastLead = sp?.last_lead_ms ?? src.last_lead_ms ?? 0;
  const unicast = sp?.unicast ?? src.unicast ?? false;

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
            <h2 className="text-lg font-semibold text-slate-800">{src.name}</h2>
            <StatusBadge status={sourceStatus(src, clients)} />
            {src.remote && (
              <span className="text-[11px] font-medium text-slate-500 bg-slate-100 rounded px-1.5 py-0.5">
                remote
              </span>
            )}
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700 rounded-md p-1 hover:bg-slate-100"
          >
            <Icon name="x" size={20} />
          </button>
        </div>

        <Readouts>
          <Stat k="Format" v={src.sample_rate ? `${src.sample_rate} Hz · ${src.channels}ch` : "—"} />
          <Stat k="Listeners" v={listeners} />
          <Stat k="Send rate" v={`${lastRate.toFixed(0)} pkt/s`} />
          <Stat k="Packets sent" v={latest ? latest.packets_sent.toLocaleString() : "—"} />
          <Stat k="Pending backlog" v={`${latest ? latest.pending_len : "—"} smp`} />
          <Stat k="Re-anchors" v={reanchors} tone={reanchors > 0 ? "warn" : ""} />
        </Readouts>

        {!src.remote && (
          <div className="mt-4 bg-slate-50 rounded-lg p-3">
            <SliderRow
              label="Lead"
              min={1}
              max={1500}
              value={lead}
              suffix="ms"
              onChange={(v) => onSend(src.source_id, { lead_ms: v })}
              title="How far ahead of play time the first packet is sent"
            />
            <SliderRow
              label="Copies"
              min={1}
              max={8}
              value={redundancy}
              suffix="×"
              onChange={(v) => onSend(src.source_id, { redundancy: v })}
              title="Identical copies of each packet (repetition FEC)"
            />
            <SliderRow
              label="Last"
              min={0}
              max={Math.max(0, lead - 1)}
              value={Math.min(lastLead, Math.max(0, lead - 1))}
              suffix="ms"
              disabled={redundancy < 2}
              onChange={(v) => onSend(src.source_id, { last_lead_ms: v })}
              title="How long before play time the last copy is sent"
            />
            <div className="flex items-center justify-between mt-3">
              <label
                className="flex items-center gap-2 text-sm text-slate-600 cursor-pointer"
                {...tip("Stream by unicast to each listening client instead of multicast")}
              >
                <input
                  type="checkbox"
                  checked={unicast}
                  className="accent-emerald-500 w-4 h-4"
                  onChange={(e) => onSend(src.source_id, { unicast: e.target.checked })}
                />
                Unicast to listeners
              </label>
              <button
                onClick={() => onReanchor(src.source_id)}
                className="inline-flex items-center gap-1.5 text-sm text-slate-600 bg-white border border-slate-200 hover:bg-slate-100 rounded-lg px-2.5 py-1.5"
                {...tip("Reset this source's send timeline to real time now")}
              >
                <Icon name="anchor" size={15} /> Re-anchor
              </button>
            </div>
          </div>
        )}

        <div className="mt-4 border-t border-slate-100 pt-3">
          <div className="text-xs font-medium text-slate-500 mb-2">Send path</div>
          <Graph
            title="Send rate"
            unit="pkt/s"
            clockOffset={clockOffset}
            series={[{ name: "sent", color: C.send, points: sendRate }]}
          />
          <Graph
            title="Pending backlog"
            unit="samples awaiting packetization"
            clockOffset={clockOffset}
            series={[{ name: "pending", color: C.pending, points: gaugeSeries(s, "pending_len") }]}
            markers={eventMarkers(s, ["reanchors"], C.under)}
          />
        </div>
      </div>
    </div>
  );
}
