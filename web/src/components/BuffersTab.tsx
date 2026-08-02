import { useEffect, useRef } from "react";
import { Icon } from "../icons";
import { Fold, SliderRow, SourceSelect, StatusBadge } from "./ui";
import { Graph } from "./Graph";
import { Readouts, Stat } from "./ui";
import { tip } from "../tooltip";
import { BufferStats } from "./BufferStats";
import { C } from "../theme";
import { eventMarkers, gaugeSeries, rateSeries } from "../lib/series";
import { clientStatus, sourceStatus } from "../lib/status";
import type {
  CatalogSource,
  Client,
  ClientStats,
  SendParams,
  ServerSource,
} from "../types";

// One source's send-timing controls + folded send-path graphs.
function SourceBufferCard({
  src,
  sp,
  clients,
  clockOffset,
  onSend,
  onReanchor,
}: {
  src: ServerSource;
  sp?: SendParams;
  clients: Client[];
  clockOffset: number;
  onSend: (id: string, patch: Record<string, unknown>) => void;
  onReanchor: (id: string) => void;
}) {
  const s = src.samples || [];
  const latest = s[s.length - 1];
  const sendRate = rateSeries(s, "packets_sent");
  const lastRate = sendRate.length ? sendRate[sendRate.length - 1].y : 0;
  const reanchors = latest ? latest.reanchors : 0;
  const lead = sp?.lead_ms ?? src.lead_ms;
  const redundancy = sp?.redundancy ?? src.redundancy ?? 1;
  const lastLead = sp?.last_lead_ms ?? src.last_lead_ms ?? 0;
  const unicast = sp?.unicast ?? src.unicast ?? false;
  return (
    <div className="bg-white rounded-xl p-4">
      <div className="flex items-center gap-2.5">
        <span className="font-semibold text-slate-800">{src.name}</span>
        <StatusBadge status={sourceStatus(src, clients)} />
        {src.remote && (
          <span className="text-[11px] font-medium text-slate-500 bg-slate-100 rounded px-1.5 py-0.5">
            remote
          </span>
        )}
        <span className="ml-auto text-xs text-slate-400">
          {src.sample_rate ? `${src.sample_rate} Hz · ${src.channels}ch` : ""}
        </span>
      </div>

      {!src.remote && (
        <div className="mt-3 bg-slate-50 rounded-lg p-3">
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

      <Fold label="Send-path graphs">
        <Readouts>
          <Stat k="Send rate" v={`${lastRate.toFixed(0)} pkt/s`} />
          <Stat k="Packets sent" v={latest ? latest.packets_sent.toLocaleString() : "—"} />
          <Stat k="Pending backlog" v={`${latest ? latest.pending_len : "—"} smp`} />
          <Stat k="Re-anchors" v={reanchors} tone={reanchors > 0 ? "warn" : ""} />
        </Readouts>
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
      </Fold>
    </div>
  );
}

// One client's source pick + delay slider + folded buffer graphs.
function ClientBufferCard({
  c,
  st,
  catalog,
  clockOffset,
  sourceSending,
  delay,
  delayMax,
  highlight,
  cardRef,
  onSource,
  onDelay,
}: {
  c: Client;
  st?: ClientStats;
  catalog: CatalogSource[];
  clockOffset: number;
  sourceSending?: boolean;
  delay: number;
  delayMax: number | undefined;
  highlight: boolean;
  cardRef: (el: HTMLDivElement | null) => void;
  onSource: (id: string, sourceId: string) => void;
  onDelay: (id: string, ms: number) => void;
}) {
  return (
    <div
      ref={cardRef}
      className={
        "bg-white rounded-xl p-4 " + (highlight ? "outline outline-2 outline-emerald-400" : "")
      }
    >
      <div className="flex items-center gap-2.5">
        <span className="font-semibold text-slate-800">{c.name}</span>
        <StatusBadge status={clientStatus(c, st, sourceSending)} />
        <span className="ml-auto text-xs font-mono text-slate-400">{c.ip}</span>
      </div>
      <div className="mt-3">
        <SourceSelect c={c} catalog={catalog} onSource={onSource} />
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
      <Fold label="Buffer graphs">
        <BufferStats st={st} clockOffset={clockOffset} />
      </Fold>
    </div>
  );
}

export function BuffersTab({
  sources,
  clients,
  catalog,
  statsById,
  sourceSending,
  sendParams,
  clockOffset,
  leadById,
  delayOf,
  jumpClientId,
  onConsumedJump,
  onSource,
  onDelay,
  onSend,
  onReanchor,
}: {
  sources: ServerSource[];
  clients: Client[];
  catalog: CatalogSource[];
  statsById: Record<string, ClientStats>;
  sourceSending: Record<string, boolean>;
  sendParams: Record<string, SendParams>;
  clockOffset: number;
  leadById: Record<string, number>;
  delayOf: (c: Client) => number;
  jumpClientId: string | null;
  onConsumedJump: () => void;
  onSource: (id: string, sourceId: string) => void;
  onDelay: (id: string, ms: number) => void;
  onSend: (id: string, patch: Record<string, unknown>) => void;
  onReanchor: (id: string) => void;
}) {
  const cardRefs = useRef<Record<string, HTMLDivElement | null>>({});
  useEffect(() => {
    if (!jumpClientId) return;
    const el = cardRefs.current[jumpClientId];
    if (el) el.scrollIntoView({ behavior: "smooth", block: "center" });
    const t = setTimeout(onConsumedJump, 2200);
    return () => clearTimeout(t);
  }, [jumpClientId, onConsumedJump]);

  const colCls = "overflow-y-auto thin-scroll pr-1 space-y-4 max-h-[calc(100vh-11rem)]";
  const headCls = "text-[11px] font-semibold uppercase tracking-wider text-slate-400 mb-2 px-1";
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <section className="min-w-0">
        <div className={headCls}>Sources · timing</div>
        <div className={colCls}>
          {sources.length === 0 && (
            <div className="text-slate-400 text-sm py-8 text-center">No sources.</div>
          )}
          {sources.map((src) => (
            <SourceBufferCard
              key={src.source_id}
              src={src}
              sp={sendParams[src.source_id]}
              clients={clients}
              clockOffset={clockOffset}
              onSend={onSend}
              onReanchor={onReanchor}
            />
          ))}
        </div>
      </section>
      <section className="min-w-0">
        <div className={headCls}>Clients · delay</div>
        <div className={colCls}>
          {clients.length === 0 && (
            <div className="text-slate-400 text-sm py-8 text-center">No clients.</div>
          )}
          {clients.map((c) => (
            <ClientBufferCard
              key={c.id}
              c={c}
              st={statsById[c.id]}
              catalog={catalog}
              clockOffset={clockOffset}
              sourceSending={sourceSending[c.selected_source_id]}
              delay={delayOf(c)}
              delayMax={leadById[c.selected_source_id]}
              highlight={c.id === jumpClientId}
              cardRef={(el) => {
                cardRefs.current[c.id] = el;
              }}
              onSource={onSource}
              onDelay={onDelay}
            />
          ))}
        </div>
      </section>
    </div>
  );
}
