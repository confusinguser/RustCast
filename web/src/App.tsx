import { useCallback, useEffect, useRef, useState } from "react";
import { SideNav, TABS } from "./components/SideNav";
import { ClientsTab } from "./components/ClientsTab";
import { BuffersTab } from "./components/BuffersTab";
import { SyncTab } from "./components/SyncTab";
import { DelayTab } from "./components/DelayTab";
import { SettingsTab } from "./components/SettingsTab";
import { ClientModal } from "./components/ClientModal";
import { SourceModal } from "./components/SourceModal";
import { TooltipHost } from "./tooltip";
import { lastCounterRate } from "./lib/series";
import type {
  CatalogSource,
  Client,
  ClientStats,
  Group,
  SendParams,
  ServerSource,
  TabId,
} from "./types";

// PUT a client setting immediately on every change (no debounce).
function putSetting(id: string, field: string, body: unknown) {
  fetch(`/api/clients/${id}/${field}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).catch(() => {});
}

interface Bucket<T> {
  meta: T;
  samples: any[];
}

export function App() {
  const [tab, setTab] = useState<TabId>("clients");
  const [clients, setClients] = useState<Client[]>([]);
  const [groups, setGroups] = useState<Group[]>([]);
  const [catalog, setCatalog] = useState<CatalogSource[]>([]);
  const [stats, setStats] = useState<{ server: ServerSource[]; clients: ClientStats[] } | null>(null);
  const [clockOffset, setClockOffset] = useState(0);
  const [volumes, setVolumes] = useState<Record<string, number>>({});
  const [delays, setDelays] = useState<Record<string, number>>({});
  const [premute, setPremute] = useState<Record<string, number>>({}); // present = muted
  const [sendParams, setSendParams] = useState<Record<string, SendParams>>({});
  const [error, setError] = useState<string | null>(null);
  const [modalId, setModalId] = useState<string | null>(null);
  const [modalSourceId, setModalSourceId] = useState<string | null>(null);
  const [jumpClientId, setJumpClientId] = useState<string | null>(null);

  // Subscribe to the live stats stream (SSE): one `snapshot` with full history,
  // then `delta` events with only new samples, appended into rolling buffers.
  const hist = useRef<{ server: Record<string, Bucket<any>>; clients: Record<string, Bucket<any>> }>({
    server: {},
    clients: {},
  });
  useEffect(() => {
    const es = new EventSource("/api/events");
    es.onmessage = (e) => {
      let msg: any;
      try {
        msg = JSON.parse(e.data);
      } catch {
        return;
      }
      const h = hist.current;
      if (msg.type === "snapshot") {
        h.server = {};
        h.clients = {};
      }
      const roll = (bucket: Record<string, Bucket<any>>, key: string, item: any) => {
        const e2 = (bucket[key] ||= { meta: item, samples: [] });
        e2.meta = item;
        if (item.samples && item.samples.length) e2.samples.push(...item.samples);
        if (e2.samples.length > 600) e2.samples.splice(0, e2.samples.length - 600);
      };
      for (const s of msg.server || []) roll(h.server, s.source_id, s);
      for (const c of msg.clients_hist || []) roll(h.clients, c.id, c);
      const liveSrc = new Set((msg.server || []).map((s: any) => s.source_id));
      for (const k of Object.keys(h.server)) if (!liveSrc.has(k)) delete h.server[k];
      const liveCli = new Set((msg.clients_hist || []).map((c: any) => c.id));
      for (const k of Object.keys(h.clients)) if (!liveCli.has(k)) delete h.clients[k];

      setClients(msg.clients || []);
      setGroups(msg.groups || []);
      setCatalog(msg.catalog || []);
      setClockOffset(msg.now_ms - Date.now());
      setStats({
        server: Object.values(h.server).map((x) => ({ ...x.meta, samples: x.samples })),
        clients: Object.values(h.clients).map((x) => ({ ...x.meta, samples: x.samples })),
      });
      setSendParams((prev) => {
        const next = { ...prev };
        for (const s of msg.server || []) {
          if (!s.remote && !(s.source_id in next)) {
            next[s.source_id] = {
              lead_ms: s.lead_ms,
              redundancy: s.redundancy,
              last_lead_ms: s.last_lead_ms,
              unicast: s.unicast,
            };
          }
        }
        return next;
      });
      setError(null);
    };
    es.onerror = () => setError("Stream interrupted; reconnecting…");
    return () => es.close();
  }, []);

  const onSource = (id: string, sourceId: string) =>
    putSetting(id, "source", { source_id: sourceId || null });
  // Board: wiring a client straight to a source also drops it out of any group.
  const onClientSource = (id: string, sourceId: string) => {
    putSetting(id, "source", { source_id: sourceId || null });
    putSetting(id, "group", { group_id: null });
  };
  const onClientGroup = (id: string, groupId: string | null) =>
    putSetting(id, "group", { group_id: groupId });
  const onGroupSource = (gid: string, sourceId: string) => {
    fetch(`/api/groups/${gid}/source`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source_id: sourceId || null }),
    }).catch(() => {});
  };
  const onGroupName = (gid: string, name: string) => {
    fetch(`/api/groups/${gid}/name`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: name || null }),
    }).catch(() => {});
  };
  const onCreateGroup = () => {
    fetch(`/api/groups`, { method: "POST" }).catch(() => {});
  };
  const onDeleteGroup = (gid: string) => {
    fetch(`/api/groups/${gid}`, { method: "DELETE" }).catch(() => {});
  };
  const doVolume = useCallback((id: string, v: number) => {
    setVolumes((prev) => ({ ...prev, [id]: v }));
    putSetting(id, "volume", { volume: v });
  }, []);
  // Dragging the slider is an explicit volume set: it clears any mute memory.
  const onVolume = (id: string, v: number) => {
    setPremute((p) => {
      if (!(id in p)) return p;
      const n = { ...p };
      delete n[id];
      return n;
    });
    doVolume(id, v);
  };
  // Toggle mute: remember the pre-mute volume and drop to 0, then restore on unmute.
  const onMute = (id: string, curVol: number) => {
    setPremute((p) => {
      if (id in p) {
        const r = p[id];
        const n = { ...p };
        delete n[id];
        doVolume(id, r);
        return n;
      }
      doVolume(id, 0);
      return { ...p, [id]: curVol };
    });
  };
  const onDelay = (id: string, ms: number) => {
    setDelays((prev) => ({ ...prev, [id]: ms }));
    putSetting(id, "delay", { delay_ms: ms });
  };
  const onName = (id: string, name: string) => putSetting(id, "name", { name: name || null });
  const onChannelMap = (id: string, map: number[]) => putSetting(id, "channelmap", { map });
  const onSend = (id: string, patch: Record<string, unknown>) => {
    setSendParams((prev) => ({ ...prev, [id]: { ...prev[id], ...patch } as SendParams }));
    fetch(`/api/sources/${id}/send`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    }).catch(() => {});
  };
  const onReanchor = (id: string) => {
    fetch(`/api/sources/${id}/reanchor`, { method: "POST" }).catch(() => {});
  };

  const statsById: Record<string, ClientStats> = {};
  if (stats) for (const cs of stats.clients) statsById[cs.id] = cs;
  const sources = stats ? stats.server || [] : [];
  const leadById: Record<string, number> = {};
  for (const s of catalog) leadById[s.source_id] = s.lead_ms;

  // Which sources are actively pushing packets. A client wired to a sending
  // source but not appending samples is in an error state, not merely paused.
  const sourceSending: Record<string, boolean> = {};
  for (const s of sources) sourceSending[s.source_id] = lastCounterRate(s.samples, "packets_sent") > 0;

  const isMuted = (id: string) => id in premute;
  // While muted the slider still shows the pre-mute percentage even though the
  // volume sent is 0. Dragging it unmutes and sets the new value.
  const volumeOf = (c: Client) => (c.id in premute ? premute[c.id] : volumes[c.id] ?? c.volume);
  const delayOf = (c: Client) => delays[c.id] ?? c.delay_ms;

  // Offline clients sink to the end (stable sort keeps the rest in order).
  const sortedClients = [...clients].sort(
    (a, b) => Number(!!b.connected) - Number(!!a.connected),
  );

  const jumpToBuffers = (id: string) => {
    setModalId(null);
    setTab("buffers");
    setJumpClientId(id);
  };
  const onConsumedJump = useCallback(() => setJumpClientId(null), []);

  const modalClient = modalId ? clients.find((c) => c.id === modalId) : null;
  const modalSource = modalSourceId ? sources.find((s) => s.source_id === modalSourceId) : null;
  const tabMeta = TABS.find((t) => t.id === tab)!;
  const summary =
    tab === "settings"
      ? "sources & server"
      : `${clients.length} client${clients.length === 1 ? "" : "s"} · ${catalog.length} source${catalog.length === 1 ? "" : "s"}`;

  return (
    <div className="min-h-screen bg-slate-100">
      <SideNav active={tab} onSelect={setTab} />
      <main className="ml-24 pr-6 py-8 pl-2">
        <div>
          <header className="flex items-baseline gap-3 mb-2 mt-2">
            <span className="absolute top-1 left-3 text-2xl font-extrabold font-mono text-slate-800">RustCast</span>
            <h1 className="text-xl font-semibold tracking-tight text-slate-800">{tabMeta.label}</h1>
          </header>
          {error && <div className="text-red-500 text-sm mb-4">{error}</div>}

          {tab === "clients" && (
            <ClientsTab
              clients={sortedClients}
              groups={groups}
              catalog={catalog}
              sources={sources}
              statsById={statsById}
              sourceSending={sourceSending}
              error={error}
              volumeOf={volumeOf}
              isMuted={isMuted}
              onVolume={onVolume}
              onMute={onMute}
              onName={onName}
              onClientSource={onClientSource}
              onClientGroup={onClientGroup}
              onGroupSource={onGroupSource}
              onGroupName={onGroupName}
              onCreateGroup={onCreateGroup}
              onDeleteGroup={onDeleteGroup}
              onOpenClient={setModalId}
              onOpenSource={setModalSourceId}
            />
          )}
          {tab === "buffers" && (
            <BuffersTab
              sources={sources}
              clients={sortedClients}
              catalog={catalog}
              statsById={statsById}
              sourceSending={sourceSending}
              sendParams={sendParams}
              clockOffset={clockOffset}
              leadById={leadById}
              delayOf={delayOf}
              jumpClientId={jumpClientId}
              onConsumedJump={onConsumedJump}
              onSource={onSource}
              onDelay={onDelay}
              onSend={onSend}
              onReanchor={onReanchor}
            />
          )}
          {tab === "sync" && (
            <SyncTab
              clients={sortedClients}
              statsById={statsById}
              sourceSending={sourceSending}
              clockOffset={clockOffset}
            />
          )}
          {tab === "delay" && <DelayTab clients={sortedClients} />}
          {tab === "settings" && <SettingsTab />}
        </div>
      </main>

      {modalClient && (
        <ClientModal
          c={modalClient}
          st={statsById[modalClient.id]}
          catalog={catalog}
          clockOffset={clockOffset}
          sourceSending={sourceSending[modalClient.selected_source_id]}
          delay={delayOf(modalClient)}
          delayMax={leadById[modalClient.selected_source_id]}
          onClose={() => setModalId(null)}
          onChannelMap={onChannelMap}
          onDelay={onDelay}
          onJumpToBuffers={jumpToBuffers}
        />
      )}

      {modalSource && (
        <SourceModal
          src={modalSource}
          clients={clients}
          sp={sendParams[modalSource.source_id]}
          clockOffset={clockOffset}
          onClose={() => setModalSourceId(null)}
          onSend={onSend}
          onReanchor={onReanchor}
        />
      )}
      <TooltipHost />
    </div>
  );
}
