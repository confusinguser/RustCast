import { StatusBadge } from "./ui";
import { SyncStats } from "./SyncStats";
import { clientStatus } from "../lib/status";
import type { Client, ClientStats } from "../types";

export function SyncTab({
  clients,
  statsById,
  sourceSending,
  clockOffset,
}: {
  clients: Client[];
  statsById: Record<string, ClientStats>;
  sourceSending: Record<string, boolean>;
  clockOffset: number;
}) {
  if (clients.length === 0) {
    return <div className="text-slate-400 text-center py-24">No clients connected yet.</div>;
  }
  return (
    <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
      {clients.map((c) => (
        <div key={c.id} className="bg-white rounded-xl p-4">
          <div className="flex items-center gap-2.5 mb-3">
            <span className="font-semibold text-slate-800">{c.name}</span>
            <StatusBadge status={clientStatus(c, statsById[c.id], sourceSending[c.selected_source_id])} />
            <span className="ml-auto text-xs font-mono text-slate-400">{c.ip}</span>
          </div>
          <SyncStats st={statsById[c.id]} clockOffset={clockOffset} />
        </div>
      ))}
    </div>
  );
}
