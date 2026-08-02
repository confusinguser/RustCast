import { Icon, type IconName } from "../icons";
import { tip } from "../tooltip";
import type { TabId } from "../types";

export const TABS: { id: TabId; icon: IconName; label: string }[] = [
  { id: "clients", icon: "speaker", label: "Clients" },
  { id: "buffers", icon: "graph", label: "Buffers" },
  { id: "sync", icon: "clock", label: "Sync" },
  { id: "settings", icon: "gear", label: "Settings" },
];

export function SideNav({ active, onSelect }: { active: TabId; onSelect: (id: TabId) => void }) {
  return (
    <nav className="fixed left-5 top-1/2 -translate-y-1/2 z-40">
      {/* Solid gray pill (no gradient) that keeps its floating shadow. */}
      <div className="flex flex-col gap-3 bg-slate-200 rounded-full p-2 shadow-md">
        {TABS.map((t) => {
          const sel = t.id === active;
          return (
            <button
              key={t.id}
              {...tip(t.label)}
              onClick={() => onSelect(t.id)}
              className={
                "w-11 h-11 rounded-full flex items-center justify-center transition-colors " +
                // Selected circle: green with a small shadow. Others: plain white.
                (sel ? "bg-emerald-500 text-white shadow" : "bg-white text-slate-500 hover:text-slate-800")
              }
            >
              <Icon name={t.icon} size={20} />
            </button>
          );
        })}
      </div>
    </nav>
  );
}
