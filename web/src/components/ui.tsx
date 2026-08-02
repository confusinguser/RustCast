import { useEffect, useState, type ReactNode } from "react";
import { Icon } from "../icons";
import { selectCls } from "../theme";
import { tip } from "../tooltip";
import type { CatalogSource, Client, Status } from "../types";

// ---- readouts ------------------------------------------------------------

export function Stat({ k, v, tone }: { k: string; v: ReactNode; tone?: string }) {
  const color =
    tone === "bad" ? "text-red-500" : tone === "warn" ? "text-amber-500" : "text-slate-800";
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wide text-slate-400">{k}</span>
      <span className={"text-sm tabular-nums " + color}>{v}</span>
    </div>
  );
}

export function Readouts({ children }: { children: ReactNode }) {
  return <div className="flex flex-wrap gap-x-5 gap-y-2">{children}</div>;
}

// ---- status badge --------------------

const BADGE: Record<Status, { label: string; cls: string; dot: string }> = {
  playing: { label: "Playing", cls: "bg-emerald-50 text-emerald-700", dot: "bg-emerald-500" },
  "no-audio": { label: "No Audio", cls: "bg-amber-50 text-amber-700", dot: "bg-amber-500" },
  error: { label: "Error", cls: "bg-red-50 text-red-600", dot: "bg-red-500" },
  "no-listeners": { label: "No Listeners", cls: "bg-slate-100 text-slate-500", dot: "bg-slate-400" },
  offline: { label: "Offline", cls: "bg-slate-100 text-slate-400", dot: "bg-slate-300" },
};

export function StatusBadge({ status }: { status: Status }) {
  const b = BADGE[status];
  return (
    <span
      className={
        "inline-flex items-center gap-1.5 text-[11px] font-medium rounded-full px-2 py-0.5 " + b.cls
      }
    >
      <span className={"w-1.5 h-1.5 rounded-full " + b.dot} />
      {b.label}
    </span>
  );
}

// ---- confirmation dialog -------------------------------------------------

export function ConfirmDialog({
  title,
  message,
  confirmLabel = "Delete",
  onConfirm,
  onCancel,
}: {
  title: string;
  message: string;
  confirmLabel?: string;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onCancel();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onCancel]);
  return (
    <div
      className="fixed inset-0 z-[60] flex items-center justify-center p-4 bg-slate-900/40"
      onClick={onCancel}
    >
      <div className="bg-white rounded-2xl w-full max-w-sm p-5" onClick={(e) => e.stopPropagation()}>
        <h3 className="text-base font-semibold text-slate-800">{title}</h3>
        <p className="text-sm text-slate-500 mt-1.5">{message}</p>
        <div className="flex justify-end gap-2 mt-5">
          <button
            onClick={onCancel}
            className="text-sm font-medium text-slate-600 bg-slate-100 hover:bg-slate-200 rounded-lg px-3 py-1.5"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            className="text-sm font-medium text-white bg-red-500 hover:bg-red-600 rounded-lg px-3 py-1.5"
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

// ---- folding section -----------------------------------------------------

export function Fold({ label, children }: { label: string; children: ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="mt-3 border-t border-slate-100 pt-2.5">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 text-xs font-medium text-slate-500 hover:text-slate-700"
      >
        <Icon name="chevron" size={14} className={"transition-transform " + (open ? "rotate-180" : "")} />
        {label}
      </button>
      {open && <div className="mt-2.5">{children}</div>}
    </div>
  );
}

// ---- labeled slider row --------------------------------------------------

export function SliderRow({
  label,
  value,
  min = 0,
  max = 100,
  step = 1,
  disabled,
  muted,
  suffix = "",
  onChange,
  title,
}: {
  label: string;
  value: number;
  min?: number;
  max?: number;
  step?: number;
  disabled?: boolean;
  muted?: boolean;
  suffix?: string;
  onChange: (v: number) => void;
  title?: string;
}) {
  return (
    <div className="flex items-center gap-3 mt-2.5" {...tip(title)}>
      <span className="w-12 text-xs text-slate-500 shrink-0">{label}</span>
      <input
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        disabled={disabled}
        onChange={(e) => onChange(Number(e.target.value))}
        className={"flex-1 " + (muted ? "is-muted" : "")}
      />
      <span className="w-14 text-right text-sm tabular-nums text-slate-600">
        {value}
        {suffix}
      </span>
    </div>
  );
}

// ---- source dropdown (Clients + Buffers tabs) ----------------------------

export function SourceSelect({
  c,
  catalog,
  onSource,
}: {
  c: Client;
  catalog: CatalogSource[];
  onSource: (id: string, sourceId: string) => void;
}) {
  return (
    <select
      value={c.selected_source_id || ""}
      onChange={(e) => onSource(c.id, e.target.value)}
      className={`ml-auto mr-5 max-w-24 ${selectCls}`}
    >
      <option value="">Off</option>
      {catalog.map((s) => (
        <option key={s.source_id} value={s.source_id}>
          {s.name}
        </option>
      ))}
    </select>
  );
}

// ---- editable device name ------------------------------------------------

export function NameField({
  mac,
  name,
  onName,
}: {
  mac: string;
  name: string;
  onName: (id: string, name: string) => void;
}) {
  const [val, setVal] = useState(name);
  const [editing, setEditing] = useState(false);
  useEffect(() => {
    if (!editing) setVal(name);
  }, [name, editing]);
  return (
    <input
      className="bg-transparent border border-transparent rounded-md text-[15px] font-semibold text-slate-800 px-1.5 py-0.5 min-w-0 max-w-[220px] hover:border-slate-200 focus:border-emerald-400 focus:bg-white focus:outline-none"
      value={val}
      {...tip("Rename device")}
      onFocus={() => setEditing(true)}
      onChange={(e) => setVal(e.target.value)}
      onBlur={() => {
        setEditing(false);
        if (val !== name) onName(mac, val);
      }}
      onKeyDown={(e) => {
        if (e.key === "Enter") (e.target as HTMLInputElement).blur();
      }}
    />
  );
}
