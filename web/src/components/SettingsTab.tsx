import { useCallback, useEffect, useState, type ReactNode } from "react";
import { Icon } from "../icons";
import { inputCls } from "../theme";
import { apiUrl } from "../lib/api";

type SourceDraft = Record<string, any>;

const NEW_SOURCE: Record<string, SourceDraft> = {
  pipe: { type: "pipe", name: "", path: "", format: "s16", channels: 2, sample_rate: 44100 },
  spotify: { type: "spotify", name: "", device_name: "RustCast", format: "f32" },
  sink: { type: "sink", name: "", device_name: "RustCast", format: "s16", channels: 2, sample_rate: 44100 },
  mic: { type: "mic", name: "", device: "", format: "s16", channels: 2, sample_rate: 44100 },
};

function Field({ label, children }: { label: string; children: ReactNode }) {
  return <label className="flex flex-col gap-1 text-[11px] text-slate-500">{label}{children}</label>;
}

function SourceFields({ s, on }: { s: SourceDraft; on: (key: string, val: any) => void }) {
  const fmt = (
    <Field label="fmt">
      <select value={s.format ?? "s16"} onChange={(e) => on("format", e.target.value)} className={inputCls + " w-24"}>
        <option value="s16">s16</option>
        <option value="f32">f32</option>
      </select>
    </Field>
  );
  const nums = (
    <>
      <Field label="ch">
        <input type="number" min="1" value={s.channels ?? 2} onChange={(e) => on("channels", Number(e.target.value))} className={inputCls + " w-20"} />
      </Field>
      <Field label="rate">
        <input type="number" value={s.sample_rate ?? 44100} onChange={(e) => on("sample_rate", Number(e.target.value))} className={inputCls + " w-24"} />
      </Field>
    </>
  );
  if (s.type === "pipe")
    return (
      <>
        <Field label="path"><input value={s.path ?? ""} onChange={(e) => on("path", e.target.value)} className={inputCls + " w-32"} /></Field>
        {fmt}
        {nums}
      </>
    );
  if (s.type === "spotify")
    return (
      <>
        <Field label="device"><input value={s.device_name ?? ""} onChange={(e) => on("device_name", e.target.value)} className={inputCls + " w-32"} /></Field>
        {fmt}
      </>
    );
  if (s.type === "sink")
    return (
      <>
        <Field label="device"><input value={s.device_name ?? ""} onChange={(e) => on("device_name", e.target.value)} className={inputCls + " w-32"} /></Field>
        {fmt}
        {nums}
      </>
    );
  if (s.type === "mic")
    return (
      <>
        <Field label="device"><input value={s.device ?? ""} onChange={(e) => on("device", e.target.value)} className={inputCls + " w-32"} /></Field>
        {fmt}
        {nums}
      </>
    );
  return null;
}

function TypeSelect({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  return (
    <Field label="type">
      <select value={value} onChange={(e) => onChange(e.target.value)} className={inputCls + " w-24"}>
        <option value="pipe">pipe</option>
        <option value="spotify">spotify</option>
        <option value="sink">sink</option>
        <option value="mic">mic</option>
      </select>
    </Field>
  );
}

function SourcesConfig() {
  const [sources, setSources] = useState<SourceDraft[] | null>(null);
  const [addType, setAddType] = useState("pipe");
  const [draft, setDraft] = useState<SourceDraft>(NEW_SOURCE.pipe);
  const [msg, setMsg] = useState("");

  const load = useCallback(() => {
    fetch(apiUrl("api/config"))
      .then((r) => r.json())
      .then((c) => setSources(c.sources || []))
      .catch(() => setSources([]));
  }, []);
  useEffect(() => {
    load();
  }, [load]);

  const setField = (idx: number, key: string, val: any) => {
    setSources((prev) =>
      (prev || []).map((s, i) => {
        if (i !== idx) return s;
        if (key === "type") return { ...NEW_SOURCE[val], name: s.name, id: s.id };
        return { ...s, [key]: val };
      }),
    );
  };
  const save = (s: SourceDraft) => {
    const { id, ...body } = s;
    fetch(apiUrl(`api/sources/${id}`), {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
      .then((r) => (r.ok ? (setMsg(`saved '${s.name}'`), load()) : r.text().then((t) => setMsg(t))))
      .catch(() => setMsg("network error"));
  };
  const del = (s: SourceDraft) => {
    fetch(apiUrl(`api/sources/${s.id}`), { method: "DELETE" })
      .then(() => load())
      .catch(() => {});
  };
  const setDraftField = (key: string, val: any) => {
    if (key === "type") {
      setAddType(val);
      setDraft({ ...NEW_SOURCE[val], name: draft.name });
    } else setDraft((d) => ({ ...d, [key]: val }));
  };
  const add = () => {
    fetch(apiUrl("api/sources"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(draft),
    })
      .then((r) =>
        r.ok ? (setMsg(`added '${draft.name}'`), setDraft(NEW_SOURCE[addType]), load()) : r.text().then((t) => setMsg(t)),
      )
      .catch(() => setMsg("network error"));
  };

  if (sources === null) return null;
  const btn = "text-sm rounded-lg px-3 py-1.5 border";
  return (
    <section>
      <div className="flex items-baseline gap-2 mb-3">
        <h2 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">Sources</h2>
        {msg && <span className="text-xs text-slate-400">· {msg}</span>}
      </div>
      <div className="space-y-3">
        {sources.map((s, idx) => (
          <div className="bg-white rounded-xl p-4 flex flex-wrap items-end gap-3" key={s.id}>
            <Field label="name"><input value={s.name ?? ""} onChange={(e) => setField(idx, "name", e.target.value)} className={inputCls + " w-32"} /></Field>
            <TypeSelect value={s.type} onChange={(v) => setField(idx, "type", v)} />
            <SourceFields s={s} on={(k, v) => setField(idx, k, v)} />
            <div className="flex gap-2 ml-auto">
              <button className={btn + " text-emerald-700 bg-emerald-50 border-emerald-200 hover:bg-emerald-100"} onClick={() => save(s)}>
                Save
              </button>
              <button className={btn + " text-red-600 bg-red-50 border-red-200 hover:bg-red-100 inline-flex items-center gap-1"} onClick={() => del(s)}>
                <Icon name="trash" size={14} /> Delete
              </button>
            </div>
          </div>
        ))}
        <div className="bg-white rounded-xl p-4 flex flex-wrap items-end gap-3">
          <span className="text-sm font-medium text-slate-500 self-center inline-flex items-center gap-1">
            <Icon name="plus" size={15} /> Add
          </span>
          <Field label="name"><input value={draft.name} onChange={(e) => setDraftField("name", e.target.value)} className={inputCls + " w-32"} /></Field>
          <TypeSelect value={addType} onChange={(v) => setDraftField("type", v)} />
          <SourceFields s={draft} on={setDraftField} />
          <button
            className={btn + " ml-auto text-emerald-700 bg-emerald-50 border-emerald-200 hover:bg-emerald-100 disabled:opacity-40"}
            onClick={add}
            disabled={!draft.name}
          >
            Add source
          </button>
        </div>
      </div>
    </section>
  );
}

// General server settings: currently just the in-process playback client.
function GeneralSettings() {
  const [enabled, setEnabled] = useState(false);
  const [name, setName] = useState("");
  const [msg, setMsg] = useState("");

  const load = useCallback(() => {
    fetch(apiUrl("api/config"))
      .then((r) => r.json())
      .then((c) => {
        const lc = c.local_client;
        setEnabled(!!lc);
        setName((lc && lc.name) || "");
      })
      .catch(() => {});
  }, []);
  useEffect(() => {
    load();
  }, [load]);

  const save = (nextEnabled: boolean, nextName: string) => {
    fetch(apiUrl("api/config/local_client"), {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: nextEnabled, name: nextName || null }),
    })
      .then((r) =>
        r.ok
          ? setMsg(nextEnabled ? "local client enabled" : "saved · disable/rename applies on restart")
          : setMsg("error"),
      )
      .catch(() => setMsg("network error"));
  };

  return (
    <section>
      <h2 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400 mb-3">Server</h2>
      <div className="bg-white rounded-xl p-4">
        <label className="flex items-center gap-3 cursor-pointer">
          <input
            type="checkbox"
            checked={enabled}
            className="accent-emerald-500 w-4 h-4"
            onChange={(e) => {
              setEnabled(e.target.checked);
              save(e.target.checked, name);
            }}
          />
          <div>
            <div className="text-sm font-medium text-slate-800">Server-side playback client</div>
            <div className="text-xs text-slate-400">
              Play a source on the server machine itself; it shows up as a normal client.
            </div>
          </div>
        </label>
        <div className={"flex items-center gap-3 mt-3 " + (enabled ? "" : "opacity-40 pointer-events-none")}>
          <span className="w-16 text-xs text-slate-500">Name</span>
          <input
            value={name}
            placeholder="hostname"
            onChange={(e) => setName(e.target.value)}
            onBlur={() => save(enabled, name)}
            onKeyDown={(e) => {
              if (e.key === "Enter") (e.target as HTMLInputElement).blur();
            }}
            className={inputCls + " flex-1"}
          />
        </div>
        {msg && <div className="text-xs text-slate-400 mt-3">{msg}</div>}
      </div>
    </section>
  );
}

export function SettingsTab() {
  return (
    <div className="max-w-4xl space-y-8">
      <SourcesConfig />
      <GeneralSettings />
    </div>
  );
}
