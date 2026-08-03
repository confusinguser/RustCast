import { useEffect, useLayoutEffect, useRef, useState } from "react";
import type { CatalogSource, Client } from "../types";

// Per-output-channel routing. `channel_map[out] = srcChannel` (-1 = silence).
// Drag from a source node (right edges) or an output node (left edges): drop on an
// output to connect, on empty space to disconnect. The per-output dropdown does the same.
type Pt = { x: number; y: number };

export function ChannelRouting({
  c,
  catalog,
  onChannelMap,
}: {
  c: Client;
  catalog: CatalogSource[];
  onChannelMap: (id: string, map: number[]) => void;
}) {
  const src = catalog.find((s) => s.source_id === c.selected_source_id);
  const srcCh = src ? src.channels : 0;
  const outCh = c.output_channels || 0;

  const wrapRef = useRef<HTMLDivElement>(null);
  const srcRefs = useRef<Record<number, HTMLElement | null>>({});
  const outRefs = useRef<Record<number, HTMLElement | null>>({});
  const [srcPts, setSrcPts] = useState<Record<number, Pt>>({});
  const [outPts, setOutPts] = useState<Record<number, Pt>>({});
  const [drag, setDrag] = useState<{ srcCh: number; fromOut: number | null } | null>(null);
  const [cursor, setCursor] = useState<Pt | null>(null);
  const [overOut, setOverOut] = useState<number | null>(null);
  const cleanupRef = useRef<(() => void) | null>(null);

  // Effective source channel feeding output `o` (identity default).
  const eff = (o: number) =>
    c.channel_map && c.channel_map.length ? c.channel_map[o] ?? -1 : o < srcCh ? o : -1;

  // Apply one or more output-to-source overrides at once.
  const applyMap = (overrides: Record<number, number>) => {
    const m: number[] = [];
    for (let o = 0; o < outCh; o++) m.push(o in overrides ? overrides[o] : eff(o));
    onChannelMap(c.id, m);
  };

  const mappingKey = Array.from({ length: outCh }, (_, o) => eff(o)).join(",");
  useLayoutEffect(() => {
    const wrap = wrapRef.current;
    if (!wrap) return;
    const recompute = () => {
      const wb = wrap.getBoundingClientRect();
      const sp: Record<number, Pt> = {};
      const op: Record<number, Pt> = {};
      for (let s = 0; s < srcCh; s++) {
        const el = srcRefs.current[s];
        if (!el) continue;
        const r = el.getBoundingClientRect();
        sp[s] = { x: r.right - wb.left, y: r.top + r.height / 2 - wb.top };
      }
      for (let o = 0; o < outCh; o++) {
        const el = outRefs.current[o];
        if (!el) continue;
        const r = el.getBoundingClientRect();
        op[o] = { x: r.left - wb.left, y: r.top + r.height / 2 - wb.top };
      }
      setSrcPts(sp);
      setOutPts(op);
    };
    recompute();
    const ro = new ResizeObserver(recompute);
    ro.observe(wrap);
    window.addEventListener("resize", recompute);
    return () => {
      ro.disconnect();
      window.removeEventListener("resize", recompute);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mappingKey, srcCh, outCh]);

  // Drop any live drag listeners if the component unmounts mid-drag.
  useEffect(() => () => cleanupRef.current?.(), []);

  if (!srcCh || !outCh) return null;

  // Which output box (if any) is under the given client coordinates.
  const outputAt = (cx: number, cy: number): number | null => {
    for (let o = 0; o < outCh; o++) {
      const el = outRefs.current[o];
      if (!el) continue;
      const r = el.getBoundingClientRect();
      if (cx >= r.left && cx <= r.right && cy >= r.top && cy <= r.bottom) return o;
    }
    return null;
  };

  const beginDrag = (
    e: React.PointerEvent,
    srcChannel: number,
    fromOut: number | null,
  ) => {
    e.preventDefault();
    const setFromEvent = (cx: number, cy: number) => {
      const wb = wrapRef.current?.getBoundingClientRect();
      if (wb) setCursor({ x: cx - wb.left, y: cy - wb.top });
      setOverOut(outputAt(cx, cy));
    };
    const move = (ev: PointerEvent) => setFromEvent(ev.clientX, ev.clientY);
    const up = (ev: PointerEvent) => {
      cleanupRef.current?.();
      const hit = outputAt(ev.clientX, ev.clientY);
      if (hit != null) {
        const ov: Record<number, number> = { [hit]: srcChannel };
        if (fromOut != null && fromOut !== hit) ov[fromOut] = -1; // moved the wire
        applyMap(ov);
      } else if (fromOut != null) {
        applyMap({ [fromOut]: -1 }); // dropped in empty space, so disconnect
      }
      setDrag(null);
      setCursor(null);
      setOverOut(null);
    };
    const cleanup = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
      cleanupRef.current = null;
    };
    cleanupRef.current = cleanup;
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
    setDrag({ srcCh: srcChannel, fromOut });
    setFromEvent(e.clientX, e.clientY);
  };

  // Bezier between two wrapper-local points.
  const path = (a: Pt, b: Pt) =>
    `M ${a.x} ${a.y} C ${(a.x + b.x) / 2} ${a.y}, ${(a.x + b.x) / 2} ${b.y}, ${b.x} ${b.y}`;

  // Fixed connections (skip the one currently being dragged off its output).
  const links: { o: number; a: Pt; b: Pt }[] = [];
  for (let o = 0; o < outCh; o++) {
    if (drag && drag.fromOut === o) continue;
    const s = eff(o);
    if (s < 0) continue;
    const a = srcPts[s];
    const b = outPts[o];
    if (a && b) links.push({ o, a, b });
  }
  const floating = drag && cursor && srcPts[drag.srcCh] ? { a: srcPts[drag.srcCh], b: cursor } : null;

  const node =
    "absolute top-1/2 -translate-y-1/2 w-3 h-3 rounded-full border-2 border-white bg-emerald-500 " +
    "shadow-sm cursor-grab active:cursor-grabbing";

  return (
    <div className="mt-3 bg-slate-50 rounded-lg p-3">
      <div className="flex items-baseline justify-between mb-3">
        <div className="text-xs text-slate-500">
          Routing · {srcCh}→{outCh} ch
        </div>
        <div className="text-[11px] text-slate-400">drag between the dots to wire channels</div>
      </div>
      <div ref={wrapRef} className="relative grid grid-cols-2 gap-x-20 select-none">
        <svg className="absolute inset-0 w-full h-full pointer-events-none overflow-visible" aria-hidden="true">
          {links.map((l) => (
            <path key={l.o} d={path(l.a, l.b)} stroke="#10b981" strokeWidth={2} fill="none" opacity={0.75} />
          ))}
          {floating && (
            <path
              d={path(floating.a, floating.b)}
              stroke="#10b981"
              strokeWidth={2.5}
              fill="none"
              strokeDasharray="5 4"
            />
          )}
        </svg>

        {/* source channels */}
        <div className="flex flex-col gap-2 items-end">
          {Array.from({ length: srcCh }, (_, s) => (
            <div
              key={s}
              ref={(el) => {
                srcRefs.current[s] = el;
              }}
              onPointerDown={(e) => beginDrag(e, s, null)}
              style={{ touchAction: "none" }}
              className={
                "relative cursor-grab active:cursor-grabbing inline-flex items-center gap-1.5 " +
                "bg-white border rounded-lg pl-3 pr-4 py-1.5 text-sm text-slate-700 " +
                (drag?.srcCh === s ? "border-emerald-400" : "border-slate-200")
              }
            >
              Src {s}
              <span className={node} style={{ right: "-6px" }} />
            </div>
          ))}
        </div>

        {/* output channels (drop targets + dropdown) */}
        <div className="flex flex-col gap-2">
          {Array.from({ length: outCh }, (_, o) => {
            const s = eff(o);
            const active = overOut === o && drag;
            return (
              <div key={o} className="flex items-center gap-2">
                <div
                  ref={(el) => {
                    outRefs.current[o] = el;
                  }}
                  onPointerDown={(e) => {
                    if (eff(o) >= 0) beginDrag(e, eff(o), o);
                  }}
                  style={{ touchAction: "none" }}
                  className={
                    "relative flex-1 min-w-[120px] inline-flex items-center justify-between gap-2 rounded-lg " +
                    "pl-4 pr-3 py-1.5 text-sm border transition-colors " +
                    (s >= 0 ? "cursor-grab active:cursor-grabbing " : "") +
                    (active ? "border-emerald-400 bg-emerald-50" : "border-slate-200 bg-white")
                  }
                >
                  <span
                    className={node + (s < 0 ? " !bg-slate-300" : "")}
                    style={{ left: "-6px" }}
                  />
                  <span className="text-slate-500">Out {o}</span>
                  <span className={s >= 0 ? "text-slate-700 font-medium" : "text-slate-400"}>
                    {s >= 0 ? `Src ${s}` : "silence"}
                  </span>
                </div>
                <select
                  value={s}
                  onChange={(e) => applyMap({ [o]: Number(e.target.value) })}
                  className="bg-white border border-slate-200 rounded-lg px-2 py-1.5 text-xs text-slate-600 focus:outline-none focus:ring-2 focus:ring-emerald-400"
                >
                  <option value={-1}>silence</option>
                  {Array.from({ length: srcCh }, (_, s2) => (
                    <option key={s2} value={s2}>
                      Src {s2}
                    </option>
                  ))}
                </select>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
