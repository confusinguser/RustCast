import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";
import { motion } from "framer-motion";
import { Icon } from "../icons";
import { ConfirmDialog, NameField, StatusBadge } from "./ui";
import { tip } from "../tooltip";
import { clientStatus, sourceStatus } from "../lib/status";
import type { CatalogSource, Client, ClientStats, Group, ServerSource, Status } from "../types";

// The Clients board has three wired columns: clients, groups, and sources. A
// client connects to a source directly or through a group, and a group connects
// to a source. You wire things up by dragging a line from one card onto another.

// Reorder animation: framer-motion `layout` on each card, eased in-and-out.
const LAYOUT_MS = 320;
const LAYOUT_TX = { layout: { duration: LAYOUT_MS / 1000, ease: "easeInOut" } } as const;

type Pt = { x: number; y: number };
// "client" drags to a group/source, "group" (right side) drags to a source,
// "groupIn" (left side) drags to a client to pull it into the group, "source"
// drags to a client/group (or to empty space, cutting all of its lines).
type DragKind = "client" | "group" | "groupIn" | "source";
type Drag = { kind: DragKind; id: string; originKey: string };

// A round connection node, sat on a card edge. `side` decides which edge.
function Node({
  side,
  reg,
  onPointerDown,
  muted,
}: {
  side: "left" | "right";
  reg: (el: HTMLElement | null) => void;
  onPointerDown?: (e: ReactPointerEvent) => void;
  muted?: boolean;
}) {
  const pos = side === "left" ? "left-0 -translate-x-1/2" : "right-0 translate-x-1/2";
  const clip = side === "left" ? "inset(0 0 0 50%)" : "inset(0 50% 0 0)";
  return (
    <span
      ref={reg}
      onPointerDown={onPointerDown}
      style={{
        clipPath: clip
      }}
      className={
        "absolute top-1/2 -translate-y-1/2 w-3 h-3 rounded-full border-2 border-white shadow-sm z-10 " +
        pos +
        (muted ? " bg-slate-300" : " bg-emerald-500") +
        (onPointerDown ? " cursor-grab active:cursor-grabbing" : "")
      }
    />
  );
}

// ---- purely-logical vertical ordering ------------------------------------
// Order each column from the connection graph alone (no measured geometry): a
// barycenter sweep pulls each card toward the average slot of what it links to,
// unconnected cards and empty groups sink to the bottom, ties keep prior order.
function computeOrder(
  clients: Client[],
  groups: Group[],
  sourceIds: string[],
): { clientOrder: string[]; groupOrder: string[]; sourceOrder: string[] } {
  const groupSet = new Set(groups.map((g) => g.id));
  const srcSet = new Set(sourceIds);

  const clientGroup = new Map<string, string>(); // client -> group
  const clientSource = new Map<string, string>(); // client -> source (direct)
  for (const c of clients) {
    if (c.group_id && groupSet.has(c.group_id)) clientGroup.set(c.id, c.group_id);
    else if (c.selected_source_id && srcSet.has(c.selected_source_id))
      clientSource.set(c.id, c.selected_source_id);
  }
  const groupSource = new Map<string, string>(); // group -> source
  const groupMembers = new Map<string, string[]>(); // group -> clients
  for (const g of groups) {
    groupMembers.set(g.id, []);
    if (g.source_id && srcSet.has(g.source_id)) groupSource.set(g.id, g.source_id);
  }
  for (const c of clients) {
    const gid = clientGroup.get(c.id);
    if (gid) groupMembers.get(gid)!.push(c.id);
  }

  let cOrder = clients.map((c) => c.id);
  let gOrder = groups.map((g) => g.id);
  let sOrder = sourceIds.slice();

  const SINK = 2; // below every normalised [0,1] slot, so it falls to the bottom
  const norm = (order: string[]) => {
    const m = new Map<string, number>();
    const n = order.length;
    order.forEach((id, i) => m.set(id, n <= 1 ? 0.5 : i / (n - 1)));
    return m;
  };
  const mean = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : SINK);
  // Stable re-sort by score: ties fall back to the prior index.
  const bump = (order: string[], score: (id: string) => number) =>
    order
      .map((id, i) => ({ id, i, s: score(id) }))
      .sort((a, b) => a.s - b.s || a.i - b.i)
      .map((x) => x.id);

  for (let iter = 0; iter < 8; iter++) {
    const cp = norm(cOrder);
    const sp = norm(sOrder);
    gOrder = bump(gOrder, (gid) =>
      mean([
        ...(groupMembers.get(gid) || []).map((m) => cp.get(m) ?? SINK),
        ...(groupSource.has(gid) ? [sp.get(groupSource.get(gid)!) ?? SINK] : []),
      ]),
    );
    const gp = norm(gOrder);
    cOrder = bump(cOrder, (cid) => {
      const gid = clientGroup.get(cid);
      if (gid) return gp.get(gid) ?? SINK;
      const s = clientSource.get(cid);
      if (s) return sp.get(s) ?? SINK;
      return SINK;
    });
    const cp2 = norm(cOrder);
    sOrder = bump(sOrder, (sid) => {
      const xs: number[] = [];
      for (const [cid, s] of clientSource) if (s === sid) xs.push(cp2.get(cid) ?? SINK);
      for (const [gid, s] of groupSource) if (s === sid) xs.push(gp.get(gid) ?? SINK);
      return mean(xs);
    });
  }
  return { clientOrder: cOrder, groupOrder: gOrder, sourceOrder: sOrder };
}

export function ClientsTab({
  clients,
  groups,
  catalog,
  sources,
  statsById,
  sourceSending,
  error,
  volumeOf,
  isMuted,
  onVolume,
  onMute,
  onName,
  onClientSource,
  onClientGroup,
  onGroupSource,
  onGroupName,
  onCreateGroup,
  onDeleteGroup,
  onOpenClient,
  onOpenSource,
}: {
  clients: Client[];
  groups: Group[];
  catalog: CatalogSource[];
  sources: ServerSource[];
  statsById: Record<string, ClientStats>;
  sourceSending: Record<string, boolean>;
  error: string | null;
  volumeOf: (c: Client) => number;
  isMuted: (id: string) => boolean;
  onVolume: (id: string, v: number) => void;
  onMute: (id: string, curVol: number) => void;
  onName: (id: string, name: string) => void;
  onClientSource: (id: string, sourceId: string) => void;
  onClientGroup: (id: string, groupId: string | null) => void;
  onGroupSource: (gid: string, sourceId: string) => void;
  onGroupName: (gid: string, name: string) => void;
  onCreateGroup: () => void;
  onDeleteGroup: (gid: string) => void;
  onOpenClient: (id: string) => void;
  onOpenSource: (id: string) => void;
}) {
  const boardRef = useRef<HTMLDivElement>(null);
  const nodeRefs = useRef<Record<string, HTMLElement | null>>({});
  const dropRefs = useRef<Record<string, HTMLElement | null>>({});
  const colRefs = useRef<(HTMLElement | null)[]>([]);

  const [pts, setPts] = useState<Record<string, Pt>>({});
  const [drag, setDrag] = useState<Drag | null>(null);
  const [cursor, setCursor] = useState<Pt | null>(null);
  const [overDrop, setOverDrop] = useState<string | null>(null);
  const [confirmDel, setConfirmDel] = useState<string | null>(null);

  const regNode = (key: string) => (el: HTMLElement | null) => {
    nodeRefs.current[key] = el;
  };
  const regDrop = (key: string) => (el: HTMLElement | null) => {
    dropRefs.current[key] = el;
  };

  const groupById = new Map(groups.map((g) => [g.id, g]));
  const clientById = new Map(clients.map((c) => [c.id, c]));
  const catById = new Map(catalog.map((s) => [s.source_id, s]));
  const sourceIds = new Set(catalog.map((s) => s.source_id));
  const membersOf = (gid: string) => clients.filter((c) => c.group_id === gid);

  // Logical column ordering (no geometry) + a signature that changes only when
  // the ordering actually changes, which is what drives the reorder animation.
  const { clientOrder, groupOrder, sourceOrder } = useMemo(
    () => computeOrder(clients, groups, catalog.map((s) => s.source_id)),
    [clients, groups, catalog],
  );
  const orderSig = clientOrder.join(",") + "|" + groupOrder.join(",") + "|" + sourceOrder.join(",");

  // Recompute all node anchor points (board-relative).
  const recompute = useCallback(() => {
    const board = boardRef.current;
    if (!board) return;
    const b = board.getBoundingClientRect();
    const next: Record<string, Pt> = {};
    for (const [key, el] of Object.entries(nodeRefs.current)) {
      if (!el) continue;
      const r = el.getBoundingClientRect();
      next[key] = { x: r.left + r.width / 2 - b.left, y: r.top + r.height / 2 - b.top };
    }
    setPts(next);
  }, []);

  useLayoutEffect(() => {
    recompute();
    const ro = new ResizeObserver(recompute);
    if (boardRef.current) ro.observe(boardRef.current);
    window.addEventListener("resize", recompute);
    const cols = colRefs.current.filter(Boolean) as HTMLElement[];
    cols.forEach((c) => c.addEventListener("scroll", recompute, { passive: true }));
    return () => {
      ro.disconnect();
      window.removeEventListener("resize", recompute);
      cols.forEach((c) => c.removeEventListener("scroll", recompute));
    };
  }, [recompute, clients, groups, catalog]);

  // Keep the wires glued to the cards while a reorder animation is running.
  const trackRef = useRef<number | null>(null);
  const trackWires = useCallback(
    (ms: number) => {
      const start = performance.now();
      const step = () => {
        recompute();
        trackRef.current = performance.now() - start < ms ? requestAnimationFrame(step) : null;
      };
      if (trackRef.current) cancelAnimationFrame(trackRef.current);
      trackRef.current = requestAnimationFrame(step);
    },
    [recompute],
  );

  // Cards slide to new slots via framer-motion `layout`, so track the wires along.
  useEffect(() => {
    trackWires(LAYOUT_MS + 40);
  }, [orderSig, trackWires]);

  // Which drop target (if any) the cursor is over, limited to what `kind` can
  // connect to: clients reach groups and sources, groups reach sources, and so on.
  const dropKeyAt = (ev: PointerEvent, kind: DragKind): string | null => {
    for (const [key, el] of Object.entries(dropRefs.current)) {
      if (!el) continue;
      const isSource = key.startsWith("source:");
      const isGroup = key.startsWith("group:");
      const isClient = key.startsWith("client:");
      if (kind === "group" && !isSource) continue;
      if (kind === "client" && !(isSource || isGroup)) continue;
      if (kind === "groupIn" && !isClient) continue;
      if (kind === "source" && !(isClient || isGroup)) continue;
      const r = el.getBoundingClientRect();
      if (ev.clientX >= r.left && ev.clientX <= r.right && ev.clientY >= r.top && ev.clientY <= r.bottom)
        return key;
    }
    return null;
  };

  const updateCursor = (ev: PointerEvent, kind: DragKind) => {
    const b = boardRef.current?.getBoundingClientRect();
    if (b) setCursor({ x: ev.clientX - b.left, y: ev.clientY - b.top });
    setOverDrop(dropKeyAt(ev, kind));
  };

  const finishDrop = (ev: PointerEvent, origin: Drag) => {
    const target = dropKeyAt(ev, origin.kind);
    const [type, id] = target ? target.split(":") : [null, null];
    if (origin.kind === "client") {
      if (type === "group") onClientGroup(origin.id, id);
      else if (type === "source") onClientSource(origin.id, id!);
      // Dropped anywhere else means disconnect. "" clears both source and group.
      else onClientSource(origin.id, "");
    } else if (origin.kind === "groupIn") {
      // Group's left side dragged onto a client, so that client joins the group.
      if (type === "client") onClientGroup(id!, origin.id);
    } else if (origin.kind === "source") {
      // Onto a client or group wires it up. Empty space cuts every line off this source.
      if (type === "client") onClientSource(id!, origin.id);
      else if (type === "group") onGroupSource(id!, origin.id);
      else {
        for (const c of clients)
          if (!c.group_id && c.selected_source_id === origin.id) onClientSource(c.id, "");
        for (const g of groups) if (g.source_id === origin.id) onGroupSource(g.id, "");
      }
    } else {
      if (type === "source") onGroupSource(origin.id, id!);
      else onGroupSource(origin.id, "");
    }
  };

  const endDrag = () => {
    setDrag(null);
    setCursor(null);
    setOverDrop(null);
  };

  // Client card: press and move to drag a wire, or just click to open the modal.
  const onClientPointerDown = (e: ReactPointerEvent, c: Client) => {
    if (e.button !== 0) return;
    e.preventDefault();
    const sx = e.clientX,
      sy = e.clientY;
    let dragging = false;
    const move = (ev: PointerEvent) => {
      if (!dragging) {
        if (Math.hypot(ev.clientX - sx, ev.clientY - sy) < 5) return;
        dragging = true;
        setDrag({ kind: "client", id: c.id, originKey: `c:${c.id}` });
      }
      updateCursor(ev, "client");
    };
    const up = (ev: PointerEvent) => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
      if (dragging) finishDrop(ev, { kind: "client", id: c.id, originKey: `c:${c.id}` });
      else onOpenClient(c.id);
      endDrag();
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  };

  // Group card: press and move to drag a wire. The right half is the outgoing
  // (to source) node, the left half the incoming node (drag onto a client to enlist).
  const onGroupCardPointerDown = (e: ReactPointerEvent, gid: string) => {
    if (e.button !== 0) return;
    e.preventDefault();
    const rect = e.currentTarget.getBoundingClientRect();
    const rightHalf = e.clientX >= rect.left + rect.width / 2;
    const kind: DragKind = rightHalf ? "group" : "groupIn";
    const originKey = rightHalf ? `go:${gid}` : `gi:${gid}`;
    const origin: Drag = { kind, id: gid, originKey };
    const sx = e.clientX,
      sy = e.clientY;
    let dragging = false;
    const move = (ev: PointerEvent) => {
      if (!dragging) {
        if (Math.hypot(ev.clientX - sx, ev.clientY - sy) < 5) return;
        dragging = true;
        setDrag(origin);
      }
      updateCursor(ev, kind);
    };
    const up = (ev: PointerEvent) => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
      if (dragging) finishDrop(ev, origin);
      endDrag();
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  };

  // Source card: press and move to drag a wire out (to a client or group, or to
  // empty space to disconnect). A plain click opens the source modal.
  const onSourcePointerDown = (e: ReactPointerEvent, sid: string) => {
    if (e.button !== 0) return;
    e.preventDefault();
    const origin: Drag = { kind: "source", id: sid, originKey: `s:${sid}` };
    const sx = e.clientX,
      sy = e.clientY;
    let dragging = false;
    const move = (ev: PointerEvent) => {
      if (!dragging) {
        if (Math.hypot(ev.clientX - sx, ev.clientY - sy) < 5) return;
        dragging = true;
        setDrag(origin);
      }
      updateCursor(ev, "source");
    };
    const up = (ev: PointerEvent) => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
      if (dragging) finishDrop(ev, origin);
      else onOpenSource(sid);
      endDrag();
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  };

  // Static wires from client to group or source, and from group to source. Skip
  // the one being dragged, since its floating line is shown instead.
  const path = (a: Pt, b: Pt) =>
    `M ${a.x} ${a.y} C ${(a.x + b.x) / 2} ${a.y}, ${(a.x + b.x) / 2} ${b.y}, ${b.x} ${b.y}`;
  const wires: { key: string; a: Pt; b: Pt }[] = [];
  for (const c of clients) {
    if (drag?.kind === "client" && drag.id === c.id) continue;
    const a = pts[`c:${c.id}`];
    if (!a) continue;
    if (c.group_id && groupById.has(c.group_id)) {
      const b = pts[`gi:${c.group_id}`];
      if (b) wires.push({ key: `c:${c.id}`, a, b });
    } else if (c.selected_source_id && sourceIds.has(c.selected_source_id)) {
      const b = pts[`s:${c.selected_source_id}`];
      if (b) wires.push({ key: `c:${c.id}`, a, b });
    }
  }
  for (const g of groups) {
    if (drag?.kind === "group" && drag.id === g.id) continue;
    if (g.source_id && sourceIds.has(g.source_id)) {
      const a = pts[`go:${g.id}`];
      const b = pts[`s:${g.source_id}`];
      if (a && b) wires.push({ key: `go:${g.id}`, a, b });
    }
  }
  const floating = drag && cursor && pts[drag.originKey] ? { a: pts[drag.originKey], b: cursor } : null;

  // Hide "new group" while an unused (no members, no source) group exists.
  const hasEmptyGroup = groups.some((g) => !g.source_id && membersOf(g.id).length === 0);

  const colWrap = "flex flex-col min-w-0";
  const colHead = "text-[11px] font-semibold uppercase tracking-wider text-slate-400 mb-2 px-2";
  // Pad the scroll box so node overhang and the 2px outline aren't clipped.
  const colScroll = "overflow-y-auto thin-scroll px-2 py-1.5 space-y-3 max-h-[calc(100vh-11rem)]";
  // Always-present 2px outline whose colour transitions in, for a true fade.
  const cardTx = "outline outline-2 transition-[outline-color,background-color] duration-150";

  return (
    <div ref={boardRef} className="relative select-none">
      {/* connector overlay */}
      <svg className="absolute inset-0 w-full h-full pointer-events-none overflow-visible z-20" aria-hidden="true">
        {wires.map((w) => (
          <path key={w.key} d={path(w.a, w.b)} stroke="#10b981" strokeWidth={2} fill="none" opacity={0.75} />
        ))}
        {floating && (
          <path d={path(floating.a, floating.b)} stroke="#10b981" strokeWidth={2.5} fill="none" strokeDasharray="5 4" />
        )}
      </svg>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-16">
        {/* ---- Clients ---- */}
        <div className={colWrap}>
          <div className={colHead}>Clients</div>
          <div ref={(el) => (colRefs.current[0] = el)} className={colScroll}>
            {clients.length === 0 && !error && (
              <div className="text-slate-400 text-sm py-10 text-center">No clients yet.</div>
            )}
            {clientOrder.map((cid) => {
              const c = clientById.get(cid);
              if (!c) return null;
              const status = clientStatus(c, statsById[c.id], sourceSending[c.selected_source_id]);
              const muted = isMuted(c.id);
              const vol = volumeOf(c);
              const pct = Math.round(vol * 100);
              const offline = status === "offline";
              const dragging = drag?.kind === "client" && drag.id === c.id;
              const isOver =
                overDrop === `client:${c.id}` &&
                (drag?.kind === "groupIn" || drag?.kind === "source");
              return (
                <motion.div
                  key={c.id}
                  layout="position"
                  transition={LAYOUT_TX}
                  ref={regDrop(`client:${c.id}`)}
                  onPointerDown={(e) => onClientPointerDown(e, c)}
                  className={
                    "relative rounded-[13.5px] p-4 cursor-pointer " +
                    cardTx +
                    (offline ? " bg-slate-50 opacity-75" : " bg-white") +
                    (dragging || isOver
                      ? " outline-emerald-400"
                      : " outline-transparent hover:outline-emerald-400")
                  }
                >
                  <div className="flex items-center gap-2.5">
                    <div onPointerDown={(e) => e.stopPropagation()}>
                      <NameField mac={c.id} name={c.name} onName={onName} />
                    </div>
                  </div>
                  <div className="flex items-center gap-2 mt-1.5 pl-1">
                    <StatusBadge status={status} />
                    <span className="font-mono text-xs text-slate-400">{c.ip}</span>
                  </div>
                  <div className="flex items-center gap-2.5 mt-3">
                    <button
                      onClick={() => onMute(c.id, vol)}
                      onPointerDown={(e) => e.stopPropagation()}
                      {...tip(muted ? "Unmute" : "Mute")}
                      className={
                        "rounded-lg p-1.5 transition-colors " +
                        (muted
                          ? "text-slate-400 bg-slate-100"
                          : "text-slate-500 hover:text-slate-800 hover:bg-slate-100")
                      }
                    >
                      <Icon name={muted ? "mute" : "volume"} size={18} />
                    </button>
                    <input
                      type="range"
                      min="0"
                      max="100"
                      value={pct}
                      onPointerDown={(e) => e.stopPropagation()}
                      onChange={(e) => onVolume(c.id, Number(e.target.value) / 100)}
                      className={"flex-1 " + (muted ? "is-muted" : "")}
                    />
                    <span className="w-10 text-right text-sm tabular-nums text-slate-600">{pct}%</span>
                  </div>
                  <Node side="right" reg={regNode(`c:${c.id}`)} />
                </motion.div>
              );
            })}
          </div>
        </div>

        {/* ---- Groups ---- */}
        <div className={colWrap}>
          <div className={colHead}>Groups</div>
          <div ref={(el) => (colRefs.current[1] = el)} className={colScroll}>
            {groupOrder.map((gid) => {
              const g = groupById.get(gid);
              if (!g) return null;
              const members = membersOf(g.id);
              const isOver = overDrop === `group:${g.id}`;
              const dragging = drag?.kind === "group" && drag.id === g.id;
              return (
                <GroupCard
                  key={g.id}
                  g={g}
                  members={members}
                  regDrop={regDrop(`group:${g.id}`)}
                  regIn={regNode(`gi:${g.id}`)}
                  regOut={regNode(`go:${g.id}`)}
                  onCardPointerDown={(e) => onGroupCardPointerDown(e, g.id)}
                  onRename={(name) => onGroupName(g.id, name)}
                  onDelete={() => setConfirmDel(g.id)}
                  highlight={isOver}
                  dragging={dragging}
                />
              );
            })}
            {!hasEmptyGroup && (
              <motion.button
                layout="position"
                transition={LAYOUT_TX}
                onClick={onCreateGroup}
                className="w-full flex items-center justify-center gap-2 rounded-xl border-2 border-dashed border-slate-300 text-slate-400 hover:border-emerald-400 hover:text-emerald-600 py-4 transition-colors"
              >
                <Icon name="plus" size={18} /> New group
              </motion.button>
            )}
          </div>
        </div>

        {/* ---- Sources ---- */}
        <div className={colWrap}>
          <div className={colHead}>Sources</div>
          <div ref={(el) => (colRefs.current[2] = el)} className={colScroll}>
            {catalog.length === 0 && (
              <div className="text-slate-400 text-sm py-10 text-center">No sources.</div>
            )}
            {sourceOrder.map((sid) => {
              const cat = catById.get(sid);
              if (!cat) return null;
              const server = sources.find((s) => s.source_id === cat.source_id);
              const status: Status = server ? sourceStatus(server, clients) : "no-listeners";
              const isOver = overDrop === `source:${cat.source_id}`;
              return (
                <motion.div
                  key={cat.source_id}
                  layout="position"
                  transition={LAYOUT_TX}
                  ref={regDrop(`source:${cat.source_id}`)}
                  onPointerDown={(e) => onSourcePointerDown(e, cat.source_id)}
                  className={
                    "relative rounded-xl p-4 cursor-pointer " +
                    cardTx +
                    (isOver && drag
                      ? " bg-emerald-50 outline-emerald-400"
                      : " bg-white outline-transparent hover:outline-emerald-400")
                  }
                >
                  <div className="flex items-center gap-2.5">
                    <span className="font-semibold text-slate-800 truncate">{cat.name}</span>
                    <StatusBadge status={status} />
                  </div>
                  <div className="flex items-center gap-2 mt-1.5 text-xs text-slate-400">
                    <span className="uppercase tracking-wide">{cat.source_type}</span>
                    <span>·</span>
                    <span>
                      {cat.sample_rate ? `${cat.sample_rate} Hz · ${cat.channels}ch · ${cat.format}` : ""}
                    </span>
                  </div>
                  <Node side="left" reg={regNode(`s:${cat.source_id}`)} />
                </motion.div>
              );
            })}
          </div>
        </div>
      </div>

      {confirmDel && (
        <ConfirmDialog
          title="Delete group?"
          message="Members will be ungrouped. This can't be undone."
          onConfirm={() => {
            onDeleteGroup(confirmDel);
            setConfirmDel(null);
          }}
          onCancel={() => setConfirmDel(null)}
        />
      )}
    </div>
  );
}

// One group card. Rename/delete controls fade in on hover. Pressing and moving
// on the card drags a wire. Its right half acts as the outgoing (to source)
// node, its left half the incoming node (drop onto a client to enlist it).
function GroupCard({
  g,
  regDrop,
  regIn,
  regOut,
  onCardPointerDown,
  onRename,
  onDelete,
  highlight,
  dragging,
}: {
  g: Group;
  members: Client[];
  regDrop: (el: HTMLElement | null) => void;
  regIn: (el: HTMLElement | null) => void;
  regOut: (el: HTMLElement | null) => void;
  onCardPointerDown: (e: ReactPointerEvent) => void;
  onRename: (name: string) => void;
  onDelete: () => void;
  highlight: boolean;
  dragging: boolean;
}) {
  const [editing, setEditing] = useState(false);
  const [val, setVal] = useState(g.name || "");

  const commit = () => {
    setEditing(false);
    if (val !== (g.name || "")) onRename(val);
  };

  return (
    <motion.div
      layout="position"
      transition={LAYOUT_TX}
      ref={regDrop}
      onPointerDown={onCardPointerDown}
      className={
        "overflow-hidden group relative rounded-xl p-4 cursor-grab active:cursor-grabbing " +
        "outline outline-2 transition-[outline-color,background-color] duration-150 " +
        (highlight && !dragging
          ? "bg-emerald-50 outline-emerald-400"
          : dragging
          ? "bg-white outline-emerald-400"
          : "bg-white outline-transparent")
      }
    >
      {/* hover controls */}
      <div className="absolute top-2 right-2 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
        <button
          onClick={() => {
            setVal(g.name || "");
            setEditing(true);
          }}
          onPointerDown={(e) => e.stopPropagation()}
          {...tip("Rename group")}
          className="text-slate-400 hover:text-slate-700 rounded-md p-1 hover:bg-slate-100 bg-white/70"
        >
          <Icon name="pencil" size={15} />
        </button>
        <button
          onClick={onDelete}
          onPointerDown={(e) => e.stopPropagation()}
          {...tip("Delete group")}
          className="text-slate-400 hover:text-red-500 rounded-md p-1 hover:bg-red-50 bg-white/70"
        >
          <Icon name="trash" size={15} />
        </button>
      </div>

      <div className="flex items-center gap-2">
        {editing ? (
          <input
            autoFocus
            value={val}
            onChange={(e) => setVal(e.target.value)}
            onBlur={commit}
            onKeyDown={(e) => {
              if (e.key === "Enter") (e.target as HTMLInputElement).blur();
              if (e.key === "Escape") setEditing(false);
            }}
            onPointerDown={(e) => e.stopPropagation()}
            className="min-w-0 flex-1 bg-white border border-emerald-400 rounded-md text-[15px] font-semibold text-slate-800 px-1.5 py-0.5 focus:outline-none"
            placeholder="Group name"
          />
        ) : (
          <span className={"ml-auto mr-auto font-semibold truncate " + (g.name ? "text-slate-800" : "text-slate-400")}>
            {g.name || "Unnamed group"}
          </span>
        )}
      </div>

      <Node side="left" reg={regIn} />
      <Node side="right" reg={regOut} muted={!g.source_id} />
    </motion.div>
  );
}
