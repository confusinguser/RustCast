import { useEffect, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { C } from "../theme";
import type { Marker, Point } from "../lib/series";

// Rolling window shown on every graph, in milliseconds.
const WINDOW_MS = 60000;

export interface Series {
  name: string;
  color: string;
  points: Point[];
}

// A thin uPlot wrapper. A rAF loop scrolls the x-window and rescales y over the
// visible range, and a draw hook adds the vertical event markers and trailing dot.
export function Graph({
  title,
  series,
  markers = [],
  clockOffset = 0,
  height = 96,
  unit = "",
  signed = false,
}: {
  title: string;
  series: Series[];
  markers?: Marker[];
  clockOffset?: number;
  height?: number;
  unit?: string;
  signed?: boolean;
}) {
  const holder = useRef<HTMLDivElement>(null);
  // Latest inputs, read by the rAF loop and draw hook without re-creating uPlot.
  const live = useRef({ series, markers, clockOffset, signed });
  live.current = { series, markers, clockOffset, signed };

  useEffect(() => {
    const el = holder.current;
    if (!el) return;
    const colors = series.map((s) => s.color);

    const fmt = (v: number) => {
      const a = Math.abs(v);
      return a >= 100 ? v.toFixed(0) : a >= 10 ? v.toFixed(1) : v.toFixed(2);
    };

    // Vertical markers + a filled dot on each series' latest point.
    const overlay: uPlot.Plugin = {
      hooks: {
        draw: (u) => {
          const ctx = u.ctx;
          const { left, top, width, height: bh } = u.bbox;
          ctx.save();
          for (const m of live.current.markers) {
            const x = Math.round(u.valToPos(m.x, "x", true));
            if (x < left || x > left + width) continue;
            ctx.strokeStyle = m.color;
            ctx.globalAlpha = 0.5;
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(x, top);
            ctx.lineTo(x, top + bh);
            ctx.stroke();
          }
          ctx.globalAlpha = 1;
          for (let i = 0; i < live.current.series.length; i++) {
            const pts = live.current.series[i].points;
            const last = pts.length ? pts[pts.length - 1] : null;
            if (!last) continue;
            const x = u.valToPos(last.x, "x", true);
            const y = u.valToPos(last.y, "y", true);
            if (x < left - 1 || x > left + width + 1) continue;
            ctx.fillStyle = colors[i];
            ctx.beginPath();
            ctx.arc(x, y, 2.4, 0, Math.PI * 2);
            ctx.fill();
          }
          ctx.restore();
        },
      },
    };

    const opts: uPlot.Options = {
      width: el.clientWidth || 600,
      height,
      padding: [8, 8, 0, 0],
      cursor: { show: false },
      legend: { show: false },
      scales: {
        x: { time: false, auto: false },
        y: { auto: false },
      },
      axes: [
        {
          stroke: C.axis,
          grid: { show: false },
          ticks: { show: false },
          font: "10px ui-monospace, monospace",
          size: 22,
          values: (u, splits) => {
            const now = u.scales.x.max ?? 0;
            return splits.map((t) => {
              const d = Math.round((now - t) / 1000);
              return d <= 0 ? "now" : `-${d}s`;
            });
          },
          splits: (u) => {
            const max = u.scales.x.max ?? 0;
            return [max - 60000, max - 30000, max];
          },
        },
        {
          stroke: C.axis,
          grid: { stroke: C.grid, width: 1 },
          ticks: { show: false },
          font: "10px ui-monospace, monospace",
          size: 40,
          values: (_u, splits) => splits.map(fmt),
          splits: (u) => {
            const { min, max } = u.scales.y;
            if (min == null || max == null) return [];
            return [min, (min + max) / 2, max];
          },
        },
      ],
      series: [
        {},
        ...series.map((s) => ({
          label: s.name,
          stroke: s.color,
          width: 1.6,
          points: { show: false },
          spanGaps: false,
        })),
      ],
      plugins: [overlay],
    };

    // Initial empty data with one column per configured series.
    const initData = [[], ...series.map(() => [])] as unknown as uPlot.AlignedData;
    const u = new uPlot(opts, initData, el);

    const ro = new ResizeObserver(() => u.setSize({ width: el.clientWidth || 600, height }));
    ro.observe(el);

    let raf = 0;
    const tick = () => {
      const { series: ser, clockOffset: off, signed: sgn } = live.current;

      // Merge every series' x values into one shared axis (they're generally
      // already aligned). Fill any gap with null so uPlot breaks the line.
      const xset = new Set<number>();
      for (const s of ser) for (const p of s.points) xset.add(p.x);
      const xs = Array.from(xset).sort((a, b) => a - b);
      const cols: (number | null)[][] = [xs];
      for (const s of ser) {
        const m = new Map(s.points.map((p) => [p.x, p.y]));
        cols.push(xs.map((x) => (m.has(x) ? (m.get(x) as number) : null)));
      }
      u.setData(cols as unknown as uPlot.AlignedData, false);

      const x1 = Date.now() + off;
      const x0 = x1 - WINDOW_MS;
      u.setScale("x", { min: x0, max: x1 });

      // Rescale y over just what's visible in the window.
      let lo = Infinity,
        hi = -Infinity;
      for (const s of ser)
        for (const p of s.points) {
          if (p.x < x0) continue;
          if (p.y < lo) lo = p.y;
          if (p.y > hi) hi = p.y;
        }
      if (hi < lo) {
        lo = 0;
        hi = 1;
      }
      let yMin: number, yMax: number;
      if (sgn) {
        yMax = Math.max(hi, 0);
        yMin = Math.min(lo, 0);
        const span = yMax - yMin || 1;
        yMax += span * 0.1;
        yMin -= span * 0.1;
      } else {
        yMin = 0;
        yMax = hi > 0 ? hi * 1.15 : 1;
      }
      u.setScale("y", { min: yMin, max: yMax });

      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);

    return () => {
      cancelAnimationFrame(raf);
      ro.disconnect();
      u.destroy();
    };
    // Re-create only if the geometry changes. Series colors and count are stable.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [height]);

  return (
    <div className="mt-2.5">
      <div className="flex flex-wrap gap-x-3 gap-y-1 items-baseline text-[11px] text-slate-500 mb-1">
        <span>
          {title}
          {unit ? ` (${unit})` : ""}
        </span>
        {series.map((s) => (
          <span className="inline-flex items-center gap-1.5" key={s.name}>
            <span className="w-2.5 h-[3px] rounded-sm inline-block" style={{ background: s.color }} />
            {s.name}
          </span>
        ))}
      </div>
      <div ref={holder} className="w-full rounded-md bg-slate-50 overflow-hidden" style={{ height }} />
    </div>
  );
}
