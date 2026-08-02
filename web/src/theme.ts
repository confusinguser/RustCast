// Graph series colors, tuned for contrast on a white card background.
export const C = {
  jitter: "#3b82f6", // network jitter buffer
  output: "#10b981", // rodio/cpal output queue
  rate: "#8b5cf6", // sample throughput
  pending: "#0ea5e9", // server pending backlog
  send: "#d97706", // server send rate
  delay: "#0891b2", // estimated delay from source
  drop: "#ef4444", // overrun/late/lost events
  under: "#f59e0b", // underrun / re-anchor events
  grid: "#e2e8f0",
  axis: "#94a3b8",
} as const;

// Shared input/select classes. Borders live on form fields, not on cards.
export const selectCls =
  "flex-1 bg-white border border-slate-200 rounded-lg px-2.5 py-1.5 text-sm text-slate-700 " +
  "focus:outline-none focus:ring-2 focus:ring-emerald-400";
export const inputCls =
  "bg-white border border-slate-200 rounded-lg px-2.5 py-1.5 text-sm text-slate-700 " +
  "focus:outline-none focus:ring-2 focus:ring-emerald-400";
