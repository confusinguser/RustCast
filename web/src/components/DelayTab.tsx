import { useEffect, useRef, useState } from "react";
import { Icon } from "../icons";
import type { Client, DelayRunResponse, DelayTestResult } from "../types";

// Measures real acoustic speaker delay end-to-end: the server plays a short,
// distinct-frequency tone burst on each selected speaker (all sharing one
// `play_at` timeline), we record the room with the browser mic, and the server
// recovers each speaker's delay. See src/delaytest.rs + the /api/delaytest/* and
// /api/time endpoints.
//
// NOTE: the browser mic requires a *secure context*. It works on http://localhost
// but browsers block it on http://<lan-ip>. If you reach the UI over the LAN,
// open it via localhost (e.g. an SSH port-forward) or launch the browser with
// --unsafely-treat-insecure-origin-as-secure for this origin.

type Phase = "idle" | "syncing" | "recording" | "analyzing" | "done" | "error";

const CLOCK_PINGS = 9;
const RECORD_TAIL_MS = 600; // keep recording past the last burst before stopping

/** NTP-style clock sync: ping /api/time a few times, keep the lowest-RTT sample.
 *  Returns the offset such that `serverMs ≈ Date.now() + offset`, plus that RTT. */
async function syncClock(): Promise<{ offset: number; rtt: number }> {
  let best = { offset: 0, rtt: Number.POSITIVE_INFINITY };
  for (let i = 0; i < CLOCK_PINGS; i++) {
    const t0 = Date.now();
    const r = await fetch("/api/time");
    const t1 = Date.now();
    const { server_ms } = await r.json();
    const rtt = t1 - t0;
    // Assume the server read its clock at the midpoint of the round trip.
    const offset = server_ms - (t0 + t1) / 2;
    if (rtt < best.rtt) best = { offset, rtt };
  }
  return best;
}

/** Captures mono mic audio into one growing buffer, tagging the first sample with
 *  a server-clock time so onsets can be placed on the shared timeline. */
class MicRecorder {
  private ctx: AudioContext;
  private stream: MediaStream;
  private processor: ScriptProcessorNode;
  private source: MediaStreamAudioSourceNode;
  private sink: GainNode;
  private chunks: Float32Array[] = [];
  captureStartServerMs: number | null = null;
  readonly sampleRate: number;

  private constructor(ctx: AudioContext, stream: MediaStream, serverOffset: number) {
    this.ctx = ctx;
    this.stream = stream;
    this.sampleRate = ctx.sampleRate;
    this.source = ctx.createMediaStreamSource(stream);
    this.processor = ctx.createScriptProcessor(4096, 1, 1);
    // Zero-gain sink: ScriptProcessorNode only fires when it reaches the
    // destination, but we must not route the mic to the speakers (feedback).
    this.sink = ctx.createGain();
    this.sink.gain.value = 0;
    this.processor.onaudioprocess = (e) => {
      const input = e.inputBuffer.getChannelData(0);
      if (this.captureStartServerMs === null) {
        // This buffer just finished capturing, so its first sample was captured
        // ~one buffer-duration ago. (Ignores mic input latency — a few ms — which
        // shifts only the absolute number, not relative delays.)
        const bufMs = (input.length / this.sampleRate) * 1000;
        this.captureStartServerMs = Date.now() + serverOffset - bufMs;
      }
      this.chunks.push(new Float32Array(input));
    };
    this.source.connect(this.processor);
    this.processor.connect(this.sink);
    this.sink.connect(ctx.destination);
  }

  static async start(serverOffset: number): Promise<MicRecorder> {
    const stream = await navigator.mediaDevices.getUserMedia({
      // Disable processing that would distort the tone onset timing.
      audio: {
        echoCancellation: false,
        noiseSuppression: false,
        autoGainControl: false,
      } as MediaTrackConstraints,
    });
    const AC: typeof AudioContext =
      window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext;
    const ctx = new AC();
    if (ctx.state === "suspended") await ctx.resume();
    return new MicRecorder(ctx, stream, serverOffset);
  }

  /** Stop capture, release the mic, and return the recording as one array. */
  stop(): Float32Array {
    this.processor.onaudioprocess = null;
    this.source.disconnect();
    this.processor.disconnect();
    this.sink.disconnect();
    for (const t of this.stream.getTracks()) t.stop();
    void this.ctx.close();
    const total = this.chunks.reduce((n, c) => n + c.length, 0);
    const out = new Float32Array(total);
    let off = 0;
    for (const c of this.chunks) {
      out.set(c, off);
      off += c.length;
    }
    return out;
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, Math.max(0, ms)));
const fmtMs = (v: number | null) => (v == null ? "—" : `${v >= 0 ? "" : ""}${v.toFixed(1)} ms`);

export function DelayTab({ clients }: { clients: Client[] }) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [phase, setPhase] = useState<Phase>("idle");
  const [statusMsg, setStatusMsg] = useState<string>("");
  const [sync, setSync] = useState<{ offset: number; rtt: number } | null>(null);
  const [result, setResult] = useState<DelayTestResult | null>(null);
  const [run, setRun] = useState<DelayRunResponse | null>(null);
  const recorderRef = useRef<MicRecorder | null>(null);

  const connected = clients.filter((c) => c.connected);
  const secure = window.isSecureContext && !!navigator.mediaDevices?.getUserMedia;
  const busy = phase === "syncing" || phase === "recording" || phase === "analyzing";

  // Prune selections for clients that went away.
  useEffect(() => {
    setSelected((prev) => {
      const live = new Set(connected.map((c) => c.id));
      let changed = false;
      const next = new Set<string>();
      for (const id of prev) {
        if (live.has(id)) next.add(id);
        else changed = true;
      }
      return changed ? next : prev;
    });
  }, [clients]); // eslint-disable-line react-hooks/exhaustive-deps

  // Release the mic if the tab unmounts mid-test.
  useEffect(() => () => void recorderRef.current?.stop(), []);

  const toggle = (id: string) =>
    setSelected((p) => {
      const n = new Set(p);
      n.has(id) ? n.delete(id) : n.add(id);
      return n;
    });
  const nameOf = (id: string) => clients.find((c) => c.id === id)?.name ?? id;

  async function runTest() {
    const ids = connected.filter((c) => selected.has(c.id)).map((c) => c.id);
    if (ids.length === 0 || !secure) return;
    setResult(null);
    setRun(null);
    try {
      setPhase("syncing");
      setStatusMsg("Aligning clocks with the server…");
      const clk = await syncClock();
      setSync(clk);

      setStatusMsg("Starting the microphone…");
      const rec = await MicRecorder.start(clk.offset);
      recorderRef.current = rec;
      // Let capture settle before the tones play.
      await sleep(300);

      setPhase("recording");
      setStatusMsg("Playing test tones and recording…");
      const runRes: DelayRunResponse = await fetch("/api/delaytest/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ client_ids: ids }),
      }).then((r) => r.json());
      setRun(runRes);

      // Record until the last burst has definitely been heard.
      const stopServerMs =
        runRes.play_at_ms + runRes.burst_count * runRes.burst_spacing_ms + RECORD_TAIL_MS;
      await sleep(stopServerMs - (Date.now() + clk.offset));

      const pcm = rec.stop();
      recorderRef.current = null;

      setPhase("analyzing");
      setStatusMsg("Analyzing the recording…");
      const qs = new URLSearchParams({
        test_id: runRes.test_id,
        sample_rate: String(rec.sampleRate),
        capture_start_ms: String(rec.captureStartServerMs ?? Date.now() + clk.offset),
      });
      const res: DelayTestResult = await fetch(`/api/delaytest/analyze?${qs}`, {
        method: "POST",
        headers: { "Content-Type": "application/octet-stream" },
        // Raw little-endian f32 PCM. `pcm` is freshly allocated, so its buffer is a
        // plain ArrayBuffer (the cast just narrows off ArrayBufferLike).
        body: pcm.buffer as ArrayBuffer,
      }).then((r) => {
        if (!r.ok) throw new Error(`analyze failed: ${r.status}`);
        return r.json();
      });
      setResult(res);
      setPhase("done");
      setStatusMsg("");
    } catch (e) {
      recorderRef.current?.stop();
      recorderRef.current = null;
      setPhase("error");
      setStatusMsg(e instanceof Error ? e.message : String(e));
    }
  }

  // Largest relative delay, for scaling the result bars.
  const maxRel = Math.max(
    1,
    ...(result?.results ?? [])
      .map((r) => r.relative_delay_ms ?? 0)
      .filter((x) => Number.isFinite(x)),
  );

  return (
    <div className="max-w-3xl space-y-4 mt-2">
      {/* Intro / placement hint */}
      <div className="bg-white rounded-2xl p-5 shadow-sm">
        <p className="text-sm text-slate-600 leading-relaxed">
          Measures each speaker's real playback delay with the browser microphone. Each selected
          speaker plays a short tone at its own frequency; from one recording the server reports how
          far each speaker lags the fastest one (<span className="font-medium">relative</span>) and
          how long from the server emitting a sound to the speaker producing it
          (<span className="font-medium">absolute</span>).
        </p>
        <div className="mt-3 flex items-start gap-2 text-xs text-slate-500">
          <Icon name="anchor" size={14} className="mt-0.5 shrink-0 text-slate-400" />
          <span>
            Place the microphone roughly equidistant from the speakers — the results include the
            sound's travel time (~3&nbsp;ms per meter), so an off-center mic skews them.
          </span>
        </div>
      </div>

      {!secure && (
        <div className="bg-amber-50 border border-amber-200 rounded-2xl p-4 text-sm text-amber-800">
          The microphone is unavailable because this page isn't a{" "}
          <span className="font-medium">secure context</span>. Browsers allow the mic on{" "}
          <code className="font-mono">http://localhost</code> but block it over a LAN IP. Open the
          UI via localhost (e.g. an SSH port-forward), or launch the browser with{" "}
          <code className="font-mono">--unsafely-treat-insecure-origin-as-secure</code> for this
          origin.
        </div>
      )}

      {/* Speaker picker + controls */}
      <div className="bg-white rounded-2xl p-5 shadow-sm">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-semibold text-slate-700">Speakers to test</h2>
          {connected.length > 0 && (
            <button
              className="text-xs font-medium text-emerald-600 hover:text-emerald-700"
              onClick={() =>
                setSelected((p) =>
                  p.size === connected.length ? new Set() : new Set(connected.map((c) => c.id)),
                )
              }
              disabled={busy}
            >
              {selected.size === connected.length ? "Clear all" : "Select all"}
            </button>
          )}
        </div>

        {connected.length === 0 ? (
          <p className="text-sm text-slate-400">No connected speakers.</p>
        ) : (
          <div className="grid grid-cols-2 gap-x-6 gap-y-1.5">
            {connected.map((c) => (
              <label
                key={c.id}
                className={
                  "flex items-center gap-2.5 py-1 cursor-pointer select-none " +
                  (busy ? "opacity-60 pointer-events-none" : "")
                }
              >
                <input
                  type="checkbox"
                  className="accent-emerald-500 w-4 h-4"
                  checked={selected.has(c.id)}
                  onChange={() => toggle(c.id)}
                />
                <span className="text-sm text-slate-700 truncate">{c.name}</span>
                <span className="text-[11px] text-slate-400 font-mono ml-auto">{c.ip}</span>
              </label>
            ))}
          </div>
        )}

        <div className="mt-4 flex items-center gap-3">
          <button
            onClick={runTest}
            disabled={busy || selected.size === 0 || !secure}
            className={
              "inline-flex items-center gap-2 text-sm font-medium rounded-lg px-4 py-2 transition-colors " +
              (busy || selected.size === 0 || !secure
                ? "bg-slate-100 text-slate-400 cursor-not-allowed"
                : "bg-emerald-500 text-white hover:bg-emerald-600")
            }
          >
            <Icon name="timer" size={16} />
            {busy ? "Testing…" : "Start test"}
          </button>
          {busy && (
            <span className="inline-flex items-center gap-2 text-sm text-slate-500">
              <span className="w-3.5 h-3.5 rounded-full border-2 border-slate-300 border-t-emerald-500 animate-spin" />
              {statusMsg}
            </span>
          )}
          {phase === "error" && <span className="text-sm text-red-500">{statusMsg}</span>}
          {sync && (phase === "done" || busy) && (
            <span className="text-xs text-slate-400 ml-auto">
              clock sync ±{(sync.rtt / 2).toFixed(0)} ms
            </span>
          )}
        </div>
      </div>

      {/* Results */}
      {result && phase === "done" && (
        <div className="bg-white rounded-2xl p-5 shadow-sm">
          <h2 className="text-sm font-semibold text-slate-700 mb-3">Results</h2>
          <div className="space-y-2.5">
            {result.results.map((r) => {
              const isFastest = r.client_id === result.fastest_client_id;
              const rel = r.relative_delay_ms;
              const weak = r.confidence < 0.25 || rel == null;
              return (
                <div key={r.client_id} className="flex items-center gap-3">
                  <div className="w-40 shrink-0 flex items-center gap-1.5 min-w-0">
                    {isFastest && (
                      <span className="text-[10px] font-semibold text-emerald-600 uppercase">
                        fastest
                      </span>
                    )}
                    <span className="text-sm text-slate-700 truncate">{nameOf(r.client_id)}</span>
                  </div>
                  {/* Relative-delay bar */}
                  <div className="flex-1 h-6 bg-slate-100 rounded-md overflow-hidden relative">
                    {rel != null && (
                      <div
                        className={
                          "h-full rounded-md " + (isFastest ? "bg-emerald-400" : "bg-emerald-500/70")
                        }
                        style={{ width: `${Math.max(2, (rel / maxRel) * 100)}%` }}
                      />
                    )}
                  </div>
                  <div className="w-44 shrink-0 text-right text-xs tabular-nums text-slate-500">
                    {weak ? (
                      <span className="text-slate-400">weak / not detected</span>
                    ) : (
                      <>
                        <span className="text-slate-700 font-medium">+{fmtMs(rel)}</span>
                        <span className="text-slate-400"> · abs {fmtMs(r.absolute_delay_ms)}</span>
                      </>
                    )}
                  </div>
                </div>
              );
            })}
          </div>

          {run && run.assignments.some((a) => !a.reachable) && (
            <p className="mt-3 text-xs text-amber-600">
              Not tested (offline):{" "}
              {run.assignments
                .filter((a) => !a.reachable)
                .map((a) => nameOf(a.client_id))
                .join(", ")}
            </p>
          )}
          <p className="mt-3 text-[11px] text-slate-400 leading-relaxed">
            Relative delay is measured against the fastest speaker and is unaffected by clock-sync
            error. Absolute delay is the server-emit → heard latency (network + buffer + hardware +
            acoustic travel), accurate to within the clock sync shown above plus a few ms of mic
            input latency.
          </p>
        </div>
      )}
    </div>
  );
}
