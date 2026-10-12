/**
 * Deterministic fake muse2api (pi-native surface) for reproducible auto-continue tests.
 * Scenario per test:
 *   bg:       "running-then-complete" (Muse keeps going after our timeout, then finishes),
 *             "stopped" (Muse turn is interrupted), "running-forever" (only chat.cancel stops it),
 *             "no-state" (state endpoint unavailable)
 * The first turn invokes `bash: echo step >> log.txt`, waits for the result, then fails with the
 * real muse2api error shape. Continuation turns reply "resumed ok".
 */
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

export interface MockOpts {
	bg: "running-then-complete" | "stopped" | "running-forever" | "no-state" | "state-crash" | "held" | "held-forever";
	/** first-turn device command (default: a non-idempotent append) */
	command?: string;
	runningMs?: number;
	errorKind?: "timeout" | "auth" | "disconnect" | "http504";
	failContinuations?: number; // continuation turns that fail again with a timeout
	slowContinuationMs?: number;
	/** /pi/v1/turn answers for existing threads: "503" (state unknown) or N x 409 closing then OK */
	turnGate?: { kind: "503" } | { kind: "closing"; times: number };
}

export function startMock(opts: MockOpts) {
	const SID = "11111111-2222-3333-4444-555555555555";
	const st = {
		turns: [] as { prompt: string; session_id?: string; at: number }[],
		inflight: 0,
		maxInflight: 0,
		turnsWhileMuseRunning: 0,
		stateCalls: 0,
		cancelCalls: 0,
		museRunningUntil: 0,
		museStatus: "completed" as string,
		pendingResult: undefined as undefined | ((b: any) => void),
		heartbeats: 0,
		host: undefined as undefined | { owner?: string; running: boolean; cancelled?: boolean; wake?: () => void },
		rejected409: 0,
		attaches: 0,
		attachToolDeviceOnline: [] as boolean[],
		inflightBySession: new Map<string, number>(),
	};
	const museRunning = () => {
		if (st.museStatus === "running" && opts.bg === "running-then-complete" && Date.now() > st.museRunningUntil) st.museStatus = "completed";
		if (st.host) return st.host.running;
		return st.museStatus === "running";
	};
	const body = (req: IncomingMessage) => new Promise<any>((r) => { let d = ""; req.on("data", (c) => (d += c)); req.on("end", () => r(d ? JSON.parse(d) : {})); });
	const json = (res: ServerResponse, code: number, v: unknown) => { res.writeHead(code, { "content-type": "application/json" }); res.end(JSON.stringify(v)); };
	const server = createServer(async (req, res) => {
		const b = req.method === "POST" ? await body(req) : {};
		if (req.url === "/pi/v1/invoke_result") { st.pendingResult?.(b); return json(res, 200, { ok: true }); }
		if (req.url === "/pi/v1/thread/state") {
			st.stateCalls++;
			if (opts.bg === "no-state") return json(res, 404, { detail: "Not Found" });
			if (opts.bg === "state-crash") { res.writeHead(200, { "content-type": "application/json" }); return res.end("{not json"); }
			if (st.host) return json(res, 200, { status: st.host.running ? "running" : "completed", held: true, busy: st.host.running, owner_is_you: b.client_id === st.host.owner });
			const running = museRunning();
			return json(res, 200, { status: st.museStatus, held: false, reply: running ? undefined : st.museStatus === "completed" ? "FINISHED IN BACKGROUND" : null });
		}
		if (req.url === "/pi/v1/thread/attach") {
			const h = st.host;
			if (!h) return json(res, 404, { detail: "not held" });
			if (b.client_id !== h.owner) return json(res, 409, { error: "thread_busy", owner_is_you: false });
			st.attaches++;
			res.writeHead(200, { "content-type": "text/event-stream" });
			const send = (ev: string, d: any) => res.write(`event: ${ev}\ndata: ${JSON.stringify(d)}\n\n`);
			send("start", { turn_id: "turn-1", attached: true });
			res.write(": keep-alive\n\n");
			// Muse keeps working while held: a device call in the detached window (device still online)
			const got = new Promise<any>((r) => (st.pendingResult = r));
			send("invoke", { turn_id: "turn-1", request_id: "req-2", command: "bash", params: { command: "echo attached >> log.txt" } });
			await got;
			st.attachToolDeviceOnline.push(true);
			if (opts.bg === "held-forever") await new Promise<void>((r) => (h.wake = r));
			else await new Promise((r) => setTimeout(r, opts.runningMs ?? 200));
			h.running = false;
			st.host = undefined;
			if (h.cancelled) send("done", { session_id: SID, status: "interrupted" });
			else { send("final_text", { text: "FINISHED VIA ATTACH" }); send("done", { session_id: SID, status: "completed" }); }
			return res.end();
		}
		if (req.url === "/pi/v1/thread/cancel") {
			st.cancelCalls++;
			if (st.host) { st.host.cancelled = true; st.museStatus = "interrupted"; st.host.wake?.(); return json(res, 200, { cancelled: true, status: "interrupted" }); } const was = museRunning(); st.museStatus = "interrupted"; return json(res, 200, { cancelled: was, status: "interrupted" }); }
		if (req.url !== "/pi/v1/turn") return json(res, 404, {});
		// ---- /pi/v1/turn
		const sess = b.session_id ?? SID;
		const gate = opts.turnGate;
		if (b.session_id && gate?.kind === "503") return json(res, 503, { error: "state_unknown", message: "state_unknown: sessions.get failed: timeout" });
		if (b.session_id && gate?.kind === "closing" && gate.times > 0) { gate.times--; st.rejected409++; return json(res, 409, { error: "thread_busy", phase: "closing", retry_after: 0.05 }); }
		if (b.session_id && (st.host?.running || (st.inflightBySession.get(sess) ?? 0) > 0)) {
			// server-side per-thread lock
			st.rejected409++;
			return json(res, 409, { error: "thread_busy", session_id: sess, phase: "watch", owner_is_you: b.client_id === st.host?.owner });
		}
		if (museRunning()) st.turnsWhileMuseRunning++;
		st.inflightBySession.set(sess, (st.inflightBySession.get(sess) ?? 0) + 1);
		res.on("close", () => st.inflightBySession.set(sess, (st.inflightBySession.get(sess) ?? 1) - 1));
		st.turns.push({ prompt: b.prompt, session_id: b.session_id, at: Date.now() });
		st.inflight++; st.maxInflight = Math.max(st.maxInflight, st.inflight);
		res.on("close", () => st.inflight--);
		if (opts.errorKind === "http504" && st.turns.length === 1) { res.writeHead(504); return res.end("Gateway Time-out"); }
		res.writeHead(200, { "content-type": "text/event-stream" });
		const send = (ev: string, d: any) => res.write(`event: ${ev}\ndata: ${JSON.stringify(d)}\n\n`);
		const hb = () => { st.heartbeats++; res.write(": keep-alive\n\n"); };
		const tid = `turn-${st.turns.length}`;
		send("start", { turn_id: tid });
		hb();
		send("session", { type: "session", change: "created", session_id: sess, status: "running" });
		const isContinuation = st.turns.length > 1;
		if (isContinuation) {
			if ((opts.failContinuations ?? 0) >= st.turns.length - 1) {
				send("error", { type: "error", message: "MuseTimeoutError: 等待助手首字响应超时", code: "first_byte_timeout", retryable: true, phase: "first_byte", session_id: sess });
				return res.end();
			}
			if (opts.slowContinuationMs) { await new Promise((r) => setTimeout(r, opts.slowContinuationMs)); hb(); }
			send("text_done", { text: "resumed ok" });
			send("done", { session_id: sess, turn_id: tid });
			return res.end();
		}
		// first turn: one non-idempotent device step, then the failure
		const got = new Promise<any>((r) => (st.pendingResult = r));
		send("invoke", { turn_id: tid, request_id: "req-1", command: "bash", params: { command: opts.command ?? "echo step >> log.txt" }, timeout_ms: 120000 });
		await got;
		hb();
		st.museStatus = opts.bg === "stopped" ? "interrupted" : "running";
		const held = opts.bg === "held" || opts.bg === "held-forever";
		if (held) st.host = { owner: b.client_id, running: true };
		st.museRunningUntil = Date.now() + (opts.runningMs ?? 1500);
		if (opts.errorKind === "auth") send("error", { type: "error", message: "HTTPException: 401 Unauthorized: cookie expired, please login again" });
		else if (opts.errorKind === "disconnect") res.destroy();
		else send("error", { type: "error", message: "MuseTimeoutError: 等待助手首字响应超时", code: "first_byte_timeout", retryable: true, phase: "first_byte", timeout_s: 2, waited_s: 2, session_id: sess, muse_status: "running", held, last_invoke: { command: "bash" } });
		res.end();
	});
	return new Promise<{ url: string; st: typeof st; SID: string; close: () => void }>((r) =>
		server.listen(0, "127.0.0.1", () => r({ url: `http://127.0.0.1:${(server.address() as any).port}`, st, SID, close: () => { server.closeAllConnections(); server.close(); } })),
	);
}
