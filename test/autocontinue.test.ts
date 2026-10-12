import assert from "node:assert/strict";
import { test } from "node:test";
import { classifyMuseError, continuationMessage, recoverTurn, ThreadLock } from "../src/autocontinue.ts";

test("classifier: retryable shapes", () => {
	for (const m of [
		"MuseGenerationError: 等待助手首字响应超时", // verbatim, pre-patch
		"MuseTimeoutError: 等待助手首字响应超时",
		"MuseTimeoutError: 等待助手回复超时",
		"terminated",
		"muse2api request failed: ECONNRESET fetch failed",
		"muse2api stream ended without a done event (mid-stream disconnect)",
	]) assert.equal(classifyMuseError(new Error(m)).retryable, true, m);
	assert.equal(classifyMuseError({ message: "x", code: "idle_timeout" }).retryable, true);
	assert.deepEqual(classifyMuseError({ message: "muse2api HTTP 504: Gateway Time-out", httpStatus: 504 }).kind, "gateway_timeout");
});

test("classifier: fatal shapes stop", () => {
	const cases: [any, string][] = [
		[{ message: "muse2api HTTP 401: invalid key", httpStatus: 401 }, "auth"],
		[new Error("HTTPException: 401 Unauthorized: cookie expired, please login again"), "auth"],
		[new Error("muse.ai quota exceeded for this account"), "quota"],
		[{ message: "muse2api HTTP 429: rate limit", httpStatus: 429 }, "quota"],
		[new Error("Muse refused: content policy violation"), "refusal"],
		[new Error("Request was aborted"), "aborted"],
		[new Error("something odd"), "other"],
		// fatal wins even if it also looks like a timeout
		[new Error("login timed out: cookie invalid"), "auth"],
	];
	for (const [e, k] of cases) {
		const c = classifyMuseError(e);
		assert.equal(c.retryable, false, String(e.message));
		assert.equal(c.kind, k, String(e.message));
	}
});

test("continuation message", () => {
	const m = continuationMessage({ tool: "bash", args: { command: "echo step >> log.txt" }, status: "ok", result: "(no output)" }, '{"todos":["a","b"]}');
	assert.match(m, /^Continue from where you stopped\. The last step already ran: bash \{"command":"echo step >> log\.txt"\} -> \(no output\)\. Do not repeat it\./);
	assert.match(m, /todo list so far/);
	assert.doesNotMatch(continuationMessage(), /last step/);
	assert.match(continuationMessage({ tool: "write", args: { path: "a" }, status: "error", result: "EACCES" }), /-> error: EACCES/);
	assert.ok(continuationMessage({ tool: "x", args: "y".repeat(5000) }).length < 400);
});

test("ThreadLock: one owner per thread, release only by owner", () => {
	const l = new ThreadLock();
	assert.ok(l.tryAcquire("t", "a"));
	assert.ok(!l.tryAcquire("t", "b"));
	l.release("t", "b");
	assert.equal(l.holder("t"), "a");
	l.release("t", "a");
	assert.ok(l.tryAcquire("t", "b"));
});

const cfg = { autoContinue: true, maxRetries: 3, backoffMs: [], maxWaitMs: 300, pollMs: 50 };
const deps = (states: any[], cancel?: () => any) => {
	let i = 0;
	return { getState: async () => { const s = states[Math.min(i++, states.length - 1)]; if (s instanceof Error) throw s; return s; },
		cancel: async () => (cancel ? cancel() : { cancelled: true }), sleep: (ms: number) => new Promise<void>((r) => setTimeout(r, ms)), log: () => {} };
};
test("recover: running then completed -> present result", async () => {
	const r = await recoverTurn("s", cfg, deps([{ status: "running" }, { status: "running" }, { status: "completed", reply: "done!" }]));
	assert.equal(r.action, "present");
});
test("recover: stopped -> continue", async () => {
	assert.deepEqual((await recoverTurn("s", cfg, deps([{ status: "interrupted" }]))).action, "continue");
});
test("recover: running past max wait -> cancel then continue", async () => {
	let n = 0;
	const r = await recoverTurn("s", cfg, deps([...Array(20).fill({ status: "running" }).map((x) => x), { status: "interrupted" }], () => (n++, { cancelled: true })));
	// after cancel, state still 'running' in this fake -> must stop, not continue
	assert.equal(r.action, "stop");
	assert.equal(n, 1);
	const states: any[] = [{ status: "running" }];
	const d = deps(states, () => { states.push({ status: "interrupted" }); return { cancelled: true }; });
	const r2 = await recoverTurn("s", cfg, { ...d, getState: async () => states[states.length - 1] });
	assert.equal(r2.action, "continue");
	assert.equal((r2 as any).cancelled, true);
});
test("recover: unknown state -> stop", async () => {
	assert.equal((await recoverTurn("s", cfg, deps([new Error("HTTP 404")]))).action, "stop");
	assert.equal((await recoverTurn("s", cfg, deps([{}]))).action, "stop");
	assert.equal((await recoverTurn(undefined, cfg, deps([{ status: "interrupted" }]))).action, "stop");
	assert.equal((await recoverTurn("s", cfg, deps([{ status: "running" }], () => { throw new Error("no cancel"); }))).action, "stop");
});
