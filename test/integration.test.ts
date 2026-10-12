/** Auto-continue against the deterministic fake muse2api, driving streamLocal with a mini Pi tool loop. */
import assert from "node:assert/strict";
import { execSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { CLIENT_ID, stats, streamLocal, threadLock } from "../src/index.ts";
import { type MockOpts, startMock } from "./mock-muse2api.ts";

const model: any = { id: "auto/auto", api: "muse-agent", provider: "muse" };
const bashTool = { name: "bash", description: "run a shell command", parameters: { type: "object", properties: { command: { type: "string" } }, required: ["command"] } };
const LOGDIR = process.env.PI_MUSE_TEST_LOGDIR;

async function drain(s: any) {
	let thinking = "";
	for await (const ev of s) {
		if (ev.type === "thinking_delta") thinking += ev.delta;
		if (ev.type === "done") return { reason: ev.reason, msg: ev.message, thinking };
		if (ev.type === "error") return { reason: "error", msg: ev.error, thinking };
	}
	throw new Error("stream ended without done/error");
}

/** Mini Pi loop: run tool calls locally (in dir) and feed results back until the turn stops. */
async function piTurn(dir: string, messages: any[], prompt: string) {
	messages.push({ role: "user", content: prompt });
	let thinking = "";
	for (let i = 0; i < 10; i++) {
		const r = await drain(streamLocal(model, { messages } as any));
		thinking += r.thinking;
		messages.push(r.msg);
		if (r.reason !== "toolUse") return { ...r, thinking };
		for (const c of r.msg.content.filter((x: any) => x.type === "toolCall")) {
			const out = execSync(c.arguments.command, { cwd: dir, encoding: "utf8" });
			messages.push({ role: "toolResult", toolCallId: c.id, toolName: c.name, content: [{ type: "text", text: out || "" }], isError: false });
		}
	}
	throw new Error("too many tool rounds");
}

async function scenario(name: string, opts: MockOpts, env: Record<string, string> = {}) {
	const mock = await startMock(opts);
	const dir = mkdtempSync(join(tmpdir(), "pimuse-"));
	const logFile = LOGDIR ? join(LOGDIR, `${name}.jsonl`) : join(dir, "turns.jsonl");
	if (LOGDIR) writeFileSync(logFile, "");
	Object.assign(process.env, {
		PI_MUSE_CONFIG: join(dir, "none.json"), MUSE2API_URL: mock.url, MUSE2API_KEY: "test-key", PI_MUSE_LOG: logFile,
		PI_MUSE_BACKOFF: "0.05,0.05,0.05", PI_MUSE_POLL: "0.1", PI_MUSE_MAX_WAIT: "5", PI_MUSE_MAX_RETRIES: "3", PI_MUSE_AUTO_CONTINUE: "1", PI_MUSE_NON_IDEMPOTENT_WINDOW: "0", ...env,
	});
	const messages: any[] = [{ role: "system", content: "sys", toolsAdded: [bashTool] }];
	process.chdir(dir);
	const before = { ...stats };
	const r = await piTurn(dir, messages, "append a line to log.txt");
	const delta = Object.fromEntries(Object.entries(stats).map(([k, v]) => [k, v - (before as any)[k]]));
	const lines = existsSync(join(dir, "log.txt")) ? readFileSync(join(dir, "log.txt"), "utf8").trim().split("\n") : [];
	mock.close();
	return { r, mock, lines, delta, messages, logFile, dir };
}

test("(b) Muse stopped -> continue on same thread with last-step summary, write not duplicated", async () => {
	const { r, mock, lines, delta } = await scenario("b-stopped-continue", { bg: "stopped" });
	assert.equal(r.reason, "stop");
	assert.equal(r.msg.content.find((c: any) => c.type === "text").text, "resumed ok");
	assert.equal(mock.st.turns.length, 2);
	const cont = mock.st.turns[1];
	assert.equal(cont.session_id, mock.SID, "same Muse thread");
	assert.match(cont.prompt, /^Continue from where you stopped\. The last step already ran: bash \{"command":"echo step >> log\.txt"\}/);
	assert.doesNotMatch(cont.prompt, /append a line|\[Pi bridge\]/, "original prompt not resent");
	assert.deepEqual(lines, ["step"], "non-idempotent step ran exactly once");
	assert.equal(delta.continuations, 1);
	assert.equal(r.msg.responseId?.startsWith(`muse:${mock.SID}`), true);
	assert.ok(mock.st.heartbeats > 0);
});

test("(a) Muse still running -> wait, fetch its result, send nothing", async () => {
	const { r, mock, lines, delta } = await scenario("a-running-wait-fetch", { bg: "running-then-complete", runningMs: 600 });
	assert.equal(r.reason, "stop");
	assert.equal(r.msg.content.find((c: any) => c.type === "text").text, "FINISHED IN BACKGROUND");
	assert.equal(mock.st.turns.length, 1, "no continuation request");
	assert.equal(mock.st.turnsWhileMuseRunning, 0);
	assert.equal(mock.st.cancelCalls, 0);
	assert.ok(mock.st.stateCalls >= 2);
	assert.deepEqual(lines, ["step"]);
	assert.equal(delta.presented, 1);
});

test("(c) wait exceeds max -> cancel, then continue", async () => {
	const { r, mock, lines } = await scenario("c-maxwait-cancel-continue", { bg: "running-forever" }, { PI_MUSE_MAX_WAIT: "0.5" });
	assert.equal(r.reason, "stop");
	assert.equal(mock.st.cancelCalls, 1);
	assert.equal(mock.st.turns.length, 2);
	assert.equal(mock.st.turnsWhileMuseRunning, 0, "continuation only after the old turn stopped");
	assert.match(r.thinking, /cancelling it/);
	assert.deepEqual(lines, ["step"]);
});

test("(d) state cannot be queried -> stop and report, no continue", async () => {
	const { r, mock, delta } = await scenario("d-no-state-stop", { bg: "no-state" });
	assert.equal(r.reason, "error");
	assert.match(r.msg.errorMessage, /Not auto-continuing: the Muse thread state could not be determined/);
	assert.equal(mock.st.turns.length, 1);
	assert.equal(delta.stopped, 1);
	assert.equal(threadLock.size(), 0, "lock released");
});

test("fatal error (auth) -> stop immediately, no state check, no continue", async () => {
	const { r, mock } = await scenario("auth-stop", { bg: "stopped", errorKind: "auth" });
	assert.equal(r.reason, "error");
	assert.match(r.msg.errorMessage, /401 Unauthorized/);
	assert.equal(mock.st.stateCalls, 0);
	assert.equal(mock.st.turns.length, 1);
});

test("mid-stream disconnect -> auto-continue", async () => {
	const { r, mock, lines } = await scenario("disconnect-continue", { bg: "stopped", errorKind: "disconnect" });
	assert.equal(r.msg.content.find((c: any) => c.type === "text")?.text, "resumed ok");
	assert.equal(mock.st.turns.length, 2);
	assert.deepEqual(lines, ["step"]);
});

test("max 3 consecutive retries, then clear error", async () => {
	const { r, mock } = await scenario("max-retries-stop", { bg: "stopped", failContinuations: 99 });
	assert.equal(r.reason, "error");
	assert.match(r.msg.errorMessage, /gave up after 3 consecutive retries/);
	assert.equal(mock.st.turns.length, 4); // original + 3 continuations
	assert.equal(threadLock.size(), 0);
});

test("autoContinue off -> stop and report", async () => {
	const { r, mock } = await scenario("auto-off", { bg: "stopped" }, { PI_MUSE_AUTO_CONTINUE: "off" });
	assert.equal(r.reason, "error");
	assert.equal(mock.st.turns.length, 1);
});

test("lock: manual 'continue' during auto-continue is rejected; exactly one in-flight request", async () => {
	const mock = await startMock({ bg: "running-then-complete", runningMs: 1500 });
	const dir = mkdtempSync(join(tmpdir(), "pimuse-"));
	Object.assign(process.env, { PI_MUSE_CONFIG: join(dir, "none.json"), MUSE2API_URL: mock.url, PI_MUSE_LOG: join(dir, "l.jsonl"), PI_MUSE_NON_IDEMPOTENT_WINDOW: "0", PI_MUSE_BACKOFF: "0.05", PI_MUSE_POLL: "0.1", PI_MUSE_MAX_WAIT: "5", PI_MUSE_AUTO_CONTINUE: "1" });
	process.chdir(dir);
	const messages: any[] = [{ role: "system", content: "sys", toolsAdded: [bashTool] }];
	const auto = piTurn(dir, messages, "append a line");
	// wait until the extension is in recovery (state polling started)
	while (mock.st.stateCalls === 0) await new Promise((r) => setTimeout(r, 20));
	const manual = await drain(streamLocal(model, { messages: [...messages.slice(0, 1), { role: "user", content: "x" }, { role: "assistant", content: [], responseId: `muse:${mock.SID}` }, { role: "user", content: "continue" }] } as any));
	assert.equal(manual.reason, "error");
	assert.match(manual.msg.errorMessage, /your message was NOT sent/);
	const r = await auto;
	assert.equal(r.msg.content.find((c: any) => c.type === "text").text, "FINISHED IN BACKGROUND");
	assert.equal(mock.st.turns.length, 1, "manual continue never reached upstream");
	assert.equal(mock.st.maxInflight, 1);
	assert.equal(mock.st.turnsWhileMuseRunning, 0);
	assert.equal(threadLock.size(), 0);
	mock.close();
});

test("lock released when recovery crashes (malformed state response)", async () => {
	const { r } = await scenario("state-crash", { bg: "state-crash" });
	assert.equal(r.reason, "error");
	assert.equal(threadLock.size(), 0);
	// the thread is usable again: a normal follow-up turn is not rejected by the lock
	const mock = await startMock({ bg: "stopped" });
	process.env.MUSE2API_URL = mock.url;
	const f = await drain(streamLocal(model, { messages: [{ role: "system", content: "s", toolsAdded: [bashTool] }, { role: "assistant", content: [], responseId: `muse:${mock.SID}` }, { role: "user", content: "hi" }] } as any));
	assert.equal(f.reason, "toolUse");
	mock.close();
});

test("HTTP 504 on turn start without a known thread -> stop (cannot verify state)", async () => {
	const { r, mock } = await scenario("http504-stop", { bg: "stopped", errorKind: "http504" });
	assert.equal(r.reason, "error");
	assert.match(r.msg.errorMessage, /504/);
	assert.equal(mock.st.turns.length, 1);
});

// ---------------------------------------------------------------- held page / attach (muse2api keeps the device online)
const followUp = (sid: string, text: string) => [
	{ role: "system", content: "sys", toolsAdded: [bashTool] },
	{ role: "user", content: "earlier" },
	{ role: "assistant", content: [{ type: "text", text: "ok" }], responseId: `muse:${sid}` },
	{ role: "user", content: text },
];

test("held: after a timeout pi-muse re-attaches; Muse's device call in that window runs; nothing re-sent", async () => {
	const { r, mock, lines, delta } = await scenario("held-attach", { bg: "held" });
	assert.equal(r.reason, "stop");
	assert.equal(r.msg.content.find((c: any) => c.type === "text").text, "FINISHED VIA ATTACH");
	assert.equal(mock.st.turns.length, 1, "no continuation sent");
	assert.equal(mock.st.attaches, 1);
	assert.deepEqual(mock.st.attachToolDeviceOnline, [true]);
	assert.deepEqual(lines, ["step", "attached"], "both device steps ran exactly once");
	assert.equal(delta.attached, 1);
});

test("held past max wait: cancel via muse2api, then continue (guard window 0)", async () => {
	const { r, mock, lines } = await scenario("held-maxwait-cancel", { bg: "held-forever" }, { PI_MUSE_MAX_WAIT: "0.4" });
	assert.equal(mock.st.cancelCalls, 1);
	assert.equal(mock.st.turns.length, 2, "one continuation after the cancel");
	assert.match(mock.st.turns[1].prompt, /The last step already ran: bash \{"command":"echo attached >> log\.txt"\}/);
	assert.equal(r.msg.content.find((c: any) => c.type === "text").text, "resumed ok");
	assert.deepEqual(lines, ["step", "attached"]);
});

test("manual 'continue' while the held turn still runs -> attach, never a parallel request", async () => {
	const { r, mock } = await scenario("manual-continue-attach", { bg: "held", runningMs: 300 }, { PI_MUSE_AUTO_CONTINUE: "off" });
	assert.equal(r.reason, "error"); // auto-continue off: stop and report
	const m2 = await startMock({ bg: "held" });
	m2.st.host = { owner: CLIENT_ID, running: true };
	process.env.MUSE2API_URL = m2.url;
	const msgs: any[] = followUp(m2.SID, "continue");
	const dir = process.cwd();
	const res = await piTurnMsgs(dir, msgs);
	assert.match(res.msg.content.find((c: any) => c.type === "text").text, /your message was not sent[\s\S]*FINISHED VIA ATTACH/);
	assert.equal(m2.st.turns.length, 0, "the manual 'continue' was not sent while Muse was running");
	assert.equal(m2.st.attaches, 1);
	m2.close();
	mock.close();
});

async function piTurnMsgs(dir: string, messages: any[], mod: any = { streamLocal }) {
	for (let i = 0; i < 10; i++) {
		const r = await drain(mod.streamLocal(model, { messages } as any));
		messages.push(r.msg);
		if (r.reason !== "toolUse") return r;
		for (const c of r.msg.content.filter((x: any) => x.type === "toolCall")) {
			const out = execSync(c.arguments.command, { cwd: dir, encoding: "utf8" });
			messages.push({ role: "toolResult", toolCallId: c.id, toolName: c.name, content: [{ type: "text", text: out || "" }], isError: false });
		}
	}
	throw new Error("too many rounds");
}

test("two Pi windows on one thread: the second is refused (preflight busy, and server 409), upstream never sees two requests", async () => {
	const winB = await import("../src/index.ts?window=B");
	assert.notEqual(winB.CLIENT_ID, CLIENT_ID);
	const mock = await startMock({ bg: "held-forever" });
	const dir = mkdtempSync(join(tmpdir(), "pimuse-"));
	Object.assign(process.env, { PI_MUSE_CONFIG: join(dir, "none.json"), MUSE2API_URL: mock.url, PI_MUSE_LOG: join(dir, "l.jsonl"), PI_MUSE_BACKOFF: "0.05", PI_MUSE_POLL: "0.1", PI_MUSE_MAX_WAIT: "3", PI_MUSE_AUTO_CONTINUE: "1", PI_MUSE_NON_IDEMPOTENT_WINDOW: "0" });
	process.chdir(dir);
	// window A: timed-out turn, re-attached, Muse still running (held-forever)
	const msgsA: any[] = [{ role: "system", content: "sys", toolsAdded: [bashTool] }];
	const a = piTurn(dir, msgsA, "append a line");
	while (mock.st.attaches === 0) await new Promise((r) => setTimeout(r, 20));
	// window B (another process' worth of module state) sends on the same thread
	process.env.PI_MUSE_MAX_WAIT = "0.3";
	const b = await drain(winB.streamLocal(model, { messages: followUp(mock.SID, "continue") } as any));
	assert.equal(b.reason, "error");
	assert.match(b.msg.errorMessage, /NOT sent: the Muse thread is busy with a turn started from another Pi window/);
	// window B skipping the preflight race: server-side lock answers 409 -> clear message
	const r409 = await fetch(`${mock.url}/pi/v1/turn`, { method: "POST", body: JSON.stringify({ prompt: "x", session_id: mock.SID, client_id: winB.CLIENT_ID }) });
	assert.equal(r409.status, 409);
	assert.equal(mock.st.turns.length, 1, "only window A's request ever reached upstream");
	process.env.PI_MUSE_MAX_WAIT = "3";
	mock.st.host!.cancelled = true;
	mock.st.host!.wake?.();
	await a.catch(() => {});
	mock.close();
});

test("busy 409 from muse2api is reported clearly and not retried", async () => {
	const mock = await startMock({ bg: "stopped" });
	process.env.MUSE2API_URL = mock.url;
	mock.st.inflightBySession.set(mock.SID, 1); // another client's request is in flight (state says idle: race)
	const r = await drain(streamLocal(model, { messages: followUp(mock.SID, "hello") } as any));
	assert.equal(r.reason, "error");
	assert.match(r.msg.errorMessage, /is busy: another request .* Your message was NOT sent/);
	assert.equal(mock.st.rejected409, 1);
	mock.close();
});

// ---------------------------------------------------------------- non-idempotent guard
test("guard: timeout right after a non-idempotent step (>>) -> stop and ask, no continuation", async () => {
	const { r, mock, lines, delta } = await scenario("guard-stop", { bg: "stopped" }, { PI_MUSE_NON_IDEMPOTENT_WINDOW: "120" });
	assert.equal(r.reason, "error");
	assert.match(r.msg.errorMessage, /Not auto-continuing: the turn broke right after a non-idempotent step \(bash .*appends to a file/);
	assert.equal(mock.st.turns.length, 1);
	assert.deepEqual(lines, ["step"]);
	assert.equal(delta.guarded, 1);
});

test("guard: idempotent step (read-only command) -> auto-continue proceeds", async () => {
	const { r, mock } = await scenario("guard-idempotent-continue", { bg: "stopped", command: "cat /etc/hostname > /dev/null; echo ok" }, { PI_MUSE_NON_IDEMPOTENT_WINDOW: "120" });
	assert.equal(r.msg.content.find((c: any) => c.type === "text").text, "resumed ok");
	assert.equal(mock.st.turns.length, 2);
});

test("503 state_unknown from muse2api -> clear 'NOT sent' error, no retry", async () => {
	const mock = await startMock({ bg: "stopped", turnGate: { kind: "503" } });
	process.env.MUSE2API_URL = mock.url;
	const r = await drain(streamLocal(model, { messages: followUp(mock.SID, "hello") } as any));
	assert.equal(r.reason, "error");
	assert.match(r.msg.errorMessage, /could not confirm the state .* Your message was NOT sent/);
	assert.equal(mock.st.turns.length, 0);
	mock.close();
});

test("409 closing (previous page still closing) -> short retry, then the turn opens", async () => {
	const mock = await startMock({ bg: "stopped", turnGate: { kind: "closing", times: 2 } });
	process.env.MUSE2API_URL = mock.url;
	const r = await drain(streamLocal(model, { messages: followUp(mock.SID, "hello") } as any));
	assert.equal(r.reason, "toolUse");
	assert.equal(mock.st.rejected409, 2);
	assert.equal(mock.st.turns.length, 1);
	mock.close();
});

test("strict mode: after a non-allowlisted step -> ask the user (even outside the guard window)", async () => {
	const { r, mock, delta } = await scenario("strict-stop", { bg: "stopped" }, { PI_MUSE_STRICT: "1", PI_MUSE_NON_IDEMPOTENT_WINDOW: "0" });
	assert.equal(r.reason, "error");
	assert.match(r.msg.errorMessage, /Not auto-continuing \(strict mode\): the last step \(bash .*\) is not on the read-only allowlist/);
	assert.equal(mock.st.turns.length, 1);
	assert.equal(delta.guarded, 1);
});

test("strict mode: after an allowlisted read-only step -> auto-continue", async () => {
	const { r, mock } = await scenario("strict-continue", { bg: "stopped", command: "ls; pwd | wc -l" }, { PI_MUSE_STRICT: "1" });
	assert.equal(r.msg.content.find((c: any) => c.type === "text").text, "resumed ok");
	assert.equal(mock.st.turns.length, 2);
});

test("503 account_busy: bounded wait honours Retry-After, then the turn opens", async () => {
	const mock = await startMock({ bg: "stopped", turnGate: { kind: "account_busy", times: 1 } });
	process.env.MUSE2API_URL = mock.url;
	process.env.PI_MUSE_ACCOUNT_BUSY_WAIT = "5";
	const t0 = Date.now();
	const r = await drain(streamLocal(model, { messages: followUp(mock.SID, "hello") } as any));
	assert.equal(r.reason, "toolUse");
	assert.ok(Date.now() - t0 >= 1000, "waited Retry-After");
	assert.equal(mock.st.turns.length, 1);
	mock.close();
});

test("503 account_busy beyond the bounded wait -> clear 'NOT sent' message", async () => {
	const mock = await startMock({ bg: "stopped", turnGate: { kind: "account_busy", times: 99 } });
	process.env.MUSE2API_URL = mock.url;
	process.env.PI_MUSE_ACCOUNT_BUSY_WAIT = "0";
	const r = await drain(streamLocal(model, { messages: followUp(mock.SID, "hello") } as any));
	assert.equal(r.reason, "error");
	assert.match(r.msg.errorMessage, /Muse account is busy with another thread \(other-thread, watch\).*NOT sent/);
	assert.equal(mock.st.turns.length, 0);
	delete process.env.PI_MUSE_ACCOUNT_BUSY_WAIT;
	mock.close();
});
