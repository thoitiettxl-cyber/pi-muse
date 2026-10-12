/** Auto-continue against the deterministic fake muse2api, driving streamLocal with a mini Pi tool loop. */
import assert from "node:assert/strict";
import { execSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { stats, streamLocal, threadLock } from "../src/index.ts";
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
		PI_MUSE_BACKOFF: "0.05,0.05,0.05", PI_MUSE_POLL: "0.1", PI_MUSE_MAX_WAIT: "5", PI_MUSE_MAX_RETRIES: "3", PI_MUSE_AUTO_CONTINUE: "1", ...env,
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
	Object.assign(process.env, { PI_MUSE_CONFIG: join(dir, "none.json"), MUSE2API_URL: mock.url, PI_MUSE_LOG: join(dir, "l.jsonl"), PI_MUSE_BACKOFF: "0.05", PI_MUSE_POLL: "0.1", PI_MUSE_MAX_WAIT: "5", PI_MUSE_AUTO_CONTINUE: "1" });
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
