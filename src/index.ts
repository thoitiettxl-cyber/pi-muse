/**
 * pi-muse: a Pi provider for the Muse agent (muse.ai).
 *
 * Modes (PI_MUSE_MODE / muse.json "mode" / /muse-mode):
 *   local  (default) Muse works on the repo open in Pi. The backend registers this machine as a Muse
 *          device node whose commands are Pi's tools; each `node.invoke.request` is surfaced as a Pi
 *          toolCall, Pi runs it locally, and the result is posted back into the same Muse turn.
 *   xml    Pi's tool schema is injected into the prompt (oh-my-pi XML dialect); `<invoke>` blocks in
 *          Muse's reply become Pi toolCalls; results go back as the next Muse turn.
 *   sync   snapshot of the git repo is pushed to ~/workspace/pi-sync/<repo> in Muse's VM before the
 *          turn; Muse's changes are diffed back afterwards (/muse-pull, /muse-apply).
 *   mirror Muse works in its own cloud VM; every VM tool call is mirrored as a pass-through
 *          `muse_tool` call (one Muse turn => two Pi model requests).
 *
 * Backend: muse2api branch `pi-native` (backend/pi_native.py), SSE /pi/v1/turn.
 */
import {
	type Api,
	type AssistantMessage,
	type AssistantMessageEventStream,
	createAssistantMessageEventStream,
	type Model,
	type SimpleStreamOptions,
	type TranscriptContext,
	Type,
} from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";
import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { homedir, hostname, tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { classifyMuseError, continuationMessage, type LastStep, nonIdempotentReason, type RecoverOpts, recoverTurn, summarize, ThreadLock } from "./autocontinue.ts";
import { collectSchemaTypes, decodeValue, type InbandTool, parseInvokes, renderToolResponses, renderToolsPrompt } from "./xml-dialect.ts";

// ---------------------------------------------------------------------------
// Configuration: env vars win, then ~/.pi/agent/muse.json
// ---------------------------------------------------------------------------
interface MuseConfig {
	baseUrl: string;
	apiKey: string;
	settleTimeout: number;
	/** local (default): Muse works on Pi's cwd via the node bridge; xml: in-band XML tools; sync: snapshot push/pull; mirror: Muse's own VM. */
	mode: string;
	/** sync mode: apply Muse's changes automatically after each turn (otherwise preview + /muse-apply). */
	syncAutoApply: boolean;
	/** local mode: resume interrupted turns (timeouts / 504 / disconnects only) in the same Muse thread. */
	autoContinue: boolean;
	maxRetries: number;
	/** max seconds to wait for a still-running Muse turn before cancelling it */
	maxWaitSeconds: number;
	pollSeconds: number;
	backoffSeconds: number[];
	/** optional per-turn overrides sent to muse2api (seconds); unset = backend defaults */
	firstByteTimeout?: number;
	idleTimeout?: number;
	/** JSON-lines log of turn errors and recovery decisions */
	logFile: string;
	/** don't auto-continue if the turn broke within this many seconds after a non-idempotent device step */
	nonIdempotentWindowSeconds: number;
}

function loadConfig(): MuseConfig {
	let file: Partial<MuseConfig> = {};
	const p = process.env.PI_MUSE_CONFIG ?? join(homedir(), ".pi", "agent", "muse.json");
	try {
		if (existsSync(p)) file = JSON.parse(readFileSync(p, "utf8"));
	} catch {
		/* ignore malformed config */
	}
	return {
		baseUrl: (process.env.MUSE2API_URL ?? file.baseUrl ?? "http://127.0.0.1:18610").replace(/\/+$/, ""),
		apiKey: process.env.MUSE2API_KEY ?? file.apiKey ?? "",
		settleTimeout: Number(process.env.PI_MUSE_SETTLE_TIMEOUT ?? file.settleTimeout ?? 45),
		mode: process.env.PI_MUSE_MODE ?? file.mode ?? "local",
		autoContinue: !/^(0|false|off|no)$/i.test(process.env.PI_MUSE_AUTO_CONTINUE ?? String(file.autoContinue ?? "true")),
		maxRetries: Number(process.env.PI_MUSE_MAX_RETRIES ?? file.maxRetries ?? 3),
		maxWaitSeconds: Number(process.env.PI_MUSE_MAX_WAIT ?? file.maxWaitSeconds ?? 600),
		pollSeconds: Number(process.env.PI_MUSE_POLL ?? file.pollSeconds ?? 5),
		backoffSeconds: (process.env.PI_MUSE_BACKOFF ? process.env.PI_MUSE_BACKOFF.split(",").map(Number) : file.backoffSeconds) ?? [2, 5, 10],
		firstByteTimeout: num(process.env.PI_MUSE_FIRST_BYTE_TIMEOUT ?? file.firstByteTimeout),
		idleTimeout: num(process.env.PI_MUSE_IDLE_TIMEOUT ?? file.idleTimeout),
		nonIdempotentWindowSeconds: Number(process.env.PI_MUSE_NON_IDEMPOTENT_WINDOW ?? file.nonIdempotentWindowSeconds ?? 120),
		logFile: process.env.PI_MUSE_LOG ?? file.logFile ?? join(homedir(), ".pi", "agent", "muse-turns.log"),
		syncAutoApply: (process.env.PI_MUSE_SYNC_AUTO_APPLY ?? String(file.syncAutoApply ?? "")) === "1" || String(file.syncAutoApply) === "true",
	};
}

function num(v: unknown): number | undefined {
	const n = Number(v);
	return v === undefined || v === null || v === "" || !Number.isFinite(n) || n <= 0 ? undefined : n;
}

/** Structured JSON-lines log (errors, recovery decisions). Never contains the API key. */
export function logTurn(cfg: MuseConfig, event: string, data: Record<string, unknown>) {
	try {
		mkdirSync(dirname(cfg.logFile), { recursive: true });
		appendFileSync(cfg.logFile, `${JSON.stringify({ ts: new Date().toISOString(), event, ...data })}\n`);
	} catch {
		/* logging must never break a turn */
	}
}

const PROVIDER = "muse";
const API = "muse-agent";
const TOOL = "muse_tool";

// Fallback catalogue (as reported by Muse's gateway `model.get`, Oct 2026).
const FALLBACK_MODELS = [
	{ id: "auto/auto", display_name: "Auto", context_window_tokens: 350000 },
	{ id: "ipnext/avocado-5.16-v0", display_name: "Avocado 5.16 v0", context_window_tokens: 350000 },
];

// ---------------------------------------------------------------------------
// Shared state between the provider stream and the pass-through tool
// ---------------------------------------------------------------------------
interface MuseToolEvent {
	id: string;
	call_id?: string;
	kind?: string;
	index?: number;
	icon?: string;
	title?: string;
	subtitle?: string;
	report?: string;
	status?: string;
	tool?: string;
	details?: unknown;
}
interface PendingTurn {
	text: string;
	sessionId?: string;
}
const toolResults = new Map<string, MuseToolEvent>(); // Pi toolCallId -> Muse event
const pendingByCall = new Map<string, PendingTurn>(); // Pi toolCallId -> reply to emit after tools
let forceNewThread = false;

/** Best-effort Muse tool name from the activity record (Muse only exposes summaries). */
function guessToolName(ev: MuseToolEvent): string {
	if (ev.tool) return ev.tool;
	const m = ev.report?.match(/^(?:Ran|Called|Used|Invoked|Executed)\s+`([A-Za-z_][\w.:-]*)`/);
	if (m) return m[1];
	const r = `${ev.report ?? ""} ${ev.title ?? ""}`.toLowerCase();
	if (/memory/.test(r)) return "memory_search";
	if (/search(ed|ing)? (the )?web|web search|browser/.test(r)) return "web_search";
	if (/shell command|\bexec(uted)? `|ran `[^`]*(&&|\||;)|running .*(&&|\|)/.test(r)) return "exec";
	if (/\bwrite|wrote|created\b/.test(r)) return "write";
	if (/\blist(ed)?\b|directory listing|enumerate/.test(r)) return "list";
	if (/\bread\b/.test(r)) return "read";
	if (/\bedit/.test(r)) return "edit";
	return ev.kind && ev.kind !== "tool_call" ? ev.kind : "tool";
}

/** Muse wraps interpolated values in Unicode FSI/PDI isolates; strip them for terminal output. */
const clean = (s?: string) => (s ?? "").replace(/[\u2068\u2069\u2066\u2067]/g, "");

function pickName(ev: MuseToolEvent & { firstName?: string }): string {
	const last = guessToolName(ev);
	const first = ev.firstName;
	return first && !["tool", "write", "read", "list"].includes(first) ? first : last;
}

function isFailure(status?: string): boolean {
	return /FAIL|ERROR|CANCEL/i.test(status ?? "");
}

// ---------------------------------------------------------------------------
// SSE client for /pi/v1/turn
// ---------------------------------------------------------------------------
async function* sse(res: Response): AsyncGenerator<{ event: string; data: any }> {
	const reader = res.body!.getReader();
	const dec = new TextDecoder();
	let buf = "";
	for (;;) {
		const { value, done } = await reader.read();
		if (done) break;
		buf += dec.decode(value, { stream: true });
		let i: number;
		while ((i = buf.indexOf("\n\n")) >= 0) {
			const raw = buf.slice(0, i);
			buf = buf.slice(i + 2);
			let event = "message";
			const data: string[] = [];
			for (const line of raw.split("\n")) {
				if (line.startsWith("event:")) event = line.slice(6).trim();
				else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
			}
			if (!data.length) continue;
			try {
				yield { event, data: JSON.parse(data.join("\n")) };
			} catch {
				/* ignore */
			}
		}
	}
}

function textOf(content: unknown): string {
	if (typeof content === "string") return content;
	if (Array.isArray(content))
		return content
			.filter((c: any) => c?.type === "text")
			.map((c: any) => c.text)
			.join("\n");
	return "";
}

// ---------------------------------------------------------------------------
// Provider stream
// ---------------------------------------------------------------------------
function streamMirror(model: Model<Api>, context: TranscriptContext, options?: SimpleStreamOptions): AssistantMessageEventStream {
	const stream = createAssistantMessageEventStream();
	const cfg = loadConfig();

	(async () => {
		const output: AssistantMessage = {
			role: "assistant",
			content: [],
			api: model.api,
			provider: model.provider,
			model: model.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "pending" as any,
			timestamp: Date.now(),
		};
		const pushText = (text: string) => {
			output.content.push({ type: "text", text: "" });
			const idx = output.content.length - 1;
			stream.push({ type: "text_start", contentIndex: idx, partial: output });
			(output.content[idx] as any).text = text;
			stream.push({ type: "text_delta", contentIndex: idx, delta: text, partial: output });
			stream.push({ type: "text_end", contentIndex: idx, content: text, partial: output });
		};

		try {
			const msgs = context.messages as any[];
			const last = msgs[msgs.length - 1];

			// ---- continuation after the pass-through tool results --------------------------
			if (last?.role === "toolResult") {
				const pend = pendingByCall.get(last.toolCallId);
				stream.push({ type: "start", partial: output });
				if (pend?.sessionId) output.responseId = `muse:${pend.sessionId}`;
				pushText(pend?.text?.trim() ? pend.text : "(Muse finished this turn.)");
				for (let i = msgs.length - 1; i >= 0 && msgs[i].role === "toolResult"; i--) pendingByCall.delete(msgs[i].toolCallId);
				output.stopReason = "stop";
				stream.push({ type: "done", reason: "stop", message: output });
				stream.end();
				return;
			}

			// ---- a new user turn ------------------------------------------------------------
			const lastUser = [...msgs].reverse().find((m) => m.role === "user");
			const userPrompt = textOf(lastUser?.content).trim();
			if (!userPrompt) throw new Error("empty prompt");
			const prompt = mirrorPrefix + userPrompt;
			let sessionId: string | undefined;
			if (!forceNewThread) {
				for (let i = msgs.length - 1; i >= 0; i--) {
					const rid = msgs[i]?.role === "assistant" ? msgs[i].responseId : undefined;
					if (typeof rid === "string" && rid.startsWith("muse:")) {
						sessionId = rid.slice(5).split("#ctx=")[0];
						break;
					}
				}
			}
			forceNewThread = false;

			let payload: any = { prompt, model: model.id, session_id: sessionId, settle_timeout: cfg.settleTimeout };
			const replaced = await options?.onPayload?.(payload, model as any);
			if (replaced !== undefined) payload = replaced;

			const res = await fetch(`${cfg.baseUrl}/pi/v1/turn`, {
				method: "POST",
				headers: { "content-type": "application/json", authorization: `Bearer ${options?.apiKey || cfg.apiKey}` },
				body: JSON.stringify(payload),
				signal: options?.signal,
			});
			await options?.onResponse?.({ status: res.status, headers: Object.fromEntries(res.headers.entries()) } as any, model as any);
			if (!res.ok || !res.body) throw new Error(`muse2api HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
			stream.push({ type: "start", partial: output });

			// live progress goes into a thinking block so the user sees what Muse is doing
			output.content.push({ type: "thinking", thinking: "" } as any);
			const thinkIdx = 0;
			stream.push({ type: "thinking_start", contentIndex: thinkIdx, partial: output });
			const think = (line: string) => {
				(output.content[thinkIdx] as any).thinking += line;
				stream.push({ type: "thinking_delta", contentIndex: thinkIdx, delta: line, partial: output });
			};

			let reply = "";
			let doneSession: string | undefined = sessionId;
			let gotText = false;
			const tools = new Map<string, MuseToolEvent>();
			const announced = new Set<string>();
			const goals = new Map<string, string>();

			for await (const { event, data } of sse(res)) {
				await options?.onProviderStreamEvent?.(data, model as any);
				if (event === "text") {
					reply += data.delta ?? "";
					if (!gotText) {
						gotText = true;
						think("✎ Muse is writing its reply…\n");
					}
				} else if (event === "text_done") {
					reply = data.text ?? reply;
				} else if (event === "final_text") {
					// authoritative transcript text (preferred over the DOM stream)
					if (data.text) reply = data.text;
				} else if (event === "tool") {
					for (const k of ["title", "subtitle", "report"]) data[k] = clean(data[k]);
					// the first ("Running: …") event often names the tool more precisely than the final summary
					const prev = tools.get(data.id) as any;
					const firstName = prev?.firstName ?? guessToolName(data);
					tools.set(data.id, { ...data, firstName });
					if (!announced.has(data.id)) {
						announced.add(data.id);
						think(`⚙ ${guessToolName(data)}: ${data.title ?? ""}\n`);
					}
				} else if (event === "activity") {
					const key = `${data.status_title}|${data.subtitle}`;
					if (goals.get(data.id) !== key) {
						goals.set(data.id, key);
						think(`▸ ${clean(data.title) || "Activity"} — ${clean(data.status_title)}${data.subtitle ? `: ${clean(data.subtitle)}` : ""}\n`);
					}
				} else if (event === "model") {
					think(`model: ${data.display_name ?? data.selected}\n`);
				} else if (event === "warning") {
					think(`⚠ ${data.message}\n`);
				} else if (event === "done") {
					doneSession = data.session_id ?? doneSession;
				} else if (event === "error") {
					throw new Error(data.message ?? "muse2api error");
				}
			}
			if (options?.signal?.aborted) throw new Error("Request was aborted");

			const th = output.content[thinkIdx] as any;
			if (!th.thinking) th.thinking = "Muse answered directly.";
			stream.push({ type: "thinking_end", contentIndex: thinkIdx, content: th.thinking, partial: output });
			if (doneSession) output.responseId = `muse:${doneSession}`;

			const ordered = [...tools.values()].sort((a, b) => (a.index ?? 0) - (b.index ?? 0));
			if (ordered.length === 0) {
				pushText(reply);
				output.stopReason = "stop";
				stream.push({ type: "done", reason: "stop", message: output });
				stream.end();
				return;
			}

			// Emit every Muse tool as a Pi tool call; results are served by the pass-through tool.
			for (const ev of ordered) {
				const id = `muse_${(ev.call_id ?? ev.id).replace(/[^A-Za-z0-9_-]/g, "").slice(-40)}`;
				const toolCall = {
					type: "toolCall" as const,
					id,
					name: TOOL,
					arguments: {
						tool: pickName(ev),
						title: ev.title ?? "",
						subtitle: ev.subtitle ?? "",
						status: ev.status ?? "",
						muse_call_id: ev.call_id ?? ev.id,
					},
				};
				output.content.push(toolCall as any);
				const idx = output.content.length - 1;
				stream.push({ type: "toolcall_start", contentIndex: idx, partial: output });
				stream.push({ type: "toolcall_delta", contentIndex: idx, delta: JSON.stringify(toolCall.arguments), partial: output });
				stream.push({ type: "toolcall_end", contentIndex: idx, toolCall: toolCall as any, partial: output });
				toolResults.set(id, ev);
				pendingByCall.set(id, { text: reply, sessionId: doneSession });
			}
			output.stopReason = "toolUse";
			stream.push({ type: "done", reason: "toolUse", message: output });
			stream.end();
		} catch (error) {
			output.stopReason = options?.signal?.aborted ? "aborted" : "error";
			output.errorMessage = error instanceof Error ? error.message : String(error);
			stream.push({ type: "error", reason: output.stopReason as any, error: output });
			stream.end();
		}
	})();
	return stream;
}

// ---------------------------------------------------------------------------
// Local-workspace modes: Muse works on the repo that is open in Pi
// ---------------------------------------------------------------------------
type Mode = "local" | "xml" | "sync" | "mirror";
const MODES: Mode[] = ["local", "xml", "sync", "mirror"];
let mirrorPrefix = "";
let modeOverride: Mode | undefined;
function currentMode(): Mode {
	const m = (modeOverride ?? loadConfig().mode) as Mode;
	return MODES.includes(m) ? m : "local";
}

/** Tools Pi currently offers (system messages carry toolsAdded / toolsRemoved). */
function collectTools(context: TranscriptContext): InbandTool[] {
	const map = new Map<string, InbandTool>();
	for (const m of context.messages as any[]) {
		if (m?.role !== "system") continue;
		for (const t of m.toolsAdded ?? []) map.set(t.name, t);
		for (const r of m.toolsRemoved ?? []) map.delete(r.name);
	}
	map.delete(TOOL);
	return [...map.values()];
}

/** JSON-schema-typed params that Muse's node registry would flatten to strings. */
function complexKeys(tool: InbandTool | undefined): Set<string> {
	const out = new Set<string>();
	for (const [k, s] of Object.entries((tool?.parameters?.properties ?? {}) as Record<string, unknown>)) {
		const t = collectSchemaTypes(s);
		if (t.has("array") || t.has("object")) out.add(k);
	}
	return out;
}

function decodeArgs(tool: InbandTool | undefined, args: Record<string, unknown>): Record<string, unknown> {
	const out: Record<string, unknown> = { ...args };
	const props = (tool?.parameters?.properties ?? {}) as Record<string, unknown>;
	for (const k of complexKeys(tool)) if (typeof out[k] === "string") out[k] = decodeValue(out[k] as string);
	for (const [k, s] of Object.entries(props)) {
		const t = collectSchemaTypes(s);
		if ((t.has("number") || t.has("integer")) && typeof out[k] === "string" && out[k] !== "") {
			const n = Number(out[k]);
			if (!Number.isNaN(n)) out[k] = n;
		}
		if (t.has("boolean") && typeof out[k] === "string") out[k] = out[k] === "true";
	}
	return out;
}

/** Pi tool definitions -> Muse node `commands_v2` (decoded from muse.ai's bundles, see docs/PROTOCOL.md). */
function nodeCommands(tools: InbandTool[]) {
	const cmds: Record<string, any> = {};
	for (const t of tools) {
		const props = (t.parameters?.properties ?? {}) as Record<string, any>;
		const req = new Set<string>(t.parameters?.required ?? []);
		const required: Record<string, any> = {};
		const optional: Record<string, any> = {};
		for (const [k, s] of Object.entries(props)) {
			const types = collectSchemaTypes(s);
			types.delete("null");
			let type = types.size === 1 ? [...types][0] : "string";
			let description = String(s?.description ?? "");
			if (type === "array" || type === "object") {
				description += ` (pass as a JSON-encoded ${type}; schema: ${JSON.stringify(s).slice(0, 500)})`;
				type = "string";
			} else if (type === "integer") type = "number";
			(req.has(k) ? required : optional)[k] = { type, description };
		}
		cmds[t.name] = { description: String(t.description ?? "").slice(0, 1500), required, optional, timeout_ms: 120000 };
	}
	return cmds;
}

function nodeIdentity(cwd: string) {
	const host = hostname().replace(/[^A-Za-z0-9-]/g, "-").slice(0, 40) || "local";
	return { node_id: `pi-${host}`.toLowerCase(), display_name: `Pi · ${basename(cwd)} (${host})` };
}

function lastMuseRef(msgs: any[]): { session?: string; ctx?: string } {
	for (let i = msgs.length - 1; i >= 0; i--) {
		const rid = msgs[i]?.role === "assistant" ? msgs[i].responseId : undefined;
		if (typeof rid === "string" && rid.startsWith("muse:")) {
			const [session, ctx] = rid.slice(5).split("#ctx=");
			return { session: session || undefined, ctx };
		}
	}
	return {};
}
function lastSessionId(msgs: any[]): string | undefined {
	return lastMuseRef(msgs).session;
}

// ---- Pi context forwarding --------------------------------------------------------------------
/**
 * Pi's assembled system prompt, replayed from the transcript: the leading system message declares
 * `content` + named `sections` (preamble, project_context = AGENTS.md/CLAUDE.md files, skills index,
 * extension snippets, ...); later system messages patch sections by name (null removes one).
 */
function assembledSystemPrompt(msgs: any[]): string {
	let base = "";
	const sections = new Map<string, string>();
	let first = true;
	for (const m of msgs) {
		if (m?.role !== "system") continue;
		const text = textOf(m.content);
		if (first) base = text;
		else if (text) base += `\n\n${text}`;
		first = false;
		for (const [k, v] of Object.entries((m.sections ?? {}) as Record<string, string | null>)) {
			if (v === null) sections.delete(k);
			else sections.set(k, v);
		}
	}
	return [base, ...sections.values()].filter((x) => x && x.trim()).join("\n\n").trim();
}

function ctxHash(text: string): string {
	return createHash("sha256").update(text).digest("hex").slice(0, 12);
}

/** Compact transcript of earlier Pi turns, for when a fresh Muse thread starts mid-conversation. */
function priorHistory(msgs: any[], maxChars = 12000): string {
	const lastUserIdx = msgs.map((m) => m?.role).lastIndexOf("user");
	const lines: string[] = [];
	for (const m of msgs.slice(0, Math.max(0, lastUserIdx))) {
		if (m?.role === "user") lines.push(`USER: ${textOf(m.content).trim().slice(0, 2000)}`);
		else if (m?.role === "assistant") {
			const t = textOf(m.content).trim();
			const calls = (m.content ?? []).filter((c: any) => c?.type === "toolCall").map((c: any) => c.name);
			if (t || calls.length) lines.push(`ASSISTANT: ${t.slice(0, 2000)}${calls.length ? ` [tools: ${calls.join(", ")}]` : ""}`);
		}
	}
	let out = lines.join("\n");
	if (out.length > maxChars) out = `…${out.slice(-maxChars)}`;
	return out;
}

function contextBlock(system: string, history: string): string {
	let s = "";
	if (system)
		s +=
			`[Pi context] Below is the system prompt Pi (the coding agent I am using) built for this project. Treat it as ` +
			`your standing instructions for this whole conversation, with higher priority than your defaults: it contains ` +
			`the project instructions (AGENTS.md / CLAUDE.md files, global and per-directory) and the available skills. ` +
			`Skills: when a task matches a skill's description, first read its SKILL.md (at the absolute <location> given) ` +
			`with the device's read command, then follow it. Absolute paths are allowed for skill files.\n` +
			`<pi_system_prompt>\n${system}\n</pi_system_prompt>\n\n`;
	if (history) s += `[Pi context] Earlier conversation in this Pi session (for continuity):\n<pi_history>\n${history}\n</pi_history>\n\n`;
	return s;
}

function newOutput(model: Model<Api>): AssistantMessage {
	return {
		role: "assistant",
		content: [],
		api: model.api,
		provider: model.provider,
		model: model.id,
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason: "pending" as any,
		timestamp: Date.now(),
	};
}

/** Small helper that owns one AssistantMessage stream. */
function emitter(stream: AssistantMessageEventStream, output: AssistantMessage) {
	let thinkIdx = -1;
	return {
		start() {
			stream.push({ type: "start", partial: output });
		},
		think(line: string) {
			if (thinkIdx < 0) {
				output.content.push({ type: "thinking", thinking: "" } as any);
				thinkIdx = output.content.length - 1;
				stream.push({ type: "thinking_start", contentIndex: thinkIdx, partial: output });
			}
			(output.content[thinkIdx] as any).thinking += line;
			stream.push({ type: "thinking_delta", contentIndex: thinkIdx, delta: line, partial: output });
		},
		endThinking() {
			if (thinkIdx >= 0) {
				const th = output.content[thinkIdx] as any;
				stream.push({ type: "thinking_end", contentIndex: thinkIdx, content: th.thinking, partial: output });
				thinkIdx = -2; // closed; never reopen in this message
			}
		},
		text(text: string) {
			this.endThinking();
			output.content.push({ type: "text", text: "" });
			const idx = output.content.length - 1;
			stream.push({ type: "text_start", contentIndex: idx, partial: output });
			(output.content[idx] as any).text = text;
			stream.push({ type: "text_delta", contentIndex: idx, delta: text, partial: output });
			stream.push({ type: "text_end", contentIndex: idx, content: text, partial: output });
		},
		toolCall(id: string, name: string, args: Record<string, unknown>) {
			this.endThinking();
			const toolCall = { type: "toolCall" as const, id, name, arguments: args };
			output.content.push(toolCall as any);
			const idx = output.content.length - 1;
			stream.push({ type: "toolcall_start", contentIndex: idx, partial: output });
			stream.push({ type: "toolcall_delta", contentIndex: idx, delta: JSON.stringify(args), partial: output });
			stream.push({ type: "toolcall_end", contentIndex: idx, toolCall: toolCall as any, partial: output });
		},
		finish(reason: "stop" | "toolUse") {
			this.endThinking();
			output.stopReason = reason;
			stream.push({ type: "done", reason, message: output });
			stream.end();
		},
		fail(error: unknown, aborted: boolean) {
			output.stopReason = aborted ? "aborted" : "error";
			output.errorMessage = error instanceof Error ? error.message : String(error);
			stream.push({ type: "error", reason: output.stopReason as any, error: output });
			stream.end();
		},
	};
}

/** muse2api reports some failures in Chinese; translate the common ones and add a recovery hint. */
const MUSE_ERRORS: [RegExp, string][] = [
	[/首字响应超时/, "timed out waiting for Muse's first response token"],
	[/Still sending|连接超时/, "Muse cloud VM connection timed out"],
	[/等待助手回复超时/, "timed out waiting for Muse's reply"],
];
function describeMuseError(error: unknown): Error {
	let msg = error instanceof Error ? error.message : String(error);
	for (const [re, en] of MUSE_ERRORS) if (re.test(msg)) msg = `${msg} (${en})`;
	if (!/aborted|Not auto-continuing|gave up|NOT sent/i.test(msg)) msg += ". The Muse turn was interrupted; send a follow-up such as \"continue\" to resume in the same Muse thread.";
	return new Error(msg);
}

function progressLine(event: string, data: any): string | undefined {
	if (event === "tool") {
		if (data.status === "RUNNING") return undefined;
		return `· Muse ${clean(data.title)}${data.subtitle ? ` — ${clean(data.subtitle)}` : ""}\n`;
	}
	if (event === "activity" && data.finished) return `▸ ${clean(data.title)}: ${clean(data.finish?.status ?? "")}\n`;
	if (event === "warning") return `⚠ ${data.message}\n`;
	if (event === "node") return data.registered ? "⇄ local device registered with Muse\n" : `⚠ node.register failed: ${data.error}\n`;
	return undefined;
}

// ---- approach A: Muse node bridge ------------------------------------------------------------
interface LiveTurn {
	it: AsyncGenerator<{ event: string; data: any }>;
	ctl: AbortController;
	turnId?: string;
	reply: string;
	session?: string;
	pending: Map<string, string>; // Pi toolCallId -> Muse request_id
	tools: InbandTool[];
	seenTools: Set<string>;
	ctx?: string;
	owner: string;
	payload: any;
	gotDone?: boolean;
	recovering?: boolean;
	retries: number;
	lastStep?: LastStep;
	startedAt: number;
	note?: string;
	attachTimer?: ReturnType<typeof setTimeout>;
	cancelledByUs?: boolean;
}
let live: LiveTurn | undefined;
/** at most one in-flight request per Muse thread */
export const threadLock = new ThreadLock();
/** test hook: counts of what the extension did */
export const stats = { continuations: 0, presented: 0, stopped: 0, rejected: 0, attached: 0, guarded: 0 };

class LockRejected extends Error {}

function lastTodo(msgs: any[]): string | undefined {
	for (let i = msgs.length - 1; i >= 0; i--) {
		const m = msgs[i];
		if (m?.role !== "assistant" || !Array.isArray(m.content)) continue;
		for (const c of [...m.content].reverse()) if (c?.type === "toolCall" && /todo/i.test(c.name ?? "")) return summarize(c.arguments, 400);
	}
	return undefined;
}

/** identifies this Pi process to muse2api (server-side per-thread lock / attach ownership) */
export const CLIENT_ID = randomUUID();

function turnBody(base: any, cfg: MuseConfig) {
	const b = { ...base, client_id: CLIENT_ID };
	if (cfg.firstByteTimeout) b.first_byte_timeout = cfg.firstByteTimeout;
	if (cfg.idleTimeout) b.idle_timeout = cfg.idleTimeout;
	return b;
}

async function openTurn(cfg: MuseConfig, apiKey: string, body: any, ctl: AbortController) {
	let res: Response;
	try {
		res = await fetch(`${cfg.baseUrl}/pi/v1/turn`, {
			method: "POST",
			headers: { "content-type": "application/json", authorization: `Bearer ${apiKey}` },
			body: JSON.stringify(body),
			signal: ctl.signal,
		});
	} catch (e) {
		throw Object.assign(new Error(`muse2api request failed: ${(e as any)?.cause?.code ?? ""} ${(e as any)?.message ?? e}`.trim()), { name: (e as any)?.name });
	}
	if (res.status === 409) {
		const j: any = await res.json().catch(() => ({}));
		throw Object.assign(
			new Error(`Muse thread ${body.session_id ?? ""} is busy: another request (${j.phase ?? "running"}${j.owner_is_you ? "" : ", started from another Pi window"}) is still in flight. Your message was NOT sent; wait for it to finish and send again.`),
			{ httpStatus: 409, busy: true },
		);
	}
	if (!res.ok || !res.body) throw Object.assign(new Error(`muse2api HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`), { httpStatus: res.status });
	return sse(res);
}

async function openAttach(cfg: MuseConfig, session: string, ctl: AbortController) {
	const res = await fetch(`${cfg.baseUrl}/pi/v1/thread/attach`, {
		method: "POST",
		headers: { "content-type": "application/json", authorization: `Bearer ${cfg.apiKey}` },
		body: JSON.stringify({ session_id: session, client_id: CLIENT_ID }),
		signal: ctl.signal,
	});
	if (!res.ok || !res.body) throw Object.assign(new Error(`attach HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`), { httpStatus: res.status });
	return sse(res);
}

function recoveryDeps(cfg: MuseConfig, signal: AbortSignal | undefined, think: (s: string) => void) {
	return {
		getState: (sid: string) => backendWith(cfg, "/pi/v1/thread/state", { session_id: sid, client_id: CLIENT_ID }),
		cancel: (sid: string) => backendWith(cfg, "/pi/v1/thread/cancel", { session_id: sid, client_id: CLIENT_ID }),
		sleep: (ms: number) => sleep(ms, signal),
		log: (ev: string, d: Record<string, unknown>) => {
			logTurn(cfg, ev, d);
			if (ev === "thread_state" && d.status === "running") think("… Muse is still working on this thread; waiting for it\n");
			if (ev === "thread_cancel") think("… Muse turn exceeded the max wait; cancelling it\n");
		},
		aborted: () => !!signal?.aborted,
	};
}

function waitCfg(cfg: MuseConfig) {
	return { autoContinue: true, maxRetries: cfg.maxRetries, backoffMs: [], maxWaitMs: cfg.maxWaitSeconds * 1000, pollMs: cfg.pollSeconds * 1000 };
}

async function backendWith(cfg: MuseConfig, path: string, body: unknown): Promise<any> {
	const res = await fetch(`${cfg.baseUrl}${path}`, {
		method: "POST",
		headers: { "content-type": "application/json", authorization: `Bearer ${cfg.apiKey}` },
		body: JSON.stringify(body),
	});
	if (!res.ok) throw new Error(`${path} HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
	return res.json();
}

function localPreamble(cwd: string, node: { node_id: string; display_name: string }, tools: InbandTool[]): string {
	return (
		`[Pi bridge] My local project "${basename(cwd)}" is open in Pi on the paired device "${node.display_name}" ` +
		`(device_id ${node.node_id}, online now). For this request the project lives ONLY on that device: inspect and ` +
		`change files exclusively by invoking that device's commands (${tools.map((t) => t.name).join(", ")}) ` +
		`with paths relative to the project root (absolute paths only for Pi skill files). Do not use your own VM shell, files or workspace for project work, ` +
		`and do not ask me to upload anything. Invoke one command at a time and wait for its result. ` +
		`When finished, reply with a short summary of what you changed.]\n\n`
	);
}

async function postResult(cfg: MuseConfig, turnId: string | undefined, requestId: string, ok: boolean, text: string) {
	const body = ok
		? { turn_id: turnId, request_id: requestId, ok: true, payload: { output: text } }
		: { turn_id: turnId, request_id: requestId, ok: false, error: text.slice(0, 4000) };
	const res = await fetch(`${cfg.baseUrl}/pi/v1/invoke_result`, {
		method: "POST",
		headers: { "content-type": "application/json", authorization: `Bearer ${cfg.apiKey}` },
		body: JSON.stringify(body),
	});
	if (!res.ok) throw new Error(`invoke_result HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
}

export function streamLocal(model: Model<Api>, context: TranscriptContext, options?: SimpleStreamOptions): AssistantMessageEventStream {
	const stream = createAssistantMessageEventStream();
	const cfg = loadConfig();
	const output = newOutput(model);
	const e = emitter(stream, output);
	(async () => {
		let turn: LiveTurn | undefined;
		try {
			const msgs = context.messages as any[];
			const last = msgs[msgs.length - 1];
			const onAbort = () => {
				const cur = live;
				cur?.ctl.abort();
				// Esc means stop: the muse2api request ending would otherwise leave Muse running in the background
				if (cur?.session) backendWith(cfg, "/pi/v1/thread/cancel", { session_id: cur.session, client_id: CLIENT_ID }).catch(() => {});
			};
			options?.signal?.addEventListener("abort", onAbort, { once: true });
			if (last?.role === "toolResult" && live) {
				// hand the results of Pi's local tool runs back to Muse
				e.start();
				const trailing: any[] = [];
				for (let i = msgs.length - 1; i >= 0 && msgs[i].role === "toolResult"; i--) trailing.unshift(msgs[i]);
				for (const r of trailing) {
					const rid = live.pending.get(r.toolCallId);
					if (!rid) continue;
					live.pending.delete(r.toolCallId);
					const text = textOf(r.content) || "(no output)";
					if (live.lastStep) Object.assign(live.lastStep, { status: r.isError ? "error" : "ok", result: summarize(text, 300), finishedAt: Date.now() });
					await postResult(cfg, live.turnId, rid, !r.isError, text);
				}
			} else if (last?.role === "toolResult") {
				// results for a turn this process no longer owns (e.g. resumed session)
				e.start();
				e.text("(The Muse turn that requested these tool results is no longer running.)");
				e.finish("stop");
				return;
			} else {
				const ref = forceNewThread ? {} : lastMuseRef(msgs);
				const sessionId = ref.session;
				if (live?.recovering || (sessionId && threadLock.holder(sessionId) && threadLock.holder(sessionId) !== live?.owner)) {
					stats.rejected++;
					logTurn(cfg, "lock_rejected", { session: sessionId ?? live?.session });
					throw new LockRejected(
						`Muse is still recovering an interrupted turn in thread ${live?.session ?? sessionId}; your message was NOT sent (one request per Muse thread). Wait for it to finish, or press Esc to cancel it, then send again.`,
					);
				}
				if (live) {
					threadLock.release(live.session, live.owner);
					live.ctl.abort();
				}
				live = undefined;
				const lastUser = [...msgs].reverse().find((m) => m.role === "user");
				const userText = textOf(lastUser?.content).trim();
				if (!userText) throw new Error("empty prompt");
				const tools = collectTools(context);
				const cwd = process.cwd();
				const node = nodeIdentity(cwd);
				forceNewThread = false;
				const system = assembledSystemPrompt(msgs);
				const hash = ctxHash(system);
				const sendSystem = !sessionId || ref.ctx !== hash;
				const ctxText = contextBlock(sendSystem ? system : "", sessionId ? "" : priorHistory(msgs));
				let payload: any = turnBody(
					{
						prompt: localPreamble(cwd, node, tools) + ctxText + (ctxText ? "[Pi] Current request:\n" : "") + userText,
						model: model.id,
						session_id: sessionId,
						settle_timeout: Math.min(cfg.settleTimeout, 20),
						node: { ...node, platform: process.platform, commands_v2: nodeCommands(tools) },
					},
					cfg,
				);
				const replaced = await options?.onPayload?.(payload, model as any);
				if (replaced !== undefined) payload = replaced;
				if (process.env.PI_MUSE_DUMP) writeFileSync(process.env.PI_MUSE_DUMP, JSON.stringify({ payload, roles: msgs.map((m) => m.role) }, null, 1));
				const owner = randomUUID();
				if (sessionId) threadLock.tryAcquire(sessionId, owner);
				const ctl = new AbortController();
				live = { it: undefined as any, ctl, reply: "", session: sessionId, pending: new Map(), tools, seenTools: new Set(), ctx: hash, owner, payload, retries: 0, startedAt: Date.now() };
				turn = live;
				e.start();
				if (sessionId) {
					// state check before EVERY send: never put a second request into a thread that is still running
					live.recovering = true;
					const pre = await recoverTurn(sessionId, waitCfg(cfg), recoveryDeps(cfg, options?.signal, (x) => e.think(x)), { foreignIsBusy: true, noCancel: true });
					live.recovering = false;
					logTurn(cfg, "preflight", { session: sessionId, ...pre });
					if (pre.action === "stop") {
						stats.stopped++;
						throw Object.assign(new Error(`Your message was NOT sent: ${pre.reason}. Wait for it to finish and send again.`), { preflight: true });
					}
					if (pre.action === "attach") {
						stats.attached++;
						e.think("⇄ Muse is still working on the previous turn of this thread; attached to it instead of sending your message\n");
						live.note = "(Muse was still finishing the previous turn, so your message was not sent; this is that turn's result. Send your message again if it is still needed.)";
						live.it = await openAttach(cfg, sessionId, ctl);
					} else if (pre.action === "present" && pre.wasRunning) {
						stats.presented++;
						live.reply = pre.reply;
						live.note = "(Muse was still finishing the previous turn, so your message was not sent; this is that turn's result. Send your message again if it is still needed.)";
						live.it = (async function* () {})();
						live.gotDone = true;
					}
				}
				if (!live.it) live.it = await openTurn(cfg, options?.apiKey || cfg.apiKey, payload, ctl);
				e.think(`Muse is working on ${basename(cwd)} through the local device bridge…\n`);
			}

			turn = live!;
			const t = turn;
			const todo = lastTodo(msgs);
			/** Returns a reply to present (Muse finished on its own) or undefined after resuming the stream; throws to stop. */
			const recover = async (err: any): Promise<string | undefined> => {
				const cls = classifyMuseError(err);
				const info = {
					session: t.session, turn_id: t.turnId, message: String(err?.message ?? err), code: err?.code ?? null, phase: err?.phase ?? null,
					timeout_s: err?.timeout_s ?? null, waited_s: err?.waited_s ?? null, elapsed_s: err?.elapsed_s ?? Math.round((Date.now() - t.startedAt) / 100) / 10,
					started_at: err?.started_at ?? new Date(t.startedAt).toISOString(), failed_at: err?.failed_at ?? new Date().toISOString(),
					bytes_received: err?.bytes_received ?? null, events_received: err?.events_received ?? null, muse_status: err?.muse_status ?? null,
					last_step: t.lastStep ?? null, backend_last_invoke: err?.last_invoke ?? null, classification: cls, retry: t.retries,
				};
				logTurn(cfg, "turn_error", info);
				if (options?.signal?.aborted || !cls.retryable || !cfg.autoContinue) throw err;
				if (t.retries >= cfg.maxRetries) {
					stats.stopped++;
					throw Object.assign(new Error(`${err?.message ?? err}. Auto-continue gave up after ${cfg.maxRetries} consecutive retries.`), { noHint: false });
				}
				t.recovering = true;
				try {
					const delay = (cfg.backoffSeconds[t.retries] ?? cfg.backoffSeconds[cfg.backoffSeconds.length - 1] ?? 10) * 1000;
					t.retries++;
					e.think(`⟳ Muse turn interrupted (${cls.reason}); checking thread state in ${delay / 1000}s (auto-continue ${t.retries}/${cfg.maxRetries})…\n`);
					await sleep(delay, options?.signal);
					const r = await recoverTurn(t.session, waitCfg(cfg), recoveryDeps(cfg, options?.signal, (x) => e.think(x)), { foreignIsBusy: true });
					logTurn(cfg, "recovery", { session: t.session, ...r });
					if (r.action === "stop") {
						stats.stopped++;
						throw Object.assign(new Error(`${err?.message ?? err}. Not auto-continuing: ${r.reason}.`), { noHint: false });
					}
					if (r.action === "present") {
						stats.presented++;
						e.think(`✓ Muse finished the interrupted turn on its own (after ${Math.round(r.waitedMs / 1000)}s); showing its result\n`);
						return r.reply;
					}
					if (r.action === "attach") {
						stats.attached++;
						e.think("⇄ muse2api kept the Muse page and device online; re-attaching to the running turn (nothing re-sent)\n");
						t.ctl.abort();
						t.ctl = new AbortController();
						t.gotDone = false;
						t.pending = new Map();
						try {
							t.it = await openAttach(cfg, t.session!, t.ctl);
						} catch (e2) {
							return await recover(e2);
						}
						// bounded wait: past maxWaitSeconds ask muse2api to cancel the held turn (it then reports done/interrupted)
						if (t.attachTimer) clearTimeout(t.attachTimer);
						t.attachTimer = setTimeout(() => {
							t.cancelledByUs = true;
							logTurn(cfg, "thread_cancel", { session: t.session, reason: "max wait while attached" });
							e.think("… Muse turn exceeded the max wait; cancelling it\n");
							backendWith(cfg, "/pi/v1/thread/cancel", { session_id: t.session, client_id: CLIENT_ID }).catch(() => {});
						}, cfg.maxWaitSeconds * 1000);
						return undefined;
					}
					const ls = t.lastStep;
					if (ls?.nonIdempotent && ls.finishedAt && Date.now() - ls.finishedAt < cfg.nonIdempotentWindowSeconds * 1000) {
						stats.guarded++;
						logTurn(cfg, "guard_stop", { session: t.session, last_step: ls });
						throw Object.assign(
							new Error(
								`${err?.message ?? err}. Not auto-continuing: the turn broke right after a non-idempotent step (${ls.tool} ${summarize(ls.args, 120)}: ${ls.nonIdempotent}). Check its effect, then send "continue" to resume in the same Muse thread`,
							),
							{ noHint: true },
						);
					}
					const msg = continuationMessage(t.lastStep, todo);
					stats.continuations++;
					logTurn(cfg, "continue", { session: t.session, cancelled: r.cancelled, status: r.status, message: msg });
					e.think(`↻ resuming Muse thread ${t.session}${r.cancelled ? " (stale turn cancelled)" : ""}\n`);
					t.ctl.abort();
					t.ctl = new AbortController();
					t.gotDone = false;
					t.pending = new Map();
					const body = { ...t.payload, prompt: msg, session_id: t.session };
					t.payload = body;
					t.startedAt = Date.now();
					try {
						t.it = await openTurn(cfg, options?.apiKey || cfg.apiKey, body, t.ctl);
					} catch (e2) {
						return await recover(e2); // a failed resume counts as the next consecutive retry
					}
					return undefined;
				} finally {
					t.recovering = false;
				}
			};

			let presented: string | undefined;
			for (;;) {
				let step: IteratorResult<{ event: string; data: any }>;
				try {
					step = await t.it.next();
				} catch (err) {
					presented = await recover(err);
					if (presented !== undefined) break;
					continue;
				}
				if (step.done) {
					if (!t.gotDone) {
						presented = await recover(new Error("muse2api stream ended without a done event (mid-stream disconnect)"));
						if (presented !== undefined) break;
						continue;
					}
					break;
				}
				const { event, data } = step.value;
				await options?.onProviderStreamEvent?.(data, model as any);
				if (event === "start") t.turnId = data.turn_id;
				else if (event === "invoke") {
					t.turnId = data.turn_id ?? t.turnId;
					const tool = t.tools.find((x) => x.name === data.command);
					if (!tool) {
						await postResult(cfg, t.turnId, data.request_id, false, `unknown command ${data.command}`);
						continue;
					}
					const id = `muse_${String(data.request_id).replace(/[^A-Za-z0-9]/g, "").slice(0, 32)}`;
					t.pending.set(id, data.request_id);
					t.retries = 0;
					const args = decodeArgs(tool, data.params ?? {});
					let existed: boolean | undefined;
					try {
						if (typeof (args as any).path === "string") existed = existsSync(resolve(process.cwd(), (args as any).path));
					} catch {
						existed = undefined;
					}
					t.lastStep = { tool: data.command, args, status: "pending", nonIdempotent: nonIdempotentReason(data.command, args, existed) };
					if (t.session) output.responseId = `muse:${t.session}#ctx=${t.ctx}`;
					e.toolCall(id, data.command, args);
					options?.signal?.removeEventListener("abort", onAbort);
					e.finish("toolUse");
					return;
				} else if (event === "text_done") {
					t.reply = data.text ?? t.reply;
					t.retries = 0;
				} else if (event === "final_text") t.reply = data.text || t.reply;
				else if (event === "session" && data.session_id) {
					if (!t.session) {
						t.session = data.session_id;
						threadLock.tryAcquire(t.session!, t.owner);
					}
				} else if (event === "done") {
					t.session = data.session_id ?? t.session;
					if (t.attachTimer) clearTimeout(t.attachTimer);
					if (data.status && data.status !== "completed" && t.cancelledByUs) {
						t.cancelledByUs = false;
						presented = await recover(Object.assign(new Error(`Muse turn cancelled after the max wait (${cfg.maxWaitSeconds}s)`), { code: "max_wait_timeout" }));
						if (presented !== undefined) break;
						continue;
					}
					t.gotDone = true;
				} else if (event === "error") {
					presented = await recover(Object.assign(new Error(data.message ?? "muse2api error"), data));
					if (presented !== undefined) break;
				} else {
					const key = event === "tool" ? `${data.id}|${data.status}` : "";
					if (key && t.seenTools.has(key)) continue;
					if (key) t.seenTools.add(key);
					const line = progressLine(event, data);
					if (line) e.think(line);
				}
			}
			if (presented !== undefined) t.reply = presented;
			if (t.attachTimer) clearTimeout(t.attachTimer);
			if (t.note) t.reply = `${t.note}\n\n${t.reply}`;
			threadLock.release(t.session, t.owner);
			t.ctl.abort();
			live = undefined;
			if (options?.signal?.aborted) throw new Error("Request was aborted");
			if (t.session) output.responseId = `muse:${t.session}#ctx=${t.ctx}`;
			e.text(t.reply.trim() || "(Muse finished this turn.)");
			e.finish("stop");
		} catch (error) {
			if (error instanceof LockRejected) {
				e.fail(error, false);
				return;
			}
			// keep the Muse thread reference so a manual follow-up resumes the same thread
			const cur = turn ?? live;
			if (cur?.session) output.responseId = `muse:${cur.session}#ctx=${cur.ctx}`;
			if (cur) {
				if (cur.attachTimer) clearTimeout(cur.attachTimer);
				threadLock.release(cur.session, cur.owner);
				cur.recovering = false;
				cur.ctl.abort();
			}
			if (live === cur) live = undefined;
			e.fail(describeMuseError(error), !!options?.signal?.aborted);
		}
	})();
	return stream;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise((res, rej) => {
		if (signal?.aborted) return rej(new Error("Request was aborted"));
		const t = setTimeout(res, ms);
		signal?.addEventListener("abort", () => {
			clearTimeout(t);
			rej(new Error("Request was aborted"));
		}, { once: true });
	});
}

// ---- approach B: XML tool protocol ---------------------------------------------------------
async function runMuseTurn(
	cfg: MuseConfig,
	payload: any,
	signal: AbortSignal | undefined,
	onEvent: (event: string, data: any) => void,
): Promise<{ reply: string; session?: string }> {
	const res = await fetch(`${cfg.baseUrl}/pi/v1/turn`, {
		method: "POST",
		headers: { "content-type": "application/json", authorization: `Bearer ${cfg.apiKey}` },
		body: JSON.stringify(payload),
		signal,
	});
	if (!res.ok || !res.body) throw new Error(`muse2api HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
	let reply = "";
	let session: string | undefined = payload.session_id;
	for await (const { event, data } of sse(res)) {
		if (event === "text_done") reply = data.text ?? reply;
		else if (event === "final_text") reply = data.text || reply;
		else if (event === "done") session = data.session_id ?? session;
		else if (event === "error") throw new Error(data.message ?? "muse2api error");
		else onEvent(event, data);
	}
	return { reply, session };
}

function xmlPreamble(cwd: string, tools: InbandTool[]): string {
	return (
		`[Pi bridge — read carefully] I am working in a local project "${basename(cwd)}" on my own machine. ` +
		`You cannot see it with your VM tools: your VM shell, files and workspace are NOT this project, so do not use them ` +
		`for project work. Instead act through the functions below; my machine runs them and returns the results in my next message.\n\n` +
		renderToolsPrompt(tools) +
		`\n\nPaths are relative to the project root. Call functions until the task is done, then answer without any <invoke>.\n\n---\nTask: `
	);
}

function streamXml(model: Model<Api>, context: TranscriptContext, options?: SimpleStreamOptions): AssistantMessageEventStream {
	const stream = createAssistantMessageEventStream();
	const cfg = loadConfig();
	const output = newOutput(model);
	const e = emitter(stream, output);
	(async () => {
		try {
			const msgs = context.messages as any[];
			const last = msgs[msgs.length - 1];
			const tools = collectTools(context);
			let sessionId = lastSessionId(msgs);
			let prompt: string;
			if (last?.role === "toolResult") {
				const trailing: any[] = [];
				for (let i = msgs.length - 1; i >= 0 && msgs[i].role === "toolResult"; i--) trailing.unshift(msgs[i]);
				prompt =
					renderToolResponses(trailing.map((r) => ({ text: (r.isError ? "ERROR: " : "") + (textOf(r.content) || "(no output)") }))) +
					`\n\n[Pi bridge] Those are the results of your calls, in order. Continue: emit further <invoke> calls if needed, otherwise give the final answer.`;
			} else {
				const lastUser = [...msgs].reverse().find((m) => m.role === "user");
				const userText = textOf(lastUser?.content).trim();
				if (!userText) throw new Error("empty prompt");
				if (forceNewThread) sessionId = undefined;
				forceNewThread = false;
				prompt = xmlPreamble(process.cwd(), tools) + userText;
			}
			e.start();
			e.think("Muse is thinking (XML tool protocol)…\n");
			const seen = new Set<string>();
			let payload: any = { prompt, model: model.id, session_id: sessionId, settle_timeout: Math.min(cfg.settleTimeout, 15) };
			const replaced = await options?.onPayload?.(payload, model as any);
			if (replaced !== undefined) payload = replaced;
			const { reply, session } = await runMuseTurn(cfg, payload, options?.signal, (ev, data) => {
				const key = ev === "tool" ? `${data.id}|${data.status}` : "";
				if (key && seen.has(key)) return;
				if (key) seen.add(key);
				const line = progressLine(ev, data);
				if (line) e.think(line);
			});
			if (session) output.responseId = `muse:${session}`;
			const { calls, prose } = parseInvokes(reply, tools);
			const known = calls.filter((c) => tools.some((t) => t.name === c.name));
			if (known.length === 0) {
				e.text(reply.trim() || "(Muse finished this turn.)");
				e.finish("stop");
				return;
			}
			if (prose) e.think(`${prose}\n`);
			for (const c of known) e.toolCall(`xml_${randomUUID().replace(/-/g, "").slice(0, 24)}`, c.name, c.arguments);
			e.finish("toolUse");
		} catch (error) {
			e.fail(error, !!options?.signal?.aborted);
		}
	})();
	return stream;
}

// ---- approach C: snapshot sync (/muse-push, /muse-pull, mode "sync") -------------------------
const SYNC_SKIP = /(^|\/)(\.git|node_modules|__pycache__|\.venv|venv|dist|build|\.pytest_cache)(\/|$)/;
const syncRemote = (cwd: string) => `workspace/pi-sync/${basename(cwd).replace(/[^A-Za-z0-9._-]/g, "_")}`;
const syncStatePath = (cwd: string) => {
	const git = join(cwd, ".git");
	return existsSync(git) ? join(git, "pi-muse-sync.json") : join(cwd, ".pi-muse-sync.json");
};
const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");

function localFiles(cwd: string): string[] {
	try {
		const out = execFileSync("git", ["ls-files", "-co", "--exclude-standard", "-z"], { cwd, maxBuffer: 64 << 20 }).toString();
		return out.split("\0").filter((p) => p && !SYNC_SKIP.test(p) && existsSync(join(cwd, p)));
	} catch {
		throw new Error("muse sync needs a git repository (files are taken from `git ls-files`)");
	}
}

async function syncPush(cwd: string): Promise<string> {
	const remote = syncRemote(cwd);
	const files = localFiles(cwd).filter((p) => statSync(join(cwd, p)).size <= 2 * 1024 * 1024);
	const base: Record<string, string> = {};
	let n = 0;
	// start from a clean copy so files left over from an earlier round are not mistaken for Muse's work
	await backend("/pi/v1/fs/delete", { paths: [remote], recursive: true }).catch(() => undefined);
	for (let i = 0; i < files.length; i += 25) {
		const batch = files.slice(i, i + 25).map((p) => {
			const buf = readFileSync(join(cwd, p));
			base[p] = sha(buf);
			return { path: `${remote}/${p}`, data_base64: buf.toString("base64") };
		});
		const r = await backend("/pi/v1/fs/write", { files: batch });
		const bad = (r.files ?? []).filter((f: any) => f.error);
		if (bad.length) throw new Error(`fs.write failed for ${bad[0].path}: ${bad[0].error}`);
		n += batch.length;
	}
	writeFileSync(syncStatePath(cwd), JSON.stringify({ remote, pushedAt: Date.now(), base }, null, 1));
	return `Pushed ${n} file(s) to muse:~/${remote}`;
}

interface SyncChange {
	path: string;
	kind: "added" | "modified" | "deleted";
	data?: Buffer;
}

async function syncDiff(cwd: string): Promise<{ remote: string; changes: SyncChange[]; conflicts: string[] }> {
	const statePath = syncStatePath(cwd);
	if (!existsSync(statePath)) throw new Error("nothing pushed yet: run /muse-push first");
	const state = JSON.parse(readFileSync(statePath, "utf8"));
	const remote: string = state.remote;
	const base: Record<string, string> = state.base ?? {};
	const listing = await backend("/pi/v1/fs/list", { path: remote, recursive: true, max_entries: 5000 });
	const remoteFiles = (listing.entries ?? [])
		.filter((e: any) => e.type === "file")
		.map((e: any) => String(e.path).replace(/^\/+/, "").slice(remote.length + 1))
		.filter((p: string) => p && !SYNC_SKIP.test(p));
	const changes: SyncChange[] = [];
	const conflicts: string[] = [];
	for (let i = 0; i < remoteFiles.length; i += 20) {
		const r = await backend("/pi/v1/fs/read", { paths: remoteFiles.slice(i, i + 20).map((p: string) => `${remote}/${p}`) });
		for (const f of r.files ?? []) {
			if (f.error) continue;
			const rel = String(f.path).slice(remote.length + 1);
			const data = Buffer.from(f.data_base64, "base64");
			const h = sha(data);
			if (base[rel] === h) continue;
			const dst = join(cwd, rel);
			const localNow = existsSync(dst) ? sha(readFileSync(dst)) : undefined;
			if (localNow === h) continue;
			if (base[rel] && localNow && localNow !== base[rel]) conflicts.push(rel); // changed on both sides
			changes.push({ path: rel, kind: base[rel] || localNow ? "modified" : "added", data });
		}
	}
	const remoteSet = new Set(remoteFiles);
	for (const p of Object.keys(base)) if (!remoteSet.has(p) && existsSync(join(cwd, p))) changes.push({ path: p, kind: "deleted" });
	return { remote, changes, conflicts };
}

function previewDiff(cwd: string, changes: SyncChange[]): string {
	const tmp = mkdtempSync(join(tmpdir(), "pi-muse-"));
	const parts: string[] = [];
	for (const c of changes) {
		const a = join(cwd, c.path);
		const b = join(tmp, c.path.replace(/\//g, "__"));
		writeFileSync(b, c.data ?? "");
		const left = existsSync(a) ? a : "/dev/null";
		const right = c.kind === "deleted" ? "/dev/null" : b;
		try {
			execFileSync("git", ["diff", "--no-index", "--no-color", "--", left, right], { cwd });
		} catch (e: any) {
			parts.push(String(e.stdout ?? "").replaceAll(left, `a/${c.path}`).replaceAll(right, `b/${c.path}`));
		}
	}
	rmSync(tmp, { recursive: true, force: true });
	return parts.join("\n");
}

function applyChanges(cwd: string, changes: SyncChange[]): string {
	const root = resolve(cwd);
	for (const c of changes) {
		const dst = resolve(root, c.path);
		if (!dst.startsWith(root + "/")) continue; // path traversal guard
		if (c.kind === "deleted") rmSync(dst, { force: true });
		else {
			mkdirSync(dirname(dst), { recursive: true });
			writeFileSync(dst, c.data!);
		}
	}
	const st = JSON.parse(readFileSync(syncStatePath(cwd), "utf8"));
	for (const c of changes) {
		if (c.kind === "deleted") delete st.base[c.path];
		else st.base[c.path] = sha(c.data!);
	}
	writeFileSync(syncStatePath(cwd), JSON.stringify(st, null, 1));
	return changes.map((c) => `${c.kind === "added" ? "A" : c.kind === "deleted" ? "D" : "M"} ${c.path}`).join("\n");
}

function streamSync(model: Model<Api>, context: TranscriptContext, options?: SimpleStreamOptions): AssistantMessageEventStream {
	const stream = createAssistantMessageEventStream();
	const cwd = process.cwd();
	(async () => {
		const msgs = context.messages as any[];
		const isNewUserTurn = msgs[msgs.length - 1]?.role === "user";
		try {
			if (isNewUserTurn) {
				await syncPush(cwd);
				mirrorPrefix =
					`[Pi sync] A snapshot of my local project "${basename(cwd)}" is in your workspace at ~/${syncRemote(cwd)} ` +
					`(that is /home/hatch/${syncRemote(cwd)}). Work ONLY inside that folder, edit files in place there, and do not ` +
					`create files elsewhere for this task. I will pull your changes back to my machine afterwards.]\n\n`;
			}
		} catch (e) {
			const out = newOutput(model);
			emitter(stream, out).fail(e, false);
			return;
		}
		const inner = streamMirror(model, context, options);
		mirrorPrefix = "";
		for await (const ev of inner as any) {
			if (ev.type === "done" && ev.reason === "stop") {
				let note = "";
				try {
					const { changes, conflicts } = await syncDiff(cwd);
					if (!changes.length) note = "\n\n[muse sync] No changes in the Muse workspace copy.";
					else if (loadConfig().syncAutoApply && !conflicts.length)
						note = `\n\n[muse sync] Applied to local files:\n${applyChanges(cwd, changes)}`;
					else
						note =
							`\n\n[muse sync] ${changes.length} change(s) pending${conflicts.length ? ` (conflicts: ${conflicts.join(", ")})` : ""}. ` +
							`Review with /muse-pull, apply with /muse-apply.\n` +
							changes.map((c) => `${c.kind[0].toUpperCase()} ${c.path}`).join("\n");
				} catch (e) {
					note = `\n\n[muse sync] pull failed: ${e instanceof Error ? e.message : e}`;
				}
				const msg = ev.message as AssistantMessage;
				msg.content.push({ type: "text", text: "" });
				const idx = msg.content.length - 1;
				stream.push({ type: "text_start", contentIndex: idx, partial: msg });
				(msg.content[idx] as any).text = note;
				stream.push({ type: "text_delta", contentIndex: idx, delta: note, partial: msg });
				stream.push({ type: "text_end", contentIndex: idx, content: note, partial: msg });
			}
			stream.push(ev);
		}
		stream.end();
	})();
	return stream;
}

function streamMuse(model: Model<Api>, context: TranscriptContext, options?: SimpleStreamOptions): AssistantMessageEventStream {
	const mode = currentMode();
	if (mode === "sync") return streamSync(model, context, options);
	if (mode === "local") return streamLocal(model, context, options);
	if (mode === "xml") return streamXml(model, context, options);
	return streamMirror(model, context, options);
}

// ---------------------------------------------------------------------------
// Helpers for workspace commands
// ---------------------------------------------------------------------------
async function backend(path: string, body?: unknown): Promise<any> {
	const cfg = loadConfig();
	const res = await fetch(`${cfg.baseUrl}${path}`, {
		method: body === undefined ? "GET" : "POST",
		headers: { "content-type": "application/json", authorization: `Bearer ${cfg.apiKey}` },
		body: body === undefined ? undefined : JSON.stringify(body),
	});
	if (!res.ok) throw new Error(`muse2api ${path}: HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
	return res.json();
}

async function fetchModels(): Promise<typeof FALLBACK_MODELS> {
	try {
		const ctl = AbortSignal.timeout(Number(process.env.PI_MUSE_MODELS_TIMEOUT_MS ?? 60000));
		const cfg = loadConfig();
		const res = await fetch(`${cfg.baseUrl}/pi/v1/models`, { headers: { authorization: `Bearer ${cfg.apiKey}` }, signal: ctl });
		if (!res.ok) return FALLBACK_MODELS;
		const d = await res.json();
		return d.available_models?.length ? d.available_models : FALLBACK_MODELS;
	} catch {
		return FALLBACK_MODELS;
	}
}

const toPiModel = (m: any) => ({
	id: m.id,
	name: `Muse · ${m.display_name ?? m.id}`,
	reasoning: false,
	input: ["text"] as ("text" | "image")[],
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
	contextWindow: m.context_window_tokens ?? 350000,
	maxTokens: 32000,
});

/** notify in the TUI; print in non-interactive runs (pi -p) where notifications are not shown */
function say(ctx: any, msg: string, level: "info" | "error" = "info") {
	if (ctx.hasUI) ctx.ui.notify(msg, level);
	else process.stdout.write(`${msg}\n`);
}

// ---------------------------------------------------------------------------
// Extension entry point
// ---------------------------------------------------------------------------
export default async function (pi: ExtensionAPI) {
	const cfg = loadConfig();
	const models = process.env.PI_MUSE_OFFLINE ? FALLBACK_MODELS : await fetchModels();

	pi.registerProvider(PROVIDER, {
		name: "Muse (muse.ai agent)",
		baseUrl: cfg.baseUrl,
		apiKey: cfg.apiKey || "$MUSE2API_KEY",
		api: API as Api,
		models: models.map(toPiModel),
		streamSimple: streamMuse,
	});

	// Pass-through tool: renders a tool call that Muse already executed in its own VM.
	pi.registerTool({
		name: TOOL,
		label: "Muse",
		description:
			"Internal: mirrors a tool call that the Muse agent already executed inside its own cloud workspace. " +
			"Only the muse provider emits it; never call it yourself.",
		parameters: Type.Object({
			tool: Type.String({ description: "Muse tool name (best effort)" }),
			title: Type.Optional(Type.String()),
			subtitle: Type.Optional(Type.String()),
			status: Type.Optional(Type.String()),
			muse_call_id: Type.Optional(Type.String()),
		}),
		async execute(toolCallId, params) {
			const ev = toolResults.get(toolCallId);
			if (!ev) {
				return {
					content: [{ type: "text", text: `[muse] ${params.tool}: ${params.title ?? ""} (${params.status ?? "unknown"})` }],
					details: { tool: params.tool, title: params.title, status: params.status, replayed: true },
				};
			}
			const text = ev.report ?? ev.subtitle ?? ev.title ?? "";
			if (isFailure(ev.status)) throw new Error(`${ev.title ?? params.tool}: ${text}`);
			return {
				content: [{ type: "text", text }],
				details: { tool: params.tool, title: ev.title, subtitle: ev.subtitle, status: ev.status, icon: ev.icon, report: ev.report },
			};
		},
		renderCall(args, theme) {
			const ok = !isFailure(args.status);
			let t = theme.fg("toolTitle", theme.bold("muse ▸ ")) + theme.fg("accent", theme.bold(args.tool ?? "tool"));
			if (args.title) t += " " + theme.fg("muted", args.title);
			if (args.status) t += " " + theme.fg(ok ? "success" : "error", `[${args.status}]`);
			return new Text(t, 0, 0);
		},
		renderResult(result, { expanded }, theme) {
			const d = result.details as any;
			const first = result.content?.[0];
			const body = first && first.type === "text" ? first.text : "";
			if (!expanded) return new Text(theme.fg("dim", d?.subtitle ?? body.split("\n")[0] ?? ""), 0, 0);
			return new Text(theme.fg("toolOutput", body), 0, 0);
		},
	});

	pi.registerCommand("muse-mode", {
		description: "Show or set how Muse reaches your files: /muse-mode [local|xml|sync|mirror]",
		handler: async (args, ctx) => {
			const m = (args || "").trim() as Mode;
			if (m) {
				if (!MODES.includes(m)) return say(ctx, `unknown mode ${m}; use one of ${MODES.join(", ")}`, "error");
				modeOverride = m;
				forceNewThread = true; // modes use different prompts; start a fresh Muse thread
			}
			say(ctx, `Muse mode: ${currentMode()} (local = Muse edits this repo through Pi's tools; xml = in-band XML tools; sync = snapshot push/pull; mirror = Muse's own VM)`, "info");
		},
	});

	pi.registerCommand("muse-push", {
		description: "Upload a snapshot of this git repo (tracked + untracked, not ignored) to ~/workspace/pi-sync/<repo> in Muse",
		handler: async (_args, ctx) => say(ctx, await syncPush(ctx.cwd), "info"),
	});

	pi.registerCommand("muse-pull", {
		description: "Preview what Muse changed in ~/workspace/pi-sync/<repo> since /muse-push (diff), and optionally apply it",
		handler: async (args, ctx) => {
			const { remote, changes, conflicts } = await syncDiff(ctx.cwd);
			if (!changes.length) return say(ctx, `No changes in muse:~/${remote}`, "info");
			const diff = previewDiff(ctx.cwd, changes);
			const head = `${changes.length} change(s) from muse:~/${remote}${conflicts.length ? `\n⚠ also changed locally since push: ${conflicts.join(", ")}` : ""}`;
			const yes = /(^|\s)(--yes|-y)(\s|$)/.test(args || "");
			if (!yes && !(ctx.hasUI && (await ctx.ui.confirm("Apply Muse changes?", `${head}\n\n${diff.slice(0, 6000)}`)))) {
				return say(ctx, `${head}\n\n${diff}\n\nNot applied. Run /muse-apply to apply.`, "info");
			}
			say(ctx, `Applied:\n${applyChanges(ctx.cwd, changes)}`, "info");
		},
	});

	pi.registerCommand("muse-apply", {
		description: "Apply Muse's changes from ~/workspace/pi-sync/<repo> to the local repo without asking",
		handler: async (_args, ctx) => {
			const { changes } = await syncDiff(ctx.cwd);
			say(ctx, changes.length ? `Applied:\n${applyChanges(ctx.cwd, changes)}` : "Nothing to apply", "info");
		},
	});

	pi.registerCommand("muse-unsync", {
		description: "Delete this repo's snapshot folder from the Muse workspace",
		handler: async (_args, ctx) => {
			await backend("/pi/v1/fs/delete", { paths: [syncRemote(ctx.cwd)], recursive: true });
			rmSync(syncStatePath(ctx.cwd), { force: true });
			say(ctx, `Removed muse:~/${syncRemote(ctx.cwd)}`, "info");
		},
	});

	pi.registerCommand("muse-new", {
		description: "Start a fresh Muse thread on the next prompt",
		handler: async (_args, ctx) => {
			forceNewThread = true;
			ctx.ui.notify("Next Muse prompt starts a new thread", "info");
		},
	});

	pi.registerCommand("muse-ls", {
		description: "List files in the Muse workspace VM: /muse-ls [path] (default: workspace)",
		handler: async (args, ctx) => {
			const path = (args || "workspace").trim();
			const r = await backend("/pi/v1/fs/list", { path });
			const lines = (r.entries ?? []).map((e: any) => `${e.type === "directory" ? "d" : "-"} ${e.name}${e.size != null ? `  (${e.size} B)` : ""}`);
			ctx.ui.notify(`muse:/${r.path}\n${lines.join("\n")}`, "info");
		},
	});

	pi.registerCommand("muse-sync", {
		description: "Download files from the Muse workspace into the local cwd: /muse-sync [remotePath] [localDir]",
		handler: async (args, ctx) => {
			const [remote = "workspace", local = "muse-workspace"] = (args || "").trim().split(/\s+/).filter(Boolean);
			const listing = await backend("/pi/v1/fs/list", { path: remote, recursive: true, max_entries: 500 });
			const files = (listing.entries ?? []).filter((e: any) => e.type === "file" && (e.size ?? 0) <= 5 * 1024 * 1024);
			const out = resolve(ctx.cwd, local);
			let n = 0;
			for (let i = 0; i < files.length; i += 20) {
				const batch = files.slice(i, i + 20).map((f: any) => f.path);
				const r = await backend("/pi/v1/fs/read", { paths: batch });
				for (const f of r.files ?? []) {
					if (f.error) continue;
					const rel = f.path.startsWith(remote) ? f.path.slice(remote.length).replace(/^\/+/, "") : f.path;
					const dst = join(out, rel);
					if (!dst.startsWith(out)) continue; // path traversal guard
					mkdirSync(dirname(dst), { recursive: true });
					writeFileSync(dst, Buffer.from(f.data_base64, "base64"));
					n++;
				}
			}
			ctx.ui.notify(`Synced ${n} file(s) from muse:/${remote} to ${out}${listing.truncated ? " (listing truncated)" : ""}`, "info");
		},
	});
}
