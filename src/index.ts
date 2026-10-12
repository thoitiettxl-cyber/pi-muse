/**
 * pi-muse: a Pi provider that talks to the Muse agent (muse.ai) and mirrors the
 * tools Muse runs inside its own cloud workspace VM as real Pi tool calls.
 *
 * Muse is not a plain LLM: every turn is executed by Muse's own agent runtime
 * (OpenClaw-style workspace with AGENTS.md / SOUL.md / TOOLS.md) on a per-user VM.
 * The backend (muse2api, branch `pi-native`) streams one Muse turn as SSE:
 *   text / text_done   assistant reply (DOM stream)
 *   activity           goal-level progress ("Run shell command", finish status)
 *   tool               one tool call Muse made (title, subtitle, report, status)
 *   session            Muse thread/session id
 *   done | error
 *
 * Mapping into Pi (one Muse turn => two Pi model requests):
 *   request #1  thinking (live progress) + toolCall blocks for every Muse tool  -> stopReason "toolUse"
 *               Pi then "executes" the pass-through tool `muse_tool`, which just returns the
 *               result Muse already produced (no local side effects).
 *   request #2  the provider sees those tool results and returns Muse's final reply text.
 * If Muse used no tools, request #1 returns the text directly.
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
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";

// ---------------------------------------------------------------------------
// Configuration: env vars win, then ~/.pi/agent/muse.json
// ---------------------------------------------------------------------------
interface MuseConfig {
	baseUrl: string;
	apiKey: string;
	settleTimeout: number;
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
	};
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
function streamMuse(model: Model<Api>, context: TranscriptContext, options?: SimpleStreamOptions): AssistantMessageEventStream {
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
			const prompt = textOf(lastUser?.content).trim();
			if (!prompt) throw new Error("empty prompt");
			let sessionId: string | undefined;
			if (!forceNewThread) {
				for (let i = msgs.length - 1; i >= 0; i--) {
					const rid = msgs[i]?.role === "assistant" ? msgs[i].responseId : undefined;
					if (typeof rid === "string" && rid.startsWith("muse:")) {
						sessionId = rid.slice(5);
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
