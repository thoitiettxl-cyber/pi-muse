/**
 * Auto-continue for interrupted Muse turns (local mode).
 *
 * Pure logic, no Pi imports, so it can be unit-tested with plain Node.
 *  - classifyMuseError(): decides whether an error is a transient transport/timeout failure
 *  - continuationMessage(): the only text sent on resume (never the original prompt)
 *  - ThreadLock: at most one in-flight request per Muse thread
 *  - recoverTurn(): wait for the still-running Muse turn (preferred), cancel only after maxWait,
 *    and refuse to continue when the thread state cannot be determined.
 */

export interface MuseErrorInfo {
	message: string;
	code?: string | null;
	retryable?: boolean;
	httpStatus?: number;
	[k: string]: unknown;
}

export type ErrorClass =
	| { retryable: true; kind: "timeout" | "gateway_timeout" | "disconnect"; reason: string }
	| { retryable: false; kind: "auth" | "quota" | "refusal" | "aborted" | "other"; reason: string };

// Checked first: anything matching these is never retried.
const FATAL: [RegExp, "auth" | "quota" | "refusal"][] = [
	[/\b(401|403)\b|unauthori[sz]ed|forbidden|invalid (api )?key|auth(entication)? (failed|required)|login|cookie|会话失效|未登录|登录/i, "auth"],
	[/\b429\b|quota|rate.?limit|credits?|insufficient|billing|额度|限额|余额/i, "quota"],
	[/refus|content (policy|filter)|safety|moderation|not allowed|violat|拒绝|违规/i, "refusal"],
];
// Observed shapes (muse2api pi-native):
//   {"type":"error","message":"MuseGenerationError: 等待助手首字响应超时"}            (before the patch)
//   {"type":"error","message":"MuseTimeoutError: 等待助手首字响应超时","code":"first_byte_timeout","retryable":true,...}
//   fetch: "terminated" / "fetch failed" / ECONNRESET / "socket hang up" / stream ended without `done`
const RETRY: [RegExp, "timeout" | "gateway_timeout" | "disconnect"][] = [
	[/首字响应超时|等待助手回复超时|_timeout\b|timed? ?out|timeout/i, "timeout"],
	[/\b504\b|gateway time-?out/i, "gateway_timeout"],
	[/terminated|fetch failed|ECONNRESET|ECONNREFUSED|EPIPE|socket hang up|other side closed|premature close|stream ended|network error|UND_ERR/i, "disconnect"],
];

export function classifyMuseError(err: unknown): ErrorClass {
	const info: MuseErrorInfo =
		err && typeof err === "object" && "message" in (err as any)
			? { ...(err as any), message: String((err as any).message), code: (err as any).code, retryable: (err as any).retryable, httpStatus: (err as any).httpStatus }
			: { message: String(err) };
	const msg = info.message ?? "";
	if ((err as any)?.name === "AbortError" || /\baborted\b/i.test(msg)) return { retryable: false, kind: "aborted", reason: "aborted by user" };
	if (info.httpStatus === 504) return { retryable: true, kind: "gateway_timeout", reason: "HTTP 504" };
	if (info.httpStatus && info.httpStatus >= 400 && info.httpStatus !== 502 && info.httpStatus !== 503) {
		for (const [re, kind] of FATAL) if (re.test(msg) || [401, 403].includes(info.httpStatus)) return { retryable: false, kind: info.httpStatus === 429 ? "quota" : kind, reason: `HTTP ${info.httpStatus}` };
		return { retryable: false, kind: "other", reason: `HTTP ${info.httpStatus}` };
	}
	for (const [re, kind] of FATAL) if (re.test(msg)) return { retryable: false, kind, reason: msg.slice(0, 120) };
	if (typeof info.code === "string" && /_timeout$/.test(info.code)) return { retryable: true, kind: "timeout", reason: info.code };
	for (const [re, kind] of RETRY) if (re.test(msg)) return { retryable: true, kind, reason: msg.slice(0, 120) };
	return { retryable: false, kind: "other", reason: msg.slice(0, 120) };
}

export interface LastStep {
	tool: string;
	args: unknown;
	status?: "ok" | "error" | "pending";
	result?: string;
	/** set when the step is classified as non-idempotent (re-running it would change state again) */
	nonIdempotent?: string;
	/** epoch ms when its result was handed back to Muse */
	finishedAt?: number;
}

// Strict mode allowlist: the only steps after which auto-continue may send a continuation message.
const STRICT_TOOLS = new Set(["read", "ls", "grep", "find"]);
const STRICT_BASH = /^(ls|cat|head|tail|grep|rg|find|pwd|wc|git\s+(status|diff|log))(\s|$)/;

/** Strict mode: true only for allowlisted read-only steps (every segment of a bash pipeline/list must qualify). */
export function isStrictReadOnly(tool: string, args: any): boolean {
	const t = tool.toLowerCase();
	if (STRICT_TOOLS.has(t)) return true;
	if (t !== "bash") return false;
	const cmd = String(args?.command ?? "").trim();
	if (!cmd || /[>`]|\$\(|<\(/.test(cmd)) return false; // redirections / substitutions
	const parts = cmd.split(/\|\||&&|;|\||\n/).map((p) => p.trim()).filter(Boolean);
	return parts.every((p) => STRICT_BASH.test(p) && !/\s-(delete|exec|execdir|ok|fprint\w*)\b/.test(p) && !/\s--output\b/.test(p));
}

const READ_ONLY_TOOLS = new Set(["read", "ls", "grep", "find", "glob", "search", "list", "view"]);
const MUTATING_BASH: [RegExp, string][] = [
	[/>>/, "appends to a file (>>)"],
	[/\btee\s+(-\w*a|--append)/, "appends to a file (tee -a)"],
	[/\bgit\s+(push|commit|merge|rebase|reset|revert|tag|cherry-pick|am|apply|stash|pull|checkout\s+-b|branch\s+-[dDmM])\b/, "mutates git state"],
	[/(^|[;&|(\s])(rm|rmdir|mv|unlink|shred|truncate)\s/, "deletes or moves files"],
	[/\bsed\s+(-\w*i|--in-place)/, "edits a file in place"],
	[/\bcurl\b[^|;&]*(-X\s*(POST|PUT|PATCH|DELETE)|--request\s+(POST|PUT|PATCH|DELETE)|\s-d\s|--data|-F\s|--form|-T\s|--upload-file)/i, "sends a mutating network request"],
	[/\b(wget|http|httpie)\b[^|;&]*(--post|POST|PUT|PATCH|DELETE)/, "sends a mutating network request"],
	[/\b(npm|pnpm|yarn|cargo|twine|gem)\s+(publish|yank|unpublish)\b|\bpoetry\s+publish\b/, "publishes a package"],
	[/\b(gh|glab|hub)\s+\S+\s+(create|merge|close|delete|edit|comment|review|reopen)\b|\bgh\s+release\s+upload\b/, "changes a remote repository"],
	[/\bdocker\s+(push|rm|rmi)\b|\bkubectl\s+(apply|delete|create|patch|replace|scale|rollout)\b|\bterraform\s+(apply|destroy)\b|\bhelm\s+(install|upgrade|uninstall)\b/, "changes infrastructure"],
	[/\b(ssh|scp|rsync|sftp)\b/, "runs on or copies to another host"],
	[/\b(psql|mysql|sqlite3|mongo|mongosh|redis-cli)\b/, "talks to a database"],
	[/\b(sendmail|mail|mutt)\b/, "sends mail"],
	[/\b(pip|npm|pnpm|yarn|apt|apt-get|brew)\s+(install|add|remove|uninstall)\b/, "installs or removes packages"],
];

/**
 * Non-idempotent tool calls: running them twice changes state twice (or fails).
 * `fileExisted` is checked by the caller before Pi runs a write/edit.
 * Returns the reason, or undefined when the call is safe to repeat.
 */
export function nonIdempotentReason(tool: string, args: any, fileExisted?: boolean): string | undefined {
	const t = tool.toLowerCase();
	if (READ_ONLY_TOOLS.has(t)) return undefined;
	if (t === "bash" || t === "shell" || t === "exec") {
		const cmd = String(args?.command ?? args?.cmd ?? "");
		for (const [re, why] of MUTATING_BASH) if (re.test(cmd)) return why;
		return undefined;
	}
	if (t === "write") return fileExisted ? "overwrites an existing file" : undefined;
	if (t === "edit" || t === "multiedit" || t === "patch" || t === "apply_patch") return "edits an existing file";
	return `unknown tool "${tool}" (treated as non-idempotent)`;
}

export function summarize(v: unknown, n = 160): string {
	let t = typeof v === "string" ? v : JSON.stringify(v ?? "");
	t = (t ?? "").replace(/\s+/g, " ").trim();
	return t.length <= n ? t : `${t.slice(0, n)}…`;
}

export function continuationMessage(last?: LastStep, todo?: string): string {
	let s = "Continue from where you stopped.";
	if (last) {
		const res = last.status === "pending" ? "(result not delivered)" : `${last.status === "error" ? "error: " : ""}${summarize(last.result ?? "(no output)")}`;
		s += ` The last step already ran: ${last.tool} ${summarize(last.args, 200)} -> ${res}. Do not repeat it.`;
	} else {
		s += " Do not redo steps that already completed; check the current state first if unsure.";
	}
	if (todo) s += ` Your todo list so far: ${summarize(todo, 400)}`;
	return s;
}

/** At most one in-flight request per Muse thread (in-process). Rejects instead of queueing. */
export class ThreadLock {
	private held = new Map<string, string>();
	tryAcquire(thread: string, owner: string): boolean {
		const cur = this.held.get(thread);
		if (cur && cur !== owner) return false;
		this.held.set(thread, owner);
		return true;
	}
	release(thread: string | undefined, owner: string) {
		if (thread && this.held.get(thread) === owner) this.held.delete(thread);
	}
	holder(thread: string): string | undefined {
		return this.held.get(thread);
	}
	size() {
		return this.held.size;
	}
}

export interface AutoContinueConfig {
	autoContinue: boolean;
	maxRetries: number;
	backoffMs: number[];
	/** max time to wait for a still-running Muse turn before cancelling it */
	maxWaitMs: number;
	pollMs: number;
}

export interface ThreadState {
	status?: string | null; // running | completed | interrupted | failed ...
	reply?: string | null;
	/** muse2api still holds the page/device for this thread (request timed out, Muse still running) */
	held?: boolean;
	owner_is_you?: boolean;
	busy?: boolean;
}

export type Recovery =
	| { action: "present"; reply: string; waitedMs: number; wasRunning: boolean } // Muse finished on its own: show its result, send nothing
	| { action: "attach"; waitedMs: number } // muse2api still holds the turn for us: re-attach to its stream, send nothing
	| { action: "continue"; cancelled: boolean; waitedMs: number; status?: string | null } // thread idle: send continuation
	| { action: "stop"; reason: string }; // cannot do it safely

export interface RecoveryDeps {
	getState(session: string): Promise<ThreadState>;
	cancel(session: string): Promise<{ cancelled: boolean; status?: string | null }>;
	sleep(ms: number): Promise<void>;
	log(event: string, data: Record<string, unknown>): void;
	aborted?(): boolean;
}

const KNOWN = new Set(["running", "completed", "interrupted", "cancelled", "canceled", "failed", "error", "idle"]);

export interface RecoverOpts {
	allowAttach?: boolean;
	/** a held turn owned by another client: report busy instead of waiting */
	foreignIsBusy?: boolean;
	/** never cancel (used before a manual send: someone else's turn is not ours to stop) */
	noCancel?: boolean;
}

export async function recoverTurn(session: string | undefined, cfg: AutoContinueConfig, deps: RecoveryDeps, opts: RecoverOpts = {}): Promise<Recovery> {
	if (!session) return { action: "stop", reason: "no Muse thread id is known for this turn, so its state cannot be checked" };
	const t0 = Date.now();
	const state = async (): Promise<ThreadState | undefined> => {
		try {
			const s = await deps.getState(session);
			deps.log("thread_state", { session, status: s?.status ?? null });
			return s && typeof s.status === "string" && KNOWN.has(s.status) ? s : undefined;
		} catch (e) {
			deps.log("thread_state_error", { session, error: String((e as any)?.message ?? e) });
			return undefined;
		}
	};
	let s = await state();
	if (!s) return { action: "stop", reason: "the Muse thread state could not be determined (state query failed or returned an unknown status)" };
	if (s.status === "running" && s.held && s.owner_is_you && opts.allowAttach !== false) return { action: "attach", waitedMs: 0 };
	if (s.status === "running" && s.held && !s.owner_is_you && opts.foreignIsBusy)
		return { action: "stop", reason: "the Muse thread is busy with a turn started from another Pi window" };
	const wasRunning = s.status === "running";
	while (s.status === "running" && Date.now() - t0 < cfg.maxWaitMs) {
		if (deps.aborted?.()) return { action: "stop", reason: "aborted by user" };
		await deps.sleep(Math.min(cfg.pollMs, Math.max(0, cfg.maxWaitMs - (Date.now() - t0))));
		s = await state();
		if (!s) return { action: "stop", reason: "the Muse thread state could not be determined while waiting" };
	}
	const waitedMs = Date.now() - t0;
	if (s.status === "running" && opts.noCancel) return { action: "stop", reason: `the Muse thread was still busy after waiting ${Math.round(waitedMs / 1000)}s` };
	if (s.status === "running") {
		deps.log("thread_cancel", { session, waitedMs });
		let c: { cancelled: boolean; status?: string | null };
		try {
			c = await deps.cancel(session);
		} catch (e) {
			return { action: "stop", reason: `the Muse turn was still running after ${Math.round(waitedMs / 1000)}s and cancelling it failed: ${String((e as any)?.message ?? e)}` };
		}
		const after = await state();
		if (!after || after.status === "running")
			return { action: "stop", reason: `the Muse turn was still running after ${Math.round(waitedMs / 1000)}s and could not be stopped` };
		return { action: "continue", cancelled: c.cancelled, waitedMs, status: after.status };
	}
	if (s.status === "completed" && s.reply && s.reply.trim()) return { action: "present", reply: s.reply, waitedMs, wasRunning };
	return { action: "continue", cancelled: false, waitedMs, status: s.status };
}
