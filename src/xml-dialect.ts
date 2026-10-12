/**
 * XML in-band tool-call dialect ("approach B").
 *
 * Adapted from oh-my-pi (https://github.com/can1357/oh-my-pi), packages/ai/src/dialect/xml.md,
 * coercion.ts and rendering.ts — MIT License, Copyright (c) 2025 Mario Zechner,
 * Copyright (c) 2025-2026 Can Bölük, Copyright (c) 2026 Stencil Labs, Inc.
 * See THIRD_PARTY_NOTICES.md. The streaming scanner was replaced by a whole-reply
 * parser because Muse only hands back complete replies.
 */

export interface InbandTool {
	name: string;
	description?: string;
	parameters?: any;
}
export interface ParsedCall {
	name: string;
	arguments: Record<string, unknown>;
}

// --- from omp dialect/xml.md (format guide + rules), lightly trimmed --------------------------
export const XML_DIALECT_GUIDE = `## Format guide

A call is one \`<invoke>\` element whose \`<parameter>\` children carry its arguments:

\`\`\`text
<invoke name="fn"><parameter name="arg">value</parameter></invoke>
\`\`\`

Emit consecutive \`<invoke>…</invoke>\` blocks for multiple calls; you MAY wrap them in \`<tool_calls>…</tool_calls>\`. Each call's result arrives in the next message as a response block:

\`\`\`text
<tool_response>
verbatim tool result
</tool_response>
\`\`\`

## Rules

- \`name\` MUST match a listed function.
- Parameter values are read literally by regex (delimiter matching), NOT a real XML parser: write them verbatim and never HTML-escape (emit \`a & b\`, never \`a &amp; b\`; \`<\`/\`>\` stay literal too). Only the body's own \`</parameter>\` closing tag is reserved. Non-string values are JSON.
- NEVER emit \`<tool_response>\` yourself, and never invent a result.
- After writing your calls, STOP and end your message. Write the complete call, then stop.`;

// --- from omp dialect/coercion.ts -----------------------------------------------------------
export function collectSchemaTypes(schema: unknown, out: Set<string> = new Set(), depth = 0): Set<string> {
	if (depth > 8 || !schema || typeof schema !== "object" || Array.isArray(schema)) return out;
	const node = schema as Record<string, unknown>;
	const type = node.type;
	if (typeof type === "string") out.add(type);
	else if (Array.isArray(type)) for (const t of type) if (typeof t === "string") out.add(t);
	for (const key of ["anyOf", "oneOf", "allOf"] as const) {
		const branch = node[key];
		if (Array.isArray(branch)) for (const sub of branch) collectSchemaTypes(sub, out, depth + 1);
	}
	return out;
}
export function isStringOnlySchema(schema: unknown): boolean {
	const types = collectSchemaTypes(schema);
	types.delete("null");
	return types.size === 1 && types.has("string");
}
export function decodeValue(raw: string): unknown {
	const trimmed = raw.trim();
	if (trimmed.length === 0) return trimmed;
	try {
		return JSON.parse(trimmed) as unknown;
	} catch {
		return raw;
	}
}

// --- prompt (shape of omp dialect/prompt-template.md) ----------------------------------------
export function renderToolsPrompt(tools: InbandTool[]): string {
	const lines = tools.map((t) =>
		JSON.stringify({ name: t.name, description: t.description ?? "", parameters: t.parameters ?? {} }),
	);
	return `# Tools

You may call one or more functions to assist with the user query.
Tool calls are emitted as text using the exact syntax below, not as native tool messages.

Available functions are listed inside \`<tools></tools>\` as one JSON object per line:

<tools>
${lines.join("\n")}
</tools>

${XML_DIALECT_GUIDE}`;
}

// --- from omp dialect/rendering.ts ----------------------------------------------------------
export function renderToolResponses(results: { text: string }[]): string {
	return results.map((r) => `<tool_response>\n${r.text}\n</tool_response>`).join("\n");
}

/** Parse every complete `<invoke name="…">…</invoke>` in a reply. */
export function parseInvokes(text: string, tools: InbandTool[]): { calls: ParsedCall[]; prose: string } {
	const byName = new Map(tools.map((t) => [t.name, t]));
	const calls: ParsedCall[] = [];
	// Muse sometimes wraps calls in a fenced code block; strip fences around invokes.
	const src = text.replace(/```(?:xml|text)?\s*\n(?=\s*<(?:tool_calls|invoke)\b)/g, "").replace(/(<\/(?:invoke|tool_calls)>)\s*\n```/g, "$1");
	const re = /<invoke\s+name\s*=\s*["']([^"']+)["']\s*>([\s\S]*?)<\/invoke>/g;
	let m: RegExpExecArray | null;
	while ((m = re.exec(src))) {
		const name = m[1].trim();
		const body = m[2];
		const props = (byName.get(name)?.parameters?.properties ?? {}) as Record<string, unknown>;
		const args: Record<string, unknown> = {};
		const pre = /<parameter\s+name\s*=\s*["']([^"']+)["']([^>]*)>([\s\S]*?)<\/parameter>/g;
		let p: RegExpExecArray | null;
		while ((p = pre.exec(body))) {
			const key = p[1].trim();
			let raw = p[3];
			// one leading/trailing newline is formatting, not content
			raw = raw.replace(/^\n/, "").replace(/\n$/, "");
			const forceJson = /string\s*=\s*["']false["']/.test(p[2]);
			args[key] = !forceJson && (isStringOnlySchema(props[key]) || !(key in props)) ? raw : decodeValue(raw);
		}
		calls.push({ name, arguments: args });
	}
	const prose = src
		.replace(/<tool_calls>|<\/tool_calls>/g, "")
		.replace(re, "")
		.trim();
	return { calls, prose };
}
