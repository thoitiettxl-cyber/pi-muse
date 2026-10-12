# Muse (muse.ai) protocol notes

Unofficial, reverse-engineered from the muse.ai web app (Oct 2026). Expect drift.

## Transport
- The web app connects to `wss://hatch.metaaivm.com/v1/noise?vm_id=<uuid>&auth_token=<jwt>`.
- Every frame is binary and **Noise-encrypted** (opcode 2, base64 in DevTools); nothing is readable on the wire.
- Inside the tunnel the client speaks **HTTP-style RPC** (method, path, headers, zstd body) plus a JSON event stream.
- The decrypted client is reachable in-page:
  ```js
  const gw = window.__hatchEarlyGatewayRuntimeState.activeRuntime.getSnapshot();
  await gw.sendRequest("model.get", {});     // RPC
  gw.onEvent("activity.updated", cb);         // events
  ```
  Decrypted event frames also pass through `TextDecoder.prototype.decode`, which pi-muse taps.

## RPC methods used
| Method | Route | Notes |
|---|---|---|
| `model.get` | GET `/model` | `{selected_model, default_model, available_models:[{id, provider, model, context_window_tokens, display_name, preview_model}]}` |
| `model.set` | POST `/model` | `{model:"auto/auto"}` — **account-wide** selection |
| `chat.history` | GET `/chat/history` | `{session_id}` → `chat_events[]` (`message.user` / `message.assistant`, `payload.content`) |
| `activity.get` / `activity.list` | GET `/activity/{id}`, `/activity` | activity feed incl. tool reports |
| `fs.list` | POST `/fs/list` | `{path}` relative to `/home/hatch` (absolute paths rejected) |
| `fs.read` | POST `/fs/read` | `{path, offset, len}` → `{len, eof, data_base64, text}` |
| others seen | | `chat.stream`, `chat.subscribe`, `chat.cancel`, `session.start`, `sessions.list`, `fs.write/mkdir/delete/search/subscribe`, … |

Models observed: `auto/auto` ("Auto", 350k ctx) and `ipnext/avocado-5.16-v0` ("Avocado 5.16 v0", 350k ctx).

## Events (decrypted JSON lines: `{"type":"event","event":…,"payload":…,"seq":…,"ts_ms":…}`)
- `sessions.updated` — `{change:"created"|"activity"|"renamed", session:{session_id, status:"running"|"completed", title, snippet}}`
- `activity.updated`, goal form (`payload.type == "goal"`):
  ```json
  {"message_id":"activity_thread:<uuid>","title":"Run shell command","status":"pending",
   "status_title":"Running","subtitle":"Executing echo and ls in workspace",
   "details":{"goal":{…},
     "goal_actions":[{"action_index":2,"kind":"tool_call","id":"ga-root-tool-<exec>-call_<id>",
        "subject_id":"call_<id>","icon":"file_created","status":"SUCCESS",
        "title":"Created hello2.txt and listed workspace contents",
        "subtitle":"Ran `echo hi > hello2.txt && ls` in ~/workspace, exit code 0",
        "report":"Executed shell command in `/home/hatch/workspace`:\n```\necho hi > hello2.txt && ls\n```\nResult: success with exit code `0` …"}],
     "finish_status":{"status":"success","reason":"Command completed with exit code 0."},
     "finish_message":"…"}}
  ```
- `activity.updated`, standalone form: `type` = `file_created` (`details.files[{path,status}]`), `web_search` (`details.{query,tool:"browser_search"}`), …
- Also: `node.connected/disconnected`, `navigation.items.updated`.

## What is *not* exposed
Raw tool names/arguments/stdout are **not** sent to the web client. Tool calls arrive as LLM-written
summaries (`title`, `subtitle`, `report`) with a stable `call_<id>` and a status. Reports are usually
written *after* the reply text and keep updating for a few seconds. pi-muse therefore waits for
`finish_status` (bounded by `settle_timeout`) and infers a tool name heuristically
(`exec`, `read`, `write`, `edit`, `list`, `memory_search`, `web_search`) — names follow OpenClaw's core tool set,
which Muse's workspace layout (`AGENTS.md`, `SOUL.md`, `TOOLS.md`, `MEMORY.md`, `HEARTBEAT.md`, `USER.md`, `IDENTITY.md`) mirrors.

## Workspace
The VM home is `/home/hatch`; the agent's working dir is `/home/hatch/workspace` (`fs` path `workspace/...`).
