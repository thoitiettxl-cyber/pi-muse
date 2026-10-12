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
| `sessions.get` | GET `/sessions/{id}` | `{id}` → `{session_id, status, snippet, updated_at_ms, …}`; `status` is `running` while a turn is in flight, then `completed`, or `interrupted` after a cancel (verified Oct 2026) |
| `chat.cancel` | POST `/chat/cancel` | `{session_id}` → `{cancelled, message_id}`; stops the running turn (`cancelled:false` if idle). The web app's Stop button uses it |
| others seen | | `chat.stream`, `chat.subscribe`, `session.start`, `sessions.list`, `fs.write/mkdir/delete/search/subscribe`, … |

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

## Device nodes (used by `local` mode)
Decoded from the web bundles (`HatchNodeRegister`, route table) and confirmed live, Oct 2026.

| Method | Route | Params |
|---|---|---|
| `node.register` | POST `/api/nodes/register` | `{node_id, display_name, platform, commands_v2:{<name>:{description, required:{<arg>:{type, description}}, optional:{…}, timeout_ms?}}}` → `{node_id, status:"registered"}`. Fails with *"noise connection has no active stream; subscribe (chat.subscribe) before node.register"* until the page has subscribed. Arg types `string`/`number`/`boolean` are kept; `array`/`object` are stored as `string`. |
| `node.heartbeat` | POST `/api/nodes/heartbeat` | `{node_id}` |
| `node.list` | POST `/api/nodes/list` | → `{nodes:[{node_id, display_name, platform, commands_v2, connection_state:"online"\|"offline", online, last_seen_at, pairing_status}]}` |
| `node.unpair` | POST `/api/nodes/unpair` | `{node_id}` |
| `nodes.invoke_result` | POST `/nodes/invoke-result` | `{request_id, node_id, result:{ok:true, payload_json:"<json>"}}` or `{…, result:{ok:false, error:{code, message}}}`. `result.payload` needs `result.transfer` and cannot be combined with `payload_json`. → `{resolved:true, submission_status:"completed"}` |

A node is "online" only while the Noise connection that registered it is alive. Sending a chat message
replaces that connection, so the node must be re-registered after the message is sent.

Muse's agent sees nodes as **devices** through a `device` tool namespace (`tool_search.load_tool_namespace {paths:["device"]}`,
then `device.list`, `device.describe`, `device.invoke`). An invocation reaches the node's connection as a
**request frame** (not an event):
```json
{"type":"req","id":"<uuid>","method":"node.invoke.request",
 "params":{"node_id":"pi-host","command":"edit","params":{"path":"a.py","edits":"[…]"},
           "request_id":"<uuid>","idempotency_key":null,"timeout_ms":30000}}
```
Unanswered invocations time out after ~30 s (`node_invoke_timeout`). The result text is shown to the
model wrapped as external content (`source=device-data tool_name=device.invoke`).

Related but unused: `client.register_capabilities` (`{client_id, platform, display_name, version,
capabilities:{data_sources, device_commands, hatch_app_commands, rendering}}`) and `client.invoke` events
answered with `client.invoke.result {invoke_id, ok, payload_json | error}`; the web app uses these for UI
commands (`ui.navigate.*`, `pin.reminder`, voice).

## File writes (used by `sync` mode)
- `fs.write` `{path, text | data_base64, overwrite, append, create_parent}` → `{path, bytes_written, append}`; the web app uploads in 512 KiB chunks with `append:true` after the first.
- `fs.delete` `{path, recursive}`, `fs.mkdir` `{path, recursive}`, `fs.rename` `{source, destination, create_parent}`.

## Timeouts vs. the running Muse turn

muse2api's timeouts only bound how long muse2api waits; they do not stop the Muse turn. Evidence (Oct 2026):
a turn sent with `first_token_timeout: 2` failed after ~5 s with
`{"type":"error","message":"MuseGenerationError: 等待助手首字响应超时"}`, but `sessions.get` kept returning
`status: "running"` and `chat.history` later contained Muse's reply (`DONE3`). `chat.cancel` on a running
thread returned `{"cancelled": true}` and `sessions.get` then reported `status: "interrupted"`.
