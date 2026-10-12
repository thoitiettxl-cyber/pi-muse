# Backend: muse2api `pi-native` endpoints

pi-muse needs a small set of extra endpoints on top of
[muse2api](https://github.com/czg86389-hub/muse2api) (MIT), which already owns the
hard parts: a headless Chrome driven over CDP, muse.ai cookie/session handling,
account pool and VM keep-alive.

## Install
```bash
cd /path/to/muse2api
git checkout -b pi-native
cp /path/to/pi-muse/backend/pi_native.py /path/to/pi-muse/backend/pi_host.py .
git apply /path/to/pi-muse/backend/muse2api-app.patch   # hook at the end of app.py + split, activity-aware timeouts in engine.py
# restart muse2api (uvicorn app:app --host 127.0.0.1 --port 18610)
```
The patch only *adds* routes under `/pi/v1/*`; existing OpenAI-compatible routes are untouched.

## Endpoints (all require `Authorization: Bearer <muse2api key>`)
| Method | Path | Purpose |
|---|---|---|
| GET | `/pi/v1/models[?refresh=true]` | Live catalogue from gateway RPC `model.get` |
| POST | `/pi/v1/turn` | SSE stream of one Muse turn (`{prompt, model?, session_id?, settle_timeout?, restore_model?}`) |
| POST | `/pi/v1/fs/list` | `{path, recursive?, max_entries?}` → gateway `fs.list` (paths relative to `/home/hatch`) |
| POST | `/pi/v1/fs/read` | `{paths:[…], max_bytes?}` → gateway `fs.read`, base64 |
| POST | `/pi/v1/fs/write` | `{files:[{path, text \| data_base64}]}` → gateway `fs.write` (overwrite, create parents) |
| POST | `/pi/v1/fs/delete` | `{paths:[…], recursive?}` → gateway `fs.delete`; only paths under `workspace/` |
| POST | `/pi/v1/invoke_result` | `{turn_id, request_id, ok, payload? \| error?}` → answers a `node.invoke.request` of a running turn |
| POST | `/pi/v1/node/unpair` | `{node_id}` → gateway `node.unpair` |
| POST | `/pi/v1/rpc` | debug only, disabled unless `PI_NATIVE_DEBUG_RPC=1`: raw gateway calls in one page session |

`/pi/v1/turn` also accepts `node: {node_id, display_name, platform, commands_v2}`. The backend then
registers that node for the duration of the turn (re-registering + heartbeating every 4 s, because the
page's Noise connection is replaced when the chat is sent), forwards every `node.invoke.request` for it as
an SSE `invoke` event `{turn_id, request_id, command, params, timeout_ms}`, and uses the long, activity-aware
first-byte timeout below because Muse is silent (no text) while it works through device tools.

## Timeouts and heartbeats
| Env | Body override | Default | Meaning |
|---|---|---|---|
| `PI_NATIVE_CONNECT_TIMEOUT` | `connect_timeout` | 20 | page/gateway ready, `node.register` |
| `PI_NATIVE_FIRST_BYTE_TIMEOUT` | `first_byte_timeout` / `first_token_timeout` | 900 (node turns) | max silence before Muse's first text; any gateway event or device call resets it |
| `PI_NATIVE_FIRST_BYTE_TIMEOUT_CHAT` | same | 60 | same, turns without a device node |
| `PI_NATIVE_IDLE_TIMEOUT` | `idle_timeout` | 300 | max gap between text chunks / events after text started |
| `PI_NATIVE_TURN_TIMEOUT` | `timeout` | 3600 (node) / 600 | hard cap per turn |
| `PI_NATIVE_HEARTBEAT` | | 10 | SSE comment `: keep-alive` while nothing else is sent (keeps the muse2api↔Pi link alive; clients ignore it) |

On failure the `error` event is structured: `{message, code (first_byte_timeout|idle_timeout|turn_timeout|null),
retryable, phase, timeout_s, waited_s, elapsed_s, started_at, failed_at, bytes_received, events_received,
session_id, turn_id, last_invoke, last_result, muse_status}`, and the same JSON is logged as `pi_turn_error`.
The timeout does **not** cancel the Muse turn (`muse_status` is usually `running`).

Thread state endpoints (used by pi-muse's auto-continue):

| Method | Path | Purpose |
|---|---|---|
| POST | `/pi/v1/thread/state` | `{session_id}` → `{status, reply?}` from `sessions.get` (+ `chat.history` reply after the last user message when not running) |
| POST | `/pi/v1/thread/cancel` | `{session_id}` → `chat.cancel` → `{cancelled, message_id, status}` |

SSE events: `start`, `node`, `model`, `session`, `activity`, `tool`, `invoke`, `invoke_ack`, `text`, `text_done`, `final_text`, `warning`, `raw`, `done`, `error`. See `../docs/PROTOCOL.md`.

## Thread hosts (page/device lifecycle, `pi_host.py`)
Each `/pi/v1/turn` runs in a **ThreadHost** that owns one muse.ai page (and the Pi device node) per Muse thread:

- A timeout ends only the SSE request (`error` with `held: true`). The page, node heartbeat and event pump
  keep running, and a watcher polls `sessions.get` every `PI_NATIVE_WATCH_INTERVAL` (3 s).
- `POST /pi/v1/thread/attach {session_id, client_id}` re-attaches the owning client. It receives buffered and
  new records, including `invoke`, so device calls succeed while detached.
- When the thread leaves `running`, the host emits `final_text` + `done{status}` and keeps the page for
  `PI_NATIVE_HOLD_GRACE` (10 s). A follow-up `/pi/v1/turn` for that thread in this window **reuses** the page;
  otherwise the page is closed.
- `/pi/v1/thread/cancel` on a held thread runs `chat.cancel`, waits until the status really leaves `running`,
  then finishes and closes.
- **Server-side lock:** a `/pi/v1/turn` for a thread whose host is busy gets **409**
  `{"error":"thread_busy", phase, turn_id, owner_is_you}`.
- **Leak protection:** the watcher enforces the turn cap (`PI_NATIVE_TURN_TIMEOUT`, 3600 s) with a cancel. A
  reaper force-closes any host older than cap + `PI_NATIVE_REAPER_SLACK` (60 s), even if its threads hang. All
  hosts are closed on muse2api shutdown.
- **Unheld existing threads are checked first:** a `/pi/v1/turn` with a `session_id` that muse2api does not
  hold opens the page, calls `sessions.get` and sends nothing unless the thread is idle. `running` gives
  409 `{"error":"thread_busy","held":false,"muse_status":"running"}`. If the state can't be read, the request
  fails closed with 503 `{"error":"state_unknown"}`, and the page and the account are released.
- **Host states:** `starting → checking → running ⇄ watch → grace → running (reuse) | closing → closed`,
  plus `broken` from any state. A host is BROKEN when it cannot prove the thread is idle: state unknown,
  a cap without confirmed cancel, a host error, or `running` at open. It never enters `grace`, is closed, and
  is removed from the registry, so the next request goes through the `sessions.get` check again. A 409 while
  a page is still closing carries `retry_after`, and pi-muse retries shortly.
- **One lock:** a single non-reentrant `Registry.lock` guards the thread/turn maps, host states and account
  reservations. The registry entry and the account reservation are removed in the same lock hold. No I/O,
  logging or queue emits happen under it; muse2api's account pool is released right after, outside it.
- **Tool results per turn:** each turn has an epoch. `/pi/v1/invoke_result` for another turn (stale) or for a
  call id already answered (duplicate) gets 409. Device calls are forwarded once per call id. Both sets live
  on the turn, so they survive re-attaching.
- A cap or cancel that Muse never confirms is reported as `cancel_unconfirmed: true` together with `muse_status`.
- `GET /pi/v1/hosts` lists the open hosts (debugging).

Tests: `python -m unittest tests.test_pi_host` (fake Muse: device online flag, watcher, reaper, cancel, reuse).
