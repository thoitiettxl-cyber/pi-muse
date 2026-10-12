# Backend: muse2api `pi-native` endpoints

pi-muse needs a small set of extra endpoints on top of
[muse2api](https://github.com/czg86389-hub/muse2api) (MIT), which already owns the
hard parts: a headless Chrome driven over CDP, muse.ai cookie/session handling,
account pool and VM keep-alive.

## Install
```bash
cd /path/to/muse2api
git checkout -b pi-native
cp /path/to/pi-muse/backend/pi_native.py .
git apply /path/to/pi-muse/backend/muse2api-app.patch   # hook at the end of app.py + configurable first-token timeout in engine.py
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
an SSE `invoke` event `{turn_id, request_id, command, params, timeout_ms}`, and raises the first-token
timeout to 240 s (`first_token_timeout` overrides it) because Muse is silent while it waits for tools.

SSE events: `start`, `node`, `model`, `session`, `activity`, `tool`, `invoke`, `invoke_ack`, `text`, `text_done`, `final_text`, `warning`, `raw`, `done`, `error`. See `../docs/PROTOCOL.md`.
