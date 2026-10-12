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
git apply /path/to/pi-muse/backend/muse2api-app.patch   # 7-line hook at the end of app.py
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

SSE events: `start`, `model`, `session`, `activity`, `tool`, `text`, `text_done`, `final_text`, `warning`, `raw`, `done`, `error`. See `../docs/PROTOCOL.md`.
