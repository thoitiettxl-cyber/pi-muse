"""Pi-native endpoints for muse2api (branch `pi-native`).

Exposes Muse's *agent* surface instead of an OpenAI-compatible facade:

  GET  /pi/v1/models            live catalogue from the gateway RPC `model.get`
  POST /pi/v1/turn              SSE stream of one Muse turn: text deltas + tool/activity events
  POST /pi/v1/fs/list           gateway `fs.list` on the Muse workspace VM
  POST /pi/v1/fs/read           gateway `fs.read` (base64) on the Muse workspace VM

How it works: muse.ai's web app talks to the user's cloud VM ("hatch") over a
Noise-encrypted WebSocket. The decrypted gateway client is reachable inside the
page at `window.__hatchEarlyGatewayRuntimeState.activeRuntime.getSnapshot()`
(`sendRequest(method, params)` / `onEvent(name, cb)`). We drive the headless
Chrome page that muse2api already controls, call RPCs through that client, and
tap decrypted gateway events by hooking `TextDecoder.prototype.decode`.
"""
from __future__ import annotations

import json
import queue
import threading
import time
import uuid
from datetime import datetime


def _ts(v) -> float | None:
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None

from fastapi import Body, Depends, HTTPException
from fastapi.responses import StreamingResponse

GW = "window.__hatchEarlyGatewayRuntimeState.activeRuntime.getSnapshot()"

# Idempotent in-page tap: keeps every decrypted gateway *event* frame in a ring buffer.
TAP_JS = r"""(function(){
  if (window.__piTap) return 'present';
  window.__piTap = [];
  var d = TextDecoder.prototype.decode;
  TextDecoder.prototype.decode = function(){
    var r = d.apply(this, arguments);
    try {
      if (r && r.length > 20 && ((r.charCodeAt(0) === 123 && r.indexOf('"type":"event"') !== -1) || r.indexOf('invoke') !== -1)) {
        window.__piTap.push(r);
        if (window.__piTap.length > 3000) window.__piTap.splice(0, 1000);
      }
    } catch (e) {}
    return r;
  };
  return 'installed';
})()"""

DRAIN_JS = "(function(){var t=window.__piTap||[];window.__piTap=[];return JSON.stringify(t);})()"

INTERESTING = ("activity.", "sessions.updated", "chat.", "message.", "fs.", "tool")


def gw_call(page, method: str, params: dict, timeout: float = 30):
    expr = ("(async()=>{try{const g=%s;return JSON.stringify({ok:true,r:await g.sendRequest(%s,%s)})}"
            "catch(e){return JSON.stringify({ok:false,e:String(e)})}})()") % (
        GW, json.dumps(method), json.dumps(params))
    raw = page.js(expr, await_promise=True, timeout=timeout)
    d = json.loads(raw or '{"ok":false,"e":"no result"}')
    if not d.get("ok"):
        raise RuntimeError(d.get("e"))
    return d.get("r")


def wait_gateway(page, timeout: float = 20) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            if page.js("(function(){var s=window.__hatchEarlyGatewayRuntimeState;var r=s&&s.activeRuntime;return !!(r&&r.getSnapshot().isConnected)})()"):
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def normalize(ev: dict):
    """Turn raw gateway events into Pi-facing records. Yields dicts."""
    name = ev.get("event") or ""
    p = ev.get("payload") or {}
    if name == "activity.updated":
        det = p.get("details") or {}
        if p.get("type") == "goal":
            goal = det.get("goal") or {}
            yield {"type": "activity", "id": p.get("message_id"), "title": p.get("title"),
                   "subtitle": p.get("subtitle"), "status": p.get("status"),
                   "status_title": p.get("status_title"),
                   "finished": bool(det.get("finish_status")),
                   "finish": det.get("finish_status"), "finish_message": det.get("finish_message"),
                   "goal_name": goal.get("name"), "timestamp": p.get("timestamp")}
            for a in det.get("goal_actions") or []:
                yield {"type": "tool", "id": a.get("id"), "call_id": a.get("subject_id"),
                       "kind": a.get("kind"), "index": a.get("action_index"),
                       "icon": a.get("icon"), "title": a.get("title"), "subtitle": a.get("subtitle"),
                       "report": a.get("report"), "status": a.get("status"),
                       "agent_depth": a.get("agent_depth"), "created_at": a.get("created_at"),
                       "goal_id": p.get("message_id")}
        else:
            # standalone activity (file_created, web_search, ...): treat as a tool event
            yield {"type": "tool", "id": p.get("message_id"), "call_id": p.get("message_id"),
                   "kind": p.get("type"), "icon": p.get("type"), "title": p.get("title"),
                   "subtitle": p.get("subtitle") or p.get("status_title"),
                   "report": json.dumps(det, ensure_ascii=False) if det else None,
                   "status": (p.get("status") or "").upper(), "details": det,
                   "tool": det.get("tool"), "timestamp": p.get("timestamp")}
    elif name == "sessions.updated":
        s = p.get("session") or {}
        yield {"type": "session", "change": p.get("change"), "session_id": s.get("session_id") or p.get("session_id"),
               "status": s.get("status"), "title": s.get("title")}
    else:
        if any(name.startswith(x) for x in INTERESTING):
            yield {"type": "raw", "event": name, "payload": p}


def register(app, g):
    """g = app.py globals (engine, store, ACCOUNT_POOL, GEN_LOCK, auth, ...)."""
    engine = g["engine"]; store = g["store"]; POOL = g["ACCOUNT_POOL"]
    GEN_LOCK = g["GEN_LOCK"]; auth = g["auth"]; CFG = g["CFG"]; log = g["log"]
    renew = g["_renew_and_persist"]; sync_cookies = g["_sync_cookies"]
    model_cache: dict = {}
    TURNS: dict = {}   # turn_id -> queue of node invoke results posted by the Pi client

    @app.post("/pi/v1/invoke_result")
    def pi_invoke_result(body: dict = Body(...), _=Depends(auth)):
        tq = TURNS.get(body.get("turn_id") or "")
        if tq is None:
            raise HTTPException(404, "unknown or finished turn")
        tq.put(body)
        return {"ok": True}


    def with_page(fn, timeout=120):
        """Run fn(page) in a worker thread that owns a fresh muse.ai page."""
        acc = store.pick_account(rotate=False, preferred_id=getattr(engine, "current_acc_id", None))
        if not acc:
            raise HTTPException(400, "no Muse account imported in muse2api")
        out = {}
        def worker():
            POOL.acquire(account_id=acc["id"], timeout=timeout)
            try:
                with GEN_LOCK:
                    engine.start()
                    page = engine.ensure_page(acc["cookies"], acc.get("cookies_exp"), account_id=acc["id"])
                    wait_gateway(page)
                    out["r"] = fn(page)
            except Exception as e:  # noqa: BLE001
                out["e"] = e
            finally:
                POOL.release(acc["id"])
                engine.close_current_page()
        t = threading.Thread(target=worker, daemon=True); t.start(); t.join(timeout + 30)
        if "e" in out:
            raise HTTPException(502, f"{type(out['e']).__name__}: {out['e']}")
        return out.get("r")

    @app.post("/pi/v1/rpc")
    def pi_rpc(body: dict = Body(...), _=Depends(auth)):
        """Debug (PI_NATIVE_DEBUG_RPC=1): run raw gateway RPCs in one page session and return results + tapped events."""
        import os
        if os.environ.get("PI_NATIVE_DEBUG_RPC") != "1":
            raise HTTPException(404, "debug rpc disabled (set PI_NATIVE_DEBUG_RPC=1)")
        calls = body.get("calls") or []
        listen = float(body.get("listen") or 0)
        def fn(p):
            p.js(TAP_JS); out = []
            for c in calls:
                if c.get("sleep"):
                    time.sleep(float(c["sleep"])); continue
                if c.get("js"):
                    out.append({"js": p.js(c["js"], await_promise=True, timeout=60)}); continue
                try:
                    out.append({"ok": True, "r": gw_call(p, c["method"], c.get("params") or {})})
                except Exception as e:  # noqa: BLE001
                    out.append({"ok": False, "e": str(e)})
            time.sleep(listen)
            evs = []
            for fr in json.loads(p.js(DRAIN_JS) or "[]"):
                for line in fr.splitlines():
                    try: evs.append(json.loads(line))
                    except Exception: pass
            return {"results": out, "events": evs}
        return with_page(fn, timeout=int(listen) + 120)

    @app.get("/pi/v1/models")
    def pi_models(refresh: bool = False, _=Depends(auth)):
        if refresh or not model_cache or time.time() - model_cache.get("t", 0) > 3600:
            r = with_page(lambda p: gw_call(p, "model.get", {}))
            model_cache.update(t=time.time(), r=r)
        return model_cache["r"]

    @app.post("/pi/v1/fs/list")
    def pi_fs_list(body: dict = Body(...), _=Depends(auth)):
        path = (body.get("path") or "").lstrip("/")
        recursive = bool(body.get("recursive"))
        max_entries = int(body.get("max_entries") or 2000)
        def fn(p):
            if not recursive:
                return gw_call(p, "fs.list", {"path": path})
            out, stack = [], [path]
            while stack and len(out) < max_entries:
                cur = stack.pop()
                r = gw_call(p, "fs.list", {"path": cur})
                for e in r.get("entries") or []:
                    rel = (cur + "/" + e["name"]).lstrip("/")
                    e = dict(e, path=rel); out.append(e)
                    if e.get("type") == "directory" and not e["name"].startswith("."):
                        stack.append(rel)
            return {"path": path, "entries": out, "truncated": bool(stack)}
        return with_page(fn)

    @app.post("/pi/v1/fs/read")
    def pi_fs_read(body: dict = Body(...), _=Depends(auth)):
        paths = body.get("paths") or [body.get("path")]
        max_bytes = int(body.get("max_bytes") or 8 * 1024 * 1024)
        def fn(p):
            res = []
            for path in paths:
                path = (path or "").lstrip("/"); chunks = []; off = 0
                try:
                    while off < max_bytes:
                        r = gw_call(p, "fs.read", {"path": path, "offset": off, "len": 1048576})
                        chunks.append(r.get("data_base64") or ""); off += int(r.get("len") or 0)
                        if r.get("eof") or not r.get("len"):
                            break
                    import base64
                    data = b"".join(base64.b64decode(c) for c in chunks)
                    res.append({"path": path, "size": len(data), "data_base64": base64.b64encode(data).decode()})
                except Exception as e:  # noqa: BLE001
                    res.append({"path": path, "error": str(e)})
            return {"files": res}
        return with_page(fn)

    @app.post("/pi/v1/fs/write")
    def pi_fs_write(body: dict = Body(...), _=Depends(auth)):
        """Batch gateway `fs.write`: {files:[{path, text | data_base64}]} (paths relative to /home/hatch)."""
        files = body.get("files") or []
        def fn(p):
            res = []
            for f in files:
                path = (f.get("path") or "").lstrip("/")
                if not path or ".." in path.split("/"):
                    res.append({"path": path, "error": "bad path"}); continue
                prm = {"path": path, "overwrite": True, "append": False, "create_parent": True}
                if f.get("data_base64") is not None:
                    prm["data_base64"] = f["data_base64"]
                else:
                    prm["text"] = f.get("text") or ""
                try:
                    r = gw_call(p, "fs.write", prm)
                    res.append({"path": path, "bytes": (r or {}).get("bytes_written")})
                except Exception as e:  # noqa: BLE001
                    res.append({"path": path, "error": str(e)})
            return {"files": res}
        return with_page(fn, timeout=300)

    @app.post("/pi/v1/fs/delete")
    def pi_fs_delete(body: dict = Body(...), _=Depends(auth)):
        paths = body.get("paths") or ([body["path"]] if body.get("path") else [])
        def fn(p):
            res = []
            for path in paths:
                path = (path or "").lstrip("/")
                if not path.startswith("workspace/") or ".." in path.split("/"):
                    res.append({"path": path, "error": "only paths under workspace/ may be deleted"}); continue
                try:
                    gw_call(p, "fs.delete", {"path": path, "recursive": bool(body.get("recursive"))})
                    res.append({"path": path, "deleted": True})
                except Exception as e:  # noqa: BLE001
                    res.append({"path": path, "error": str(e)})
            return {"files": res}
        return with_page(fn)

    @app.post("/pi/v1/node/unpair")
    def pi_node_unpair(body: dict = Body(...), _=Depends(auth)):
        return with_page(lambda p: gw_call(p, "node.unpair", {"node_id": body.get("node_id")}))

    @app.post("/pi/v1/turn")
    def pi_turn(body: dict = Body(...), _=Depends(auth)):
        prompt = (body.get("prompt") or "").strip()
        if not prompt:
            raise HTTPException(400, "prompt is empty")
        model = body.get("model")              # e.g. "auto/auto", "ipnext/avocado-5.16-v0"
        session_id = body.get("session_id")    # continue an existing Muse thread
        timeout = int(body.get("timeout") or CFG.chat_timeout)
        settle = float(body.get("settle_timeout") or 45)
        acc = store.pick_account(rotate=True, preferred_id=getattr(engine, "current_acc_id", None))
        if not acc:
            raise HTTPException(400, "no Muse account imported in muse2api")
        q: queue.Queue = queue.Queue()
        stop = threading.Event()
        tid = "turn-" + uuid.uuid4().hex[:12]
        results_q: queue.Queue = queue.Queue()
        TURNS[tid] = results_q

        def worker():
            acc_id = acc["id"]; cookies = acc["cookies"]; exp = acc.get("cookies_exp")
            POOL.acquire(account_id=acc_id, timeout=timeout)
            seen_session = {"id": session_id}
            t_start = time.time()
            state = {"goal_seen": False, "goal_done": False, "last_ev": time.time()}
            try:
                fresh = renew(acc_id, wake_vm=True)
                if fresh:
                    cookies = fresh["cookies"]; exp = fresh.get("cookies_exp")
                with GEN_LOCK:
                    engine.start()
                    page = engine.ensure_page(cookies, exp, account_id=acc_id)
                    if session_id:
                        page.send("Page.navigate", {"url": f"https://muse.ai/thread/{session_id}"})
                        time.sleep(1.0)
                        engine._wait_ws_ready(page, timeout=15.0)
                    wait_gateway(page)
                    page.js(TAP_JS)
                    node = body.get("node")
                    if node:
                        end_n = time.time() + 20; err = None
                        while time.time() < end_n:
                            try:
                                gw_call(page, "node.register", node); err = None; break
                            except Exception as e:  # noqa: BLE001
                                err = e; time.sleep(0.5)
                        q.put({"type": "node", "registered": err is None, "error": str(err) if err else None})
                    prev_model = None
                    if model:
                        try:
                            prev_model = (gw_call(page, "model.get", {}) or {}).get("selected_model")
                            if prev_model == model:
                                prev_model = None
                            r = gw_call(page, "model.set", {"model": model})
                            q.put({"type": "model", "selected": (r or {}).get("selected_model"),
                                   "display_name": (r or {}).get("display_name")})
                        except Exception as e:  # noqa: BLE001
                            q.put({"type": "warning", "message": f"model.set failed: {e}"})

                    def drain():
                        try:
                            page.js(TAP_JS)   # re-install after any navigation
                            frames = json.loads(page.js(DRAIN_JS) or "[]")
                        except Exception:
                            return
                        for fr in frames:
                            for line in fr.splitlines():
                                try:
                                    ev = json.loads(line)
                                except Exception:
                                    if body.get("debug_events") and "invoke" in line:
                                        q.put({"type": "raw_frame", "text": line[:4000]})
                                    continue
                                if isinstance(ev, dict) and ev.get("method") == "node.invoke.request":
                                    pr = ev.get("params") or {}
                                    if node and pr.get("node_id") == node.get("node_id"):
                                        q.put({"type": "invoke", "turn_id": tid, "request_id": pr.get("request_id") or ev.get("id"),
                                               "command": pr.get("command"), "params": pr.get("params") or {},
                                               "timeout_ms": pr.get("timeout_ms")})
                                        state["last_ev"] = time.time()
                                    continue
                                if not isinstance(ev, dict) or ev.get("type") != "event":
                                    if body.get("debug_events") and "invoke" in line:
                                        q.put({"type": "raw_frame", "text": line[:4000]})
                                    continue
                                if body.get("debug_events"):
                                    q.put({"type": "raw_event", "event": ev.get("event"), "payload": ev.get("payload")})
                                for rec in normalize(ev):
                                    # navigation into an existing thread replays old activity: drop it
                                    ts = _ts(rec.get("created_at") or rec.get("timestamp"))
                                    if rec["type"] in ("tool", "activity") and ts is not None and ts < t_start - 5:
                                        continue
                                    if rec["type"] == "session" and rec.get("session_id"):
                                        if rec["change"] == "created" or not seen_session["id"]:
                                            seen_session["id"] = rec["session_id"]
                                    if rec["type"] == "activity":
                                        state["goal_seen"] = True
                                        state["goal_done"] = rec["finished"]
                                    state["last_ev"] = time.time()
                                    q.put(rec)

                    # text comes from the existing DOM streamer; events are drained in a side thread
                    alive = threading.Event(); alive.set()
                    def send_result(res):
                        ok = bool(res.get("ok", True))
                        payload = res.get("payload")
                        inner = {"ok": ok}
                        if ok:
                            inner["payload_json"] = json.dumps(payload, ensure_ascii=False)
                        else:
                            inner["error"] = {"code": res.get("code") or "tool_error", "message": str(res.get("error") or "failed")}
                        params = {"request_id": res["request_id"], "node_id": node["node_id"], "result": inner}
                        try:
                            r = gw_call(page, "nodes.invoke_result", params, timeout=20)
                            q.put({"type": "invoke_ack", "request_id": res["request_id"], "response": r})
                        except Exception as e:  # noqa: BLE001
                            q.put({"type": "warning", "message": f"nodes.invoke_result failed: {e}"})

                    def pump():
                        last_reg = 0.0
                        while alive.is_set() and not stop.is_set():
                            if node and time.time() - last_reg > 4:
                                last_reg = time.time()
                                try:
                                    gw_call(page, "node.register", node, timeout=10)
                                    gw_call(page, "node.heartbeat", {"node_id": node["node_id"]}, timeout=10)
                                except Exception as e:  # noqa: BLE001
                                    q.put({"type": "warning", "message": f"node re-register: {e}"})
                            while True:
                                try:
                                    res = results_q.get_nowait()
                                except queue.Empty:
                                    break
                                send_result(res)
                            drain(); time.sleep(0.3)
                    pt = threading.Thread(target=pump, daemon=True); pt.start()
                    text = []
                    engine.first_token_timeout = float(body.get("first_token_timeout") or (240 if node else 40))
                    try:
                        for chunk in engine.chat_stream(cookies, prompt, exp, timeout,
                                                        account_id=acc_id, stop_event=stop):
                            text.append(chunk)
                            q.put({"type": "text", "delta": chunk})
                    finally:
                        alive.clear(); pt.join(2)
                    q.put({"type": "text_done", "text": "".join(text)})
                    # Muse writes activity reports asynchronously after the reply; wait for them.
                    t_end = time.time() + settle
                    first_quiet = time.time() + 6
                    while time.time() < t_end and not stop.is_set():
                        drain()
                        if state["goal_done"] and time.time() - state["last_ev"] > 1.5:
                            break
                        if not state["goal_seen"] and time.time() > first_quiet:
                            break
                        time.sleep(0.4)
                    if not seen_session["id"]:
                        try:
                            href = page.js("location.pathname") or ""
                            if href.startswith("/thread/") and href != "/thread/new":
                                seen_session["id"] = href.split("/")[2]
                        except Exception:
                            pass
                    # authoritative reply text from the transcript (the DOM stream can repeat bubbles)
                    final_text = None
                    if seen_session["id"]:
                        try:
                            h = gw_call(page, "chat.history", {"session_id": seen_session["id"]})
                            evs = [e for e in (h or {}).get("chat_events") or []
                                   if e.get("event_name") == "message.assistant"]
                            if evs:
                                # all assistant messages after the last user message of this turn
                                allev = (h or {}).get("chat_events") or []
                                last_user = max((i for i, e in enumerate(allev) if e.get("event_name") == "message.user"), default=-1)
                                parts = [(e.get("payload") or {}).get("content") or e.get("display_text") or ""
                                         for e in allev[last_user + 1:] if e.get("event_name") == "message.assistant"]
                                final_text = "\n\n".join(p for p in parts if p) or None
                        except Exception as e:  # noqa: BLE001
                            q.put({"type": "warning", "message": f"chat.history failed: {e}"})
                    if prev_model and body.get("restore_model", True):
                        # model selection is account-wide on Muse; don't leak it into other muse2api clients
                        try:
                            gw_call(page, "model.set", {"model": prev_model})
                        except Exception:
                            pass
                    if final_text:
                        q.put({"type": "final_text", "text": final_text})
                store.mark(acc_id, True, "")
                try:
                    sync_cookies(acc_id)
                except Exception:
                    pass
                q.put({"type": "done", "session_id": seen_session["id"], "turn_id": tid})
            except Exception as e:  # noqa: BLE001
                log.warning("pi turn failed: %s", e)
                q.put({"type": "error", "message": f"{type(e).__name__}: {e}"})
            finally:
                POOL.release(acc_id)
                engine.first_token_timeout = 40.0
                TURNS.pop(tid, None)
                engine.close_current_page()
                q.put(None)

        threading.Thread(target=worker, daemon=True).start()

        def gen():
            try:
                yield "event: start\ndata: " + json.dumps({"turn_id": tid}) + "\n\n"
                while True:
                    try:
                        rec = q.get(timeout=15)
                    except queue.Empty:
                        yield ": keepalive\n\n"; continue
                    if rec is None:
                        break
                    yield f"event: {rec['type']}\ndata: {json.dumps(rec, ensure_ascii=False)}\n\n"
            finally:
                stop.set()
        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
