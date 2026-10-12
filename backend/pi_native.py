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
from fastapi.responses import JSONResponse, StreamingResponse

from pi_host import Registry, ThreadHost, sse_from_host

HOSTS_REG: dict = {}

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

import os


def _envf(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name) or default)
    except ValueError:
        return default


# Timeouts (seconds). Env wins; a /pi/v1/turn body may override per turn.
#   connect:    page/gateway ready + node.register
#   first_byte: max silence (no text AND no gateway event / device call) before Muse's first text
#   idle:       max gap between text chunks / events once text has started
#   turn:       hard cap for one turn
#   heartbeat:  interval of SSE ": keep-alive" comments while nothing else is sent
def timeouts(body: dict, node: bool) -> dict:
    return {
        "connect": float(body.get("connect_timeout") or _envf("PI_NATIVE_CONNECT_TIMEOUT", 20)),
        "first_byte": float(body.get("first_token_timeout") or body.get("first_byte_timeout")
                            or _envf("PI_NATIVE_FIRST_BYTE_TIMEOUT" if node else "PI_NATIVE_FIRST_BYTE_TIMEOUT_CHAT",
                                     900 if node else 60)),
        "idle": float(body.get("idle_timeout") or _envf("PI_NATIVE_IDLE_TIMEOUT", 300)),
        "turn": float(body.get("timeout") or _envf("PI_NATIVE_TURN_TIMEOUT", 3600 if node else 600)),
        "heartbeat": max(1.0, _envf("PI_NATIVE_HEARTBEAT", 10)),
    }


def summarize(v, n: int = 160) -> str:
    t = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
    t = " ".join(t.split())
    return t if len(t) <= n else t[:n] + "…"


def session_status(page, sid: str) -> dict:
    """Muse thread state via gateway `sessions.get {id}` -> status running|completed|interrupted|..."""
    r = gw_call(page, "sessions.get", {"id": sid}, timeout=15) or {}
    return {"status": r.get("status"), "updated_at_ms": r.get("updated_at_ms"), "snippet": r.get("snippet")}


def reply_after_last_user(page, sid: str) -> str | None:
    h = gw_call(page, "chat.history", {"session_id": sid}) or {}
    allev = h.get("chat_events") or []
    last_user = max((i for i, e in enumerate(allev) if e.get("event_name") == "message.user"), default=-1)
    parts = [(e.get("payload") or {}).get("content") or e.get("display_text") or ""
             for e in allev[last_user + 1:] if e.get("event_name") == "message.assistant"]
    return "\n\n".join(p for p in parts if p) or None


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

    @app.post("/pi/v1/invoke_result")
    def pi_invoke_result(body: dict = Body(...), _=Depends(auth)):
        reg = HOSTS_REG.get("reg")
        h = reg.by_turn.get(body.get("turn_id") or "") if reg else None
        if h is None or h.closed.is_set():
            raise HTTPException(404, "unknown or finished turn")
        h.results.put(body)
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

    @app.post("/pi/v1/thread/state")
    def pi_thread_state(body: dict = Body(...), _=Depends(auth)):
        """{session_id} -> {status, reply?}. status comes from gateway sessions.get (running|completed|interrupted)."""
        sid = body.get("session_id")
        if not sid:
            raise HTTPException(400, "session_id required")
        h = HOSTS_REG["reg"].get(sid) if HOSTS_REG.get("reg") else None
        if h is not None:
            try:
                st = {"status": h.ops.status(h, sid)} if h.page_open else {"status": "running" if h.busy else h.last_status}
            except Exception as e:  # noqa: BLE001
                st = {"status": None, "error": str(e)}
            st.update(h.info(body.get("client_id")))
            log.info("pi_thread_state %s", json.dumps({"session_id": sid, "status": st.get("status"), "held": True}))
            return st
        def fn(p):
            st = session_status(p, sid)
            st["held"] = False
            if st["status"] and st["status"] != "running" and body.get("with_reply", True):
                try:
                    st["reply"] = reply_after_last_user(p, sid)
                except Exception as e:  # noqa: BLE001
                    st["reply_error"] = str(e)
            return st
        r = with_page(fn, timeout=60)
        log.info("pi_thread_state %s", json.dumps({"session_id": sid, "status": r.get("status")}))
        return r

    @app.post("/pi/v1/thread/cancel")
    def pi_thread_cancel(body: dict = Body(...), _=Depends(auth)):
        """{session_id} -> gateway chat.cancel {session_id} -> {cancelled, message_id, status}."""
        sid = body.get("session_id")
        if not sid:
            raise HTTPException(400, "session_id required")
        h = HOSTS_REG["reg"].get(sid) if HOSTS_REG.get("reg") else None
        if h is not None:
            r = h.request_cancel(wait=True)
            log.warning("pi_thread_cancel %s", json.dumps({"session_id": sid, "held": True, **r}))
            return r
        def fn(p):
            r = gw_call(p, "chat.cancel", {"session_id": sid}, timeout=20) or {}
            time.sleep(1.5)
            return {"cancelled": bool(r.get("cancelled")), "message_id": r.get("message_id"),
                    "status": session_status(p, sid)["status"]}
        r = with_page(fn, timeout=60)
        log.warning("pi_thread_cancel %s", json.dumps({"session_id": sid, **r}))
        return r

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

    # ------------------------------------------------------------------ thread hosts
    REG = Registry(log)
    HOSTS_REG["reg"] = REG

    @app.on_event("shutdown")
    def _pi_hosts_shutdown():
        REG.close_all("muse2api shutdown")

    def host_cfg(TO):
        return {"cap": TO["turn"], "watch": _envf("PI_NATIVE_WATCH_INTERVAL", 3),
                "grace": _envf("PI_NATIVE_HOLD_GRACE", 10), "heartbeat": TO["heartbeat"],
                "reaper_slack": _envf("PI_NATIVE_REAPER_SLACK", 60), "cancel_wait": 30}

    class RealOps:
        """Page operations for a ThreadHost, backed by muse2api's engine (runs on the host thread)."""

        def __init__(self, body, TO, acc):
            self.body, self.TO, self.acc = body, TO, acc
            self.node = body.get("node")
            self.page = None; self.ctx_id = None; self.prev_model = None
            self.state = {"goal_seen": False, "goal_done": False, "last_ev": time.time(), "events": 0,
                          "bytes": 0, "last_invoke": None, "last_result": None}
            self.t_turn = time.time()
            self.page_lock = threading.RLock()

        def open(self, host):
            acc_id = self.acc["id"]; cookies = self.acc["cookies"]; exp = self.acc.get("cookies_exp")
            POOL.acquire(account_id=acc_id, timeout=self.TO["turn"])
            self.pool_held = True
            if not GEN_LOCK.acquire(timeout=self.TO["turn"]):
                raise TimeoutError("connect_timeout: no free browser slot")
            self.gen_held = True
            fresh = renew(acc_id, wake_vm=True)
            if fresh:
                cookies = fresh["cookies"]; exp = fresh.get("cookies_exp")
            self.cookies, self.exp = cookies, exp
            engine.start()
            page = engine.ensure_page(cookies, exp, account_id=acc_id)
            self.page = page; self.ctx_id = engine.current_context_id
            sid = host.session
            if sid:
                page.send("Page.navigate", {"url": f"https://muse.ai/thread/{sid}"})
                time.sleep(1.0)
                engine._wait_ws_ready(page, timeout=self.TO["connect"])
            if not wait_gateway(page, timeout=self.TO["connect"]):
                host.emit({"type": "warning", "message": f"Muse gateway not ready after {self.TO['connect']:.0f}s (connect timeout); continuing"})
            page.js(TAP_JS)
            if self.node:
                end_n = time.time() + self.TO["connect"]; err = None
                while time.time() < end_n:
                    try:
                        gw_call(page, "node.register", self.node); err = None; break
                    except Exception as e:  # noqa: BLE001
                        err = e; time.sleep(0.5)
                host.emit({"type": "node", "registered": err is None, "error": str(err) if err else None})

        def _model(self, host):
            model = self.body.get("model")
            if not model:
                return
            try:
                pm = (gw_call(self.page, "model.get", {}) or {}).get("selected_model")
                if pm != model and self.prev_model is None:
                    self.prev_model = pm
                r = gw_call(self.page, "model.set", {"model": model})
                host.emit({"type": "model", "selected": (r or {}).get("selected_model"), "display_name": (r or {}).get("display_name")})
            except Exception as e:  # noqa: BLE001
                host.emit({"type": "warning", "message": f"model.set failed: {e}"})

        # pump: node keep-alive, invoke results, gateway events (runs for the whole host lifetime)
        def pump(self, host):
            page, node, st = self.page, self.node, self.state
            if page is None:
                return
            now = time.time()
            if node and now - getattr(self, "_last_reg", 0) > 4:
                self._last_reg = now
                try:
                    gw_call(page, "node.register", node, timeout=10)
                    gw_call(page, "node.heartbeat", {"node_id": node["node_id"]}, timeout=10)
                except Exception as e:  # noqa: BLE001
                    host.emit({"type": "warning", "message": f"node re-register: {e}"})
            while True:
                try:
                    res = host.results.get_nowait()
                except queue.Empty:
                    break
                self._send_result(host, res)
            self._drain(host)

        def _send_result(self, host, res):
            ok = bool(res.get("ok", True)); payload = res.get("payload")
            inner = {"ok": ok}
            if ok:
                inner["payload_json"] = json.dumps(payload, ensure_ascii=False)
            else:
                inner["error"] = {"code": res.get("code") or "tool_error", "message": str(res.get("error") or "failed")}
            self.state["last_result"] = {"request_id": res.get("request_id"), "ok": ok, "result": summarize(payload if ok else res.get("error"))}
            self.state["last_ev"] = time.time()
            try:
                r = gw_call(self.page, "nodes.invoke_result", {"request_id": res["request_id"], "node_id": self.node["node_id"], "result": inner}, timeout=20)
                host.emit({"type": "invoke_ack", "request_id": res["request_id"], "response": r})
            except Exception as e:  # noqa: BLE001
                host.emit({"type": "warning", "message": f"nodes.invoke_result failed: {e}"})

        def _drain(self, host):
            st, node, body = self.state, self.node, self.body
            try:
                self.page.js(TAP_JS)
                frames = json.loads(self.page.js(DRAIN_JS) or "[]")
            except Exception:
                return
            for fr in frames:
                for line in fr.splitlines():
                    try:
                        ev = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(ev, dict) and ev.get("method") == "node.invoke.request":
                        pr = ev.get("params") or {}
                        if node and pr.get("node_id") == node.get("node_id"):
                            host.emit({"type": "invoke", "turn_id": host.tid, "request_id": pr.get("request_id") or ev.get("id"),
                                       "command": pr.get("command"), "params": pr.get("params") or {}, "timeout_ms": pr.get("timeout_ms")})
                            st["last_ev"] = time.time(); st["events"] += 1
                            st["last_invoke"] = {"request_id": pr.get("request_id") or ev.get("id"), "command": pr.get("command"),
                                                 "args": summarize(pr.get("params") or {}), "at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
                        continue
                    if not isinstance(ev, dict) or ev.get("type") != "event":
                        continue
                    if body.get("debug_events"):
                        host.emit({"type": "raw_event", "event": ev.get("event"), "payload": ev.get("payload")})
                    for rec in normalize(ev):
                        ts = _ts(rec.get("created_at") or rec.get("timestamp"))
                        if rec["type"] in ("tool", "activity") and ts is not None and ts < self.t_turn - 5:
                            continue
                        if rec["type"] == "session" and rec.get("session_id"):
                            if rec["change"] == "created" or not host.session:
                                host.set_session(rec["session_id"])
                        if rec["type"] == "activity":
                            st["goal_seen"] = True; st["goal_done"] = rec["finished"]
                        st["last_ev"] = time.time(); st["events"] += 1
                        host.emit(rec)

        def stream(self, host, prompt):
            self.t_turn = time.time()
            self.state.update({"goal_seen": False, "goal_done": False, "last_ev": time.time(), "bytes": 0})
            self._model(host)
            engine.first_token_timeout = self.TO["first_byte"]
            engine.idle_timeout = self.TO["idle"]
            engine.activity_fn = (lambda: self.state["last_ev"]) if self.node else None
            try:
                for chunk in engine.chat_stream(self.cookies, prompt, self.exp, int(self.TO["turn"]),
                                                account_id=self.acc["id"], stop_event=host.stop_stream):
                    self.state["bytes"] += len(chunk.encode())
                    yield chunk
            finally:
                if not host.session:
                    try:
                        href = self.page.js("location.pathname") or ""
                        if href.startswith("/thread/") and href != "/thread/new":
                            host.set_session(href.split("/")[2])
                    except Exception:
                        pass

        def settle(self, host):
            settle = float(self.body.get("settle_timeout") or 45)
            t_end = time.time() + settle; first_quiet = time.time() + 6
            while time.time() < t_end and not host.cancel_req.is_set():
                if self.state["goal_done"] and time.time() - self.state["last_ev"] > 1.5:
                    break
                if not self.state["goal_seen"] and time.time() > first_quiet:
                    break
                time.sleep(0.4)
            if not host.session:
                try:
                    href = self.page.js("location.pathname") or ""
                    if href.startswith("/thread/") and href != "/thread/new":
                        host.set_session(href.split("/")[2])
                except Exception:
                    pass

        def status(self, host, sid):
            return session_status(self.page, sid)["status"]

        def cancel(self, host, sid):
            return gw_call(self.page, "chat.cancel", {"session_id": sid}, timeout=20)

        def reply(self, host, sid):
            return reply_after_last_user(self.page, sid)

        def after_turn(self, host):
            if self.prev_model and self.body.get("restore_model", True):
                try:
                    gw_call(self.page, "model.set", {"model": self.prev_model})
                except Exception:
                    pass
                self.prev_model = None
            store.mark(self.acc["id"], True, "")
            try:
                sync_cookies(self.acc["id"])
            except Exception:
                pass

        def error_details(self, host):
            s = self.state
            return {"bytes_received": s["bytes"], "events_received": s["events"],
                    "last_invoke": s["last_invoke"], "last_result": s["last_result"]}

        def close(self, host):
            # may run on the reaper thread: close by handle, not via the engine's thread-local page
            try:
                if self.page is not None:
                    self.page.close()
            except Exception:
                pass
            try:
                if self.ctx_id and engine.browser:
                    engine.browser.send("Target.disposeBrowserContext", {"browserContextId": self.ctx_id})
                    with engine._contexts_lock:
                        engine._active_contexts.discard(self.ctx_id)
            except Exception:
                pass
            self.page = None
            if threading.current_thread() is host.thread:
                try:
                    engine.page = None; engine.current_context_id = None; engine.current_acc_id = None
                except Exception:
                    pass
                engine.first_token_timeout = 40.0; engine.idle_timeout = 0; engine.activity_fn = None
            if getattr(self, "gen_held", False):
                self.gen_held = False; GEN_LOCK.release()
            if getattr(self, "pool_held", False):
                self.pool_held = False; POOL.release(self.acc["id"])

    @app.post("/pi/v1/turn")
    def pi_turn(body: dict = Body(...), _=Depends(auth)):
        prompt = (body.get("prompt") or "").strip()
        if not prompt:
            raise HTTPException(400, "prompt is empty")
        session_id = body.get("session_id")
        client_id = body.get("client_id")
        TO = timeouts(body, bool(body.get("node")))
        tid = "turn-" + uuid.uuid4().hex[:12]
        with REG.lock:
            h = REG.get(session_id)
            if h is not None:
                # server-side per-thread lock: one in-flight request per Muse thread, across all clients
                if h.submit(prompt, tid, client_id):
                    log.warning("pi_host %s", json.dumps({"event": "page_reused", "session_id": session_id, "turn_id": tid}))
                    return StreamingResponse(sse_from_host(h, TO["heartbeat"]), media_type="text/event-stream",
                                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
                log.warning("pi_host %s", json.dumps({"event": "busy_rejected", "session_id": session_id, "phase": h.phase}))
                return JSONResponse(status_code=409, content={"error": "thread_busy", "session_id": session_id,
                                                              "phase": h.phase, "turn_id": h.tid,
                                                              "owner_is_you": bool(client_id and client_id == h.owner),
                                                              "message": "Muse thread already has a turn in flight"})
            acc = store.pick_account(rotate=True, preferred_id=getattr(engine, "current_acc_id", None))
            if not acc:
                raise HTTPException(400, "no Muse account imported in muse2api")
            h = ThreadHost(REG, RealOps(body, TO, acc), client_id, host_cfg(TO), log, session_id=session_id)
            h.start(prompt, tid)
        return StreamingResponse(sse_from_host(h, TO["heartbeat"]), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.post("/pi/v1/thread/attach")
    def pi_thread_attach(body: dict = Body(...), _=Depends(auth)):
        """Re-attach to a held thread whose request timed out: streams buffered + live records (incl. invokes)."""
        h = REG.get(body.get("session_id"))
        if h is None:
            raise HTTPException(404, "thread is not held by muse2api")
        if body.get("client_id") != h.owner:
            return JSONResponse(status_code=409, content={"error": "thread_busy", "owner_is_you": False,
                                                          "message": "thread is held for another client"})
        TO = timeouts(body, True)
        log.warning("pi_host %s", json.dumps({"event": "attached", "session_id": h.session, "turn_id": h.tid}))
        return StreamingResponse(sse_from_host(h, TO["heartbeat"], first={"type": "start", "turn_id": h.tid, "attached": True}),
                                 media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/pi/v1/hosts")
    def pi_hosts(_=Depends(auth)):
        return [{"session_id": h.session, **h.info(None), "age_s": round(time.time() - h.started, 1)} for h in REG.open_hosts()]
