"""Thread hosts for pi-native turns (branch `pi-native`).

A ThreadHost owns ONE muse.ai page (and with it the Pi device node) for ONE Muse thread. Its lifetime
is tied to the Muse thread's state, not to the HTTP request:

  * a timeout ends only the SSE request (an `error` record with `held: true`); the page, the node
    registration/heartbeat and the gateway event pump keep running;
  * a watcher polls `sessions.get` every `watch` seconds and closes the page only once the thread is
    no longer `running` (completed / interrupted / failed), after emitting `final_text` + `done`;
  * a client may re-attach (`/pi/v1/thread/attach`) and receives everything buffered meanwhile,
    including device `invoke` requests, so device calls made by Muse in that window still succeed;
  * after a turn the host waits `grace` seconds; a follow-up turn for the same thread in that window
    reuses the page (no second device per thread), otherwise the page is closed;
  * `request_cancel()` -> chat.cancel, wait until the status really leaves `running`, then close;
  * leak protection: a hard cap (`cap`, default = turn cap 3600 s) enforced by the watcher, plus a
    module-level reaper that force-closes any host past `cap + slack` even if its threads hang/crash,
    and `close_all()` on shutdown.

The page operations are behind `ops` so tests can drive the host with a fake Muse.
"""
from __future__ import annotations

import json
import queue
import threading
import time
from datetime import datetime

END = "_end"     # record flag: the SSE request ends after this record (the host may live on)


class ThreadBusy(Exception):
    def __init__(self, host: "ThreadHost"):
        super().__init__("thread_busy")
        self.host = host


class Registry:
    def __init__(self, log=None):
        self.lock = threading.RLock()
        self.by_session: dict[str, ThreadHost] = {}
        self.by_turn: dict[str, ThreadHost] = {}
        self.all: set[ThreadHost] = set()
        self.log = log
        self._reaper = None

    def get(self, session_id: str | None):
        if not session_id:
            return None
        with self.lock:
            h = self.by_session.get(session_id)
            return h if h and not h.closed.is_set() else None

    def bind_session(self, host: "ThreadHost", sid: str):
        with self.lock:
            other = self.by_session.get(sid)
            if other is not None and other is not host and not other.closed.is_set():
                return False
            self.by_session[sid] = host
            return True

    def bind_turn(self, host, tid):
        with self.lock:
            self.by_turn[tid] = host

    def add(self, host):
        with self.lock:
            self.all.add(host)
        self.ensure_reaper()

    def remove(self, host):
        with self.lock:
            self.all.discard(host)
            for d in (self.by_session, self.by_turn):
                for k in [k for k, v in d.items() if v is host]:
                    d.pop(k, None)

    def open_hosts(self):
        with self.lock:
            return [h for h in self.all if not h.closed.is_set()]

    def reap_once(self, now: float | None = None):
        now = now or time.time()
        for h in self.open_hosts():
            if now > h.started + h.cfg["cap"] + h.cfg["reaper_slack"]:
                h._log("host_reaped", age_s=round(now - h.started, 1))
                h.force_close("reaped: hard cap exceeded")

    def ensure_reaper(self):
        if self._reaper and self._reaper.is_alive():
            return
        def loop():
            while True:
                time.sleep(self_interval[0])
                try:
                    self.reap_once()
                except Exception:  # noqa: BLE001
                    pass
        self_interval = [5.0]
        self._reaper = threading.Thread(target=loop, daemon=True, name="pi-host-reaper")
        self._reaper.start()

    def close_all(self, reason="shutdown"):
        for h in self.open_hosts():
            h.request_cancel(wait=False) if h.busy else None
            h.force_close(reason)


class ThreadHost:
    """cfg: cap, watch, grace, heartbeat, reaper_slack, cancel_wait"""

    def __init__(self, registry: Registry, ops, owner: str | None, cfg: dict, log=None, session_id: str | None = None):
        self.reg, self.ops, self.owner, self.cfg, self.logger = registry, ops, owner, cfg, log
        self.session = session_id
        self.out: queue.Queue = queue.Queue()
        self.cmd: queue.Queue = queue.Queue()
        self.results: queue.Queue = queue.Queue()
        self.closed = threading.Event()
        self.cancel_req = threading.Event()
        self.stop_stream = threading.Event()
        self.started = time.time()
        self.busy = True            # a Muse turn is in flight (streaming or detached+watched)
        self.phase = "starting"     # starting|first_byte|streaming|settle|watch|grace|closing|closed
        self.page_open = False
        self.tid = None
        self.last_status = None
        self.attached = 0
        self._close_lock = threading.Lock()
        self.thread = None
        registry.add(self)
        if session_id:
            registry.bind_session(self, session_id)

    # ---- helpers
    def _log(self, ev, **kw):
        if self.logger:
            try:
                self.logger.warning("pi_host %s", json.dumps({"event": ev, "session_id": self.session, "turn_id": self.tid, **kw}, ensure_ascii=False))
            except Exception:  # noqa: BLE001
                pass

    def emit(self, rec: dict):
        self.out.put(rec)

    def set_session(self, sid: str):
        if sid and not self.session:
            self.session = sid
            self.reg.bind_session(self, sid)

    def new_turn_id(self, tid: str):
        self.tid = tid
        self.reg.bind_turn(self, tid)

    # ---- lifecycle
    def start(self, prompt: str, tid: str):
        self.new_turn_id(tid)
        self.thread = threading.Thread(target=self._run, args=(prompt,), daemon=True, name=f"pi-host-{tid}")
        self.thread.start()

    def submit(self, prompt: str, tid: str, owner: str | None) -> bool:
        """Reuse this host (same page/device) for a follow-up turn while it is in its grace period."""
        with self.reg.lock:
            if self.closed.is_set() or self.busy or self.phase != "grace":
                return False
            self.busy = True
            self.owner = owner or self.owner
            while True:            # drop records nobody attached for (previous turn's tail)
                try:
                    self.out.get_nowait()
                except queue.Empty:
                    break
            self.new_turn_id(tid)
            self.cmd.put(prompt)
            return True

    def _run(self, prompt: str):
        try:
            self.ops.open(self)
            self.page_open = True
            self._log("page_open")
            pump = threading.Thread(target=self._pump, daemon=True, name=f"pi-host-pump-{self.tid}")
            pump.start()
            while prompt is not None and not self.closed.is_set():
                self.busy = True
                self.cancel_req.clear(); self.stop_stream.clear()
                self._turn(prompt)
                with self.reg.lock:
                    self.busy = False
                    self.phase = "grace"
                prompt = self._next_prompt()
        except Exception as e:  # noqa: BLE001
            self._log("host_error", error=f"{type(e).__name__}: {e}")
            self.emit({"type": "error", "message": f"{type(e).__name__}: {e}", "code": getattr(e, "code", None),
                       "retryable": False, "session_id": self.session, "turn_id": self.tid, END: True})
        finally:
            self.force_close("host finished")

    def _next_prompt(self):
        try:
            return self.cmd.get(timeout=self.cfg["grace"])
        except queue.Empty:
            with self.reg.lock:
                if not self.cmd.empty():
                    return self.cmd.get_nowait()
                self.phase = "closing"
            return None

    def _pump(self):
        while not self.closed.is_set():
            try:
                self.ops.pump(self)
            except Exception as e:  # noqa: BLE001
                self._log("pump_error", error=str(e))
            self.closed.wait(self.cfg.get("pump_interval", 0.3))

    def _turn(self, prompt: str):
        t0 = time.time()
        self.emit({"type": "start", "turn_id": self.tid, "held_page": True})
        try:
            self.phase = "first_byte"
            text = []
            for chunk in self.ops.stream(self, prompt):
                self.phase = "streaming"
                text.append(chunk)
                self.emit({"type": "text", "delta": chunk})
            if self.cancel_req.is_set():
                self._watch()      # performs the cancel and the wait
                return
            self.phase = "settle"
            self.emit({"type": "text_done", "text": "".join(text)})
            self.ops.settle(self)
            self._finish(status="completed")
        except Exception as e:  # noqa: BLE001
            code = getattr(e, "code", None)
            retryable = bool(code and str(code).endswith("_timeout"))
            st = None
            if self.session:
                try:
                    st = self.ops.status(self, self.session)
                except Exception:  # noqa: BLE001
                    st = None
            held = retryable and st == "running"
            rec = {"type": "error", "message": f"{type(e).__name__}: {e}", "code": code, "retryable": retryable,
                   "phase": getattr(e, "phase", None) or self.phase, "timeout_s": getattr(e, "timeout_s", None),
                   "waited_s": getattr(e, "waited_s", None), "elapsed_s": round(time.time() - t0, 1),
                   "started_at": datetime.fromtimestamp(t0).astimezone().isoformat(timespec="seconds"),
                   "failed_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                   "session_id": self.session, "turn_id": self.tid, "muse_status": st,
                   "held": held, "muse_cancelled": False, END: True}
            rec.update(self.ops.error_details(self))
            self._log("turn_error", **{k: v for k, v in rec.items() if k != END})
            self.emit(rec)
            if held:
                self._watch()

    def _finish(self, status):
        reply = None
        if self.session:
            try:
                reply = self.ops.reply(self, self.session)
            except Exception as e:  # noqa: BLE001
                self.emit({"type": "warning", "message": f"chat.history failed: {e}"})
        if reply:
            self.emit({"type": "final_text", "text": reply})
        self.ops.after_turn(self)
        self.emit({"type": "done", "session_id": self.session, "turn_id": self.tid, "status": status, END: True})
        self._log("turn_done", status=status)

    def _watch(self):
        """Detached: keep the page + device alive until the Muse thread leaves `running`."""
        self.phase = "watch"
        self._log("watch_start")
        fails = 0
        while not self.closed.is_set():
            if time.time() > self.started + self.cfg["cap"]:
                self._log("cap_cancel")
                self._do_cancel()
                self.emit({"type": "error", "message": "turn exceeded the hard cap; cancelled", "code": "turn_cap",
                           "retryable": False, "session_id": self.session, "turn_id": self.tid,
                           "muse_status": self.last_status, END: True})
                return
            if self.cancel_req.is_set():
                self._do_cancel()
                self._finish(status=self.last_status or "interrupted")
                return
            try:
                st = self.ops.status(self, self.session)
                fails = 0
            except Exception as e:  # noqa: BLE001
                fails += 1
                self._log("watch_status_error", error=str(e), fails=fails)
                if fails >= 5:
                    self.emit({"type": "error", "message": f"cannot read Muse thread state: {e}", "code": "state_unknown",
                               "retryable": False, "session_id": self.session, "turn_id": self.tid, END: True})
                    return
                st = "running"
            self.last_status = st
            if st != "running":
                self._log("watch_end", status=st)
                self._finish(status=st)
                return
            self.cancel_req.wait(self.cfg["watch"])

    def _do_cancel(self):
        try:
            r = self.ops.cancel(self, self.session) or {}
        except Exception as e:  # noqa: BLE001
            r = {"error": str(e)}
        end = time.time() + self.cfg.get("cancel_wait", 30)
        st = None
        while time.time() < end:
            try:
                st = self.ops.status(self, self.session)
            except Exception:  # noqa: BLE001
                st = None
            if st and st != "running":
                break
            time.sleep(min(1.0, self.cfg["watch"]))
        self.last_status = st
        self._log("cancelled", result=r, status=st)
        return st

    def request_cancel(self, wait: bool = True, timeout: float = 45) -> dict:
        self.cancel_req.set(); self.stop_stream.set()
        if not wait:
            return {"requested": True}
        end = time.time() + timeout
        while time.time() < end and self.busy and not self.closed.is_set():
            time.sleep(0.1)
        return {"cancelled": True, "status": self.last_status, "closed": self.closed.is_set()}

    def force_close(self, reason: str):
        with self._close_lock:
            if self.closed.is_set():
                return
            self.phase = "closing"
            self.closed.set()
        try:
            self.ops.close(self)
        except Exception as e:  # noqa: BLE001
            self._log("close_error", error=str(e))
        self.page_open = False
        self.phase = "closed"
        self.busy = False
        self._log("page_closed", reason=reason, age_s=round(time.time() - self.started, 1))
        self.reg.remove(self)
        self.out.put(None)

    # ---- state for /pi/v1/thread/state
    def info(self, client_id: str | None):
        return {"held": True, "busy": self.busy, "phase": self.phase, "turn_id": self.tid,
                "owner_is_you": bool(client_id and client_id == self.owner), "page_open": self.page_open}


def sse_from_host(host: ThreadHost, heartbeat: float, first: dict | None = None):
    """SSE generator reading a host's queue. Ends after an END record; never stops the host."""
    host.attached += 1
    try:
        if first:
            yield f"event: {first['type']}\ndata: {json.dumps(first, ensure_ascii=False)}\n\n"
        while True:
            try:
                rec = host.out.get(timeout=heartbeat)
            except queue.Empty:
                if host.closed.is_set() and host.out.empty():
                    break
                yield ": keep-alive\n\n"
                continue
            if rec is None:
                break
            end = rec.pop(END, False)
            yield f"event: {rec['type']}\ndata: {json.dumps(rec, ensure_ascii=False)}\n\n"
            if end:
                break
    finally:
        host.attached -= 1
