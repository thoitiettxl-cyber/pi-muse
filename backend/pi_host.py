"""Thread hosts for pi-native turns (branch `pi-native`).

A ThreadHost owns ONE muse.ai page (and with it the Pi device node) for ONE Muse thread. Its lifetime
is tied to the Muse thread's state, not to the HTTP request:

  * a timeout ends only the SSE request (an `error` record with `held: true`); the page, the node
    registration/heartbeat and the gateway event pump keep running;
  * a watcher polls `sessions.get` every `watch` seconds and finishes the turn only once the thread is
    no longer `running` (completed / interrupted / failed), emitting `final_text` + `done`;
  * a client may re-attach (`/pi/v1/thread/attach`) and receives everything buffered meanwhile,
    including device `invoke` requests, so device calls made by Muse in that window still succeed;
  * after a turn the host waits `grace` seconds (state GRACE); a follow-up turn for the same thread in
    that window reuses the page (no second device per thread), otherwise the page is closed;
  * a host that cannot prove the Muse thread is idle becomes BROKEN: it never takes another turn, it is
    closed and removed from the registry, and the next request goes through the `sessions.get` check again;
  * leak protection: a hard cap enforced by the watcher, a reaper that force-closes any host past
    `cap + slack` even if its threads hang, and `close_all()` on shutdown.

Locking: ONE plain (non-reentrant) lock, `Registry.lock`, guards the session/turn maps, host states and
the account reservations. Nothing does I/O, logging or queue emits while holding it.

Host states:  STARTING -> CHECKING (existing thread: sessions.get) -> RUNNING <-> WATCH -> GRACE
              GRACE -> RUNNING (reuse) | CLOSING ;  any -> BROKEN -> CLOSING ;  CLOSING -> CLOSED (removed)
"""
from __future__ import annotations

import json
import queue
import threading
import time
from datetime import datetime

END = "_end"     # record flag: the SSE request ends after this record (the host may live on)
NOT_RUNNING = ("completed", "interrupted", "cancelled", "canceled", "failed", "error", "idle")

STARTING, CHECKING, RUNNING, WATCH, GRACE, BROKEN, CLOSING, CLOSED = (
    "starting", "checking", "running", "watch", "grace", "broken", "closing", "closed")


class Turn:
    """Per-turn epoch: tool results are accepted only for the current turn, once per call id.
    Lives on the host, so it survives re-attaching (a new SSE connection)."""

    def __init__(self, tid: str, epoch: int):
        self.tid, self.epoch = tid, epoch
        self.invoked: set[str] = set()     # request ids forwarded to a client
        self.answered: set[str] = set()    # request ids whose result was accepted


class Registry:
    def __init__(self, log=None):
        self.lock = threading.Lock()                 # the ONE lock (never nested, never held over I/O)
        self.by_session: dict[str, ThreadHost] = {}
        self.by_turn: dict[str, ThreadHost] = {}
        self.hosts: set[ThreadHost] = set()
        self.accounts: dict[str, ThreadHost] = {}    # account id -> host holding it (pi-native reservation)
        self.log = log
        self._reaper = None

    # ---- lookups (lock held briefly, no I/O)
    def get(self, session_id: str | None):
        if not session_id:
            return None
        with self.lock:
            return self.by_session.get(session_id)

    def get_turn(self, tid: str | None):
        with self.lock:
            return self.by_turn.get(tid or "")

    def open_hosts(self):
        with self.lock:
            return [h for h in self.hosts if h.state != CLOSED]

    # ---- reservation: per-thread lock + reuse decision, all in one lock hold
    def reserve(self, session_id, client_id, prompt, tid, make_host):
        """-> ("reuse", host) | ("new", host) | ("busy", info). Never blocks, never does I/O."""
        with self.lock:
            h = self.by_session.get(session_id) if session_id else None
            if h is not None:
                if h.state == GRACE:
                    h._accept_turn_locked(prompt, tid, client_id)
                    self.by_turn[tid] = h
                    return "reuse", h
                return "busy", {"error": "thread_busy", "session_id": session_id, "phase": h.state, "turn_id": h.tid,
                                "held": True, "retry_after": 1 if h.state in (CLOSING, BROKEN) else None,
                                "muse_status": "running" if h.state in (RUNNING, WATCH) else h.last_status,
                                "owner_is_you": bool(client_id and client_id == h.owner)}
            h = make_host()                     # constructor touches no registry state
            h._new_turn_locked(tid)
            self.hosts.add(h)
            self.by_turn[tid] = h
            if session_id:
                self.by_session[session_id] = h
            return "new", h

    def bind_session(self, host, sid: str) -> bool:
        with self.lock:
            other = self.by_session.get(sid)
            if other is not None and other is not host:
                return False
            self.by_session[sid] = host
            return True

    def mark_account(self, host, account_id: str):
        with self.lock:
            self.accounts[account_id] = host
            host.account_id = account_id

    def release(self, host):
        """Remove the host AND its account reservation in the same lock hold."""
        with self.lock:
            self.hosts.discard(host)
            for d in (self.by_session, self.by_turn, self.accounts):
                for k in [k for k, v in d.items() if v is host]:
                    del d[k]
            host.state = CLOSED

    # ---- leak protection
    def reap_once(self, now: float | None = None):
        now = now or time.time()
        for h in self.open_hosts():
            if now > h.started + h.cfg["cap"] + h.cfg["reaper_slack"]:
                h._log("host_reaped", age_s=round(now - h.started, 1))
                h.force_close("reaped: hard cap exceeded")

    def ensure_reaper(self, interval: float = 5.0):
        if self._reaper and self._reaper.is_alive():
            return
        def loop():
            while True:
                time.sleep(interval)
                try:
                    self.reap_once()
                except Exception:  # noqa: BLE001
                    pass
        self._reaper = threading.Thread(target=loop, daemon=True, name="pi-host-reaper")
        self._reaper.start()

    def close_all(self, reason="shutdown"):
        for h in self.open_hosts():
            if h.state in (RUNNING, WATCH):
                h.request_cancel(wait=False)
            h.force_close(reason)


class ThreadHost:
    """cfg: cap, watch, grace, heartbeat, reaper_slack, cancel_wait, check_running"""

    def __init__(self, registry: Registry, ops, owner: str | None, cfg: dict, log=None, session_id: str | None = None):
        self.reg, self.ops, self.owner, self.cfg, self.logger = registry, ops, owner, cfg, log
        self.session = session_id
        self.state = STARTING
        self.phase = "starting"          # finer-grained, for logs: first_byte|streaming|settle|watch|...
        self.out: queue.Queue = queue.Queue()
        self._queues = [self.out]        # one queue per turn; readers keep theirs until its END
        self.ready = threading.Event()   # set once the page is open and checked (or that failed)
        self.muse_busy = False           # Muse reported the thread `running` before we sent anything
        self.open_error = None           # page open / state check failed: fail closed
        self.broken = None               # reason the host must not take another turn
        self.cmd: queue.Queue = queue.Queue()
        self.results: queue.Queue = queue.Queue()
        self.closed = threading.Event()
        self.cancel_req = threading.Event()
        self.stop_stream = threading.Event()
        self.started = time.time()
        self.page_open = False
        self.tid = None
        self.turn: Turn | None = None
        self._epoch = 0
        self.account_id = None
        self.last_status = None
        self.attached = 0
        self._close_lock = threading.Lock()   # only serializes force_close; never taken with reg.lock
        self.thread = None

    # ---- called with reg.lock held: pure in-memory
    def _new_turn_locked(self, tid):
        self._epoch += 1
        self.tid = tid
        self.turn = Turn(tid, self._epoch)

    def _accept_turn_locked(self, prompt, tid, owner):
        self.state = RUNNING
        self.owner = owner or self.owner
        self.out = queue.Queue()          # fresh queue: a reader of the previous turn ends at its own END
        self._queues.append(self.out)
        del self._queues[:-2]
        self._new_turn_locked(tid)
        self.cmd.put(prompt)

    @property
    def busy(self):
        return self.state not in (GRACE, CLOSED)

    # ---- helpers (never called with reg.lock held)
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

    def note_invoke(self, request_id) -> bool:
        """True the first time this turn sees a device call id (dedupe across re-attaches)."""
        with self.reg.lock:
            t = self.turn
            if t is None or request_id in t.invoked:
                return False
            t.invoked.add(request_id)
            return True

    def accept_result(self, body: dict) -> tuple[bool, str]:
        """Tool result from a client: only for the current turn epoch, once per call id."""
        with self.reg.lock:
            t = self.turn
            if self.state in (CLOSING, CLOSED, BROKEN) or t is None:
                return False, "turn is not running"
            if body.get("turn_id") != t.tid:
                return False, f"stale turn {body.get('turn_id')} (current epoch {t.epoch}: {t.tid})"
            rid = body.get("request_id")
            if rid in t.answered:
                return False, f"duplicate result for {rid}"
            t.answered.add(rid)
        self.results.put(body)
        return True, "ok"

    def _set_state(self, st):
        with self.reg.lock:
            if self.state not in (CLOSING, CLOSED, BROKEN):
                self.state = st

    def _break(self, reason):
        with self.reg.lock:
            self.broken = self.broken or reason
            if self.state not in (CLOSING, CLOSED):
                self.state = BROKEN
        self._log("broken", reason=reason)

    # ---- lifecycle
    def start(self, prompt: str):
        self.thread = threading.Thread(target=self._run, args=(prompt,), daemon=True, name=f"pi-host-{self.tid}")
        self.thread.start()
        self.reg.ensure_reaper()

    def _run(self, prompt: str):
        try:
            acc = self.ops.open(self)                 # page + account (blocking I/O, no lock held)
            if acc:
                self.reg.mark_account(self, acc)
            self.page_open = True
            self._log("page_open")
            if self.session and self.cfg.get("check_running", True):
                # existing thread that muse2api did not hold: Muse may still be running it
                self._set_state(CHECKING)
                try:
                    st = self.ops.status(self, self.session)
                except Exception as e:  # noqa: BLE001
                    self.open_error = f"state_unknown: sessions.get failed: {e}"
                    self._break(self.open_error)
                    return                             # fail closed: finally releases page + account
                self.last_status = st
                if st not in NOT_RUNNING:
                    self.muse_busy = True
                    self._break(f"muse_status {st} at open")
                    return
            self._set_state(RUNNING)
            if self.state != RUNNING:                  # broken/closed while opening (e.g. check timed out)
                return
            self.ready.set()
            pump = threading.Thread(target=self._pump, daemon=True, name=f"pi-host-pump-{self.tid}")
            pump.start()
            while prompt is not None:
                safe = self._turn(prompt)              # True only if Muse is confirmed not running
                if not safe:
                    self._break(self.broken or "thread state not confirmed idle")
                    break                              # BROKEN: never enters GRACE, never reused
                with self.reg.lock:
                    if self.state not in (RUNNING, WATCH):  # closed/broken meanwhile (reaper, shutdown)
                        break
                    self.state = GRACE
                self.phase = "grace"
                prompt = self._next_prompt()
        except Exception as e:  # noqa: BLE001
            if not self.ready.is_set():
                self.open_error = f"{type(e).__name__}: {e}"
            self._break(f"host error: {type(e).__name__}: {e}")
            self.emit({"type": "error", "message": f"{type(e).__name__}: {e}", "code": getattr(e, "code", None),
                       "retryable": False, "session_id": self.session, "turn_id": self.tid, END: True})
        finally:
            self.ready.set()
            self.force_close("broken: " + self.broken if self.broken else "host finished")

    def _next_prompt(self):
        """Grace period: wait for a follow-up turn; on expiry move to CLOSING atomically."""
        try:
            return self.cmd.get(timeout=self.cfg["grace"])
        except queue.Empty:
            pass
        with self.reg.lock:
            if self.state == RUNNING:          # reserve() accepted a turn in the same instant
                return self.cmd.get_nowait()
            if self.state == GRACE:
                self.state = CLOSING           # from now on reserve() answers busy/closing, not reuse
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
                return self._watch()      # performs the cancel and the wait
            self.phase = "settle"
            self.emit({"type": "text_done", "text": "".join(text)})
            self.ops.settle(self)
            self._finish(status="completed")
            return True
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
                return self._watch()
            self.last_status = st
            return st in NOT_RUNNING     # unknown (None) or still running -> not safe for reuse

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
        self._set_state(WATCH)
        self._log("watch_start")
        fails = 0
        while not self.closed.is_set():
            if time.time() > self.started + self.cfg["cap"]:
                st = self._do_cancel()
                unconfirmed = st not in NOT_RUNNING
                self._log("cap_cancel", muse_status=st, cancel_unconfirmed=unconfirmed)
                self.emit({"type": "error", "message": "turn exceeded the hard cap; cancelled" + (" (cancel NOT confirmed by Muse)" if unconfirmed else ""),
                           "code": "turn_cap", "retryable": False, "session_id": self.session, "turn_id": self.tid,
                           "muse_status": st, "cancel_unconfirmed": unconfirmed, END: True})
                return False   # never reuse after the cap
            if self.cancel_req.is_set():
                st = self._do_cancel()
                unconfirmed = st not in NOT_RUNNING
                if unconfirmed:
                    self._log("cancel_unconfirmed", muse_status=st)
                    self.emit({"type": "error", "message": "cancel requested but Muse still reports the thread as running",
                               "code": "cancel_unconfirmed", "retryable": False, "session_id": self.session,
                               "turn_id": self.tid, "muse_status": st, "cancel_unconfirmed": True, END: True})
                    return False
                self._finish(status=st)
                return True
            try:
                st = self.ops.status(self, self.session)
                fails = 0
            except Exception as e:  # noqa: BLE001
                fails += 1
                self._log("watch_status_error", error=str(e), fails=fails)
                if fails >= 5:
                    self.emit({"type": "error", "message": f"cannot read Muse thread state: {e}", "code": "state_unknown",
                               "retryable": False, "session_id": self.session, "turn_id": self.tid,
                               "muse_status": None, END: True})
                    return False
                st = "running"
            self.last_status = st
            if st != "running":
                self._log("watch_end", status=st)
                self._finish(status=st)
                return st in NOT_RUNNING
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
        while time.time() < end and self.state in (RUNNING, WATCH, STARTING, CHECKING):
            time.sleep(0.1)
        return {"cancelled": self.last_status in NOT_RUNNING, "status": self.last_status,
                "cancel_unconfirmed": self.last_status not in NOT_RUNNING, "closed": self.closed.is_set()}

    def force_close(self, reason: str):
        with self._close_lock:
            if self.closed.is_set():
                return
            self.closed.set()
        with self.reg.lock:
            if self.state != CLOSED:
                self.state = CLOSING
        try:
            self.ops.close_page(self)             # I/O, no lock held
        except Exception as e:  # noqa: BLE001
            self._log("close_error", error=str(e))
        self.page_open = False
        self.reg.release(self)                    # registry entry + account reservation, one lock hold
        try:
            self.ops.release_account(self)        # muse2api's account pool (its own lock, not nested)
        except Exception as e:  # noqa: BLE001
            self._log("release_error", error=str(e))
        self.ready.set()
        for q in list(self._queues):
            q.put(None)
        self._log("page_closed", reason=reason, age_s=round(time.time() - self.started, 1), broken=self.broken)

    # ---- state for /pi/v1/thread/state
    def info(self, client_id: str | None):
        return {"held": True, "busy": self.busy, "state": self.state, "phase": self.phase, "turn_id": self.tid,
                "owner_is_you": bool(client_id and client_id == self.owner), "page_open": self.page_open}


def sse_from_host(host: ThreadHost, heartbeat: float, first: dict | None = None):
    """SSE generator reading a host's queue. Ends after an END record; never stops the host.
    The queue is bound NOW (not at first iteration), so a later turn's fresh queue never leaks in."""
    q = host.out
    host.attached += 1

    def gen():
        try:
            if first:
                yield f"event: {first['type']}\ndata: {json.dumps(first, ensure_ascii=False)}\n\n"
            while True:
                try:
                    rec = q.get(timeout=heartbeat)
                except queue.Empty:
                    if host.closed.is_set() and q.empty():
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
    return gen()


def start_or_busy(reg: Registry, session_id, client_id, prompt, tid, make_host, wait_s: float):
    """Server-side per-thread lock + Muse state check.
    Returns ("reuse"|"new", host) | ("busy", info) | ("unavailable", info) [fail closed -> 503]."""
    kind, h = reg.reserve(session_id, client_id, prompt, tid, make_host)
    if kind != "new":
        return kind, h
    h.start(prompt)
    if session_id:
        if not h.ready.wait(wait_s):
            h._break("state check did not finish in time")
            h.request_cancel(wait=False)
            return "unavailable", {"error": "state_unknown", "session_id": session_id,
                                   "message": "could not confirm the Muse thread state in time; nothing was sent"}
        if h.muse_busy:
            return "busy", {"error": "thread_busy", "session_id": session_id, "phase": "muse_running",
                            "held": False, "muse_status": h.last_status, "owner_is_you": False}
        if h.open_error:
            return "unavailable", {"error": "state_unknown", "session_id": session_id, "message": h.open_error}
    return "new", h
