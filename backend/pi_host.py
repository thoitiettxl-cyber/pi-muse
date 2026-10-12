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

mono = time.monotonic      # all durations / deadlines; wall clock only for printed timestamps

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
    def reserve(self, session_id, client_id, prompt, tid, make_host, accounts=None):
        """Reserve the thread AND an account in ONE lock hold, before any page is opened.

        accounts: candidate account dicts in preference order (a memory snapshot taken by the caller;
        picking among them here is pure in-memory work). make_host(account) builds the host.
        -> ("reuse", host) | ("new", host) | ("busy", info) | ("account_busy", info) | ("closing", host)
        Never blocks, never does I/O; a CLOSING host is returned so the caller can wait OUTSIDE the lock."""
        with self.lock:
            h = self.by_session.get(session_id) if session_id else None
            if h is not None:
                if h.state == GRACE:
                    h._accept_turn_locked(prompt, tid, client_id)
                    self.by_turn[tid] = h
                    return "reuse", h
                if h.state in (CLOSING, CLOSED):
                    return "closing", h
                return "busy", {"error": "thread_busy", "session_id": session_id, "phase": h.phase, "turn_id": h.tid,
                                "held": True, "retry_after": 1 if h.state == BROKEN else None,
                                "muse_status": "running" if h.state in (RUNNING, WATCH) else h.last_status,
                                "owner_is_you": bool(client_id and client_id == h.owner)}
            acc = None
            if accounts is not None:
                acc = next((a for a in accounts if a["id"] not in self.accounts), None)
                if acc is None:
                    # muse2api drives one page per account: every candidate is held by another thread
                    other = self.accounts.get(accounts[0]["id"]) if accounts else None
                    return "account_busy", {"error": "account_busy",
                                            "account_id": accounts[0]["id"] if accounts else None,
                                            "held_by_thread": other.session if other else None,
                                            "held_state": other.state if other else None,
                                            "retry_after_s": max(1, int(other.retry_after_s())) if other else 5}
            h = make_host(acc)                  # constructor touches no registry state, no I/O
            h._new_turn_locked(tid)
            if acc is not None:
                self.accounts[acc["id"]] = h    # account marked busy in the same hold as the thread
                h.account_id = acc["id"]
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


    def release(self, host):
        """Remove the host AND its account reservation in the same lock hold.
        Only entries that point at THIS host are removed (an account re-reserved by another host is untouched)."""
        with self.lock:
            owned = host.account_id is not None and self.accounts.get(host.account_id) is host
            self.hosts.discard(host)
            for d in (self.by_session, self.by_turn, self.accounts):
                for k in [k for k, v in d.items() if v is host]:
                    del d[k]
            host.state = CLOSED
            return owned

    # ---- leak protection
    def reap_once(self, now: float | None = None):
        now = now or mono()
        for h in self.open_hosts():
            if h.state in (STARTING, CHECKING) and now > h.started + h.cfg.get("start_deadline", 45):
                h._log("host_reaped", reason="stuck opening/checking", age_s=round(now - h.started, 1), state=h.state)
                h.force_close("reaped: start deadline exceeded")
                continue
            if now > h.turn_started + h.cfg["cap"] + h.cfg["reaper_slack"]:
                h._log("host_reaped", turn_age_s=round(now - h.turn_started, 1))
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
        self._sub = "starting"           # sub-phase while RUNNING (first_byte|streaming|settle); host thread only
        self.out: queue.Queue = queue.Queue()
        self._queues = [self.out]        # one queue per turn; readers keep theirs until its END
        self.ready = threading.Event()   # set once the page is open and checked (or that failed)
        self.muse_busy = False           # Muse reported the thread `running` before we sent anything
        self.open_error = None           # page open / state check failed: fail closed
        self.broken = None               # reason the host must not take another turn
        self.cmd: queue.Queue = queue.Queue()
        self.results: queue.Queue = queue.Queue()
        self.closed = threading.Event()
        self.released = threading.Event()   # set once the registry entry + account are gone
        self.cancel_req = threading.Event()
        self.stop_stream = threading.Event()
        self.started = mono()            # host creation
        self.turn_started = self.started # reset at every turn: the cap is per turn
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
        self.turn_started = mono()
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
    def phase(self):
        """Derived, never stored: the state, refined by the sub-phase while a turn streams."""
        st = self.state
        return self._sub if st == RUNNING else st

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

    def _status(self, timeout: float | None = None):
        """ops.status with its own deadline: a hung sessions.get can never block cap/cancel handling."""
        timeout = timeout or self.cfg.get("status_timeout", 15)
        box = {}
        def call():
            try:
                box["v"] = self.ops.status(self, self.session)
            except BaseException as e:  # noqa: BLE001
                box["e"] = e
        t = threading.Thread(target=call, daemon=True, name=f"pi-host-status-{self.tid}")
        t.start(); t.join(timeout)
        if t.is_alive():
            raise TimeoutError(f"sessions.get did not answer within {timeout:.0f}s")
        if "e" in box:
            raise box["e"]
        return box.get("v")

    def retry_after_s(self) -> float:
        """Rough hint for account_busy: grace remainder, else a poll interval."""
        if self.state == GRACE:
            return self.cfg.get("grace", 10)
        return max(5.0, self.cfg.get("watch", 3) * 2)

    def _cas(self, frm: tuple, to: str) -> bool:
        """Compare-and-set: change state only from one of `frm` (never overwrites CLOSING/BROKEN/CLOSED)."""
        with self.reg.lock:
            if self.state in frm:
                self.state = to
                return True
            return False

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
            self.ops.open(self)                       # page (blocking I/O, no lock held); account reserved in reserve()
            self.page_open = True
            self._log("page_open")
            if self.session and self.cfg.get("check_running", True):
                # existing thread that muse2api did not hold: Muse may still be running it
                self._set_state(CHECKING)
                try:
                    st = self._status()
                except Exception as e:  # noqa: BLE001
                    self.open_error = f"state_unknown: sessions.get failed: {e}"
                    self._break(self.open_error)
                    return                             # fail closed: finally releases page + account
                self.last_status = st
                if st not in NOT_RUNNING:
                    self.muse_busy = True
                    self._break(f"muse_status {st} at open")
                    return
            if not self._cas((STARTING, CHECKING), RUNNING):   # broken/closed meanwhile (reaper, timeout)
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
        t0 = time.time(); m0 = mono()
        self.emit({"type": "start", "turn_id": self.tid, "held_page": True})
        try:
            self._sub = "first_byte"
            text = []
            for chunk in self.ops.stream(self, prompt):
                self._sub = "streaming"
                text.append(chunk)
                self.emit({"type": "text", "delta": chunk})
            if self.cancel_req.is_set():
                return self._watch()      # performs the cancel and the wait
            self._sub = "settle"
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
                    st = self._status()
                except Exception:  # noqa: BLE001
                    st = None
            held = retryable and st == "running"
            rec = {"type": "error", "message": f"{type(e).__name__}: {e}", "code": code, "retryable": retryable,
                   "phase": getattr(e, "phase", None) or self.phase, "timeout_s": getattr(e, "timeout_s", None),
                   "waited_s": getattr(e, "waited_s", None), "elapsed_s": round(mono() - m0, 1),
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
        self._set_state(WATCH)
        self._log("watch_start")
        fails = 0
        while not self.closed.is_set():
            if mono() > self.turn_started + self.cfg["cap"]:
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
                st = self._status()
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
        end = mono() + self.cfg.get("cancel_wait", 30)
        st = None
        while mono() < end:
            try:
                st = self._status()
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
        end = mono() + timeout
        while mono() < end and self.state in (RUNNING, WATCH, STARTING, CHECKING):
            time.sleep(0.1)
        return {"cancelled": self.last_status in NOT_RUNNING, "status": self.last_status,
                "cancel_unconfirmed": self.last_status not in NOT_RUNNING, "closed": self.closed.is_set()}

    def force_close(self, reason: str):
        with self._close_lock:
            again = self.closed.is_set()
            self.closed.set()
        if again:
            # closed before (e.g. by the reaper while open() was still creating the page): close again,
            # idempotently, so a page created after the first close cannot leak
            try:
                self.ops.close_page(self)
            except Exception as e:  # noqa: BLE001
                self._log("close_error", error=str(e))
            return
        with self.reg.lock:
            if self.state != CLOSED:
                self.state = CLOSING
        try:
            self.ops.close_page(self)             # always attempted, idempotent (page may exist without page_open)
        except Exception as e:  # noqa: BLE001
            self._log("close_error", error=str(e))
        self.page_open = False
        self.reg.release(self)                    # registry entry + account reservation (only if ours), one lock hold
        self.released.set()
        try:
            self.ops.release_account(self)        # muse2api's account pool: ops releases only what IT acquired
        except Exception as e:  # noqa: BLE001
            self._log("release_error", error=str(e))
        self.ready.set()
        for q in list(self._queues):
            q.put(None)
        self._log("page_closed", reason=reason, age_s=round(mono() - self.started, 1), broken=self.broken)

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


def start_or_busy(reg: Registry, session_id, client_id, prompt, tid, make_host, wait_s: float, accounts=None,
                  closing_wait_s: float = 5.0, closing_retries: int = 3):
    """Server-side per-thread lock + account reservation + Muse state check.
    Returns ("reuse"|"new", host) | ("busy"|"account_busy"|"unavailable", info)."""
    for _ in range(closing_retries + 1):
        kind, h = reg.reserve(session_id, client_id, prompt, tid, make_host, accounts=accounts)
        if kind != "closing":
            break
        h.released.wait(closing_wait_s)   # the previous page of this thread is closing: wait OUTSIDE the lock
    else:
        return "busy", {"error": "thread_busy", "session_id": session_id, "phase": "closing", "retry_after": 1,
                        "held": True, "muse_status": None, "owner_is_you": False}
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
