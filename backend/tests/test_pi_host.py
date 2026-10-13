"""ThreadHost lifecycle against a fake Muse (no browser). Run: venv/bin/python -m unittest tests.test_pi_host -v"""
import sys, threading, time, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pi_host import BROKEN, END, GRACE, Registry, ThreadHost, sse_from_host, start_or_busy  # noqa: E402


class Timeout(Exception):
    code = "first_byte_timeout"; phase = "first_byte"; timeout_s = 0.2; waited_s = 0.2


class FakeMuse:
    """Fake page/device. device_online == page open and node registered."""
    def __init__(self, run_s=1.5, invoke_at=0.4, hang_status=False, never_finish=False, timeout=True,
                 open_status=None, status_fails_after_error=False, cancel_noop=False, status_none_on_error=False):
        self.open_status, self.fail_status, self.cancel_noop = open_status, status_fails_after_error, cancel_noop
        self.none_on_error = status_none_on_error; self.errored = False
        self.pool_busy = False; self.open_raises = None; self.hang_stream = False; self.hang_in_watch = False
        self.run_s, self.invoke_at, self.hang, self.never, self.timeout = run_s, invoke_at, hang_status, never_finish, timeout
        self.opens = 0; self.closes = 0; self.device_online = False; self.cancels = 0
        self.t0 = None; self.status_v = "running"; self.invoked = False; self.results = []; self.prompts = []
        self.hang_ev = threading.Event()
    open_pages = 0          # class-wide: pages alive across all fakes
    def open(self, h):
        self.opens += 1; self.pool_busy = True
        FakeMuse.open_pages += 1; self.page = True
        if getattr(self, "block_open", None):
            self.block_open.wait()
        if getattr(self, "open_raises_after_page", None):
            raise self.open_raises_after_page
        self.device_online = True
    def pump(self, h):
        if self.t0 and not self.invoked and time.time() - self.t0 > self.invoke_at:
            self.invoked = True
            h.emit({"type": "invoke", "turn_id": h.tid, "request_id": "r1", "command": "bash", "params": {"command": "echo x >> steps.txt"}})
        while not h.results.empty():
            r = h.results.get()
            self.results.append({**r, "device_online": self.device_online})
    def stream(self, h, prompt):
        self.prompts.append(prompt); self.t0 = time.time(); self.status_v = "running"; self.invoked = False
        h.set_session("S1")
        if self.hang_stream:
            self.hang_ev.wait(); return
        if self.timeout:
            time.sleep(0.2); self.errored = True; raise Timeout("等待助手首字响应超时")
        time.sleep(0.3)
        yield "hello"
    def settle(self, h): pass
    def status(self, h, sid):
        if self.t0 is None and self.open_raises:
            raise self.open_raises
        if self.t0 is None and self.open_status:
            return self.open_status
        if self.errored and self.none_on_error:
            raise RuntimeError("gateway gone")
        if self.errored and self.fail_status and h.phase == "watch":
            raise RuntimeError("sessions.get failed")
        if self.hang or (self.hang_in_watch and h.state == "watch"):
            self.hang_ev.wait()
        if self.status_v == "running" and not self.never and time.time() - self.t0 > self.run_s:
            self.status_v = "completed"
        return self.status_v
    def cancel(self, h, sid):
        self.cancels += 1
        if self.cancel_noop:
            return {"cancelled": False}
        threading.Timer(0.3, lambda: setattr(self, "status_v", "interrupted")).start()
        return {"cancelled": True}
    def reply(self, h, sid): return "BG RESULT" if self.status_v == "completed" else None
    def after_turn(self, h): pass
    def error_details(self, h): return {"bytes_received": 0, "events_received": 1}
    def close_page(self, h):
        if getattr(self, "slow_close", 0):
            time.sleep(self.slow_close)
        if getattr(self, "page", False):
            self.page = False; FakeMuse.open_pages -= 1
        self.closes += 1; self.device_online = False
    def release_account(self, h): self.pool_busy = False


CFG = {"cap": 30, "watch": 0.1, "grace": 0.5, "heartbeat": 1, "reaper_slack": 0.2, "cancel_wait": 5, "pump_interval": 0.05,
       "status_timeout": 0.3}


def mk(reg, m, cfg, owner="c1", session=None, prompt="x", tid="t1"):
    kind, h = reg.reserve(session, owner, prompt, tid, lambda acc=None: ThreadHost(reg, m, owner, cfg, None, session_id=session))
    assert kind == "new", kind
    h.start(prompt)
    return h


def submit(reg, h, prompt, tid, owner):
    if not h.session or reg.get(h.session) is not h:
        return False          # not (or no longer) registered for its thread: cannot be reused
    kind, _ = reg.reserve(h.session, owner, prompt, tid, lambda: (_ for _ in ()).throw(AssertionError("new host")))
    return kind == "reuse"


def read_until(h, pred, timeout=10):
    end = time.time() + timeout; got = []
    while time.time() < end:
        try:
            r = h.out.get(timeout=0.1)
        except Exception:
            continue
        if r is None:
            break
        got.append(r)
        if pred(r):
            break
    return got


class HostTests(unittest.TestCase):
    def test_timeout_keeps_device_online_and_tool_succeeds_then_closes(self):
        reg = Registry(); m = FakeMuse()
        h = mk(reg, m, dict(CFG), prompt="do it")
        recs = read_until(h, lambda r: r["type"] == "error")
        err = recs[-1]
        self.assertTrue(err["held"]); self.assertEqual(err["muse_status"], "running"); self.assertTrue(err[END])
        self.assertTrue(m.device_online, "device stays online after the request timed out")
        recs = read_until(h, lambda r: r["type"] == "invoke")        # re-attached client sees the invoke
        self.assertEqual(recs[-1]["type"], "invoke")
        h.results.put({"request_id": "r1", "ok": True, "payload": {"output": ""}})
        recs = read_until(h, lambda r: r["type"] == "done")
        self.assertEqual(m.results[0]["device_online"], True, "tool result delivered while device online")
        self.assertEqual([r for r in recs if r["type"] == "final_text"][0]["text"], "BG RESULT")
        self.assertEqual(recs[-1]["status"], "completed")
        t_done = time.time()
        h.closed.wait(5)
        self.assertTrue(h.closed.is_set()); self.assertLess(time.time() - t_done, CFG["grace"] + 1.0)
        self.assertEqual((m.opens, m.closes), (1, 1)); self.assertEqual(reg.open_hosts(), [])

    def test_watcher_hung_cap_reaper_closes_page(self):
        reg = Registry(); m = FakeMuse(); m.hang_stream = True   # host thread stuck: only the reaper can help
        h = mk(reg, m, dict(CFG, cap=0.8))
        time.sleep(1.2)
        self.assertFalse(h.closed.is_set())
        reg.reap_once()
        self.assertTrue(h.closed.is_set()); self.assertEqual(m.closes, 1); self.assertFalse(m.device_online)
        m.hang_ev.set()

    def test_watcher_cap_cancels(self):
        reg = Registry(); m = FakeMuse(never_finish=True)
        h = mk(reg, m, dict(CFG, cap=1.0))
        h.closed.wait(8)
        self.assertTrue(h.closed.is_set()); self.assertEqual(m.cancels, 1); self.assertEqual(m.status_v, "interrupted")

    def test_max_wait_cancel_waits_for_interrupted_then_closes(self):
        reg = Registry(); m = FakeMuse(never_finish=True)
        h = mk(reg, m, dict(CFG))
        read_until(h, lambda r: r["type"] == "error")
        r = h.request_cancel(wait=True, timeout=5)
        self.assertEqual(m.cancels, 1); self.assertEqual(r["status"], "interrupted")
        recs = read_until(h, lambda r: r["type"] == "done")
        self.assertEqual(recs[-1]["status"], "interrupted")
        h.closed.wait(3); self.assertTrue(h.closed.is_set()); self.assertEqual(m.closes, 1)

    def test_busy_then_reuse_same_page_in_grace(self):
        reg = Registry(); m = FakeMuse(timeout=False)
        h = mk(reg, m, dict(CFG, grace=3))
        self.assertFalse(submit(reg, h, "y", "t2", "c2"), "busy thread rejects a second turn")
        read_until(h, lambda r: r["type"] == "done")
        time.sleep(0.1)
        self.assertIs(reg.get("S1"), h)
        self.assertEqual(h.state, GRACE)
        self.assertTrue(submit(reg, h, "continue", "t2", "c1"))
        read_until(h, lambda r: r["type"] == "done")
        self.assertEqual(m.prompts, ["x", "continue"]); self.assertEqual(m.opens, 1, "one page/device per thread")
        h.closed.wait(5); self.assertEqual(m.closes, 1)

    def test_close_all(self):
        reg = Registry(); m = FakeMuse(never_finish=True)
        h = mk(reg, m, dict(CFG))
        time.sleep(0.4); reg.close_all()
        self.assertTrue(h.closed.is_set()); self.assertEqual(m.closes, 1)


class RaceTests(unittest.TestCase):
    def test_1_existing_thread_running_in_muse_not_held_is_busy(self):
        reg = Registry(); m = FakeMuse(open_status="running")
        kind, info = start_or_busy(reg, "S1", "B", "continue", "t1",
                                   lambda acc=None: ThreadHost(reg, m, "B", dict(CFG), None, session_id="S1"), wait_s=5)
        self.assertFalse(m.pool_busy); self.assertEqual(reg.accounts, {})
        self.assertEqual(kind, "busy"); self.assertEqual(info["muse_status"], "running"); self.assertFalse(info["held"])
        self.assertEqual(m.prompts, [], "nothing was sent to the running thread")
        time.sleep(0.3); self.assertEqual(reg.open_hosts(), []); self.assertEqual(m.closes, 1)

    def test_1b_two_windows_race_only_one_passes(self):
        reg = Registry(); m = FakeMuse(timeout=False, open_status="completed")
        out = []
        def go(c):
            out.append(start_or_busy(reg, "S1", c, "hi", f"t-{c}", lambda acc=None: ThreadHost(reg, m, c, dict(CFG), None, session_id="S1"), wait_s=5)[0])
        ts = [threading.Thread(target=go, args=(c,)) for c in "AB"]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(sorted(out), ["busy", "new"]); self.assertEqual(m.opens, 1)

    def test_2a_state_unknown_in_watch_poisons_no_grace(self):
        reg = Registry(); m = FakeMuse(never_finish=True, status_fails_after_error=True)
        h = mk(reg, m, dict(CFG, grace=5))
        recs = read_until(h, lambda r: r["type"] == "error" and r.get("code") == "state_unknown")
        self.assertEqual(recs[-1]["code"], "state_unknown")
        self.assertFalse(submit(reg, h, "continue", "t2", "c1"), "no follow-up turn while Muse may still run")
        h.closed.wait(2); self.assertTrue(h.closed.is_set()); self.assertTrue(h.broken)
        self.assertEqual(m.prompts, ["x"])

    def test_2b_error_with_unknown_status_not_held_poisons(self):
        reg = Registry(); m = FakeMuse(status_none_on_error=True)
        h = mk(reg, m, dict(CFG, grace=5))
        recs = read_until(h, lambda r: r["type"] == "error")
        self.assertFalse(recs[-1]["held"]); self.assertIsNone(recs[-1]["muse_status"])
        time.sleep(0.2)
        self.assertFalse(submit(reg, h, "continue", "t2", "c1"))
        h.closed.wait(2); self.assertTrue(h.closed.is_set()); self.assertIsNone(reg.get("S1"))

    def test_3_cap_cancel_unconfirmed_is_flagged(self):
        reg = Registry(); m = FakeMuse(never_finish=True, cancel_noop=True)
        h = mk(reg, m, dict(CFG, cap=0.8, cancel_wait=0.5))
        recs = read_until(h, lambda r: r.get("code") == "turn_cap", timeout=8)
        self.assertTrue(recs[-1]["cancel_unconfirmed"]); self.assertEqual(recs[-1]["muse_status"], "running")
        h.closed.wait(3); self.assertTrue(h.closed.is_set()); self.assertFalse(submit(reg, h, "y", "t2", "c1"))

    def test_4_reuse_does_not_steal_previous_turns_tail(self):
        reg = Registry(); m = FakeMuse(timeout=False)
        h = mk(reg, m, dict(CFG, grace=3))
        time.sleep(0.9)        # turn 1 finished; its records (incl. END) are still unread
        self.assertEqual(h.state, GRACE)
        reader1 = sse_from_host(h, 0.2)   # bound to turn 1's queue
        self.assertTrue(submit(reg, h, "continue", "t2", "c1"))
        reader2 = sse_from_host(h, 0.2)
        r1 = [x for x in reader1 if not x.startswith(":")]
        self.assertTrue(r1[-1].startswith("event: done") and '"t1"' in r1[-1], "reader 1 ends at turn 1 END")
        self.assertFalse(any('"t2"' in x for x in r1))
        r2 = [x for x in reader2 if not x.startswith(":")]
        self.assertTrue('"t2"' in r2[-1] and r2[-1].startswith("event: done"))
        self.assertEqual(m.opens, 1)


class ReservationTests(unittest.TestCase):
    def test_sessions_get_raises_during_reservation_fails_closed(self):
        reg = Registry(); m = FakeMuse(); m.open_raises = TimeoutError("sessions.get timed out")
        kind, info = start_or_busy(reg, "S1", "A", "continue", "t1",
                                   lambda acc=None: ThreadHost(reg, m, "A", dict(CFG), None, session_id="S1"), wait_s=5, accounts=[{"id": "acc1"}])
        self.assertEqual(kind, "unavailable"); self.assertIn("state_unknown", info["message"])
        time.sleep(0.2)
        self.assertEqual(m.prompts, [], "nothing sent while the state is unknown")
        self.assertEqual((reg.by_session, reg.by_turn, reg.accounts, reg.hosts), ({}, {}, {}, set()), "REG clean")
        self.assertFalse(m.pool_busy, "account free")
        self.assertGreaterEqual(m.closes, 1)

    def test_broken_host_removed_then_new_turn_rechecks_muse(self):
        reg = Registry(); m = FakeMuse(status_none_on_error=True)
        h = mk(reg, m, dict(CFG, grace=5), session="S1")
        read_until(h, lambda r: r["type"] == "error")
        h.closed.wait(2)
        self.assertTrue(h.broken); self.assertIsNone(reg.get("S1"), "BROKEN host removed from REG")
        self.assertFalse(m.pool_busy)
        # Muse still running -> 409
        m2 = FakeMuse(open_status="running")
        kind, info = start_or_busy(reg, "S1", "A", "continue", "t2", lambda acc=None: ThreadHost(reg, m2, "A", dict(CFG), None, session_id="S1"), wait_s=5)
        self.assertEqual((kind, info["muse_status"]), ("busy", "running"))
        time.sleep(0.2); self.assertIsNone(reg.get("S1")); self.assertFalse(m2.pool_busy)
        # Muse stopped -> opens normally and sends
        m3 = FakeMuse(timeout=False, open_status="interrupted")
        kind, h3 = start_or_busy(reg, "S1", "A", "continue", "t3", lambda acc=None: ThreadHost(reg, m3, "A", dict(CFG), None, session_id="S1"), wait_s=5)
        self.assertEqual(kind, "new")
        read_until(h3, lambda r: r["type"] == "done")
        self.assertEqual(m3.prompts, ["continue"])

    def test_epoch_and_call_id_dedupe_per_turn(self):
        reg = Registry(); m = FakeMuse(timeout=False)
        h = mk(reg, m, dict(CFG, grace=3))
        self.assertTrue(h.note_invoke("r1")); self.assertFalse(h.note_invoke("r1"), "same call id forwarded once per turn")
        self.assertEqual(h.accept_result({"turn_id": "t1", "request_id": "r1"})[0], True)
        self.assertEqual(h.accept_result({"turn_id": "t1", "request_id": "r1"})[0], False, "duplicate result rejected")
        r1 = sse_from_host(h, 0.2); list(r1)   # a reconnect does not reset the turn's dedupe state
        self.assertFalse(h.accept_result({"turn_id": "t1", "request_id": "r1"})[0])
        time.sleep(0.1)                         # r1 already read turn 1 up to its END (done)
        self.assertTrue(submit(reg, h, "next", "t2", "c1"))
        ok, why = h.accept_result({"turn_id": "t1", "request_id": "r9"})
        self.assertFalse(ok); self.assertIn("stale", why)
        self.assertTrue(h.accept_result({"turn_id": "t2", "request_id": "r1"})[0], "new epoch, new dedupe set")
        self.assertTrue(h.note_invoke("r1"))


class AccountAndTimingTests(unittest.TestCase):
    def test_barrier_two_threads_same_account(self):
        reg = Registry(); out = {}; bar = threading.Barrier(2)
        def go(sid):
            m = FakeMuse(timeout=False, open_status="completed")
            bar.wait()
            out[sid] = start_or_busy(reg, sid, "c", "hi", f"t-{sid}", lambda acc=None: ThreadHost(reg, m, "c", dict(CFG), None, session_id=sid),
                                     wait_s=5, accounts=[{"id": "acc1"}])
        ts = [threading.Thread(target=go, args=(s,)) for s in ("S1", "S2")]
        [t.start() for t in ts]; [t.join() for t in ts]
        kinds = sorted(k for k, _ in out.values())
        self.assertEqual(kinds, ["account_busy", "new"])
        busy = next(v for k, v in out.values() if k == "account_busy")
        winner = next(v for k, v in out.values() if k == "new")
        self.assertEqual(busy["held_by_thread"], winner.session); self.assertGreaterEqual(busy["retry_after_s"], 1)
        self.assertIs(reg.accounts["acc1"], winner)
        winner.force_close("test")
        self.assertEqual(reg.accounts, {}, "account released with host removal")
        kind, _ = reg.reserve("S3", "c", "x", "t3", lambda acc=None: ThreadHost(reg, FakeMuse(), "c", dict(CFG), None, session_id="S3"), accounts=[{"id": "acc1"}])
        self.assertEqual(kind, "new")

    def test_status_hang_does_not_block_cap(self):
        reg = Registry(); m = FakeMuse(never_finish=True); m.hang_in_watch = True
        t0 = time.monotonic()
        h = mk(reg, m, dict(CFG, cap=0.4, cancel_wait=0.5, status_timeout=0.2))
        recs = read_until(h, lambda r: r.get("code") == "turn_cap", timeout=5)
        self.assertEqual(recs[-1]["code"], "turn_cap"); self.assertTrue(recs[-1]["cancel_unconfirmed"])
        h.closed.wait(3)
        self.assertTrue(h.closed.is_set()); self.assertLess(time.monotonic() - t0, 3.0)
        self.assertEqual(m.cancels, 1); m.hang_ev.set()

    def test_cap_is_per_turn_and_phase_derived(self):
        reg = Registry(); m = FakeMuse(timeout=False)
        h = mk(reg, m, dict(CFG, grace=3, cap=30))
        read_until(h, lambda r: r["type"] == "done"); time.sleep(0.1)
        self.assertEqual((h.state, h.phase), ("grace", "grace"))
        first = h.turn_started
        time.sleep(0.05)
        self.assertTrue(submit(reg, h, "again", "t2", "c1"))
        self.assertGreater(h.turn_started, first, "cap clock restarts per turn")
        self.assertNotIn("phase", vars(h), "phase is derived, never stored")


class ReviewTests(unittest.TestCase):
    def test_status_hang_at_check_reg_clean_account_free(self):
        reg = Registry(); m = FakeMuse(); m.hang = True   # sessions.get hangs
        t0 = time.monotonic()
        kind, info = start_or_busy(reg, "S1", "A", "continue", "t1", lambda acc=None: ThreadHost(reg, m, "A", dict(CFG, status_timeout=0.3), None, session_id="S1"),
                                   wait_s=5, accounts=[{"id": "acc1"}])
        self.assertEqual(kind, "unavailable"); self.assertIn("did not answer", info["message"])
        self.assertLess(time.monotonic() - t0, 2)
        time.sleep(0.2)
        self.assertEqual((reg.by_session, reg.accounts, reg.hosts), ({}, {}, set())); self.assertFalse(m.pool_busy)
        self.assertEqual(m.prompts, []); m.hang_ev.set()

    def test_open_raises_after_page_created_no_leak(self):
        before = FakeMuse.open_pages
        reg = Registry(); m = FakeMuse(); m.open_raises_after_page = RuntimeError("node.register failed")
        h = mk(reg, m, dict(CFG))
        h.closed.wait(3)
        self.assertEqual(FakeMuse.open_pages, before, "open pages = 0 after a failed open")
        self.assertEqual(reg.hosts, set())

    def test_reserve_hits_closing_waits_then_opens_new_host(self):
        reg = Registry(); m = FakeMuse(timeout=False, open_status="completed"); m.slow_close = 0.6
        h = mk(reg, m, dict(CFG, grace=0.2), session="S1")
        read_until(h, lambda r: r["type"] == "done")
        while h.state != "closing":
            time.sleep(0.01)
        m2 = FakeMuse(timeout=False, open_status="completed")
        kind, h2 = start_or_busy(reg, "S1", "c1", "next", "t2", lambda acc=None: ThreadHost(reg, m2, "c1", dict(CFG), None, session_id="S1"), wait_s=5)
        self.assertEqual(kind, "new", "waited for the closing host instead of answering 409")
        self.assertTrue(h.closed.is_set()); self.assertIsNot(h2, h)
        read_until(h2, lambda r: r["type"] == "done"); self.assertEqual(m2.prompts, ["next"])

    def test_same_account_one_held_in_open_other_503_untouched(self):
        reg = Registry(); m1 = FakeMuse(timeout=False); m1.block_open = threading.Event()
        bar = threading.Barrier(2); res = {}
        def first():
            bar.wait()
            res["a"] = start_or_busy(reg, None, "A", "x", "tA", lambda acc=None: ThreadHost(reg, m1, "A", dict(CFG), None), wait_s=5, accounts=[{"id": "acc1"}])
        th = threading.Thread(target=first); th.start(); bar.wait(); th.join()
        while m1.opens == 0:
            time.sleep(0.01)                # first host is inside ops.open (blocked)
        m2 = FakeMuse(timeout=False)
        kind, info = start_or_busy(reg, "S2", "B", "y", "tB", lambda acc=None: ThreadHost(reg, m2, "B", dict(CFG), None, session_id="S2"), wait_s=5, accounts=[{"id": "acc1"}])
        self.assertEqual(kind, "account_busy"); self.assertEqual(info["held_state"], "starting")
        self.assertEqual(m2.opens, 0, "no page opened for the second thread")
        self.assertEqual(m1.closes, 0, "first thread's page untouched")
        m1.block_open.set()
        read_until(res["a"][1], lambda r: r["type"] == "done")

    def test_reaper_start_deadline_for_stuck_open(self):
        reg = Registry(); m = FakeMuse(); m.block_open = threading.Event()
        h = mk(reg, m, dict(CFG, start_deadline=0.3))
        time.sleep(0.5); reg.reap_once()
        self.assertTrue(h.closed.is_set()); self.assertEqual(reg.accounts, {}); self.assertEqual(reg.hosts, set())
        before = FakeMuse.open_pages
        m.block_open.set(); time.sleep(0.3)   # open() returns after the reap: the page it created must not leak
        self.assertLessEqual(FakeMuse.open_pages, before)



class GraceRetryAfterTests(unittest.TestCase):
    def test_retry_after_reflects_remaining_grace(self):
        import pi_host as P
        clock = [1000.0]
        orig = P.mono
        P.mono = lambda: clock[0]
        try:
            reg = Registry()
            h = ThreadHost(reg, FakeMuse(timeout=False), "c1", dict(CFG, grace=10), None, session_id="G1")
            with reg.lock:
                h.state = P.GRACE
                h.grace_until = P.mono() + h.cfg["grace"]
            self.assertEqual(h.retry_after_s(), 10.0)
            clock[0] += 3.5
            self.assertEqual(h.retry_after_s(), 6.5)
            clock[0] += 9
            self.assertEqual(h.retry_after_s(), 0.5)   # floor once grace has run out
            with reg.lock:
                h.state = P.RUNNING
            self.assertEqual(h.retry_after_s(), max(5.0, CFG.get("watch", 3) * 2))
        finally:
            P.mono = orig

    def test_grace_until_set_on_real_grace_transition(self):
        reg = Registry(); m = FakeMuse(timeout=False, open_status="completed")
        h = mk(reg, m, dict(CFG, grace=5), session="G2")
        read_until(h, lambda r: r["type"] == "done")
        for _ in range(200):
            if h.state == "grace": break
            time.sleep(0.01)
        self.assertEqual(h.state, "grace")
        self.assertTrue(0.5 <= h.retry_after_s() <= 5.0)
        h.force_close("test")

if __name__ == "__main__":
    unittest.main()
