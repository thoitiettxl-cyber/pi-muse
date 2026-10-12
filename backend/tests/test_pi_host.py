"""ThreadHost lifecycle against a fake Muse (no browser). Run: venv/bin/python -m unittest tests.test_pi_host -v"""
import sys, threading, time, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pi_host import END, Registry, ThreadHost  # noqa: E402


class Timeout(Exception):
    code = "first_byte_timeout"; phase = "first_byte"; timeout_s = 0.2; waited_s = 0.2


class FakeMuse:
    """Fake page/device. device_online == page open and node registered."""
    def __init__(self, run_s=1.5, invoke_at=0.4, hang_status=False, never_finish=False, timeout=True):
        self.run_s, self.invoke_at, self.hang, self.never, self.timeout = run_s, invoke_at, hang_status, never_finish, timeout
        self.opens = 0; self.closes = 0; self.device_online = False; self.cancels = 0
        self.t0 = None; self.status_v = "running"; self.invoked = False; self.results = []; self.prompts = []
        self.hang_ev = threading.Event()
    def open(self, h): self.opens += 1; self.device_online = True
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
        if self.timeout:
            time.sleep(0.2); raise Timeout("等待助手首字响应超时")
        yield "hello"
    def settle(self, h): pass
    def status(self, h, sid):
        if self.hang:
            self.hang_ev.wait()
        if self.status_v == "running" and not self.never and time.time() - self.t0 > self.run_s:
            self.status_v = "completed"
        return self.status_v
    def cancel(self, h, sid):
        self.cancels += 1
        threading.Timer(0.3, lambda: setattr(self, "status_v", "interrupted")).start()
        return {"cancelled": True}
    def reply(self, h, sid): return "BG RESULT" if self.status_v == "completed" else None
    def after_turn(self, h): pass
    def error_details(self, h): return {"bytes_received": 0, "events_received": 1}
    def close(self, h): self.closes += 1; self.device_online = False


CFG = {"cap": 30, "watch": 0.1, "grace": 0.5, "heartbeat": 1, "reaper_slack": 0.2, "cancel_wait": 5, "pump_interval": 0.05}


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
        h = ThreadHost(reg, m, "c1", dict(CFG), None); h.start("do it", "t1")
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
        reg = Registry(); m = FakeMuse(hang_status=True)
        h = ThreadHost(reg, m, "c1", dict(CFG, cap=0.8), None); h.start("x", "t1")
        time.sleep(1.2)
        self.assertFalse(h.closed.is_set())
        reg.reap_once()
        self.assertTrue(h.closed.is_set()); self.assertEqual(m.closes, 1); self.assertFalse(m.device_online)
        m.hang_ev.set()

    def test_watcher_cap_cancels(self):
        reg = Registry(); m = FakeMuse(never_finish=True)
        h = ThreadHost(reg, m, "c1", dict(CFG, cap=1.0), None); h.start("x", "t1")
        h.closed.wait(8)
        self.assertTrue(h.closed.is_set()); self.assertEqual(m.cancels, 1); self.assertEqual(m.status_v, "interrupted")

    def test_max_wait_cancel_waits_for_interrupted_then_closes(self):
        reg = Registry(); m = FakeMuse(never_finish=True)
        h = ThreadHost(reg, m, "c1", dict(CFG), None); h.start("x", "t1")
        read_until(h, lambda r: r["type"] == "error")
        r = h.request_cancel(wait=True, timeout=5)
        self.assertEqual(m.cancels, 1); self.assertEqual(r["status"], "interrupted")
        recs = read_until(h, lambda r: r["type"] == "done")
        self.assertEqual(recs[-1]["status"], "interrupted")
        h.closed.wait(3); self.assertTrue(h.closed.is_set()); self.assertEqual(m.closes, 1)

    def test_busy_then_reuse_same_page_in_grace(self):
        reg = Registry(); m = FakeMuse(timeout=False)
        h = ThreadHost(reg, m, "c1", dict(CFG, grace=3), None); h.start("x", "t1")
        self.assertFalse(h.submit("y", "t2", "c2"), "busy thread rejects a second turn")
        read_until(h, lambda r: r["type"] == "done")
        time.sleep(0.1)
        self.assertIs(reg.get("S1"), h)
        self.assertTrue(h.submit("continue", "t2", "c1"))
        read_until(h, lambda r: r["type"] == "done")
        self.assertEqual(m.prompts, ["x", "continue"]); self.assertEqual(m.opens, 1, "one page/device per thread")
        h.closed.wait(5); self.assertEqual(m.closes, 1)

    def test_close_all(self):
        reg = Registry(); m = FakeMuse(never_finish=True)
        h = ThreadHost(reg, m, "c1", dict(CFG), None); h.start("x", "t1")
        time.sleep(0.4); reg.close_all()
        self.assertTrue(h.closed.is_set()); self.assertEqual(m.closes, 1)


if __name__ == "__main__":
    unittest.main()
