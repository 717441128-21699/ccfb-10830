"""Per-store sync workers: pull CDC batches by position, apply at HQ, record
metrics. Links can be paused/resumed to simulate network outages — the store
keeps logging and the worker catches up from its last position."""
import threading
import time

from . import store_db


class Worker(threading.Thread):
    def __init__(self, service, store_id, db_path):
        super().__init__(daemon=True, name=f"sync-{store_id}")
        self.service = service
        self.store_id = store_id
        self.db_path = db_path
        self.link_up = True
        self.stop_flag = threading.Event()
        self.failed = 0
        self.applied_total = 0
        self._last_metric = 0.0
        self._last_sig_check = 0.0

    def run(self):
        hq = self.service.hq
        conn = store_db.connect(self.db_path)
        while not self.stop_flag.is_set():
            if not self.link_up:
                hq.set_state(self.store_id, "link_down")
                # store-side agent keeps sampling so HQ sees backlog/lag grow
                self._maybe_metric(conn)
                time.sleep(0.3)
                continue
            try:
                # backstop schema detection (store-side agent normally does this)
                if time.time() - self._last_sig_check > 5:
                    store_db.detect_schema_change(conn)
                    self._last_sig_check = time.time()
                last = hq.last_seq(self.store_id)
                batch = store_db.read_changes(conn, last, self.service.batch_size)
                if batch:
                    try:
                        n = hq.apply_batch(self.store_id, batch)
                        self.applied_total += n
                    except Exception as exc:  # retry next loop; position not advanced
                        self.failed += len(batch)
                        hq.set_state(self.store_id, "error", str(exc))
                        time.sleep(1.0)
                        continue
                self._maybe_metric(conn)
                if not batch:
                    time.sleep(self.service.idle_sleep)
            except Exception as exc:  # store db temporarily unavailable etc.
                hq.set_state(self.store_id, "error", str(exc))
                time.sleep(1.0)
        conn.close()

    def _maybe_metric(self, conn):
        now = time.time()
        if now - self._last_metric < 1.0:
            return
        self._last_metric = now
        hq = self.service.hq
        last = hq.last_seq(self.store_id)
        top = store_db.max_seq(conn)
        backlog = max(0, top - last)
        lag = 0.0
        if backlog:
            row = conn.execute("SELECT ts FROM _cdc_log WHERE seq=?", (last + 1,)).fetchone()
            if row:
                lag = max(0.0, now - row[0])
        hq.record_metric(self.store_id, lag, backlog, self.applied_total, self.failed)

    def drain(self, timeout=60):
        """Wait until the store's CDC log is fully applied."""
        conn = store_db.connect(self.db_path)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.service.hq.last_seq(self.store_id) >= store_db.max_seq(conn):
                conn.close()
                return True
            time.sleep(0.2)
        conn.close()
        return False


class SyncService:
    def __init__(self, hq, batch_size=500, idle_sleep=0.2):
        self.hq = hq
        self.batch_size = batch_size
        self.idle_sleep = idle_sleep
        self.workers = {}

    def add_store(self, store_id, db_path, start=True):
        if store_id in self.workers:
            return self.workers[store_id]
        w = Worker(self, store_id, db_path)
        self.workers[store_id] = w
        if start:
            w.start()
        return w

    def set_link(self, store_id, up):
        if store_id in self.workers:
            self.workers[store_id].link_up = up

    def link_states(self):
        return {sid: {"up": w.link_up, "applied": w.applied_total, "failed": w.failed}
                for sid, w in self.workers.items()}

    def drain_all(self, timeout=120):
        ok = True
        for sid, w in self.workers.items():
            was_down = not w.link_up
            if was_down:
                continue  # a down link cannot drain; caller decides
            ok = w.drain(timeout) and ok
        return ok

    def stop_all(self):
        for w in self.workers.values():
            w.stop_flag.set()
        for w in self.workers.values():
            w.join(timeout=5)
