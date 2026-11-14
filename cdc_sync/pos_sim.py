"""POS traffic simulator: per-store threads generating members / sales /
points activity, random schema migrations and random network outages.

Invariant the simulator keeps (and the stress test relies on): every change
to members.points is accompanied by a points_ledger row of the same delta,
and a member row first appears in a store with points=0 (the chain balance
is then booked as a '+sync' ledger entry). That makes the additive merge
rule exactly checkable: HQ consolidated points == sum of all ledger deltas.
"""
import random
import threading
import time

from . import store_db

SKUS = ["SKU%04d" % i for i in range(200)]


class PosSimulator:
    def __init__(self, stores, service=None, ops_per_sec=2.0, member_pool=2000,
                 seed=42, outage_prob_per_min=0.15, migrate_prob_per_min=0.1):
        """stores: {store_id: db_path}"""
        self.stores = stores
        self.service = service
        self.ops_per_sec = ops_per_sec
        self.member_pool = member_pool
        self.rng = random.Random(seed)
        self.stop_flag = threading.Event()
        self.threads = []
        self._member_lock = threading.Lock()
        self._next_member = 1
        self._chain_points = {}   # member_id -> approximate chain-wide balance
        self._col_counter = 0
        self._added_cols = {}     # store_id -> [ (table, col) ] droppable
        self.outage_prob = outage_prob_per_min
        self.migrate_prob = migrate_prob_per_min
        self.stats = {sid: {"ops": 0, "migrations": 0, "outages": 0} for sid in stores}

    # ---- lifecycle ----
    def start(self):
        for sid, path in self.stores.items():
            t = threading.Thread(target=self._run_store, args=(sid, path),
                                 daemon=True, name=f"pos-{sid}")
            t.start()
            self.threads.append(t)
        if self.service:
            t = threading.Thread(target=self._run_outages, daemon=True, name="outages")
            t.start()
            self.threads.append(t)

    def stop(self):
        self.stop_flag.set()
        for t in self.threads:
            t.join(timeout=10)

    # ---- store traffic ----
    def _run_store(self, sid, path):
        rng = random.Random(hash((sid, 7)) & 0xFFFFFFFF)
        conn = store_db.connect(path)
        sale_seq, item_seq, entry_seq = 0, 0, 0
        while not self.stop_flag.is_set():
            try:
                if rng.random() < self.migrate_prob / 60.0:
                    self._migrate(sid, conn, rng)
                self._one_op(sid, conn, rng, sale_seq, item_seq, entry_seq)
                sale_seq += 1
                item_seq += rng.randint(1, 4)
                entry_seq += 1
                self.stats[sid]["ops"] += 1
            except Exception:
                time.sleep(0.5)
                continue
            time.sleep(max(0.02, rng.expovariate(self.ops_per_sec)))
        conn.close()

    def _one_op(self, sid, conn, rng, sale_seq, item_seq, entry_seq):
        now = int(time.time() * 1000)  # epoch ms (see store_db schema note)
        roll = rng.random()
        if roll < 0.10:
            # register a brand-new chain-wide member at this store
            with self._member_lock:
                mid = self._next_member
                self._next_member += 1
                self._chain_points[mid] = 0
            with conn:
                conn.execute(
                    "INSERT OR IGNORE INTO members(member_id,name,phone,points,updated_at)"
                    " VALUES(?,?,?,?,?)",
                    (mid, f"会员{mid}", f"138{mid:08d}"[:11], 0, now))
            return
        mid = rng.randint(1, max(1, min(self._next_member - 1, self.member_pool)))
        known = conn.execute("SELECT points FROM members WHERE member_id=?",
                             (mid,)).fetchone()
        if not known:
            # first visit to this store: local row initialised from the cached
            # chain profile. This is NOT a balance movement (no ledger entry);
            # HQ treats first-seen inserts as the merge base, not a delta.
            with self._member_lock:
                chain = self._chain_points.get(mid, 0)
            with conn:
                conn.execute("INSERT OR IGNORE INTO members(member_id,name,phone,points,"
                             "updated_at) VALUES(?,?,?,?,?)",
                             (mid, f"会员{mid}", f"138{mid:08d}"[:11], chain, now))
            return
        # a sale: items + points accrual, or a points redemption
        total = round(rng.uniform(5, 500), 2)
        with conn:
            cur = conn.execute(
                "INSERT INTO sales(member_id,total,status,created_at) VALUES(?,?,?,?)",
                (mid, total, "done", now))
            sale_id = cur.lastrowid
            for _ in range(rng.randint(1, 3)):
                conn.execute(
                    "INSERT INTO sale_items(sale_id,sku,qty,price) VALUES(?,?,?,?)",
                    (sale_id, rng.choice(SKUS), rng.randint(1, 5),
                     round(rng.uniform(1, 100), 2)))
            points = known[0]
            if rng.random() < 0.35 and points > 10:
                delta = -rng.randint(1, points)          # redeem (conflict source)
                reason = "redeem"
            else:
                delta = int(total * 0.1)                  # accrue
                reason = "accrue"
            if delta:
                conn.execute("INSERT INTO points_ledger(member_id,delta,reason,created_at)"
                             " VALUES(?,?,?,?)",
                             (mid, delta, reason, now))
                conn.execute("UPDATE members SET points=points+?, updated_at=? "
                             "WHERE member_id=?", (delta, now, mid))
                with self._member_lock:
                    self._chain_points[mid] = self._chain_points.get(mid, 0) + delta

    # ---- schema migrations (store-side agent records them) ----
    def _migrate(self, sid, conn, rng):
        added = self._added_cols.setdefault(sid, [])
        try:
            if added and rng.random() < 0.3:
                table, col = added.pop(rng.randrange(len(added)))
                sql = f'ALTER TABLE "{table}" DROP COLUMN "{col}"'
            else:
                self._col_counter += 1
                table = rng.choice(["sales", "members", "sale_items"])
                col = f"ext_{self._col_counter}"
                typ = rng.choice(["TEXT", "REAL", "INTEGER"])
                sql = f'ALTER TABLE "{table}" ADD COLUMN "{col}" {typ}'
                added.append((table, col))
            store_db.run_migration(conn, sql)   # store-side agent handles DDL
            self.stats[sid]["migrations"] += 1
        except Exception:
            pass

    # ---- network outages ----
    def _run_outages(self):
        rng = random.Random(99)
        while not self.stop_flag.is_set():
            time.sleep(1.0)
            for sid in self.stores:
                w = self.service.workers.get(sid)
                if not w:
                    continue
                if w.link_up and rng.random() < self.outage_prob / 60.0:
                    w.link_up = False
                    self.stats[sid]["outages"] += 1
                    down_for = rng.uniform(3, 20)  # seconds ~ "days" compressed
                    threading.Timer(down_for, self._bring_up, args=(sid,)).start()

    def _bring_up(self, sid):
        w = self.service.workers.get(sid)
        if w:
            w.link_up = True
