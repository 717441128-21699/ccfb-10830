"""HQ side: central SQLite holding per-store current tables, a full journal
(for point-in-time queries), schema evolution, conflict resolution, metrics
and alerts.

Exactly-once: each batch is applied and the store position (`sync_state.last_seq`)
is advanced in the SAME transaction, so a crash never loses or duplicates a row.
"""
import json
import sqlite3
import threading
import time

META_DDL = [
    """CREATE TABLE IF NOT EXISTS stores(
         store_id TEXT PRIMARY KEY, name TEXT, db_path TEXT,
         status TEXT, schema_version INTEGER DEFAULT 1, created_at REAL)""",
    """CREATE TABLE IF NOT EXISTS sync_state(
         store_id TEXT PRIMARY KEY, last_seq INTEGER DEFAULT 0,
         state TEXT, error TEXT, updated_at REAL)""",
    """CREATE TABLE IF NOT EXISTS journal(
         hq_seq INTEGER PRIMARY KEY AUTOINCREMENT,
         store_id TEXT, table_name TEXT, op TEXT,
         pk_json TEXT, old_json TEXT, new_json TEXT,
         store_seq INTEGER, applied_at REAL)""",
    "CREATE INDEX IF NOT EXISTS journal_store ON journal(store_id, table_name, hq_seq)",
    "CREATE INDEX IF NOT EXISTS journal_seq ON journal(store_id, store_seq)",
    """CREATE TABLE IF NOT EXISTS schema_events(
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         store_id TEXT, version INTEGER, ddl_json TEXT,
         added_json TEXT, removed_json TEXT, detected_at REAL)""",
    """CREATE TABLE IF NOT EXISTS member_points(
         member_id INTEGER PRIMARY KEY, points INTEGER, base_points INTEGER DEFAULT 0,
         updated_at REAL, last_store TEXT, last_seq INTEGER)""",
    """CREATE TABLE IF NOT EXISTS member_inits(
         member_id INTEGER, store_id TEXT, points INTEGER, updated_at REAL,
         PRIMARY KEY(member_id, store_id))""",
    """CREATE TABLE IF NOT EXISTS merge_rules(
         table_name TEXT, column TEXT, rule TEXT, params TEXT,
         PRIMARY KEY(table_name, column))""",
    """CREATE TABLE IF NOT EXISTS conflicts(
         id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL,
         member_id INTEGER, store_id TEXT, other_store TEXT,
         field TEXT, incoming_val INTEGER, existing_val INTEGER,
         resolved_val INTEGER, rule TEXT)""",
    """CREATE TABLE IF NOT EXISTS metrics(
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         store_id TEXT, ts REAL, lag_seconds REAL,
         backlog INTEGER, applied INTEGER, failed INTEGER)""",
    "CREATE INDEX IF NOT EXISTS metrics_store ON metrics(store_id, ts)",
    """CREATE TABLE IF NOT EXISTS alerts(
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         store_id TEXT, ts REAL, kind TEXT, value REAL,
         threshold REAL, message TEXT)""",
    """CREATE TABLE IF NOT EXISTS alert_config(
         kind TEXT PRIMARY KEY, threshold REAL)""",
]

DEFAULT_ALERTS = {"lag_seconds": 30.0, "backlog": 5000.0, "failed": 10.0}
ALERT_COOLDOWN = 60.0


def _q(name):
    return '"' + name.replace('"', '""') + '"'


class HQ:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, timeout=60, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=60000")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.lock = threading.RLock()
        self._last_alert = {}
        with self.lock, self.conn:
            for ddl in META_DDL:
                self.conn.execute(ddl)
            for kind, thr in DEFAULT_ALERTS.items():
                self.conn.execute(
                    "INSERT OR IGNORE INTO alert_config(kind,threshold) VALUES(?,?)",
                    (kind, thr))
            self.conn.execute(
                "INSERT OR IGNORE INTO merge_rules VALUES('members','points','additive','{}')")

    # ---------------- stores ----------------
    def register_store(self, store_id, name, db_path):
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO stores VALUES(?,?,?,?,?,?)",
                (store_id, name, db_path, "active", 1, time.time()))
            self.conn.execute(
                "INSERT OR IGNORE INTO sync_state(store_id,last_seq,state,updated_at) "
                "VALUES(?,0,'init',?)", (store_id, time.time()))

    def stores(self):
        with self.lock:
            return [dict(r) for r in self.conn.execute(
                "SELECT s.*, st.last_seq, st.state, st.error FROM stores s "
                "LEFT JOIN sync_state st ON st.store_id=s.store_id ORDER BY s.store_id")]

    def cur_table(self, store_id, table):
        return f"cur_{store_id}_{table}"

    def cur_table_schema(self, store_id, table):
        """Declared [(name, type)] and pk of the HQ current table — the
        authoritative shape for reseeding (keeps dropped-column types)."""
        with self.lock:
            info = self.conn.execute(
                f"PRAGMA table_info({_q(self.cur_table(store_id, table))})").fetchall()
        return ([(r["name"], r["type"] or "TEXT") for r in info],
                [r["name"] for r in info if r["pk"]])

    def _cur_columns(self, cur):
        return [r["name"] for r in self.conn.execute(f"PRAGMA table_info({_q(cur)})")]

    def _ensure_cur_table(self, store_id, table, columns, pk):
        cur = self.cur_table(store_id, table)
        existing = self._cur_columns(cur)
        if not existing:
            cols_sql = ", ".join(f"{_q(c)} {t or 'TEXT'}" for c, t in columns)
            pk_sql = ", ".join(_q(c) for c in pk) if pk else _q(columns[0][0])
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {_q(cur)}({cols_sql}, PRIMARY KEY({pk_sql}))")
            return [c for c, _ in columns]
        added = []
        for c, t in columns:
            if c not in existing:
                self.conn.execute(f"ALTER TABLE {_q(cur)} ADD COLUMN {_q(c)} {t or 'TEXT'}")
                added.append(c)
        return added

    # ---------------- apply ----------------
    def apply_batch(self, store_id, rows):
        """Apply one CDC batch + advance position atomically. Returns applied count."""
        if not rows:
            return 0
        now = time.time()
        with self.lock, self.conn:
            for r in rows:
                table, op = r["table_name"], r["op"]
                new = json.loads(r["new_json"]) if r["new_json"] else None
                old = json.loads(r["old_json"]) if r["old_json"] else None
                if table == "__schema__":
                    self._apply_schema_event(store_id, new, now)
                else:
                    self._apply_row(store_id, table, op,
                                    json.loads(r["pk_json"]), old, new)
                self.conn.execute(
                    "INSERT INTO journal(store_id,table_name,op,pk_json,old_json,new_json,"
                    "store_seq,applied_at) VALUES(?,?,?,?,?,?,?,?)",
                    (store_id, table, op, r["pk_json"], r["old_json"], r["new_json"],
                     r["seq"], now))
            self.conn.execute(
                "INSERT OR REPLACE INTO sync_state(store_id,last_seq,state,updated_at) "
                "VALUES(?,?,'ok',?)", (store_id, rows[-1]["seq"], now))
        return len(rows)

    def _apply_row(self, store_id, table, op, pk, old, new):
        cur = self.cur_table(store_id, table)
        if not self._cur_columns(cur):
            # first row for this table and no schema event seen: derive shape
            src = new or old
            cols = [(c, "REAL" if isinstance(v, float) else
                     "INTEGER" if isinstance(v, int) else "TEXT") for c, v in src.items()]
            self._ensure_cur_table(store_id, table, cols, list(pk.keys()))
        if op == "INSERT":
            cols = list(new.keys())
            self.conn.execute(
                f"INSERT OR REPLACE INTO {_q(cur)}({', '.join(_q(c) for c in cols)}) "
                f"VALUES({', '.join('?' for _ in cols)})",
                [new[c] for c in cols])
        elif op == "UPDATE":
            sets = ", ".join(f"{_q(c)}=?" for c in new)
            where = " AND ".join(f"{_q(c)}=?" for c in pk)
            n = self.conn.execute(
                f"UPDATE {_q(cur)} SET {sets} WHERE {where}",
                [*new.values(), *pk.values()]).rowcount
            if n == 0:  # missed the insert (e.g. pre-snapshot): upsert
                merged = dict(old or {})
                merged.update(new)
                cols = list(merged.keys())
                self.conn.execute(
                    f"INSERT OR REPLACE INTO {_q(cur)}({', '.join(_q(c) for c in cols)}) "
                    f"VALUES({', '.join('?' for _ in cols)})",
                    [merged[c] for c in cols])
        elif op == "DELETE":
            where = " AND ".join(f"{_q(c)}=?" for c in pk)
            self.conn.execute(f"DELETE FROM {_q(cur)} WHERE {where}", list(pk.values()))
        if table == "members" and op in ("INSERT", "UPDATE"):
            self._consolidate_points(store_id, pk["member_id"], old, new)

    # ---------------- conflict resolution ----------------
    def get_rule(self, table, column):
        with self.lock:
            r = self.conn.execute(
                "SELECT rule, params FROM merge_rules WHERE table_name=? AND column=?",
                (table, column)).fetchone()
        if not r:
            return "additive", {}
        return r["rule"], json.loads(r["params"] or "{}")

    def set_rule(self, table, column, rule, params=None):
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO merge_rules VALUES(?,?,?,?)",
                (table, column, rule, json.dumps(params or {})))

    def rules(self):
        with self.lock:
            return [dict(r) for r in self.conn.execute("SELECT * FROM merge_rules")]

    def _consolidate_points(self, store_id, member_id, old, new):
        """Merge a store's members.points change into the chain-wide balance.

        additive (default): a store first introducing the member sets the base
        (deterministically the earliest updated_at, rebased if an earlier init
        arrives late); every UPDATE contributes its delta. Concurrent offline
        deductions therefore merge as base + dA + dB, and the result is
        independent of arrival order."""
        rule, params = self.get_rule("members", "points")
        existing = self.conn.execute(
            "SELECT * FROM member_points WHERE member_id=?", (member_id,)).fetchone()
        incoming = new["points"] if new else 0
        op = "INSERT" if old is None else "UPDATE"
        conflict = (existing is not None and existing["last_store"] != store_id
                    and op == "UPDATE")
        if rule == "additive":
            if op == "INSERT":
                self.conn.execute(
                    "INSERT OR REPLACE INTO member_inits VALUES(?,?,?,?)",
                    (member_id, store_id, incoming, (new or {}).get("updated_at") or 0))
                base = self.conn.execute(
                    "SELECT points FROM member_inits WHERE member_id=? "
                    "ORDER BY updated_at, store_id LIMIT 1", (member_id,)).fetchone()[0]
                old_base = existing["base_points"] if existing else 0
                resolved = (existing["points"] if existing else 0) + (base - old_base)
                new_base = base
            else:
                delta = (new["points"] if new else 0) - (old["points"] if old else 0)
                resolved = (existing["points"] if existing else 0) + delta
                new_base = existing["base_points"] if existing else 0
        elif rule == "lww":
            if not existing or (new and (new.get("updated_at") or 0) >= (existing["updated_at"] or 0)):
                resolved = incoming
            else:
                resolved = existing["points"]
            new_base = existing["base_points"] if existing else 0
        elif rule == "priority":
            prio = params.get("priority", [])
            def rank(s):
                return prio.index(s) if s in prio else len(prio)
            if not existing or rank(store_id) <= rank(existing["last_store"]):
                resolved = incoming
            else:
                resolved = existing["points"]
            new_base = existing["base_points"] if existing else 0
        elif rule == "max":
            resolved = max(incoming, existing["points"] if existing else incoming)
            new_base = existing["base_points"] if existing else 0
        elif rule == "min":
            resolved = min(incoming, existing["points"] if existing else incoming)
            new_base = existing["base_points"] if existing else 0
        else:
            resolved = incoming
            new_base = existing["base_points"] if existing else 0
        self.conn.execute(
            "INSERT OR REPLACE INTO member_points VALUES(?,?,?,?,?,?)",
            (member_id, resolved, new_base, (new or {}).get("updated_at"), store_id,
             int(time.time())))
        if conflict:
            self.conn.execute(
                "INSERT INTO conflicts(ts,member_id,store_id,other_store,field,"
                "incoming_val,existing_val,resolved_val,rule) VALUES(?,?,?,?,?,?,?,?,?)",
                (time.time(), member_id, store_id, existing["last_store"], "points",
                 incoming, existing["points"], resolved, rule))

    def conflicts(self, limit=200):
        with self.lock:
            return [dict(r) for r in self.conn.execute(
                "SELECT * FROM conflicts ORDER BY id DESC LIMIT ?", (limit,))]

    # ---------------- schema evolution ----------------
    def _apply_schema_event(self, store_id, ddl, now):
        version = ddl.get("version", 0)
        all_added, all_removed = {}, {}
        for table, info in ddl["tables"].items():
            cur = self.cur_table(store_id, table)
            before = set(self._cur_columns(cur))
            added = self._ensure_cur_table(store_id, table, info["columns"], info["pk"])
            after = set(self._cur_columns(cur))
            # columns HQ still has but the store dropped stay in place (deprecated)
            removed = sorted(before - {c for c, _ in info["columns"]}) if before else []
            if added:
                all_added[table] = added
            if removed:
                all_removed[table] = removed
        self.conn.execute(
            "INSERT INTO schema_events(store_id,version,ddl_json,added_json,removed_json,"
            "detected_at) VALUES(?,?,?,?,?,?)",
            (store_id, version, json.dumps(ddl), json.dumps(all_added),
             json.dumps(all_removed), now))
        self.conn.execute("UPDATE stores SET schema_version=? WHERE store_id=?",
                          (version, store_id))

    def schema_events(self, store_id=None):
        with self.lock:
            if store_id:
                rows = self.conn.execute(
                    "SELECT * FROM schema_events WHERE store_id=? ORDER BY id",
                    (store_id,)).fetchall()
            else:
                rows = self.conn.execute("SELECT * FROM schema_events ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    # ---------------- snapshot load (backfill) ----------------
    def load_snapshot(self, store_id, data, seq0):
        now = time.time()
        with self.lock, self.conn:
            for table, payload in data.items():
                info = payload["info"]
                self._ensure_cur_table(store_id, table, info["columns"], info["pk"])
                for row in payload["rows"]:
                    cols = list(row.keys())
                    self.conn.execute(
                        f"INSERT OR REPLACE INTO {_q(self.cur_table(store_id, table))}"
                        f"({', '.join(_q(c) for c in cols)}) "
                        f"VALUES({', '.join('?' for _ in cols)})",
                        [row[c] for c in cols])
                    if table == "members":
                        # snapshot rows are the member's first sighting: they
                        # seed the merge base exactly like an INSERT event
                        self._consolidate_points(store_id, row["member_id"], None, row)
            self.conn.execute(
                "INSERT OR REPLACE INTO sync_state(store_id,last_seq,state,updated_at) "
                "VALUES(?,?,'incremental',?)", (store_id, seq0, now))

    # ---------------- point in time ----------------
    def table_as_of(self, store_id, table, ts=None, hq_seq=None):
        """Reconstruct the table as it looked at ts / hq_seq by undoing later
        journal entries on top of the current state."""
        with self.lock:
            cur = self.cur_table(store_id, table)
            if not self._cur_columns(cur):
                return []
            rows = {}
            for r in self.conn.execute(f"SELECT * FROM {_q(cur)}"):
                d = dict(r)
                info = self.conn.execute(f"PRAGMA table_info({_q(cur)})").fetchall()
                pk = tuple(d[c["name"]] for c in info if c["pk"])
                rows[pk] = d
            cond, args = "store_id=? AND table_name=?", [store_id, table]
            if hq_seq is not None:
                cond += " AND hq_seq>?"
                args.append(hq_seq)
            else:
                cond += " AND applied_at>?"
                args.append(ts)
            for j in self.conn.execute(
                    f"SELECT * FROM journal WHERE {cond} ORDER BY hq_seq DESC", args):
                pk = tuple(json.loads(j["pk_json"]).values())
                if j["op"] == "INSERT":
                    rows.pop(pk, None)
                else:  # UPDATE / DELETE -> restore old image
                    rows[pk] = json.loads(j["old_json"])
            return sorted(rows.values(), key=lambda d: tuple(str(v) for v in d.values()))

    def hq_seq_for_store_seq(self, store_id, store_seq, direction="le"):
        op = "<=" if direction == "le" else "<"
        with self.lock:
            r = self.conn.execute(
                f"SELECT MAX(hq_seq) FROM journal WHERE store_id=? AND store_seq {op} ?",
                (store_id, store_seq)).fetchone()
        return r[0] or 0

    # ---------------- metrics & alerts ----------------
    def record_metric(self, store_id, lag, backlog, applied, failed):
        now = time.time()
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO metrics(store_id,ts,lag_seconds,backlog,applied,failed) "
                "VALUES(?,?,?,?,?,?)", (store_id, now, lag, backlog, applied, failed))
            for kind, value in (("lag_seconds", lag), ("backlog", backlog),
                                ("failed", failed)):
                thr = self.conn.execute(
                    "SELECT threshold FROM alert_config WHERE kind=?", (kind,)).fetchone()[0]
                last = self._last_alert.get((store_id, kind), 0)
                if value > thr and now - last > ALERT_COOLDOWN:
                    self._last_alert[(store_id, kind)] = now
                    self.conn.execute(
                        "INSERT INTO alerts(store_id,ts,kind,value,threshold,message) "
                        "VALUES(?,?,?,?,?,?)",
                        (store_id, now, kind, value, thr,
                         f"{store_id} {kind}={value:.1f} > threshold {thr}"))

    def metrics(self, store_id=None, since=None, limit=2000):
        q, args = "SELECT * FROM metrics WHERE 1=1", []
        if store_id:
            q += " AND store_id=?"
            args.append(store_id)
        if since:
            q += " AND ts>?"
            args.append(since)
        q += " ORDER BY ts LIMIT ?"
        args.append(limit)
        with self.lock:
            return [dict(r) for r in self.conn.execute(q, args)]

    def alerts(self, limit=100):
        with self.lock:
            return [dict(r) for r in self.conn.execute(
                "SELECT * FROM alerts ORDER BY id DESC LIMIT ?", (limit,))]

    def alert_config(self):
        with self.lock:
            return {r["kind"]: r["threshold"]
                    for r in self.conn.execute("SELECT * FROM alert_config")}

    def set_alert_threshold(self, kind, threshold):
        with self.lock, self.conn:
            self.conn.execute("INSERT OR REPLACE INTO alert_config VALUES(?,?)",
                              (kind, threshold))

    def set_state(self, store_id, state, error=None):
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE sync_state SET state=?, error=?, updated_at=? WHERE store_id=?",
                (state, error, time.time(), store_id))

    def last_seq(self, store_id):
        with self.lock:
            r = self.conn.execute(
                "SELECT last_seq FROM sync_state WHERE store_id=?", (store_id,)).fetchone()
        return r[0] if r else 0

    # ---------------- verification ----------------
    def compare_with_store(self, store_path, store_id):
        """Row-by-row comparison of every business table. Returns a report."""
        from . import store_db
        src = store_db.connect(store_path)
        report = {"store_id": store_id, "tables": {}, "ok": True}
        with self.lock:
            for t in store_db.business_tables(src):
                cols = [c for c, _ in store_db.table_info(src, t)["columns"]]
                pk = store_db.table_info(src, t)["pk"] or cols[:1]
                order = ", ".join(_q(c) for c in pk)
                store_rows = [tuple(r) for r in src.execute(
                    f"SELECT {', '.join(_q(c) for c in cols)} FROM {_q(t)} ORDER BY {order}")]
                cur = self.cur_table(store_id, t)
                hq_cols = self._cur_columns(cur)
                missing = [c for c in cols if c not in hq_cols]
                if missing:
                    report["tables"][t] = {"ok": False, "error": f"HQ missing cols {missing}"}
                    report["ok"] = False
                    continue
                hq_rows = [tuple(r) for r in self.conn.execute(
                    f"SELECT {', '.join(_q(c) for c in cols)} FROM {_q(cur)} ORDER BY {order}")]
                diffs = []
                if store_rows != hq_rows:
                    s_set, h_set = set(store_rows), set(hq_rows)
                    for row in list(s_set - h_set)[:5]:
                        diffs.append(f"only in store: {row}")
                    for row in list(h_set - s_set)[:5]:
                        diffs.append(f"only in HQ:    {row}")
                ok = not diffs
                report["tables"][t] = {
                    "ok": ok, "store_rows": len(store_rows), "hq_rows": len(hq_rows),
                    "diffs": diffs}
                report["ok"] = report["ok"] and ok
        src.close()
        return report

    def consolidated_points_check(self, store_paths):
        """HQ consolidated member points must equal the sum of every store's
        points_ledger deltas (additive rule invariant)."""
        from . import store_db
        sums = {}
        for sid, path in store_paths.items():
            src = store_db.connect(path)
            for mid, d in src.execute(
                    "SELECT member_id, SUM(delta) FROM points_ledger GROUP BY member_id"):
                sums[mid] = sums.get(mid, 0) + d
            src.close()
        bad = []
        with self.lock:
            for mid, expect in sums.items():
                r = self.conn.execute(
                    "SELECT points FROM member_points WHERE member_id=?", (mid,)).fetchone()
                got = r[0] if r else 0
                if got != expect:
                    bad.append((mid, expect, got))
        return {"ok": not bad, "checked": len(sums), "mismatches": bad[:10]}
