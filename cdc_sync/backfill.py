"""Backfill & resync.

* onboard_store: snapshot-first-then-incremental cutover. The CDC position is
  captured BEFORE the snapshot, so changes made during the seconds of the
  switch are replayed from the log — nothing is lost; overlaps are upserts.
* resync_store: replay a store's CDC range into a scratch HQ and prove the
  result is identical to what the original sync produced (determinism).
"""
import os
import tempfile
import time

from . import store_db
from .hq_db import HQ


def onboard_store(hq, service, store_id, name, db_path):
    """Register a brand-new store: full snapshot, then incremental from seq0."""
    if not os.path.exists(db_path):
        store_db.init_store_db(db_path).close()
    hq.register_store(store_id, name, db_path)
    hq.set_state(store_id, "backfill")
    seq0, data = store_db.snapshot(db_path)
    hq.load_snapshot(store_id, data, seq0)
    service.add_store(store_id, db_path)  # worker resumes from seq0
    return {"store_id": store_id, "snapshot_seq": seq0,
            "tables": {t: len(p["rows"]) for t, p in data.items()}}


def resync_store(hq, store_id, db_path, from_seq=0, to_seq=None):
    """Re-sync [from_seq, to_seq] into a scratch HQ and compare against the
    state the original sync produced for the same range (via the journal).

    Determinism guarantee: application order is fixed by seq and conflict
    resolution uses data timestamps (never wall clock), so a replay yields
    byte-identical rows."""
    src = store_db.connect(db_path)
    if to_seq is None:
        to_seq = store_db.max_seq(src)
    rows = store_db.read_changes(src, from_seq - 1 if from_seq > 0 else 0, 10_000_000)
    rows = [r for r in rows if r["seq"] <= to_seq]

    tmp = tempfile.NamedTemporaryFile(prefix="resync_", suffix=".db", delete=False)
    tmp.close()
    try:
        scratch = HQ(tmp.name)
        scratch.register_store(store_id, "resync", db_path)
        # copy merge rules so resolution is identical
        for rule in hq.rules():
            scratch.set_rule(rule["table_name"], rule["column"], rule["rule"],
                             __import__("json").loads(rule["params"] or "{}"))
        # seed scratch with the state just before the range
        if from_seq > 0:
            hq_seq_start = hq.hq_seq_for_store_seq(store_id, from_seq, "lt")
            for t in store_db.business_tables(src):
                seed_rows = hq.table_as_of(store_id, t, hq_seq=hq_seq_start)
                if seed_rows:
                    # seed with the HQ current table's declared shape so
                    # deprecated (dropped-at-store) columns keep their types
                    cols, pk = hq.cur_table_schema(store_id, t)
                    info = {"columns": cols, "pk": pk or list(seed_rows[0].keys())[:1]}
                    scratch.load_snapshot(store_id,
                                          {t: {"info": info, "rows": seed_rows}}, 0)
        # replay
        for i in range(0, len(rows), 500):
            scratch.apply_batch(store_id, rows[i:i + 500])

        # reference state: what the original sync had produced at to_seq
        hq_seq_end = hq.hq_seq_for_store_seq(store_id, to_seq, "le")
        report = {"store_id": store_id, "from_seq": from_seq, "to_seq": to_seq,
                  "replayed": len(rows), "tables": {}, "ok": True}
        for t in store_db.business_tables(src):
            expected = hq.table_as_of(store_id, t, hq_seq=hq_seq_end)
            got = scratch.table_as_of(store_id, t, hq_seq=10**18)
            norm = lambda rs: sorted(tuple(sorted(r.items())) for r in rs)
            e_set, g_set = norm(expected), norm(got)
            ok = e_set == g_set
            diff = []
            if not ok:
                e_map, g_map = set(e_set), set(g_set)
                diff = ([f"expected-only: {r}" for r in list(e_map - g_map)[:3]]
                        + [f"resync-only:  {r}" for r in list(g_map - e_map)[:3]])
            report["tables"][t] = {"ok": ok, "expected_rows": len(expected),
                                   "resync_rows": len(got), "diff": diff}
            report["ok"] = report["ok"] and ok
        return report
    finally:
        src.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(tmp.name + suffix)
            except OSError:
                pass
