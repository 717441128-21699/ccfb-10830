"""Stress soak: N stores, simulated POS traffic + outages + schema migrations
for --duration seconds, then drain and row-by-row compare HQ vs every store.

  py stress.py --stores 50 --duration 3600        # the full 1-hour soak
  py stress.py --stores 50 --duration 120 --ops 5 # quick shakedown
"""
import argparse
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cdc_sync import backfill, store_db
from cdc_sync.hq_db import HQ
from cdc_sync.pos_sim import PosSimulator
from cdc_sync.sync_service import SyncService


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stores", type=int, default=50)
    ap.add_argument("--duration", type=float, default=3600)
    ap.add_argument("--ops", type=float, default=2.0, help="ops/sec per store")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--workdir", default=None)
    ap.add_argument("--keep", action="store_true", help="keep workdir afterwards")
    args = ap.parse_args()

    workdir = args.workdir or tempfile.mkdtemp(prefix=f"cdc_stress{args.stores}_")
    stores_dir = os.path.join(workdir, "stores")
    os.makedirs(stores_dir, exist_ok=True)
    hq = HQ(os.path.join(workdir, "hq.db"))
    svc = SyncService(hq)

    paths = {}
    t0 = time.time()
    for i in range(1, args.stores + 1):
        sid = f"S{i:03d}"
        p = os.path.join(stores_dir, f"{sid}.db")
        backfill.onboard_store(hq, svc, sid, f"门店{sid}", p)
        paths[sid] = p
    print(f"[stress] {args.stores} stores onboarded in {time.time()-t0:.1f}s, "
          f"workdir={workdir}")

    sim = PosSimulator(paths, service=svc, ops_per_sec=args.ops, seed=args.seed)
    sim.start()
    print(f"[stress] traffic for {args.duration}s "
          f"(~{args.ops}/s/store, random outages + migrations)")

    # mid-run: a brand-new store joins (backfill + cutover under load)
    new_sid = f"S{args.stores + 1:03d}"
    new_path = os.path.join(stores_dir, f"{new_sid}.db")
    onboarded = False

    deadline = time.time() + args.duration
    while time.time() < deadline:
        time.sleep(min(15, max(1, deadline - time.time())))
        if not onboarded and time.time() > deadline - args.duration * 0.6:
            info = backfill.onboard_store(hq, svc, new_sid, "中途新店", new_path)
            sim.stores[new_sid] = new_path
            sim.stats[new_sid] = {"ops": 0, "migrations": 0, "outages": 0}
            paths[new_sid] = new_path
            onboarded = True
            print(f"[stress] mid-run onboard {new_sid} at snapshot_seq="
                  f"{info['snapshot_seq']}")
        lagging = sum(1 for sid, w in svc.workers.items()
                      if w.link_up and hq.last_seq(sid) < _max_seq(paths[sid]))
        down = sum(1 for w in svc.workers.values() if not w.link_up)
        print(f"[stress] t+{time.time()-t0:.0f}s lagging={lagging} links_down={down}")

    print("[stress] traffic stopped, draining ...")
    sim.stop()
    for sid in paths:
        svc.set_link(sid, True)
    drained = svc.drain_all(timeout=max(120, args.stores * 4))
    print(f"[stress] drained={drained}")

    # ---- final row-by-row comparison, HQ vs every store ----
    all_ok, total_rows = True, 0
    t1 = time.time()
    for sid, p in sorted(paths.items()):
        rep = hq.compare_with_store(p, sid)
        rows = sum(v["hq_rows"] for v in rep["tables"].values())
        total_rows += rows
        if not rep["ok"]:
            all_ok = False
            print(f"  [DIFF] {sid}: "
                  + "; ".join(f"{t} {v['diffs'][:2]}" for t, v in rep["tables"].items()
                              if not v["ok"]))
    print(f"[stress] row-by-row compare: {total_rows} rows x {len(paths)} stores "
          f"in {time.time()-t1:.1f}s -> {'IDENTICAL' if all_ok else 'MISMATCH'}")

    pts = hq.consolidated_points_check(paths)
    print(f"[stress] consolidated points: {pts['checked']} members checked -> "
          f"{'OK' if pts['ok'] else pts['mismatches']}")

    # determinism spot-check: full replay of one store must reproduce HQ exactly
    rep = backfill.resync_store(hq, "S001", paths["S001"])
    print(f"[stress] determinism replay S001: {rep['replayed']} events -> "
          f"{'IDENTICAL' if rep['ok'] else rep}")

    ops = sum(s["ops"] for s in sim.stats.values())
    mig = sum(s["migrations"] for s in sim.stats.values())
    out = sum(s["outages"] for s in sim.stats.values())
    n_conf = hq.conn.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0]
    n_alert = hq.conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
    n_metric = hq.conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
    print(f"[stress] totals: pos_ops={ops} migrations={mig} outages={out} "
          f"conflicts={n_conf} alerts={n_alert} metric_points={n_metric}")
    svc.stop_all()
    ok = all_ok and pts["ok"] and rep["ok"] and drained
    print(f"[stress] {'PASS' if ok else 'FAIL'}")
    if not args.keep and ok:
        shutil.rmtree(workdir, ignore_errors=True)
    else:
        print(f"[stress] workdir kept: {workdir}")
    return 0 if ok else 1


def _max_seq(path):
    conn = store_db.connect(path)
    try:
        return store_db.max_seq(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
