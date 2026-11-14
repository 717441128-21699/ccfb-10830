"""Demo: boot N stores + sync service + POS simulator + web console.

  py run_demo.py --stores 8 --port 8080
then open http://127.0.0.1:8080  (pages: 概览/门店接入/同步链路/冲突处理/延迟监控/回灌)
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cdc_sync import backfill, webapp
from cdc_sync.hq_db import HQ
from cdc_sync.pos_sim import PosSimulator
from cdc_sync.sync_service import SyncService


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stores", type=int, default=8)
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--ops", type=float, default=1.5)
    ap.add_argument("--workdir", default="demo_data")
    args = ap.parse_args()

    stores_dir = os.path.join(args.workdir, "stores")
    os.makedirs(stores_dir, exist_ok=True)
    hq = HQ(os.path.join(args.workdir, "hq.db"))
    svc = SyncService(hq)

    paths = {}
    for i in range(1, args.stores + 1):
        sid = f"S{i:03d}"
        p = os.path.join(stores_dir, f"{sid}.db")
        if not os.path.exists(p):
            backfill.onboard_store(hq, svc, sid, f"门店{sid}", p)
        else:
            hq.register_store(sid, f"门店{sid}", p)
            svc.add_store(sid, p)
        paths[sid] = p

    sim = PosSimulator(paths, service=svc, ops_per_sec=args.ops)
    sim.start()
    app = webapp.App(hq, svc, sim=sim, stores_dir=stores_dir)
    print(f"{args.stores} 家门店模拟中,Ctrl+C 停止")
    try:
        webapp.serve(app, port=args.port)
    except KeyboardInterrupt:
        pass
    finally:
        sim.stop()
        svc.stop_all()


if __name__ == "__main__":
    main()
