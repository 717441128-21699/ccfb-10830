"""Smoke test for the web console: boots 3 stores + traffic, fetches every
page, exercises the POST endpoints. Run: py smoke_web.py"""
import os
import shutil
import sys
import tempfile
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cdc_sync import backfill, webapp
from cdc_sync.hq_db import HQ
from cdc_sync.pos_sim import PosSimulator
from cdc_sync.sync_service import SyncService

tmp = tempfile.mkdtemp(prefix="cdc_web_")
stores_dir = os.path.join(tmp, "stores")
os.makedirs(stores_dir)
hq = HQ(os.path.join(tmp, "hq.db"))
svc = SyncService(hq, idle_sleep=0.05)
paths = {}
for i in range(1, 4):
    sid = f"S{i:03d}"
    p = os.path.join(stores_dir, f"{sid}.db")
    backfill.onboard_store(hq, svc, sid, f"门店{sid}", p)
    paths[sid] = p
sim = PosSimulator(paths, service=svc, ops_per_sec=3.0)
sim.start()
app = webapp.App(hq, svc, sim=sim, stores_dir=stores_dir)
port = 18099
t = threading.Thread(target=webapp.serve, args=(app, port), daemon=True)
t.start()
time.sleep(4)  # let traffic + metrics accumulate

base = f"http://127.0.0.1:{port}"
ok = True


def get(path, expect=None):
    global ok
    with urllib.request.urlopen(base + path, timeout=10) as r:
        body = r.read().decode()
    good = r.status == 200 and (expect is None or expect in body)
    print(f"[{'PASS' if good else 'FAIL'}] GET {path} ({len(body)}B)")
    ok = ok and good
    return body


def post(path, data, expect_location):
    global ok
    req = urllib.request.Request(base + path, data=data.encode(),
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *a):
            return None
    opener = urllib.request.build_opener(NoRedirect)
    try:
        r = opener.open(req, timeout=30)
        status, location = r.status, r.headers.get("Location", "")
    except urllib.error.HTTPError as e:  # 303 surfaces as an "error"
        status, location = e.code, e.headers.get("Location", "")
    good = status == 303 and expect_location in location
    print(f"[{'PASS' if good else 'FAIL'}] POST {path} -> {location}")
    ok = ok and good


get("/", "门店同步总览")
get("/stores", "门店接入")
get("/links", "同步链路")
get("/links/S001", "时间点回看")
get("/conflicts", "合并规则")
get("/metrics", "报警阈值")
get("/backfill", "回灌")
post("/rules", "table=members&column=points&rule=additive&params={}", "/conflicts")
post("/alerts/config", "lag_seconds=30&backlog=5000&failed=10", "/metrics")
post("/links/S001/toggle", "to=down", "/links")
time.sleep(1)
post("/links/S001/toggle", "to=up", "/links")
post("/stores/create", "store_id=S099&name=新开店", "/stores")
time.sleep(2)
get("/links/S099", "S099")
post("/backfill/resync", "store_id=S001&from_seq=0&to_seq=", "/backfill")
body = get("/backfill", "重同步")
print("[PASS] resync 结果一致" if "完全一致" in body else "[FAIL] resync 结果不一致")
ok = ok and "完全一致" in body

sim.stop()
svc.stop_all()
shutil.rmtree(tmp, ignore_errors=True)
print("==>", "ALL PASS" if ok else "SOME FAILED")
sys.exit(0 if ok else 1)
