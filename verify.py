"""Acceptance checks for the 7 requirements. Run: py verify.py
Each check prints PASS/FAIL; exit code 0 only if all pass."""
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cdc_sync import backfill, store_db
from cdc_sync.hq_db import HQ
from cdc_sync.sync_service import SyncService

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


def wait_until(fn, timeout=20, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return True
        time.sleep(interval)
    return False


def main():
    tmp = tempfile.mkdtemp(prefix="cdc_verify_")
    hq = HQ(os.path.join(tmp, "hq.db"))
    svc = SyncService(hq, idle_sleep=0.05)

    # ---- store S1 with some initial data, onboarded (req 6: backfill+cutover)
    p1 = os.path.join(tmp, "S1.db")
    conn = store_db.init_store_db(p1)
    with conn:
        conn.execute("INSERT INTO members VALUES(1,'张三','13800000001',100,1.0)")
        conn.execute("INSERT INTO sales(member_id,total,status,created_at) VALUES(1,50,'done',1.0)")
    info = backfill.onboard_store(hq, svc, "S1", "一号店", p1)
    check("6. 新店先灌全量再切增量", hq.last_seq("S1") == info["snapshot_seq"]
          and svc.workers["S1"].is_alive(), f"snapshot_seq={info['snapshot_seq']}")

    # changes DURING the cutover window must not be lost (req 6)
    with conn:
        conn.execute("INSERT INTO sales(member_id,total,status,created_at) VALUES(1,60,'done',2.0)")
        conn.execute("UPDATE members SET points=110 WHERE member_id=1")
    ok = wait_until(lambda: hq.last_seq("S1") >= store_db.max_seq(store_db.connect(p1)))
    rep = hq.compare_with_store(p1, "S1")
    check("6. 切换窗口内的变更不丢", ok and rep["ok"])

    # ---- req 1: outage resume, exactly once
    svc.set_link("S1", False)
    with conn:
        for i in range(50):
            conn.execute("INSERT INTO sale_items(sale_id,sku,qty,price) VALUES(1,?,?,?)",
                         (f"SKU{i}", i + 1, 9.9))
        conn.execute("DELETE FROM sale_items WHERE sku='SKU0'")
    time.sleep(0.5)  # link down: nothing flows
    blocked = hq.last_seq("S1")
    svc.set_link("S1", True)
    ok = wait_until(lambda: svc.workers["S1"].drain(timeout=5))
    rep = hq.compare_with_store(p1, "S1")
    n_journal = hq.conn.execute(
        "SELECT COUNT(*) FROM journal WHERE store_id='S1'").fetchone()[0]
    n_cdc = store_db.connect(p1).execute("SELECT COUNT(*) FROM _cdc_log").fetchone()[0]
    # snapshot-covered rows never re-enter the journal; everything after must
    expected = n_cdc - info["snapshot_seq"]
    check("1. 断网恢复后续传,一条不多一条不少",
          ok and rep["ok"] and n_journal == expected,
          f"journal={n_journal} cdc_after_snapshot={expected}")

    # ---- req 2: schema evolution, old data still queryable
    store_db.run_migration(conn, "ALTER TABLE sales ADD COLUMN discount REAL")
    with conn:
        conn.execute("INSERT INTO sales(member_id,total,status,created_at,discount) "
                     "VALUES(1,80,'done',3.0,0.85)")
    wait_until(lambda: svc.workers["S1"].drain(timeout=5))
    store_db.run_migration(conn, "ALTER TABLE sales DROP COLUMN discount")
    with conn:
        conn.execute("INSERT INTO sales(member_id,total,status,created_at) VALUES(1,90,'done',4.0)")
    wait_until(lambda: svc.workers["S1"].drain(timeout=5))
    cols = [r[1] for r in hq.conn.execute("PRAGMA table_info(cur_S1_sales)")]
    old_row = hq.conn.execute(
        "SELECT discount FROM cur_S1_sales WHERE total=80").fetchone()
    evs = hq.schema_events("S1")
    check("2. 加列删列总部自动跟随,老数据可查",
          "discount" in cols and old_row and abs(old_row[0] - 0.85) < 1e-9 and len(evs) >= 2,
          f"cols={cols} events={len(evs)}")

    # ---- req 3: same member, two stores, concurrent deduction, rule merge
    p2 = os.path.join(tmp, "S2.db")
    c2 = store_db.init_store_db(p2)
    backfill.onboard_store(hq, svc, "S2", "二号店", p2)
    svc.set_link("S1", False)
    svc.set_link("S2", False)
    with conn:  # S1: member 1 has 110, deduct 40
        conn.execute("INSERT INTO points_ledger(member_id,delta,reason,created_at) VALUES(1,-40,'redeem',10.0)")
        conn.execute("UPDATE members SET points=70, updated_at=10.0 WHERE member_id=1")
    with c2:  # S2 concurrently: thinks 110, deduct 30
        c2.execute("INSERT INTO members VALUES(1,'张三','13800000001',110,9.0)")
        c2.execute("INSERT INTO points_ledger(member_id,delta,reason,created_at) VALUES(1,-30,'redeem',10.5)")
        c2.execute("UPDATE members SET points=80, updated_at=10.5 WHERE member_id=1")
        c2.commit()
    hq.set_rule("members", "points", "additive")
    svc.set_link("S1", True)
    svc.set_link("S2", True)
    wait_until(lambda: svc.workers["S1"].drain(timeout=5) and svc.workers["S2"].drain(timeout=5))
    pts = hq.conn.execute("SELECT points FROM member_points WHERE member_id=1").fetchone()[0]
    n_conf = hq.conn.execute("SELECT COUNT(*) FROM conflicts WHERE member_id=1").fetchone()[0]
    if pts != 40 or n_conf < 1:  # debug context on failure
        for r in hq.conn.execute(
                "SELECT store_id,op,old_json,new_json FROM journal "
                "WHERE table_name='members' ORDER BY hq_seq"):
            print("   journal:", dict(r))
        print("   member_points:", dict(hq.conn.execute(
            "SELECT * FROM member_points WHERE member_id=1").fetchone()))
    check("3. 两店同时扣积分 additive 合并", pts == 40 and n_conf >= 1,
          f"points={pts} conflicts={n_conf}")
    # switch rule to lww and re-drive a conflict
    hq.set_rule("members", "points", "lww")
    with conn:
        conn.execute("UPDATE members SET points=55, updated_at=20.0 WHERE member_id=1")
    with c2:
        c2.execute("UPDATE members SET points=66, updated_at=21.0 WHERE member_id=1")
        c2.commit()
    wait_until(lambda: svc.workers["S1"].drain(timeout=5) and svc.workers["S2"].drain(timeout=5))
    pts = hq.conn.execute("SELECT points FROM member_points WHERE member_id=1").fetchone()[0]
    check("3. 规则可配置(lww 取时间戳最新)", pts == 66, f"points={pts}")
    hq.set_rule("members", "points", "additive")

    # ---- req 4: point-in-time
    t_mid = time.time()
    time.sleep(0.05)
    with conn:
        conn.execute("INSERT INTO members VALUES(2,'李四','13800000002',5,30.0)")
        conn.execute("DELETE FROM sale_items WHERE sku='SKU1'")
    wait_until(lambda: svc.workers["S1"].drain(timeout=5))
    past = hq.table_as_of("S1", "members", ts=t_mid)
    past_items = hq.table_as_of("S1", "sale_items", ts=t_mid)
    now_items = hq.table_as_of("S1", "sale_items", ts=time.time() + 10)
    check("4. 任意时间点回看",
          all(r["member_id"] != 2 for r in past)
          and any(r.get("sku") == "SKU1" for r in past_items)
          and not any(r.get("sku") == "SKU1" for r in now_items),
          f"past_members={len(past)} past_items={len(past_items)} now_items={len(now_items)}")

    # ---- req 5: metrics + alert
    hq.set_alert_threshold("backlog", 1)
    svc.set_link("S1", False)
    with conn:
        for i in range(5):
            conn.execute("INSERT INTO sale_items(sale_id,sku,qty,price) VALUES(2,'ALERT',1,1.0)")
    time.sleep(2.5)  # agent-side sampler observes the growing backlog
    svc.set_link("S1", True)
    wait_until(lambda: svc.workers["S1"].drain(timeout=5))
    time.sleep(1.5)  # let a metric sample land
    n_metrics = hq.conn.execute("SELECT COUNT(*) FROM metrics WHERE store_id='S1'").fetchone()[0]
    n_alerts = hq.conn.execute(
        "SELECT COUNT(*) FROM alerts WHERE store_id='S1' AND kind='backlog'").fetchone()[0]
    check("5. 延迟/积压/失败指标与超阈值报警", n_metrics > 0 and n_alerts > 0,
          f"metrics={n_metrics} backlog_alerts={n_alerts}")

    # ---- req 7: deterministic resync of a range
    top = store_db.max_seq(store_db.connect(p1))
    rep = backfill.resync_store(hq, "S1", p1, from_seq=top // 2, to_seq=top)
    if not rep["ok"]:
        for t, v in rep["tables"].items():
            if not v["ok"]:
                print(f"   resync diff {t}: {v['diff']}")
    check("7. 挑店挑区间重同步,结果与第一次完全一致", rep["ok"],
          f"replayed={rep['replayed']}")
    rep_full = backfill.resync_store(hq, "S2", p2)
    check("7. 全量重放一致性(S2)", rep_full["ok"], f"replayed={rep_full['replayed']}")

    svc.stop_all()
    print("\n==>", "ALL PASS" if all(results) else "SOME FAILED",
          f"({sum(results)}/{len(results)})")
    shutil.rmtree(tmp, ignore_errors=True)
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
