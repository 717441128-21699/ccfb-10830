"""Web console (stdlib only): store onboarding, sync links, conflict rules,
latency monitoring and backfill/resync. Charts are server-rendered SVG."""
import json
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import backfill, store_db

NAV = [("/", "概览"), ("/stores", "门店接入"), ("/links", "同步链路"),
       ("/conflicts", "冲突处理"), ("/metrics", "延迟监控"), ("/backfill", "回灌")]

PAGE = """<!doctype html><html lang=zh><head><meta charset=utf-8>
<meta http-equiv=refresh content="{refresh}"><title>{title} · CDC 同步控制台</title>
<style>
body{{font:14px/1.5 "Microsoft YaHei",system-ui,sans-serif;margin:0;background:#f4f6f8;color:#222}}
header{{background:#1f3a5f;color:#fff;padding:10px 20px;display:flex;gap:18px;align-items:center}}
header a{{color:#cfe0f5;text-decoration:none}}header a.cur{{color:#fff;font-weight:700}}
main{{padding:18px 24px}}table{{border-collapse:collapse;background:#fff;width:100%}}
th,td{{border:1px solid #dde3ea;padding:5px 9px;text-align:left;font-size:13px}}
th{{background:#eef2f7}}.ok{{color:#15803d}}.bad{{color:#b91c1c}}.warn{{color:#b45309}}
.card{{background:#fff;border:1px solid #dde3ea;border-radius:6px;padding:14px 16px;margin-bottom:16px}}
input,select{{padding:4px 6px}}button{{padding:4px 14px;cursor:pointer}}
h2{{margin:6px 0 12px}}code{{background:#eef2f7;padding:1px 5px;border-radius:3px}}
</style></head><body><header><b>CDC 同步控制台</b>{nav}</header><main>{body}</main></body></html>"""


def esc(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def ts(t):
    return time.strftime("%H:%M:%S", time.localtime(t)) if t else "-"


def table(headers, rows):
    h = "".join(f"<th>{esc(x)}" for x in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}" for c in r) + "</tr>" for r in rows)
    return f"<table><tr>{h}</tr>{body}</table>"


def svg_chart(series, width=640, height=150):
    """series: [(label, color, [(ts, value), ...])]"""
    if not any(s[2] for s in series):
        return "<p>暂无数据</p>"
    all_pts = [p for s in series for p in s[2]]
    t0, t1 = min(p[0] for p in all_pts), max(p[0] for p in all_pts)
    vmax = max((p[1] for p in all_pts), default=1) or 1
    if t1 <= t0:
        t1 = t0 + 1
    pad = 30
    def xy(p):
        x = pad + (p[0] - t0) / (t1 - t0) * (width - 2 * pad)
        y = height - pad - (p[1] / vmax) * (height - 2 * pad)
        return f"{x:.1f},{y:.1f}"
    out = [f'<svg width={width} height={height} style="background:#fff;border:1px solid #dde3ea">']
    out.append(f'<text x=4 y=14 font-size=11>max {vmax:.1f}</text>')
    out.append(f'<text x=4 y={height-4} font-size=11>{ts(t0)}</text>')
    out.append(f'<text x={width-70} y={height-4} font-size=11>{ts(t1)}</text>')
    lx = pad
    for label, color, pts in series:
        if pts:
            out.append(f'<polyline fill=none stroke={color} stroke-width=1.5 '
                       f'points="{" ".join(xy(p) for p in pts)}"/>')
        out.append(f'<text x={lx} y=14 font-size=11 fill={color}>{esc(label)}</text>')
        lx += 90
    out.append("</svg>")
    return "".join(out)


class App:
    def __init__(self, hq, service, sim=None, stores_dir=""):
        self.hq = hq
        self.service = service
        self.sim = sim
        self.stores_dir = stores_dir

    # ---------- pages ----------
    def overview(self):
        rows = []
        links = self.service.link_states()
        for s in self.hq.stores():
            st = links.get(s["store_id"], {})
            link = "🟢 通" if st.get("up") else "🔴 断"
            rows.append([
                esc(s["store_id"]), esc(s["name"]), link, esc(s.get("state") or "-"),
                s.get("last_seq") or 0, st.get("applied", 0),
                f'<span class={"bad" if st.get("failed") else "ok"}>{st.get("failed", 0)}</span>',
                s.get("schema_version") or 1])
        body = "<div class=card><h2>门店同步总览</h2>" + table(
            ["门店", "名称", "链路", "状态", "已到位点", "累计应用", "失败条数", "结构版本"],
            rows) + "</div>"
        alerts = self.hq.alerts(10)
        if alerts:
            body += "<div class=card><h2>最新报警</h2>" + table(
                ["时间", "门店", "类型", "当前值", "阈值", "说明"],
                [[ts(a["ts"]), esc(a["store_id"]), a["kind"], f'{a["value"]:.1f}',
                  a["threshold"], esc(a["message"])] for a in alerts]) + "</div>"
        return body

    def stores_page(self, msg=""):
        rows = [[esc(s["store_id"]), esc(s["name"]), esc(s["db_path"]),
                 esc(s["status"]), ts(s["created_at"])] for s in self.hq.stores()]
        body = f"<div class=card><h2>门店接入</h2>{'<p class=ok>'+esc(msg)+'</p>' if msg else ''}"
        body += """<form method=post action=/stores/create>
          新门店编号 <input name=store_id placeholder=S051>
          名称 <input name=name placeholder=XX路店>
          <button>建库 + 全量回灌 + 切增量</button></form><br>"""
        body += table(["编号", "名称", "库路径", "状态", "接入时间"], rows) + "</div>"
        return body

    def links_page(self, msg=""):
        links = self.service.link_states()
        rows = []
        for s in self.hq.stores():
            sid = s["store_id"]
            st = links.get(sid, {})
            action = "down" if st.get("up") else "up"
            label = "断开(模拟断网)" if st.get("up") else "恢复"
            rows.append([
                f'<a href=/links/{sid}>{esc(sid)}</a>',
                "🟢" if st.get("up") else "🔴",
                esc(s.get("state") or "-"), s.get("last_seq") or 0,
                st.get("applied", 0), st.get("failed", 0),
                f'<form method=post action=/links/{sid}/toggle style="display:inline">'
                f'<input type=hidden name=to value={action}><button>{label}</button></form>'])
        body = f"<div class=card><h2>同步链路</h2>{'<p class=ok>'+esc(msg)+'</p>' if msg else ''}"
        body += table(["门店", "链路", "状态", "位点", "累计应用", "失败", "操作"], rows)
        body += "</div>"
        return body

    def link_detail(self, sid, query):
        s = next((x for x in self.hq.stores() if x["store_id"] == sid), None)
        if not s:
            return "<p class=bad>门店不存在</p>"
        body = f"<div class=card><h2>链路 {esc(sid)}</h2>"
        body += (f"<p>状态 <code>{esc(s.get('state'))}</code> 位点 <code>{s.get('last_seq')}</code> "
                 f"结构版本 <code>{s.get('schema_version')}</code></p>")
        evs = self.hq.schema_events(sid)
        if evs:
            body += "<h3>结构变更史</h3>" + table(
                ["时间", "版本", "新增列", "删除列(总部保留)"],
                [[ts(e["detected_at"]), e["version"], esc(e["added_json"]),
                  esc(e["removed_json"])] for e in evs])
        # point-in-time
        q = urllib.parse.parse_qs(query)
        body += f"""<h3>时间点回看</h3><form method=get action=/links/{sid}>
        表 <select name=table><option>members<option>sales<option>sale_items
        <option>points_ledger</select>
        时间 <input name=t placeholder="2026-11-14 12:00:00 或 unix 秒">
        <button>回看</button></form>"""
        if q.get("table") and q.get("t"):
            t_raw = q["t"][0]
            try:
                t_val = float(t_raw)
            except ValueError:
                t_val = time.mktime(time.strptime(t_raw, "%Y-%m-%d %H:%M:%S"))
            rows = self.hq.table_as_of(sid, q["table"][0], ts=t_val)
            body += f"<p>共 {len(rows)} 行(最多显示 50)</p>"
            if rows:
                cols = list(rows[0].keys())
                body += table(cols, [[esc(r.get(c)) for c in cols] for r in rows[:50]])
        body += "</div>"
        return body

    def conflicts_page(self, msg=""):
        rules = self.hq.rules()
        body = f"<div class=card><h2>合并规则</h2>{'<p class=ok>'+esc(msg)+'</p>' if msg else ''}"
        body += table(["表", "字段", "规则", "参数"],
                      [[r["table_name"], r["column"], r["rule"], esc(r["params"])]
                       for r in rules])
        body += """<br><form method=post action=/rules>
        表 <input name=table value=members size=8> 字段 <input name=column value=points size=8>
        规则 <select name=rule><option>additive<option>lww<option>priority<option>max
        <option>min</select>
        参数JSON <input name=params value='{"priority":["S001","S002"]}' size=30>
        <button>保存规则</button></form>
        <p>additive=按差额累加(积分默认) lww=时间戳最新优先 priority=门店优先级 max/min=取极值</p></div>"""
        cfs = self.hq.conflicts(100)
        body += "<div class=card><h2>冲突记录(同一会员多店同时变更)</h2>"
        body += table(["时间", "会员", "来店", "与门店", "字段", "来值", "现值", "合并结果", "规则"],
                      [[ts(c["ts"]), c["member_id"], c["store_id"], c["other_store"],
                        c["field"], c["incoming_val"], c["existing_val"],
                        f'<b>{c["resolved_val"]}</b>', c["rule"]] for c in cfs])
        body += "</div>"
        return body

    def metrics_page(self, msg=""):
        cfg = self.hq.alert_config()
        body = f"<div class=card><h2>报警阈值</h2>{'<p class=ok>'+esc(msg)+'</p>' if msg else ''}"
        body += ("<form method=post action=/alerts/config>"
                 + " ".join(f"{k} <input name={k} value='{v}' size=6>" for k, v in cfg.items())
                 + " <button>保存</button></form> "
                 "<p>lag_seconds=延迟秒 backlog=积压条数 failed=失败条数</p></div>")
        since = time.time() - 3600
        for s in self.hq.stores():
            sid = s["store_id"]
            ms = self.hq.metrics(sid, since=since, limit=4000)
            if not ms:
                continue
            lag = [(m["ts"], m["lag_seconds"]) for m in ms]
            backlog = [(m["ts"], m["backlog"]) for m in ms]
            failed = [(m["ts"], m["failed"]) for m in ms]
            body += (f"<div class=card><h3>{esc(sid)}</h3>"
                     + svg_chart([("延迟s", "#b91c1c", lag), ("积压", "#b45309", backlog),
                                  ("失败", "#1d4ed8", failed)]) + "</div>")
        alerts = self.hq.alerts(50)
        body += "<div class=card><h2>报警记录</h2>" + table(
            ["时间", "门店", "类型", "当前值", "阈值", "说明"],
            [[ts(a["ts"]), esc(a["store_id"]), a["kind"], f'{a["value"]:.1f}',
              a["threshold"], esc(a["message"])] for a in alerts]) + "</div>"
        return body

    def backfill_page(self, msg=""):
        rows = [[esc(s["store_id"]), esc(s.get("state") or "-"), s.get("last_seq") or 0]
                for s in self.hq.stores()]
        opts = "".join(f"<option>{esc(s['store_id'])}" for s in self.hq.stores())
        body = f"<div class=card><h2>回灌 / 重同步</h2>{'<p class=ok>'+esc(msg)+'</p>' if msg else ''}"
        body += "<h3>链路状态</h3>" + table(["门店", "阶段", "位点"], rows)
        body += f"""<h3>区间重同步(确定性校验)</h3>
        <form method=post action=/backfill/resync>
        门店 <select name=store_id>{opts}</select>
        起始位点 <input name=from_seq value=0 size=6> 结束位点 <input name=to_seq value= size=6>
        <button>重新同步并比对</button></form>
        <p>把该店该区间重新跑一遍到临时库,与总部当时状态逐行比对,结果必须完全一致。</p></div>"""
        return body

    # ---------- mutations ----------
    def create_store(self, form):
        sid = form["store_id"][0].strip()
        name = form.get("name", [sid])[0].strip() or sid
        path = f"{self.stores_dir}/{sid}.db"
        info = backfill.onboard_store(self.hq, self.service, sid, name, path)
        if self.sim is not None:
            self.sim.stores[sid] = path
            self.sim.stats[sid] = {"ops": 0, "migrations": 0, "outages": 0}
        return f"门店 {sid} 已接入:快照位点 {info['snapshot_seq']},已切增量"

    def resync(self, form):
        sid = form["store_id"][0]
        from_seq = int(form.get("from_seq", ["0"])[0] or 0)
        to_raw = form.get("to_seq", [""])[0].strip()
        to_seq = int(to_raw) if to_raw else None
        path = next(s["db_path"] for s in self.hq.stores() if s["store_id"] == sid)
        rep = backfill.resync_store(self.hq, sid, path, from_seq, to_seq)
        status = "✅ 完全一致" if rep["ok"] else "❌ 存在差异"
        detail = "; ".join(f"{t}: {v['expected_rows']}/{v['resync_rows']}行"
                           for t, v in rep["tables"].items())
        return f"重同步 [{rep['from_seq']},{rep['to_seq']}] 重放 {rep['replayed']} 条 → {status}({detail})"


def make_handler(app):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, body, title="CDC", refresh=10):
            nav = " ".join(
                f'<a href={u}{" class=cur" if self.path.split("?")[0]==u else ""}>{t}</a>'
                for u, t in NAV)
            html = PAGE.format(title=title, nav=nav, body=body, refresh=refresh)
            data = html.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _redirect(self, url):
            self.send_response(303)
            self.send_header("Location", url)
            self.end_headers()

        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            p = u.path
            msg = urllib.parse.parse_qs(u.query).get("msg", [""])[0]
            if p == "/":
                body = app.overview()
            elif p == "/stores":
                body = app.stores_page(msg)
            elif p == "/links":
                body = app.links_page(msg)
            elif p.startswith("/links/"):
                body = app.link_detail(p.split("/")[2], u.query)
            elif p == "/conflicts":
                body = app.conflicts_page(msg)
            elif p == "/metrics":
                body = app.metrics_page(msg)
            elif p == "/backfill":
                body = app.backfill_page(msg)
            else:
                body = "<p>404</p>"
            self._send(body)

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            form = urllib.parse.parse_qs(self.rfile.read(length).decode())
            p = urllib.parse.urlparse(self.path).path
            if p == "/stores/create":
                msg = app.create_store(form)
                self._redirect("/stores?msg=" + urllib.parse.quote(msg))
            elif p.startswith("/links/") and p.endswith("/toggle"):
                sid = p.split("/")[2]
                app.service.set_link(sid, form["to"][0] == "up")
                self._redirect("/links")
            elif p == "/rules":
                params = json.loads(form.get("params", ["{}"])[0] or "{}")
                app.hq.set_rule(form["table"][0], form["column"][0],
                                form["rule"][0], params)
                self._redirect("/conflicts")
            elif p == "/alerts/config":
                for k, v in form.items():
                    try:
                        app.hq.set_alert_threshold(k, float(v[0]))
                    except ValueError:
                        pass
                self._redirect("/metrics")
            elif p == "/backfill/resync":
                msg = app.resync(form)
                self._redirect("/backfill?msg=" + urllib.parse.quote(msg))
            else:
                self._send("<p>404</p>")
    return H


def serve(app, port=8080):
    httpd = ThreadingHTTPServer(("127.0.0.1", port), make_handler(app))
    print(f"控制台: http://127.0.0.1:{port}")
    httpd.serve_forever()
