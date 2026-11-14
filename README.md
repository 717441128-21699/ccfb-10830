# 门店 CDC 准实时同步服务

三百多家门店、每家收银机本地 SQLite,总部准实时汇总销售与会员积分。
门店侧只用**触发器**捕获变更(收银程序代码不动),总部按位点拉取、断点续传。

纯 Python 标准库实现(sqlite3 / threading / http.server),无第三方依赖。

## 运行

```bash
py verify.py      # 七条需求的自动化验收(10 项检查,约 30 秒)
py stress.py --stores 50 --duration 3600   # 50 家店压一小时,最后逐行比对
py stress.py --stores 50 --duration 120 --ops 3   # 快速版
py run_demo.py --stores 8 --port 8080      # 起演示环境 + Web 控制台
py smoke_web.py   # 控制台冒烟测试
```

控制台五个页面:**门店接入 / 同步链路 / 冲突处理 / 延迟监控 / 回灌**。

## 架构

```
门店 SQLite (POS 库)
  ├─ 业务表 members / sales / sale_items / points_ledger
  ├─ _cdc_log(seq 自增, 表, 操作, 主键, 旧值, 新值, 时间)   ← 触发器写入
  └─ _cdc_meta(结构版本)                ← 门店侧代理检测 DDL 后重建触发器
        │  按 seq 位点批量拉取(断网时本地累积)
        ▼
同步服务 sync_service.Worker(每店一条链路,可断可续)
        ▼
总部 HQ 库(单库,全局写锁串行应用)
  ├─ cur_<店>_<表>   每店当前态(结构跟随该店)
  ├─ journal         全量变更流水(时间点回看的依据)
  ├─ member_points   全链会员积分合并视图(可配置规则)
  ├─ metrics/alerts  延迟·积压·失败指标与报警
  └─ sync_state      每店位点,与数据同事务提交 ← 一条不多一条不少的保证
```

## 需求 → 机制

1. **断网几天回来接着传,一条不多一条不少**
   每条变更在 `_cdc_log` 有单调位点 `seq`;总部把"应用数据 + 推进位点"
   放在**同一个事务**里提交,崩溃后从位点重拉,不重不漏。
2. **加列删列总部自动跟随,老数据照样能查**
   门店侧代理发现 sqlite_master 签名变化 → 重建触发器 + 往日志里插一条
   `SCHEMA` 事件(带完整 DDL)。总部按序收到后自动 `ADD COLUMN`;
   删列时总部**保留旧列**(标记废弃),老数据继续可查,结构变更史在
   `schema_events` 里。
3. **同一会员两店同时扣积分,按可配置规则合并**
   `merge_rules` 表按(表,字段)配规则:`additive`(默认,各店差额累加)、
   `lww`(时间戳最新)、`priority`(门店优先级)、`max/min`。additive 下
   首次建档只定基准(按 updated_at 最早者,迟到会重定基),之后的变更
   按差额累加,结果与到达顺序无关;跨店并发变更记入 `conflicts` 表。
4. **任意时间点回看**
   每条应用都写 `journal`(新旧值全存)。回看 = 当前态 + 逆序撤销
   该时刻之后的流水,`HQ.table_as_of(store, table, ts/hq_seq)`。
5. **延迟/积压/失败曲线 + 超阈值报警**
   链路每秒采样进 `metrics`(断网期间门店侧代理继续采样,积压可见),
   阈值在 `alert_config`,超限写 `alerts`(60 秒冷却),控制台画 SVG 曲线。
6. **新店先灌全量再切增量,切换窗口不丢**
   先记位点 seq0 → 一致性快照(WAL 读事务)→ 灌入总部 → 从 seq0 起
   增量。快照期间的变更都在日志里,幂等 upsert 吸收重叠。
7. **挑店挑区间重同步,结果完全一致**
   `backfill.resync_store`:用 journal 重建区间起点状态灌进临时库,
   重放该区间日志,与总部当时状态逐行比对。应用顺序按 seq 固定、
   冲突合并只用数据内时间戳(不用墙钟),所以结果确定。

## 已知边界(真实部署要注意)

- **SQLite `json_object` 用 %.15g 渲染 REAL**,16-17 位有效数字的 double
  (如带微秒的 float 时间戳)会丢精度。业务库应把时间存 INTEGER 毫秒、
  金额存分为整数或两位小数 REAL(本方案即如此,逐行比对是精确的)。
- 触发器方案下,POS 的 DDL 迁移要走门店侧代理的 `run_migration`
  (先摘触发器→改结构→重建并记 SCHEMA 事件),否则 SQLite 会拒绝
  删被触发器引用的列。
- 单 HQ 库 + 全局写锁在 50 店 x 3 ops/s 下余量很大;三百店规模建议
  HQ 换 PostgreSQL,应用层协议(位点、批量、SCHEMA 事件)不变。
