"""Store-side SQLite: business schema, CDC triggers, change reading, schema tracking.

The POS application code is untouchable; everything here is additive:
  * business tables are created once (they belong to the POS schema)
  * `_cdc_log` / `_cdc_meta` tables and `_cdc_*` triggers are ours
Every INSERT/UPDATE/DELETE on a business table is captured by triggers into
`_cdc_log` with a monotonically increasing `seq` — that seq is the sync position.
"""
import hashlib
import json
import sqlite3
import time

# Initial POS schema (the part the cash register uses).
# NOTE: timestamps are INTEGER epoch-millis and money is 2-decimal REAL.
# SQLite's json_object() renders REALs with %.15g, so a CDC payload cannot
# carry doubles that need 16-17 significant digits (e.g. float epoch seconds
# with microseconds). Integer ms / cents is the standard fix and is what a
# real POS schema should do too.
INITIAL_DDL = [
    """CREATE TABLE members(
         member_id INTEGER PRIMARY KEY,
         name TEXT, phone TEXT,
         points INTEGER NOT NULL DEFAULT 0,
         updated_at INTEGER)""",
    """CREATE TABLE sales(
         sale_id INTEGER PRIMARY KEY,
         member_id INTEGER, total REAL,
         status TEXT, created_at INTEGER)""",
    """CREATE TABLE sale_items(
         item_id INTEGER PRIMARY KEY,
         sale_id INTEGER, sku TEXT,
         qty INTEGER, price REAL)""",
    """CREATE TABLE points_ledger(
         entry_id INTEGER PRIMARY KEY,
         member_id INTEGER, delta INTEGER,
         reason TEXT, created_at INTEGER)""",
]

CDC_DDL = [
    """CREATE TABLE IF NOT EXISTS _cdc_log(
         seq INTEGER PRIMARY KEY AUTOINCREMENT,
         table_name TEXT NOT NULL,
         op TEXT NOT NULL,               -- INSERT | UPDATE | DELETE | SCHEMA
         pk_json TEXT,
         old_json TEXT,
         new_json TEXT,
         ts REAL NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS _cdc_log_seq ON _cdc_log(seq)",
    """CREATE TABLE IF NOT EXISTS _cdc_meta(
         k TEXT PRIMARY KEY, v TEXT)""",
]

TS_EXPR = "((julianday('now') - 2440587.5) * 86400.0)"


def connect(path):
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def business_tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE '\\_cdc%' ESCAPE '\\' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
    return [r[0] for r in rows]


def table_info(conn, table):
    """-> {'columns': [(name, type)], 'pk': [name, ...]}"""
    cols, pk = [], []
    for r in conn.execute(f'PRAGMA table_info("{table}")'):
        cols.append((r["name"], r["type"] or "TEXT"))
        if r["pk"]:
            pk.append(r["name"])
    return {"columns": cols, "pk": pk}


def _json_obj(cols, prefix):
    parts = ", ".join(f"'{c}', {prefix}.\"{c}\"" for c in cols)
    return f"json_object({parts})"


def install_triggers(conn):
    """(Re)build CDC triggers for the current schema. Ours to manage."""
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '\\_cdc%' ESCAPE '\\'")
    for (name,) in cur.fetchall():
        conn.execute(f'DROP TRIGGER IF EXISTS "{name}"')
    for t in business_tables(conn):
        info = table_info(conn, t)
        cols = [c for c, _ in info["columns"]]
        pk = info["pk"] or cols[:1]
        new_obj = _json_obj(cols, "NEW")
        old_obj = _json_obj(cols, "OLD")
        pk_new = _json_obj(pk, "NEW")
        pk_old = _json_obj(pk, "OLD")
        conn.executescript(f"""
        CREATE TRIGGER "_cdc_{t}_ai" AFTER INSERT ON "{t}" BEGIN
          INSERT INTO _cdc_log(table_name,op,pk_json,old_json,new_json,ts)
          VALUES('{t}','INSERT',{pk_new},NULL,{new_obj},{TS_EXPR});
        END;
        CREATE TRIGGER "_cdc_{t}_au" AFTER UPDATE ON "{t}" BEGIN
          INSERT INTO _cdc_log(table_name,op,pk_json,old_json,new_json,ts)
          VALUES('{t}','UPDATE',{pk_new},{old_obj},{new_obj},{TS_EXPR});
        END;
        CREATE TRIGGER "_cdc_{t}_ad" AFTER DELETE ON "{t}" BEGIN
          INSERT INTO _cdc_log(table_name,op,pk_json,old_json,new_json,ts)
          VALUES('{t}','DELETE',{pk_old},{old_obj},NULL,{TS_EXPR});
        END;""")


def schema_signature(conn):
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE '\\_cdc%' ESCAPE '\\' ORDER BY name").fetchall()
    h = hashlib.sha256()
    for name, sql in rows:
        h.update(name.encode())
        h.update((sql or "").encode())
    return h.hexdigest()


def current_schema_ddl(conn):
    return {t: table_info(conn, t) for t in business_tables(conn)}


def init_store_db(path):
    conn = connect(path)
    with conn:
        for ddl in INITIAL_DDL + CDC_DDL:
            conn.execute(ddl)
        install_triggers(conn)
        conn.execute("INSERT OR REPLACE INTO _cdc_meta(k,v) VALUES('schema_version','1')")
        conn.execute("INSERT OR REPLACE INTO _cdc_meta(k,v) VALUES('schema_sig',?)",
                     (schema_signature(conn),))
    return conn


def detect_schema_change(conn):
    """Store-side agent hook: if the POS schema changed, rebuild triggers and
    append a SCHEMA event to the CDC log so HQ follows in order."""
    old_sig = conn.execute("SELECT v FROM _cdc_meta WHERE k='schema_sig'").fetchone()
    old_sig = old_sig[0] if old_sig else ""
    new_sig = schema_signature(conn)
    if new_sig == old_sig:
        return None
    version = int(conn.execute("SELECT v FROM _cdc_meta WHERE k='schema_version'").fetchone()[0]) + 1
    install_triggers(conn)
    ddl = current_schema_ddl(conn)
    with conn:
        conn.execute(
            "INSERT INTO _cdc_log(table_name,op,pk_json,old_json,new_json,ts) "
            "VALUES('__schema__','SCHEMA',NULL,NULL,?,?)",
            (json.dumps({"version": version, "tables": ddl}), time.time()))
        conn.execute("INSERT OR REPLACE INTO _cdc_meta(k,v) VALUES('schema_version',?)",
                     (str(version),))
        conn.execute("INSERT OR REPLACE INTO _cdc_meta(k,v) VALUES('schema_sig',?)", (new_sig,))
    return version


def drop_triggers(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' "
        "AND name LIKE '\\_cdc%' ESCAPE '\\'").fetchall()
    for (name,) in rows:
        conn.execute(f'DROP TRIGGER IF EXISTS "{name}"')


def run_migration(conn, sql):
    """Store-side agent: apply a POS schema migration safely under CDC.

    SQLite refuses ALTER ... DROP COLUMN while a trigger references the column,
    so the agent parks its triggers, applies the DDL, then re-detects the
    schema (which rebuilds triggers and logs the SCHEMA event in one go)."""
    drop_triggers(conn)
    try:
        conn.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        install_triggers(conn)   # never leave the store without CDC
        conn.commit()
        raise
    return detect_schema_change(conn)


def read_changes(conn, after_seq, limit=500):
    return conn.execute(
        "SELECT seq,table_name,op,pk_json,old_json,new_json,ts FROM _cdc_log "
        "WHERE seq>? ORDER BY seq LIMIT ?", (after_seq, limit)).fetchall()


def max_seq(conn):
    return conn.execute("SELECT COALESCE(MAX(seq),0) FROM _cdc_log").fetchone()[0]


def snapshot(path):
    """Consistent full dump + the CDC position taken BEFORE the dump.

    Anything written during/after the dump lands in _cdc_log at seq > seq0,
    so replaying from seq0 after loading the snapshot loses nothing; the
    overlap is absorbed by idempotent upserts at HQ."""
    conn = connect(path)
    seq0 = max_seq(conn)
    conn.execute("BEGIN")  # read snapshot (WAL snapshot isolation)
    try:
        data = {}
        for t in business_tables(conn):
            info = table_info(conn, t)
            rows = conn.execute(f'SELECT * FROM "{t}"').fetchall()
            data[t] = {"info": info,
                       "rows": [dict(r) for r in rows]}
    finally:
        conn.rollback()
        conn.close()
    return seq0, data
