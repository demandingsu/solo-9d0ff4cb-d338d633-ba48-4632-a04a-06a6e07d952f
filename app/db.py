"""SQLite 连接与事务管理。

所有读写都通过 session() 进行：全局可重入锁 + 单连接事务。
发布 / 上传 / 查询任一操作要么整体完成、要么整体回滚，
保证"结构"与"数据"永远成对出现，交错调用不会混用。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager

_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version_no  INTEGER NOT NULL UNIQUE,
    status      TEXT NOT NULL CHECK (status IN ('active', 'superseded')),
    note        TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schema_columns (
    version_id  INTEGER NOT NULL REFERENCES schema_versions(id) ON DELETE CASCADE,
    column_id   TEXT NOT NULL,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL,
    position    INTEGER NOT NULL,
    nullable    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (version_id, column_id),
    UNIQUE (version_id, name)
);
CREATE TABLE IF NOT EXISTS batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id  INTEGER NOT NULL REFERENCES schema_versions(id),
    filename    TEXT NOT NULL DEFAULT '',
    row_count   INTEGER NOT NULL,
    uploaded_at TEXT NOT NULL,
    source_library TEXT,
    source_batch   TEXT,
    content_hash   TEXT
);
CREATE TABLE IF NOT EXISTS observations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    station     TEXT NOT NULL,
    hour_utc    TEXT NOT NULL,
    values_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_obs_station_hour ON observations (station, hour_utc);
CREATE INDEX IF NOT EXISTS idx_obs_batch ON observations (batch_id);
-- 补录批次预览：钉住预览时的原始文件、字段配对与现役版本，入库只采用这份快照
CREATE TABLE IF NOT EXISTS import_previews (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source_library TEXT NOT NULL,
    source_batch   TEXT NOT NULL,
    filename       TEXT NOT NULL DEFAULT '',
    content_hash   TEXT NOT NULL,
    raw_text       TEXT NOT NULL,
    version_id     INTEGER NOT NULL REFERENCES schema_versions(id),
    mapping_json   TEXT NOT NULL,
    row_count      INTEGER NOT NULL,
    error_count    INTEGER NOT NULL,
    status         TEXT NOT NULL DEFAULT 'open'
                       CHECK (status IN ('open', 'committed')),
    batch_id       INTEGER REFERENCES batches(id),
    created_at     TEXT NOT NULL
);
"""

# 依赖 batches 新增列的索引，须在旧库 ALTER 迁移完成后再创建
_SOURCE_INDEX_SQL = """
-- 同一源库的同号补录批次只允许入库一次（CSV 直传批次这三列为 NULL，不受约束）
CREATE UNIQUE INDEX IF NOT EXISTS idx_batches_source
    ON batches(source_library, source_batch)
    WHERE source_library IS NOT NULL;
"""

# 旧库升级：batches 补三列（新库建表时已含，这里仅对存量库生效）
_BATCHES_MIGRATIONS = (
    ("source_library", "ALTER TABLE batches ADD COLUMN source_library TEXT"),
    ("source_batch", "ALTER TABLE batches ADD COLUMN source_batch TEXT"),
    ("content_hash", "ALTER TABLE batches ADD COLUMN content_hash TEXT"),
)


def db_path() -> str:
    return os.environ.get("DB_PATH") or os.path.join(os.getcwd(), "data", "obs.db")


def connect() -> sqlite3.Connection:
    path = db_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.Error:
        pass
    return conn


def init_db() -> None:
    with _lock:
        with connect() as conn:
            conn.executescript(SCHEMA)
            existing = {r[1] for r in conn.execute("PRAGMA table_info(batches)")}
            for col, ddl in _BATCHES_MIGRATIONS:
                if col not in existing:
                    conn.execute(ddl)
            conn.executescript(_SOURCE_INDEX_SQL)


@contextmanager
def session():
    """全局锁 + 单连接事务：正常退出提交，异常回滚并关闭连接。"""
    with _lock:
        conn = connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()
