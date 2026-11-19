"""离线观测库补录批次（单批 JSON）迁入中心库的核心逻辑。

业务背景：各区县离线观测库各自给批次编号，测站撤并后，档案员把补录批次
迁入中心观测库。流程为「上传 → 按稳定列标识配对字段 → 预览读数与校验错误
→ 确认入库」。

设计要点：
- 幂等键 = (源库号, 源批次号)：同库同号文件原样重传（content_hash 相同）
  直接返回原入库批次号；内容有变（hash 不同）一律拒收（409）。
- 预览即快照：preview 行钉住原始文件文本、字段配对与当时的现役版本；
  commit 只采用这份快照，且要求现役版本号未变，否则要求重新预览。
- 先完整校验、再单事务写入：任一行不合格则不能入库；写入失败整体回滚，
  不留下半批数据。
- 入库数据写入既有 batches / observations 表，读数按目标现役列的
  column_id 存 values_json，因此与 CSV 批次一样可在观测页统一查询，
  并额外带源库号、源批次号。
"""
from __future__ import annotations

import hashlib
import json

from . import core

MAX_IMPORT_BYTES = 20 * 1024 * 1024
MAX_FIELD_LEN = 100
MAX_ID_LEN = 100


class ConflictError(core.DomainError):
    """同库同号但内容变更等冲突（映射为 HTTP 409）。"""


# ---------------------------------------------------------------- 文档解析

def parse_import_document(text: str) -> dict:
    """解析并结构性校验补录 JSON 文档。文档格式：

    {
      "source_library": "QX-01",
      "source_batch": "B-007",
      "fields": [{"field_id": "f1", "field_name": "气温"}, ...],
      "rows": [
        {"station": "S01", "hour_utc": "2026-11-18T00:00",
         "readings": {"f1": 5, "f2": 1013}}
      ]
    }
    """
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as e:
        raise core.DomainError(f"JSON 解析失败：{e}")
    if not isinstance(doc, dict):
        raise core.DomainError("文档顶层必须是 JSON 对象")

    library = (doc.get("source_library") or "").strip()
    batch = (doc.get("source_batch") or "").strip()
    if not library:
        raise core.DomainError("缺少源库号 source_library")
    if not batch:
        raise core.DomainError("缺少源批次号 source_batch")
    if len(library) > MAX_ID_LEN or len(batch) > MAX_ID_LEN:
        raise core.DomainError(f"源库号 / 源批次号长度不能超过 {MAX_ID_LEN} 字符")

    fields = doc.get("fields")
    if not isinstance(fields, list) or not fields:
        raise core.DomainError("fields 必须是非空数组（字段编号与名称）")
    field_specs, seen_ids, errors = [], set(), []
    for i, f in enumerate(fields, 1):
        if not isinstance(f, dict):
            errors.append(f"第 {i} 个字段：必须是对象")
            continue
        fid = (f.get("field_id") or "").strip() if isinstance(f.get("field_id"), str) else ""
        fname = f.get("field_name")
        fname = fname.strip() if isinstance(fname, str) else ""
        if not fid:
            errors.append(f"第 {i} 个字段：field_id 不能为空")
            continue
        if len(fid) > MAX_FIELD_LEN:
            errors.append(f"字段 {fid}：field_id 过长（超过 {MAX_FIELD_LEN} 字符）")
            continue
        if fid in seen_ids:
            errors.append(f"字段编号重复：{fid}")
            continue
        seen_ids.add(fid)
        field_specs.append({"field_id": fid, "field_name": fname[:MAX_FIELD_LEN]})

    rows = doc.get("rows")
    if not isinstance(rows, list) or not rows:
        errors.append("rows 必须是非空数组（每行含站号、UTC 小时与读数）")
    if errors:
        raise core.DomainError("补录文档结构校验失败", errors)
    return {
        "source_library": library,
        "source_batch": batch,
        "fields": field_specs,
        "rows": rows,
    }


# ---------------------------------------------------------------- 配对与逐行校验

def resolve_mapping(mapping, field_specs, active_cols) -> list[dict]:
    """校验字段配对，返回 [{field_id, field_name, column_id, name, type}]。

    只允许源字段配到现役的数据列（站号/小时为系统列，不能作为配对目标），
    且一个现役列最多被一个源字段配对。未出现在 mapping 中的源字段不入库，
    其读数在校验时静默跳过。
    """
    if not isinstance(mapping, list) or not mapping:
        raise core.DomainError(
            "mapping 必须是非空数组，每项 {field_id, column_id}；"
            "请先按预览返回的 fields 与现役列完成配对"
        )
    data_cols = [c for c in active_cols if c["type"] not in core.SYSTEM_TYPES]
    col_by_id = {c["column_id"]: c for c in data_cols}
    field_by_id = {f["field_id"]: f for f in field_specs}
    used_cols, used_fields, errors = set(), set(), []
    pairs = []
    for i, m in enumerate(mapping, 1):
        if not isinstance(m, dict):
            errors.append(f"第 {i} 项配对：必须是对象")
            continue
        fid = (m.get("field_id") or "").strip() if isinstance(m.get("field_id"), str) else ""
        cid = (m.get("column_id") or "").strip() if isinstance(m.get("column_id"), str) else ""
        if not fid or not cid:
            errors.append(f"第 {i} 项配对：field_id 与 column_id 都不能为空")
            continue
        if fid not in field_by_id:
            errors.append(f"配对 {fid} → {cid}：源文档中不存在字段 {fid}")
            continue
        if fid in used_fields:
            errors.append(f"字段 {fid} 被配对了多次")
            continue
        if cid not in col_by_id:
            errors.append(
                f"配对 {fid} → {cid}：现役结构中不存在数据列 {cid}"
                "（站号/小时为系统列，不能作为配对目标）"
            )
            continue
        if cid in used_cols:
            errors.append(f"现役列 {cid} 被多个源字段配对，不允许合并写入")
            continue
        used_fields.add(fid)
        used_cols.add(cid)
        col = col_by_id[cid]
        pairs.append({
            "field_id": fid,
            "field_name": field_by_id[fid]["field_name"],
            "column_id": cid,
            "name": col["name"],
            "type": col["type"],
        })
    if errors:
        raise core.DomainError("字段配对校验失败", errors)
    return pairs


def _check_reading(v, ctype) -> tuple[object, str | None]:
    """按目标现役列类型校验单个读数。JSON 布尔值一律拒绝（避免 True 混进整数列）。"""
    if v is None:
        return None, None
    if isinstance(v, bool):
        return None, f"布尔值不是合法的 {ctype} 读数"
    if ctype == "integer":
        if not isinstance(v, int):
            return None, f"期望整数，得到 {json.dumps(v, ensure_ascii=False)}"
        return v, None
    if ctype == "float":
        if isinstance(v, int):
            return v, None  # 保留读数原始形态（5 不写成 5.0）
        if isinstance(v, float):
            if v != v or v in (float("inf"), float("-inf")):
                return None, "小数必须有限"
            return v, None
        return None, f"期望小数，得到 {json.dumps(v, ensure_ascii=False)}"
    if not isinstance(v, str):
        return None, f"文本列只接受字符串，得到 {json.dumps(v, ensure_ascii=False)}"
    return v, None


def validate_rows(rows, pairs, known_field_ids=None) -> tuple[list, list, list]:
    """逐行校验。返回 (可入库行, 错误列表, 预览行)。

    可入库行元素：(station, hour, values_json_str)
    预览行元素：{line, station, hour_utc, values: {现役列名: 值}, errors: [...]}
    未配对字段的读数静默跳过；既未配对也不在文档 fields 中的编号才算未知字段。
    """
    pair_by_fid = {p["field_id"]: p for p in pairs}
    known = set(known_field_ids or ()) | set(pair_by_fid)
    valid_rows, errors, preview_rows = [], [], []
    for ln, raw in enumerate(rows, start=1):
        row_err: list[str] = []
        if not isinstance(raw, dict):
            row_err.append(f"第 {ln} 行：必须是 JSON 对象")
            preview_rows.append({"line": ln, "station": None, "hour_utc": None,
                                 "values": {}, "errors": row_err})
            errors.extend(row_err)
            continue
        station = (raw.get("station") or "").strip() if isinstance(raw.get("station"), str) else ""
        if not station:
            row_err.append(f"第 {ln} 行：站号为空")
        elif len(station) > 100:
            row_err.append(f"第 {ln} 行：站号过长（超过 100 字符）")
        try:
            hour = core.normalize_hour(raw.get("hour_utc"))
        except ValueError as e:
            hour = None
            row_err.append(f"第 {ln} 行：{e}")
        readings = raw.get("readings")
        if not isinstance(readings, dict):
            row_err.append(f"第 {ln} 行：readings 必须是对象（按字段编号记录读数）")
            readings = {}
        else:
            unknown = [k for k in readings if k not in known]
            if unknown:
                row_err.append(f"第 {ln} 行：出现未知的字段编号：" + "、".join(map(str, unknown)))
        rec, values = {}, {}
        for fid, p in pair_by_fid.items():
            v, err = _check_reading(readings.get(fid), p["type"])
            if err:
                row_err.append(f"第 {ln} 行 字段「{fid}」→列「{p['name']}」：{err}")
            else:
                rec[p["column_id"]] = v
                values[p["name"]] = v
        preview_rows.append({
            "line": ln, "station": station or None,
            "hour_utc": hour, "values": values, "errors": row_err,
        })
        if row_err:
            errors.extend(row_err)
        else:
            valid_rows.append((station, hour, json.dumps(rec, ensure_ascii=False)))
        if len(errors) >= core.MAX_ERRORS:
            errors.append("……错误过多，仅显示前部分")
            break
    return valid_rows, errors, preview_rows


# ---------------------------------------------------------------- 预览 / 入库

def _find_committed(conn, library, batch):
    return conn.execute(
        "SELECT * FROM batches WHERE source_library=? AND source_batch=?",
        (library, batch),
    ).fetchone()


def create_preview(conn, filename, text, mapping=None):
    """创建预览（或返回配对建议）。

    - 不带 mapping：只解析文档并返回字段清单与现役数据列，供网页做配对；
    - 带 mapping：完整校验并落一条 import_previews 快照（status=open）。
    同库同号已入库时：原样重传返回原批次信息，内容变更抛 ConflictError。
    """
    doc = parse_import_document(text)
    active = core.get_active_version(conn)
    if active is None:
        raise core.DomainError("尚未发布任何结构版本，请先发布初版结构后再导入补录批次")
    active_cols = core.version_columns(conn, active["id"])
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()

    existing = _find_committed(conn, doc["source_library"], doc["source_batch"])
    if existing is not None:
        if existing["content_hash"] == digest:
            return {
                "already_imported": True,
                "batch_id": existing["id"],
                "source_library": doc["source_library"],
                "source_batch": doc["source_batch"],
                "content_hash": digest,
                "message": "该源库同号文件已入库，原样重传，返回原入库批次号",
            }
        raise ConflictError(
            f"源库 {doc['source_library']} 的批次 {doc['source_batch']} 已入库"
            f"（批次 #{existing['id']}），但本次文件内容与入库时不同；"
            "同号文件内容有变一律拒收，请使用新的源批次号",
        )

    base = {
        "already_imported": False,
        "source_library": doc["source_library"],
        "source_batch": doc["source_batch"],
        "content_hash": digest,
        "version_no": active["version_no"],
        "fields": doc["fields"],
        "active_columns": [c for c in active_cols if c["type"] not in core.SYSTEM_TYPES],
        "total_rows": len(doc["rows"]),
    }
    if mapping is None:
        base["mapping_required"] = True
        return base

    pairs = resolve_mapping(mapping, doc["fields"], active_cols)
    known_ids = [f["field_id"] for f in doc["fields"]]
    valid_rows, errors, preview_rows = validate_rows(
        doc["rows"], pairs, known_field_ids=known_ids
    )
    cur = conn.execute(
        """
        INSERT INTO import_previews
          (source_library, source_batch, filename, content_hash, raw_text,
           version_id, mapping_json, row_count, error_count, status, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,'open',?)
        """,
        (doc["source_library"], doc["source_batch"], filename, digest, text,
         active["id"], json.dumps(pairs, ensure_ascii=False),
         len(valid_rows), len(errors), core._now()),
    )
    return {
        **base,
        "mapping_required": False,
        "preview_id": cur.lastrowid,
        "mapping": pairs,
        "valid": not errors,
        "row_count": len(valid_rows),
        "error_count": len(errors),
        "errors": errors,
        "preview_rows": preview_rows,
        "committable": not errors,
    }


def commit_preview(conn, preview_id):
    """按预览快照入库。现役结构换版或内容冲突则拒绝；写入在单事务内完成。"""
    r = conn.execute("SELECT * FROM import_previews WHERE id=?", (preview_id,)).fetchone()
    if r is None:
        raise core.NotFoundError(f"预览 #{preview_id} 不存在")
    if r["status"] == "committed":
        return {
            "batch_id": r["batch_id"], "idempotent": True,
            "source_library": r["source_library"], "source_batch": r["source_batch"],
            "message": "该预览已入库，重复确认返回原入库批次号",
        }

    active = core.get_active_version(conn)
    if active is None or active["id"] != r["version_id"]:
        pinned = conn.execute(
            "SELECT version_no FROM schema_versions WHERE id=?", (r["version_id"],)
        ).fetchone()
        pinned_no = pinned["version_no"] if pinned else "?"
        current_no = active["version_no"] if active else "无"
        raise core.DomainError(
            f"预览基于 v{pinned_no}，现役结构已换版为 v{current_no}；"
            "首次入库只采用预览时的文件与配对，请重新预览后再入库"
        )

    # 重新解析快照并完整校验：保证入库内容与预览一致，任何错误都不写入
    doc = parse_import_document(r["raw_text"])
    pairs = json.loads(r["mapping_json"])
    active_cols = core.version_columns(conn, active["id"])
    valid_col_ids = {c["column_id"] for c in active_cols if c["type"] not in core.SYSTEM_TYPES}
    if any(p["column_id"] not in valid_col_ids for p in pairs):
        raise core.DomainError("预览配对的现役列已不存在，请重新预览后再入库")
    valid_rows, errors, _ = validate_rows(
        doc["rows"], pairs,
        known_field_ids=[f["field_id"] for f in doc["fields"]],
    )
    if errors:
        raise core.DomainError(
            f"预览含 {len(errors)} 处校验错误，不能入库；请修正源文件后重新预览", errors
        )

    existing = _find_committed(conn, r["source_library"], r["source_batch"])
    if existing is not None:
        if existing["content_hash"] == r["content_hash"]:
            conn.execute(
                "UPDATE import_previews SET status='committed', batch_id=? WHERE id=?",
                (existing["id"], r["id"]),
            )
            return {
                "batch_id": existing["id"], "idempotent": True,
                "source_library": r["source_library"], "source_batch": r["source_batch"],
                "message": "同库同号原样文件已入库，返回原入库批次号",
            }
        raise ConflictError(
            f"源库 {r['source_library']} 的批次 {r['source_batch']} 已以不同内容入库，拒收"
        )

    cur = conn.execute(
        """
        INSERT INTO batches
          (version_id, filename, row_count, uploaded_at,
           source_library, source_batch, content_hash)
        VALUES (?,?,?,?,?,?,?)
        """,
        (active["id"], r["filename"], len(valid_rows), core._now(),
         r["source_library"], r["source_batch"], r["content_hash"]),
    )
    bid = cur.lastrowid
    conn.executemany(
        "INSERT INTO observations (batch_id, station, hour_utc, values_json) VALUES (?,?,?,?)",
        [(bid, s, h, j) for s, h, j in valid_rows],
    )
    conn.execute(
        "UPDATE import_previews SET status='committed', batch_id=? WHERE id=?",
        (bid, r["id"]),
    )
    return {
        "batch_id": bid, "idempotent": False, "version_no": active["version_no"],
        "row_count": len(valid_rows),
        "source_library": r["source_library"], "source_batch": r["source_batch"],
        "content_hash": r["content_hash"],
    }
