#!/usr/bin/env python3
"""补录批次导入端到端自测。

覆盖：同号两源库导入、原样重传幂等、内容变更拒收、预览校验错误不可入库、
现役结构换版后旧预览失效（须重新预览）、原子性、入库后在观测页可查
读数与源库号/源批次号。

用法：python tests/selftest_import.py
退出码 0 = 全部通过；1 = 有失败项。
"""
import json
import os
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
EXAMPLES = os.path.join(REPO, "examples")

_fd, DB = tempfile.mkstemp(prefix="obs-import-selftest-", suffix=".db")
os.close(_fd)
os.remove(DB)
os.environ["DB_PATH"] = DB

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, extra=""):
    if cond:
        PASS.append(name)
        print(f"  ✓ {name}")
    else:
        FAIL.append(name)
        print(f"  ✗ {name}  {extra}")


def load_example(fname):
    with open(os.path.join(EXAMPLES, fname), encoding="utf-8") as f:
        return f.read()


def post_json(c, fname, text, mapping=None):
    data = {}
    if mapping is not None:
        data["mapping"] = json.dumps(mapping, ensure_ascii=False)
    return c.post(
        "/api/imports/preview",
        files={"file": (fname, text.encode("utf-8"), "application/json")},
        data=data,
    )


def main():
    with TestClient(app) as c:
        print("== 1. 准备现役结构 v1（站号/小时/气温 float/气压 float）==")
        r = c.post("/api/versions", json={"note": "初版", "columns": [
            {"role": "station", "name": "站号"},
            {"role": "hour", "name": "UTC小时"},
            {"name": "气温", "type": "float"},
            {"name": "气压", "type": "float"},
        ]})
        check("发布初版 v1", r.status_code == 201, r.text[:300])
        cur = r.json()
        col_id = {x["name"]: x["column_id"] for x in cur["columns"]}

        print("== 2. 无 mapping 上传：返回字段清单与现役列，要求配对 ==")
        donghai = load_example("import_QX-DONGHAI_B-007.json")
        r = post_json(c, "donghai.json", donghai)
        d = r.json()
        check("首次上传返回 mapping_required",
              r.status_code == 200 and d["mapping_required"] is True, r.text[:300])
        check("返回源库号/源批次号",
              d["source_library"] == "QX-DONGHAI" and d["source_batch"] == "B-007")
        check("返回全部源字段(3个)", len(d["fields"]) == 3)
        check("返回现役数据列(2个,不含系统列)",
              [x["name"] for x in d["active_columns"]] == ["气温", "气压"])

        print("== 3. 配对并生成预览：读数与校验错误 ==")
        mapping = [
            {"field_id": "f-temp", "column_id": col_id["气温"]},
            {"field_id": "f-pres", "column_id": col_id["气压"]},
        ]
        r = post_json(c, "donghai.json", donghai, mapping)
        d = r.json()
        check("预览成功且无错误可入库（f-wind 未配对，读数跳过）",
              r.status_code == 200 and d["valid"] and d["committable"], r.text[:300])
        check("预览3行全部可入库", d["row_count"] == 3 and d["error_count"] == 0)
        check("未配对字段不产生输出列",
              all("风向备注" not in row["values"] for row in d["preview_rows"]))
        bad_map = mapping + [{"field_id": "f-temp", "column_id": col_id["气压"]}]
        check("一列配多源被拒绝", post_json(c, "donghai.json", donghai, bad_map).status_code == 400)
        check("配到系统列被拒绝",
              post_json(c, "donghai.json", donghai,
                        mapping + [{"field_id": "f-wind", "column_id": "station"}]).status_code == 400)
        check("不存在的源字段被拒绝",
              post_json(c, "donghai.json", donghai,
                        mapping + [{"field_id": "nope", "column_id": col_id["气压"]}]).status_code == 400)
        check("同一源字段重复配对被拒绝",
              post_json(c, "donghai.json", donghai,
                        mapping + [{"field_id": "f-temp", "column_id": col_id["气温"]}]).status_code == 400)
        preview_id = d["preview_id"]

        print("== 4. 确认入库（原子写入）==")
        r = c.post(f"/api/imports/{preview_id}/commit")
        d = r.json()
        check("入库 201，3 行", r.status_code == 201 and d["row_count"] == 3, r.text[:300])
        batch_a = d["batch_id"]
        check("回执带源库号/源批次号/hash",
              d["source_library"] == "QX-DONGHAI" and d["source_batch"] == "B-007"
              and len(d["content_hash"]) == 64)
        r2 = c.post(f"/api/imports/{preview_id}/commit")
        check("重复确认幂等返回原批次号(200)",
              r2.status_code == 200 and r2.json()["idempotent"] is True
              and r2.json()["batch_id"] == batch_a, r2.text[:200])

        print("== 5. 同库同号原样重传 → 原批次号；内容变更 → 拒收 409 ==")
        r = post_json(c, "donghai.json", donghai)
        check("原样重传返回 already_imported 与原批次号",
              r.status_code == 200 and r.json()["already_imported"] is True
              and r.json()["batch_id"] == batch_a, r.text[:200])
        changed = json.loads(donghai)
        changed["rows"][0]["readings"]["f-temp"] = 99
        r = post_json(c, "donghai.json", json.dumps(changed, ensure_ascii=False))
        check("内容变更拒收 409", r.status_code == 409, r.text[:200])
        r = post_json(c, "donghai.json", json.dumps(changed, ensure_ascii=False), mapping)
        check("带配对的内容变更同样拒收 409", r.status_code == 409, r.text[:200])

        print("== 6. 另一源库同号 B-007 独立编号，可正常导入 ==")
        xishan = load_example("import_QX-XISHAN_B-007.json")
        r = post_json(c, "xishan.json", xishan)
        check("西山库同号批次不冲突", r.status_code == 200 and r.json()["mapping_required"] is True)
        xishan_mapping = [
            {"field_id": "t01", "column_id": col_id["气温"]},
            {"field_id": "p01", "column_id": col_id["气压"]},
        ]
        r = post_json(c, "xishan.json", xishan, xishan_mapping)
        d2 = r.json()
        check("西山库预览成功(3行)", r.status_code == 200 and d2["row_count"] == 3, r.text[:300])
        check("西山库 h01 未配对，读数跳过",
              all("湿度" not in row["values"] for row in d2["preview_rows"]))
        r = c.post(f"/api/imports/{d2['preview_id']}/commit")
        check("西山库批次入库 201", r.status_code == 201 and r.json()["row_count"] == 3, r.text[:200])
        batch_b = r.json()["batch_id"]
        check("两个源库同号生成不同中心批次号", batch_a != batch_b)

        print("== 7. 观测页可查读数、源库号、源批次号 ==")
        r = c.get("/api/observations", params={"station": "S01"})
        d = r.json()
        check("S01 查询返回 2 行（东海库 T00/T01）", d["row_count"] == 2, r.text[:200])
        row0 = next(x for x in d["rows"]
                    if x["values"]["UTC小时"] == "2026-11-18T00:00")
        check("读数按现役列名展示(气温=5,气压=1013)",
              row0["values"]["气温"] == 5 and row0["values"]["气压"] == 1013)
        check("行带源库号/源批次号",
              row0["source_library"] == "QX-DONGHAI" and row0["source_batch"] == "B-007")
        r = c.get("/api/observations", params={"station": "S03"})
        rows_s03 = r.json()["rows"]
        check("西山库 S03 数据可查(2行)且带西山源信息",
              len(rows_s03) == 2 and rows_s03[0]["source_library"] == "QX-XISHAN"
              and rows_s03[0]["source_batch"] == "B-007")
        r = c.get("/api/batches")
        batches = r.json()["batches"]
        srcs = {b["id"]: (b["source_library"], b["source_batch"]) for b in batches}
        check("批次列表含源信息",
              srcs.get(batch_a) == ("QX-DONGHAI", "B-007")
              and srcs.get(batch_b) == ("QX-XISHAN", "B-007"))

        print("== 8. 含错误的文件：预览报错且不能入库，不产生半成品 ==")
        bad_doc = load_example("import_B-008_with_errors.json")
        r = post_json(c, "bad.json", bad_doc, mapping)
        d = r.json()
        check("预览返回校验错误", r.status_code == 200 and d["valid"] is False
              and d["error_count"] > 0, r.text[:300])
        check("错误覆盖坏小时/坏整数/空站号/未知字段",
              "not-a-time" in json.dumps(d, ensure_ascii=False)
              and "abc" in json.dumps(d, ensure_ascii=False)
              and "站号为空" in json.dumps(d, ensure_ascii=False)
              and "f-unknown" in json.dumps(d, ensure_ascii=False))
        check("有错误时不允许入库", d["committable"] is False)
        r = c.post(f"/api/imports/{d['preview_id']}/commit")
        check("强行确认被 400 拒绝", r.status_code == 400, r.text[:200])
        n_batches = len(c.get("/api/batches").json()["batches"])
        check("拒绝后批次数量不变(无半成品)", n_batches == 2, f"actual={n_batches}")
        check("拒绝后观测总数不变(仍6行)",
              c.get("/api/observations").json()["row_count"] == 6)

        print("== 9. 现役结构换版：旧预览失效，须重新预览 ==")
        new_doc = json.loads(donghai)
        new_doc["source_batch"] = "B-009"
        new_text = json.dumps(new_doc, ensure_ascii=False)
        fresh = post_json(c, "b009.json", new_text, mapping).json()
        pid_old = fresh["preview_id"]
        r = c.post("/api/versions", json={"note": "v2 改名", "columns": [
            {"source": "station", "name": "station_code"},
            {"source": "hour_utc", "name": "UTC小时"},
            {"source": col_id["气温"], "name": "温度"},
            {"source": col_id["气压"], "name": "气压"},
        ]})
        check("发布 v2", r.status_code == 201 and r.json()["version_no"] == 2, r.text[:200])
        r = c.post(f"/api/imports/{pid_old}/commit")
        check("换版后旧预览入库被拒(400,提示重新预览)",
              r.status_code == 400 and "重新预览" in r.text, r.text[:200])
        r = post_json(c, "b009.json", new_text)
        d = r.json()
        check("换版后重新预览钉住 v2",
              r.status_code == 200 and d["version_no"] == 2 and d["mapping_required"] is True)
        col_id2 = {x["name"]: x["column_id"] for x in
                   c.get("/api/versions/current").json()["columns"]}
        mapping2 = [
            {"field_id": "f-temp", "column_id": col_id2["温度"]},
            {"field_id": "f-pres", "column_id": col_id2["气压"]},
        ]
        r = post_json(c, "b009.json", new_text, mapping2)
        d = r.json()
        check("按 v2 重新预览成功", r.status_code == 200 and d["committable"] is True, r.text[:200])
        r = c.post(f"/api/imports/{d['preview_id']}/commit")
        check("重新预览后入库成功(201)", r.status_code == 201 and r.json()["row_count"] == 3,
              r.text[:200])
        r = c.get("/api/observations", params={"station": "S01"})
        check("入库读数按 v2 现役列名展示(温度=5)",
              any(x["values"].get("温度") == 5 for x in r.json()["rows"]), r.text[:200])
        # 同库同号 B-007 已入库 → 即使换版，原样文件仍走幂等，不产生第二批次
        r = post_json(c, "donghai.json", donghai, mapping2)
        check("换版后原样重传仍幂等返回原批次",
              r.status_code == 200 and r.json()["already_imported"] is True
              and r.json()["batch_id"] == batch_a, r.text[:200])

        print("== 10. 文档级结构错误 ==")
        check("非法 JSON 拒绝",
              post_json(c, "x.json", "{not json").status_code == 400)
        check("缺源库号拒绝",
              post_json(c, "x.json", json.dumps(
                  {"source_batch": "B1", "fields": [{"field_id": "a"}],
                   "rows": [{"station": "S", "hour_utc": "2026-11-18T00:00",
                             "readings": {}}]})).status_code == 400)
        check("字段编号重复拒绝",
              post_json(c, "x.json", json.dumps(
                  {"source_library": "L", "source_batch": "B1",
                   "fields": [{"field_id": "a"}, {"field_id": "a"}],
                   "rows": [{"station": "S", "hour_utc": "2026-11-18T00:00",
                             "readings": {}}]})).status_code == 400)
        check("空 rows 拒绝",
              post_json(c, "x.json", json.dumps(
                  {"source_library": "L", "source_batch": "B1",
                   "fields": [{"field_id": "a"}], "rows": []})).status_code == 400)
        check("不存在的预览号 404", c.post("/api/imports/9999/commit").status_code == 404)
        r = c.get("/")
        check("首页含导入区", r.status_code == 200 and "补录批次导入" in r.text)

    print()
    print(f"通过 {len(PASS)} 项，失败 {len(FAIL)} 项")
    if FAIL:
        print("失败项：")
        for n in FAIL:
            print(" -", n)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
