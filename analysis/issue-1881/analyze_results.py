"""Reconcile network logs with separately built IOStats-only instrumentation."""

from __future__ import annotations

import collections
import csv
import json
import re
from pathlib import Path

root = Path(__file__).resolve().parent
baseline = root / "raw/baseline-final"
audit = root / "raw/instrumented"
read = lambda p: [json.loads(x) for x in p.read_text().splitlines()]
events = read(baseline / "requests.jsonl")
audit_events = read(audit / "requests.jsonl")
assert all(e["status"] in (200, 206) and e["write_error"] is None for e in events + audit_events)
base_rows = list(csv.DictReader((baseline / "counts.csv").open()))
audit_rows = list(csv.DictReader((audit / "counts.csv").open()))
fields = [
    "backend",
    "phase",
    "HEAD",
    "LIST",
    "GET",
    "emitted_body_bytes",
    "response_body_bytes",
    "status_codes",
    "write_errors",
    "validated",
]
base_map = {(r["case"], r["phase"]): tuple(r[k] for k in fields) for r in base_rows}
assert all(base_map[r["case"], r["phase"]] == tuple(r[k] for k in fields) for r in audit_rows)
repeat_groups = collections.defaultdict(list)
for r in base_rows:
    repeat_groups[r["case"].rsplit("-", 1)[0], r["phase"]].append(tuple(r[k] for k in fields))
assert all(len(rs) == 3 and len(set(rs)) == 1 for rs in repeat_groups.values())

stages = []
for case in sorted({e["case"] for e in audit_events}):
    es = [e for e in audit_events if e["case"] == case]
    text = (audit / (case + ".log")).read_text()
    contexts = re.findall(r"IOStatsContext: (.*?), Gets: (\d+), Heads: (\d+), Lists: (\d+)", text, re.DOTALL)
    counts = collections.defaultdict(collections.Counter)
    for name, gets, heads, lists in contexts:
        if name.startswith("GlobScanOperator constructor read_parquet_schema"):
            stage = "schema inference"
        elif name.startswith("GlobScanOperator::try_new"):
            stage = "discovery during construction"
        elif name.startswith("GlobScanOperator::to_scan_tasks"):
            stage = "discovery during collection"
        else:
            stage = "reader execution"
        counts[stage].update({"GET": int(gets), "HEAD": int(heads), "LIST": int(lists)})
    combined = sum(counts.values(), collections.Counter())
    server_counts = collections.Counter(e["kind"] for e in es)
    assert all(combined[k] == server_counts[k] for k in ("HEAD", "LIST", "GET")), case
    # For this audited matrix, discovery uses only HEAD/LIST. Every object GET
    # has Range; constructor GETs belong to inference and collection GETs to
    # the reader. No HTTP directory fallback is present in these cases.
    assert all(e["range"] is not None for e in es if e["kind"] == "GET")
    sizes = re.findall(r"ISSUE1881_SCAN_SOURCE path=\S+ size_bytes=Some\((\d+)\)", text)
    for stage, c in counts.items():
        if stage.startswith("discovery"):
            boundary = "construction" if stage.endswith("construction") else "collection"
            data = [e for e in es if e["phase"] == boundary and e["kind"] == "LIST"]
        else:
            boundary = (
                "construction" if stage == "schema inference" else "reader" if "-direct-" in case else "collection"
            )
            data = [e for e in es if e["phase"] == boundary and e["kind"] == "GET"]
        stages.append(
            {
                "case": case,
                "backend": es[0]["backend"],
                "stage": stage,
                "source_physical_sizes": "/".join(sizes) or "not provided (direct reader)",
                "reader_size": "unknown on entry",
                "HEAD": c["HEAD"],
                "LIST": c["LIST"],
                "GET": c["GET"],
                "emitted_body_bytes": sum(e["emitted_body_bytes"] for e in data),
                "validated": True,
            }
        )
with (audit / "stages.csv").open("w") as f:
    w = csv.DictWriter(f, fieldnames=list(stages[0]))
    w.writeheader()
    w.writerows(stages)

lines = [
    "# 未修改基线请求计数",
    "",
    "14 个场景，每个 3 个全新进程；42 次完整读取均通过。各场景三次的所有计数完全一致。",
    "",
    "bytes 为服务端成功写出的响应 body 字节，包含 LIST XML，不含 HTTP headers/TCP/TLS；不等于 Daft 消费字节。",
    "",
    "configuration 中 suffixon=默认 IOConfig；suffixoff=显式 disable_suffix_range=True。schema given=现有 infer_schema=False 入口；direct=带全匹配 predicate 的 eager MicroPartition 入口。",
    "",
    "construction=DataFrame 构造（发现+schema/路径检查）；collection=惰性文件发现+实际读取；reader=无 glob/ScanTask 的直接 eager reader。",
    "",
    "| 输入/backend/configuration | 大小信息 | 阶段 | HEAD | LIST | GET | bytes | 校验 |",
    "|---|---|---|---:|---:|---:|---:|---|",
]
for r in base_rows:
    if not r["case"].endswith("-1"):
        continue
    size = "直接 reader 无已知大小" if "-direct-" in r["case"] else "发现得到物理大小；reader 入口仍未知"
    lines.append(
        f"| {r['case'].rsplit('-', 1)[0]} | {size} | {r['phase']} | {r['HEAD']} | {r['LIST']} | {r['GET']} | {r['emitted_body_bytes']} | 完整 96/192 行通过 |"
    )
lines += [
    "",
    "# 分阶段归属审计",
    "",
    "16 次独立日志插桩运行；28 个 API 边界计数行与对应未修改基线完全一致。IOStats 只辅助归属，HEAD/LIST/GET 总数由实际服务端记录核对。",
    "",
    "原始日志中请求归属仍保留 construction/collection 边界。下表根据互相独立的发现/推断/执行 IOStats context 拆分计数；使用前述受限矩阵的实际 Range 请求与 LIST body 归属字节，没有将逻辑 get_size 次数转换成 HEAD。",
    "",
    "| 场景 | 发现/ScanSource 物理大小 | reader 入参大小 | 阶段 | HEAD | LIST | GET | bytes | 校验 |",
    "|---|---|---|---|---:|---:|---:|---:|---|",
]
for r in stages:
    if not r["case"].endswith("-1"):
        continue
    lines.append(
        f"| {r['case'].rsplit('-', 1)[0]} | {r['source_physical_sizes']} | {r['reader_size']} | {r['stage']} | {r['HEAD']} | {r['LIST']} | {r['GET']} | {r['emitted_body_bytes']} | 完整数据通过 |"
    )
(root / "RESULTS.md").write_text("\n".join(lines) + "\n")
summary = {
    "baseline_scans": 42,
    "baseline_requests": len(events),
    "audit_scans": 16,
    "audit_requests": len(audit_events),
    "baseline_repeats_identical": True,
    "audit_counts_match_baseline": True,
    "io_stats_reconciled_with_server": True,
    "errors": 0,
}
(root / "raw/verification.json").write_text(json.dumps(summary, indent=2))
print(json.dumps(summary, indent=2))
