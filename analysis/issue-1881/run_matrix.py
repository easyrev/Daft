"""Generate/upload once; repeat each case in a fresh process; preserve raw logs."""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

import boto3
from botocore.config import Config
from generate_inputs import generate

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--label", default="baseline")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--audit", action="store_true", help="Smaller matrix for separately saved logging instrumentation")
    args = p.parse_args()
    here = Path(__file__).resolve().parent
    data = here / "generated"
    generate(data)
    # Test-only fixed anonymous credentials; no real account/secret is used.
    client = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )
    buckets = [b["Name"] for b in client.list_buckets()["Buckets"]]
    if "public-bucket-issue1881" not in buckets:
        client.create_bucket(Bucket="public-bucket-issue1881")
    for file in data.glob("*.parquet"):
        client.put_object(Bucket="public-bucket-issue1881", Key="data/" + file.name, Body=file.read_bytes())
    raw = here / "raw" / args.label
    raw.mkdir(parents=True, exist_ok=False)
    server = subprocess.Popen(
        [sys.executable, str(here / "request_server.py"), "--data", str(data), "--log", str(raw / "requests.jsonl")],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert server.stdout.readline().strip() == "ready"
    env = dict(
        os.environ,
        PYTHONPATH=str(here.parents[1]),
        DAFT_RUNNER="native",
        DAFT_LOG="trace",
        DAFT_ANALYTICS_ENABLED="0",
        DAFT_PROGRESS_BAR="0",
    )
    cases = [
        (backend, shape, disabled, schema)
        for backend, shape, schema in [
            ("http", "single", False),
            ("http", "multi", False),
            ("s3", "single", False),
            ("s3", "glob", False),
            ("s3", "multi", True),
            ("http", "direct", False),
            ("http", "largefooter", False),
        ]
        for disabled in (False, True)
    ]
    if args.audit:
        cases = [
            c
            for c in cases
            if (c[0], c[1]) in [("http", "single"), ("http", "direct"), ("s3", "glob"), ("s3", "multi")]
        ]
    try:
        for backend, shape, disabled, schema in cases:
            for repeat in range(args.repeats):
                case = f"{backend}-{shape}-suffix{'off' if disabled else 'on'}-schema{'given' if schema else 'infer'}-{repeat + 1}"
                cmd = [
                    sys.executable,
                    str(here / "run_case.py"),
                    "--case",
                    case,
                    "--backend",
                    backend,
                    "--shape",
                    shape,
                    "--data",
                    str(data),
                    "--output",
                    str(raw / (case + ".json")),
                ]
                if disabled:
                    cmd.append("--disable-suffix")
                if schema:
                    cmd.append("--schema")
                with (raw / (case + ".log")).open("w") as log:
                    subprocess.run(cmd, cwd=here.parents[1], env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
                print(case, "PASS", flush=True)
    finally:
        server.terminate()
        server.wait(timeout=10)
    entries = [json.loads(line) for line in (raw / "requests.jsonl").read_text().splitlines()]
    rows = []
    for case, phase in sorted({(e["case"], e["phase"]) for e in entries}):
        es = [e for e in entries if (e["case"], e["phase"]) == (case, phase)]
        result = json.loads((raw / (case + ".json")).read_text())
        rows.append(
            {
                "case": case,
                "backend": es[0]["backend"],
                "phase": phase,
                "HEAD": sum(e["kind"] == "HEAD" for e in es),
                "LIST": sum(e["kind"] == "LIST" for e in es),
                "GET": sum(e["kind"] == "GET" for e in es),
                "emitted_body_bytes": sum(e["emitted_body_bytes"] for e in es),
                "response_body_bytes": sum(e["response_body_bytes"] for e in es),
                "status_codes": ",".join(map(str, sorted({e["status"] for e in es}))),
                "write_errors": sum(e["write_error"] is not None for e in es),
                "validated": result["full_result_validated"],
            }
        )
    with (raw / "counts.csv").open("w") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print("saved", raw / "counts.csv")
