"""One real native-runner scan in a fresh process, with full row validation."""

from __future__ import annotations

import argparse
import functools
import hashlib
import json
import logging
import operator
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

import pyarrow

import daft
import daft.daft
from daft.io import IOConfig, S3Config


def control(case, phase):
    req = Request(
        "http://127.0.0.1:9001/__control__",
        json.dumps({"case": case, "phase": phase}).encode(),
        {"Content-Type": "application/json"},
    )
    with urlopen(req) as response:
        response.read()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--case", required=True)
    p.add_argument("--backend", choices=["http", "s3"], required=True)
    p.add_argument("--shape", choices=["single", "multi", "glob", "largefooter", "direct"], required=True)
    p.add_argument("--disable-suffix", action="store_true")
    p.add_argument("--schema", action="store_true")
    p.add_argument("--data", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("daft_io.stats").setLevel(5)
    daft.refresh_logger()
    root = Path(__file__).resolve().parents[2]
    extension = Path(daft.daft.__file__).resolve()
    assert Path(daft.__file__).resolve().is_relative_to(root / "daft")
    assert extension.is_relative_to(root / "daft")
    assert (
        hashlib.sha256(extension.read_bytes()).digest()
        == hashlib.sha256((root / "target/debug/libdaft.so").read_bytes()).digest()
    )
    daft.set_runner_native()
    config = IOConfig(
        s3=S3Config(
            endpoint_url="http://127.0.0.1:9001", region_name="us-east-1", anonymous=True, use_ssl=False, num_tries=1
        )
    )
    if args.disable_suffix:
        config = IOConfig(s3=config.s3, disable_suffix_range=True)
    base = "s3://public-bucket-issue1881/data/" if args.backend == "s3" else "http://127.0.0.1:9001/http/"
    names = (
        ["large-footer.parquet"]
        if args.shape == "largefooter"
        else ["part-0.parquet", "part-1.parquet"]
        if args.shape in ("multi", "glob")
        else ["part-0.parquet"]
    )
    paths = (
        base + "part-*.parquet"
        if args.shape == "glob"
        else [base + n for n in names]
        if args.shape == "multi"
        else base + names[0]
    )
    manifest = json.loads((args.data / "manifest.json").read_text())
    expected = {
        k: functools.reduce(operator.iadd, [manifest[n]["expected"][k] for n in names], [])
        for k in ("id", "label", "value")
    }
    control(args.case, "construction")
    start = time.perf_counter()
    if args.shape == "direct":
        from daft.recordbatch import MicroPartition

        control(args.case, "reader")
        # A non-empty predicate takes the existing eager MicroPartition path,
        # avoiding its separate pre-read metadata path. All 96 rows match.
        result = MicroPartition.read_parquet(paths, predicate=daft.col("id") >= 0, io_config=config).to_pydict()
        constructed = time.perf_counter()
    else:
        kw = (
            {
                "infer_schema": False,
                "schema": {
                    "id": daft.DataType.int64(),
                    "label": daft.DataType.string(),
                    "value": daft.DataType.float64(),
                },
            }
            if args.schema
            else {}
        )
        df = daft.read_parquet(paths, io_config=config, **kw)
        constructed = time.perf_counter()
        control(args.case, "collection")
        result = df.to_pydict()
    elapsed = time.perf_counter() - start
    actual_rows = sorted(zip(*(result[k] for k in ("id", "label", "value"))))
    expected_rows = sorted(zip(*(expected[k] for k in ("id", "label", "value"))))
    assert actual_rows == expected_rows, (actual_rows, expected_rows)
    control(args.case, "validated")
    out = {
        "case": args.case,
        "backend": args.backend,
        "shape": args.shape,
        "disable_suffix_range": args.disable_suffix,
        "infer_schema": not args.schema,
        "rows": len(actual_rows),
        "full_result_validated": True,
        "result": result,
        "construction_seconds": constructed - start,
        "total_seconds": elapsed,
        "python": sys.version,
        "daft": daft.__version__,
        "pyarrow": pyarrow.__version__,
        "package": daft.__file__,
        "extension": str(extension),
        "extension_sha256": hashlib.sha256(extension.read_bytes()).hexdigest(),
        "runner": "native",
    }
    args.output.write_text(json.dumps(out, indent=2))
    print(json.dumps({k: out[k] for k in ("case", "rows", "full_result_validated", "total_seconds")}))
