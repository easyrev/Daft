"""Deterministic small Parquet inputs; PyArrow is only the writer/oracle."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def generate(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    manifest = {}
    for name, offset, padding in [
        ("part-0.parquet", 0, 0),
        ("part-1.parquet", 96, 0),
        ("large-footer.parquet", 0, 150000),
    ]:
        table = pa.table(
            {
                "id": list(range(offset, offset + 96)),
                "label": [None if i % 7 == 0 else f"label-{i % 11}" for i in range(offset, offset + 96)],
                "value": [None if i % 13 == 0 else i / 4 for i in range(offset, offset + 96)],
            }
        )
        if padding:
            table = table.replace_schema_metadata({b"padding": b"x" * padding})
        path = root / name
        pq.write_table(table, path, row_group_size=32, compression="snappy")
        data = path.read_bytes()
        oracle = pq.read_table(path).to_pydict()
        assert oracle == table.to_pydict()
        manifest[name] = {
            "bytes": len(data),
            "footer_bytes": int.from_bytes(data[-8:-4], "little") + 8,
            "sha256": hashlib.sha256(data).hexdigest(),
            "rows": 96,
            "row_groups": 3,
            "expected": oracle,
        }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(
        json.dumps(
            {k: {x: v[x] for x in ("bytes", "footer_bytes", "sha256")} for k, v in generate(args.directory).items()},
            indent=2,
        )
    )
