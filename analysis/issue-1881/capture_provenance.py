"""Run with PYTHONPATH pointing to the checkout; never use version alone as proof."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pyarrow

import daft
import daft.daft

p = argparse.ArgumentParser()
p.add_argument("output", type=Path)
args = p.parse_args()
root = Path(__file__).resolve().parents[2]
extension = Path(daft.daft.__file__).resolve()
artifact = root / "target/debug/libdaft.so"
digest = lambda f: hashlib.sha256(f.read_bytes()).hexdigest()
assert Path(daft.__file__).resolve().is_relative_to(root / "daft")
assert extension.is_relative_to(root / "daft")
assert digest(extension) == digest(artifact)
record = {
    "sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
    "python": sys.version,
    "executable": sys.executable,
    "pyarrow": pyarrow.__version__,
    "daft": daft.__version__,
    "package": daft.__file__,
    "extension": str(extension),
    "artifact": str(artifact),
    "sha256": digest(extension),
    "rust": subprocess.check_output(["rustc", "-Vv"], cwd=root, text=True),
    "build_type": daft.get_build_type(),
    "runner": "native",
    "git_status": subprocess.check_output(["git", "status", "--short"], cwd=root, text=True),
    "source_diff": subprocess.check_output(["git", "diff", "--", "src"], cwd=root, text=True),
}
args.output.write_text(json.dumps(record, indent=2))
print(json.dumps({k: record[k] for k in ["sha", "extension", "sha256", "git_status"]}, indent=2))
