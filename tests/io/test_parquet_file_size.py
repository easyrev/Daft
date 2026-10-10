from __future__ import annotations

import io
import multiprocessing
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft.exceptions import DaftCoreException
from daft.io import IOConfig
from daft.recordbatch import MicroPartition
from tests.conftest import get_tests_daft_runner_name


def _serve_parquet(files, requests, failures, ready):
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.respond()

        def do_GET(self):
            self.respond()

        def respond(self):
            name = unquote(urlsplit(self.path).path).lstrip("/")
            data = files.get(name)
            status = failures.get((self.command, name), 200 if data is not None else 404)
            data = data or b""
            start, end = 0, len(data)
            header = self.headers.get("Range")
            if status == 200 and header:
                first, last = header.removeprefix("bytes=").split("-")
                if first:
                    start, end = int(first), min(int(last) + 1 if last else len(data), len(data))
                else:
                    start = max(0, len(data) - int(last))
                status = 206 if start < end else 416
            body = data[start:end] if status in (200, 206) else b""
            with lock:
                requests.append((self.command, name, header, status, len(body)))
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Accept-Ranges", "bytes")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end - 1}/{len(data)}")
            self.end_headers()
            if self.command == "GET":
                self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    ready.send(server.server_port)
    try:
        ready.recv()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


@pytest.fixture
def parquet_server():
    """Record real HTTP requests in a process independent of the planning GIL."""
    context = multiprocessing.get_context("spawn")
    with context.Manager() as manager:
        files, requests, failures = manager.dict(), manager.list(), manager.dict()
        parent, child = context.Pipe()
        process = context.Process(target=_serve_parquet, args=(files, requests, failures, child))
        process.start()
        try:
            assert parent.poll(15), "HTTP server did not start"
            port = parent.recv()
            yield f"http://127.0.0.1:{port}", files, requests, failures
        finally:
            parent.send("stop")
            process.join(timeout=10)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            parent.close()
            child.close()
            assert process.exitcode == 0


def parquet_bytes(offset=0, large_footer=False):
    # Unequal row groups and files catch accidental use of task or row-group sizes.
    table = pa.table(
        {
            "id": list(range(offset, offset + 11)),
            "label": [None if i % 3 == 0 else "x" * (i + offset + 1) for i in range(11)],
            "value": [None if i % 4 == 0 else i * 0.5 for i in range(11)],
        }
    )
    if large_footer:
        table = table.replace_schema_metadata({b"padding": b"x" * 150_000})
    output = io.BytesIO()
    with pq.ParquetWriter(output, table.schema) as writer:
        writer.write_table(table.slice(0, 2))
        writer.write_table(table.slice(2, 6))
        writer.write_table(table.slice(8))
    return output.getvalue(), table.replace_schema_metadata(None)


def counts(requests):
    return Counter(r[0] for r in requests)


@pytest.mark.parametrize("disable_suffix", [False, True])
def test_discovered_file_size_avoids_reader_head(parquet_server, disable_suffix):
    base, files, requests, _ = parquet_server
    files["data.parquet"], expected = parquet_bytes()
    df = daft.read_parquet(f"{base}/data.parquet", io_config=IOConfig(disable_suffix_range=disable_suffix))
    # Construction: one discovery HEAD and one schema footer GET.
    assert counts(requests) == {"HEAD": 1, "GET": 1}
    requests[:] = []
    assert df.to_pydict() == expected.to_pydict()
    # Collection: one discovery HEAD, one footer GET, three row-group GETs.
    assert counts(requests) == {"HEAD": 1, "GET": 4}
    assert all(r[3] in (200, 206) for r in requests)


@pytest.mark.parametrize("disable_suffix", [False, True])
def test_unknown_file_size_queries_once(parquet_server, disable_suffix):
    base, files, requests, _ = parquet_server
    files["data.parquet"], expected = parquet_bytes()
    # A predicate selects the existing eager path, without discovery or schema inference.
    result = MicroPartition.read_parquet(
        f"{base}/data.parquet", predicate=daft.col("id") >= 0, io_config=IOConfig(disable_suffix_range=disable_suffix)
    )
    assert result.to_pydict() == expected.to_pydict()
    assert counts(requests) == {"HEAD": 1, "GET": 4}


def test_known_size_large_footer(parquet_server):
    base, files, requests, _ = parquet_server
    files["large.parquet"], expected = parquet_bytes(large_footer=True)
    assert pq.ParquetFile(io.BytesIO(files["large.parquet"])).metadata.serialized_size > 128 * 1024
    df = daft.read_parquet(f"{base}/large.parquet")
    assert counts(requests) == {"HEAD": 1, "GET": 2}
    requests[:] = []
    assert df.to_pydict() == expected.to_pydict()
    assert counts(requests) == {"HEAD": 1, "GET": 5}
    assert all(not r[2].startswith("bytes=-") for r in requests if r[0] == "GET")


def test_remote_projection_filter_limit(parquet_server):
    base, files, _, _ = parquet_server
    files["data.parquet"], expected = parquet_bytes()
    result = daft.read_parquet(f"{base}/data.parquet").where(daft.col("id") >= 3).select("id", "label").limit(5)
    assert result.to_pydict() == expected.slice(3, 5).select(["id", "label"]).to_pydict()


@pytest.mark.parametrize("data", [b"", b"PAR1", b"not a parquet file"])
def test_known_size_invalid_file(parquet_server, data):
    base, files, requests, _ = parquet_server
    files["bad.parquet"] = data
    with pytest.raises(DaftCoreException, match="CorruptFile"):
        daft.read_parquet(f"{base}/bad.parquet")
    assert counts(requests)["HEAD"] == 1
    if len(data) < 12:
        assert counts(requests)["GET"] == 0


@pytest.mark.parametrize("method", ["HEAD", "GET"])
def test_io_errors_are_not_ignored(parquet_server, method):
    base, files, requests, failures = parquet_server
    files["data.parquet"], _ = parquet_bytes()
    url = f"{base}/data.parquet"
    failures[method, "data.parquet"] = 403
    with pytest.raises(DaftCoreException, match="403") as error:
        daft.read_parquet(url, ignore_corrupt_files=True).to_pydict()
    assert url in str(error.value)
    assert any(r[0] == method and r[1] == "data.parquet" and r[3] == 403 for r in requests)


@pytest.mark.parametrize("disable_suffix", [False, True])
def test_known_size_execution_get_error_is_not_ignored(parquet_server, disable_suffix):
    base, files, requests, failures = parquet_server
    files["data.parquet"], _ = parquet_bytes()
    url = f"{base}/data.parquet"
    # Avoid planning-time footer reads for task splitting, so the failing GET
    # below comes from the execution reader after successful schema inference.
    with daft.execution_config_ctx(enable_scan_task_split_and_merge=False):
        df = daft.read_parquet(url, ignore_corrupt_files=True, io_config=IOConfig(disable_suffix_range=disable_suffix))
        assert counts(requests) == {"HEAD": 1, "GET": 1}
        assert all(r[3] in (200, 206) for r in requests)
        requests[:] = []
        failures["GET", "data.parquet"] = 403
        with pytest.raises(DaftCoreException, match="403") as error:
            df.to_pydict()
    assert url in str(error.value)
    # Discovery succeeds; the known-size reader issues only its failing footer GET.
    assert counts(requests) == {"HEAD": 1, "GET": 1}
    assert all(r[1] == "data.parquet" and r[3] == (200 if r[0] == "HEAD" else 403) for r in requests)


def test_missing_object(parquet_server):
    base, _, _, _ = parquet_server
    with pytest.raises(FileNotFoundError, match="404") as error:
        MicroPartition.read_parquet(f"{base}/missing.parquet", predicate=daft.col("id") >= 0)
    assert f"{base}/missing.parquet" in str(error.value)


@pytest.mark.parametrize("disable_suffix,method", [(False, "HEAD"), (True, "HEAD"), (True, "GET")])
def test_unknown_size_io_failure(parquet_server, disable_suffix, method):
    base, files, requests, failures = parquet_server
    files["data.parquet"], _ = parquet_bytes()
    url = f"{base}/data.parquet"
    failures[method, "data.parquet"] = 403
    with pytest.raises(DaftCoreException, match="403") as error:
        MicroPartition.read_parquet(
            url,
            predicate=daft.col("id") >= 0,
            io_config=IOConfig(disable_suffix_range=disable_suffix),
        )
    assert url in str(error.value)
    assert any(r[0] == method and r[1] == "data.parquet" and r[3] == 403 for r in requests)


@pytest.mark.parametrize("split", [True, False], ids=["split", "merge"])
def test_remote_task_transforms(parquet_server, split):
    base, files, requests, _ = parquet_server
    files["a.parquet"], a = parquet_bytes()
    files["b.parquet"], b = parquet_bytes(100)
    assert len(files["a.parquet"]) != len(files["b.parquet"])
    with daft.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1 if split else 1_000_000,
        scan_tasks_max_size_bytes=1 if split else 10_000_000,
    ):
        df = daft.read_parquet([f"{base}/a.parquet", f"{base}/b.parquet"])
        if get_tests_daft_runner_name() == "ray":
            assert df.num_partitions() == (6 if split else 1)
        requests[:] = []
        result = df.to_pydict()
    expected = pa.concat_tables([a, b]).to_pydict()
    assert sorted(zip(*(result[k] for k in expected))) == sorted(zip(*(expected[k] for k in expected)))
    # Only the two discovery HEADs remain, including after Ray serializes the
    # split or merged tasks. Planning may read footers more than once.
    assert counts(requests)["HEAD"] == 2
    assert all(r[3] in (200, 206) for r in requests)
