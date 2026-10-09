"""Loopback HTTP range server + transparent recorder for real SeaweedFS S3.

Only request method/path/range and response data are logged; no auth headers.
The S3 path forwards every request once, without retries or redirects.
"""

from __future__ import annotations

import argparse
import http.client
import json
import threading
import time
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_POST(self):
        assert self.path == "/__control__"
        values = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with self.server.lock:
            self.server.context.update(values)
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def do_HEAD(self):
        self.respond()

    def do_GET(self):
        self.respond()

    def respond(self):
        start = time.time_ns()
        with self.server.lock:
            context = self.server.context.copy()
            self.server.sequence += 1
            sequence = self.server.sequence
        parsed = urlsplit(self.path)
        backend = (
            "s3-compatible"
            if parsed.path.startswith("/public-bucket-issue1881/") or parsed.path == "/public-bucket-issue1881"
            else "http"
        )
        is_list = backend == "s3-compatible" and ("list-type=" in parsed.query or "delimiter=" in parsed.query)
        kind = "LIST" if is_list else self.command
        response_headers = []
        error = None
        content_range = None
        listing_entries = None
        if backend == "s3-compatible":
            connection = http.client.HTTPConnection("127.0.0.1", self.server.upstream, timeout=30)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in {"host", "connection"}}
            connection.request(self.command, self.path, headers=headers)
            response = connection.getresponse()
            status = response.status
            response_headers = [
                (k, v)
                for k, v in response.getheaders()
                if k.lower() not in {"connection", "transfer-encoding", "content-length"}
            ]
            content_length = response.getheader("Content-Length")
            content_range = response.getheader("Content-Range")
            body = response.read()
            connection.close()
            if is_list and status == 200:
                xml = ET.fromstring(body)
                listing_entries = [
                    {"key": x.findtext("{*}Key"), "physical_size": int(x.findtext("{*}Size"))}
                    for x in xml.findall("{*}Contents")
                ]
        else:
            name = unquote(parsed.path.removeprefix("/http/"))
            path = self.server.data / name
            if not path.is_file() or path.parent != self.server.data:
                status, body = 404, b"not found"
            else:
                data = path.read_bytes()
                status, body = 200, data
                range_header = self.headers.get("Range")
                if range_header:
                    assert range_header.startswith("bytes=") and "," not in range_header
                    first, last = range_header[6:].split("-")
                    if first:
                        low = int(first)
                        high = min(int(last) if last else len(data) - 1, len(data) - 1)
                    else:
                        low, high = max(0, len(data) - int(last)), len(data) - 1
                    if low >= len(data) or high < low:
                        status, body = 416, b""
                        content_range = f"bytes */{len(data)}"
                    else:
                        status, body = 206, data[low : high + 1]
                        content_range = f"bytes {low}-{high}/{len(data)}"
                response_headers = [("Content-Type", "application/octet-stream"), ("Accept-Ranges", "bytes")]
                if content_range:
                    response_headers.append(("Content-Range", content_range))
            content_length = str(len(body))
        self.send_response(status)
        for k, v in response_headers:
            self.send_header(k, v)
        self.send_header(
            "Content-Length", content_length if self.command == "HEAD" and content_length else str(len(body))
        )
        self.end_headers()
        sent = 0
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
                self.wfile.flush()
                sent = len(body)
            except (BrokenPipeError, ConnectionResetError) as e:
                error = type(e).__name__
        entry = dict(
            **context,
            sequence=sequence,
            start_ns=start,
            end_ns=time.time_ns(),
            backend=backend,
            method=self.command,
            kind=kind,
            path=self.path,
            range=self.headers.get("Range"),
            status=status,
            response_body_bytes=len(body) if self.command != "HEAD" else 0,
            emitted_body_bytes=sent,
            content_length=content_length,
            content_range=content_range,
            write_error=error,
            listing_entries=listing_entries,
        )
        with self.server.lock, self.server.log.open("a") as f:
            f.write(json.dumps(entry) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, type=Path)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--upstream", type=int, default=9000)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.data, server.log, server.upstream = args.data.resolve(), args.log, args.upstream
    server.lock, server.sequence = threading.Lock(), 0
    server.context = {"case": "setup", "phase": "setup"}
    print("ready", flush=True)
    server.serve_forever()
