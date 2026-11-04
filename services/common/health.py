"""Minimal HTTP server for /healthz, /readyz and /metrics in non-API services.

Runs in a daemon thread so it never blocks the service's own loop. `/healthz` answers 200 as long
as the process is alive; `/readyz` calls `readiness_fn()` which returns `(ok, detail)` and maps to
200 / 503 with the detail as JSON.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

from services.common import metrics

ReadinessFn = Callable[[], tuple[bool, dict]]


def _handler(readiness_fn: ReadinessFn):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):  # keep stdout for JSON logs only
            pass

        def _send(self, code: int, body: bytes, ctype: str = "application/json") -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path == "/healthz":
                self._send(200, b'{"status":"ok"}')
            elif path == "/readyz":
                try:
                    ok, detail = readiness_fn()
                except Exception as e:  # a broken readiness check is itself "not ready"
                    ok, detail = False, {"error": repr(e)}
                self._send(200 if ok else 503, json.dumps({"ready": ok, **detail}, default=str).encode())
            elif path == "/metrics":
                self._send(200, metrics.render(), "text/plain; version=0.0.4; charset=utf-8")
            else:
                self._send(404, b'{"error":"not_found"}')

    return Handler


class HealthServer:
    def __init__(self, port: int, readiness_fn: ReadinessFn, host: str = "0.0.0.0"):
        self._server = ThreadingHTTPServer((host, port), _handler(readiness_fn))
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, name="health", daemon=True)

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    def start(self) -> "HealthServer":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def serve_health(port: int, readiness_fn: ReadinessFn) -> HealthServer:
    return HealthServer(port, readiness_fn).start()
