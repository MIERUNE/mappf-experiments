"""Deterministic 304 upstream for the wedge regression.

Sprites carry `s-maxage=10` + ETag and honor If-None-Match, so a style swap
after ~10s forces the exact incident path: stale entry -> native withholds the
body -> conditional GET -> 304. Every request is logged with its INM header and
status; this log is the witness that the 304 path was exercised.
"""
import base64, json, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)
SPRITE_TTL = "public, max-age=5, s-maxage=10"
ETAG = '"sprite-v1"'

def style(name):
    return json.dumps({
        "version": 8, "name": name,
        "sprite": "http://mock304:8080/sprite",
        "sources": {},
        "layers": [{"id": "bg", "type": "background",
                    "paint": {"background-color": "#dde"}}],
    }).encode()

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def _send(self, code, body=b"", ctype="application/json", cache=None, etag=None):
        self.send_response(code)
        if etag: self.send_header("ETag", etag)
        if cache: self.send_header("Cache-Control", cache)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body: self.wfile.write(body)
    def do_GET(self):
        inm = self.headers.get("If-None-Match", "-")
        path = self.path
        if path.startswith("/styles/") and path.endswith("style.json"):
            name = path.split("/")[-2]
            self._send(200, style(name), cache="public, max-age=3600")
            code = 200
        elif path.startswith("/sprite"):
            if inm == ETAG:
                self._send(304, cache=SPRITE_TTL, etag=ETAG)
                code = 304
            elif path.endswith(".json"):
                self._send(200, b"{}", cache=SPRITE_TTL, etag=ETAG)
                code = 200
            elif path.endswith(".png"):
                self._send(200, PNG_1X1, "image/png", SPRITE_TTL, ETAG)
                code = 200
            else:
                self._send(404); code = 404
        else:
            self._send(404); code = 404
        print(f"{path} inm={inm} -> {code}", flush=True)

ThreadingHTTPServer(("0.0.0.0", 8080), H).serve_forever()
