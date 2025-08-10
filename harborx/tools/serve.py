#!/usr/bin/env python3
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import mimetypes, os, sys

WEBROOT = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8080

mimetypes.add_type('application/javascript', '.js')
mimetypes.add_type('application/wasm', '.wasm')
mimetypes.add_type('application/json', '.json')

class Handler(SimpleHTTPRequestHandler):
    def translate_path(self, path):
        rel = path.lstrip("/")
        return os.path.join(WEBROOT, rel)

if __name__ == "__main__":
    print(f"Serving {WEBROOT} on http://127.0.0.1:{PORT}")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
