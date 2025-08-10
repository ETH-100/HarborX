#!/usr/bin/env python3
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import mimetypes, os

mimetypes.add_type('application/javascript', '.js')
mimetypes.add_type('application/javascript', '.mjs')
mimetypes.add_type('application/wasm', '.wasm')
mimetypes.add_type('application/json', '.json')
mimetypes.add_type('text/css', '.css')

WEBROOT = os.path.dirname(__file__)
os.chdir(WEBROOT)

class Handler(SimpleHTTPRequestHandler):
    pass

if __name__ == "__main__":
    print("Serving", os.getcwd(), "on http://127.0.0.1:8080")
    ThreadingHTTPServer(("127.0.0.1", 8080), Handler).serve_forever()
