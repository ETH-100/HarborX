#!/usr/bin/env python3
# Minimal static file server with proper MIME types for HarborX web demo
import http.server, socketserver, os, sys, mimetypes

class Handler(http.server.SimpleHTTPRequestHandler):
    extensions_map = {
        **mimetypes.types_map,
        ".js":   "application/javascript",
        ".mjs":  "application/javascript",
        ".json": "application/json",
        ".map":  "application/json",
        ".css":  "text/css",
        ".wasm": "application/wasm",
        ".arrow": "application/vnd.apache.arrow.file",
        ".ipc":   "application/vnd.apache.arrow.stream",
        ".feather":"application/vnd.apache.arrow.stream",
        ".parquet":"application/x-parquet",
        "": "application/octet-stream",
    }
    def log_message(self, fmt, *args):
        sys.stdout.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

def main():
    webroot = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8080
    os.chdir(webroot)
    with socketserver.ThreadingTCPServer(("", port), Handler) as httpd:
        print(f"[serve] http://127.0.0.1:{port} (root={webroot})")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n[serve] stopped")

if __name__ == "__main__":
    main()
