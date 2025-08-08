#!/usr/bin/env python3
import argparse, os, sys, subprocess
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
VENV = os.path.join(ROOT, ".venv")
VENV_PY = os.path.join(VENV, "Scripts", "python.exe") if os.name == "nt" else os.path.join(VENV, "bin", "python")
def run(cmd, check=True):
    print(">", " ".join(cmd)); return subprocess.run(cmd, check=check)
def ensure_venv():
    if not os.path.exists(VENV):
        print("[setup] creating venv at", VENV)
        run([sys.executable, "-m", "venv", VENV])
    print("[setup] upgrading pip & installing requirements")
    pip = os.path.join(os.path.dirname(VENV_PY), "pip.exe") if os.name == "nt" else os.path.join(os.path.dirname(VENV_PY), "pip")
    run([VENV_PY, "-m", "pip", "install", "--upgrade", "pip"])
    run([VENV_PY, "-m", "pip", "install", "-r", os.path.join(ROOT, "requirements.txt")])
def py(args):
    pyexe = VENV_PY if os.path.exists(VENV_PY) else sys.executable
    return run([pyexe] + args)
def main():
    ap = argparse.ArgumentParser(prog="bloblake")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup", help="create venv and install requirements")
    p_gen = sub.add_parser("gen", help="generate zkRollup-like Blob")
    p_gen.add_argument("--rows", type=int, default=1_000_000); p_gen.add_argument("--parts", type=int, default=1)
    p_gen.add_argument("--out", default="lake/blob")
    p_arrow = sub.add_parser("to-arrow", help="convert Blob -> Arrow")
    p_arrow.add_argument("--blob", required=True); p_arrow.add_argument("--out", required=True)
    p_sqlite = sub.add_parser("to-sqlite", help="load Blob -> SQLite")
    p_sqlite.add_argument("--blob", required=True); p_sqlite.add_argument("--db", default="lake/sqlite.db")
    p_sqlite.add_argument("--reset", action="store_true")
    p_q1 = sub.add_parser("q-arrow", help="query Arrow with DataFusion or DuckDB")
    p_q1.add_argument("--sql", required=True); p_q1.add_argument("--arrow", default="./lake/hot")
    p_q1.add_argument("--duckdb", action="store_true")
    p_q2 = sub.add_parser("q-sqlite", help="query SQLite")
    p_q2.add_argument("--sql", required=True); p_q2.add_argument("--db", default="lake/sqlite.db")
    p_bench = sub.add_parser("bench", help="end-to-end bench")
    p_bench.add_argument("--rows", type=int, default=1_000_000)
    p_bench.add_argument("--sql", default='SELECT COUNT(*) FROM state WHERE position < 100000')
    args = ap.parse_args()
    if args.cmd == "setup":
        ensure_venv(); print("[setup] done.")
        if os.name == "nt":
            print(r"Activate later:  .venv\Scripts\activate")
            print(r"PowerShell: .\.venv\Scripts\Activate.ps1  (may need Set-ExecutionPolicy -Scope Process Bypass)")
        else:
            print("Activate later:  source .venv/bin/activate")
        return
    if args.cmd == "gen":
        py([os.path.join("scripts", "gen_blob.py"), "--rows", str(args.rows), "--parts", str(args.parts), "--out", args.out])
    elif args.cmd == "to-arrow":
        py([os.path.join("scripts", "blob_to_arrow.py"), "--blob", args.blob, "--out", args.out])
    elif args.cmd == "to-sqlite":
        cmd=[os.path.join("scripts","blob_to_sqlite.py"), "--blob", args.blob, "--db", args.db]
        if args.reset: cmd.append("--reset")
        py(cmd)
    elif args.cmd == "q-arrow":
        cmd=[os.path.join("scripts","query_arrow.py"), "--sql", args.sql, "--arrow", args.arrow]
        if args.duckdb: cmd.append("--duckdb")
        py(cmd)
    elif args.cmd == "q-sqlite":
        py([os.path.join("scripts", "query_arrow.py"), "--sql", args.sql, "--sqlite", args.db])
    elif args.cmd == "bench":
        py([os.path.join("scripts", "bench_e2e.py"), "--rows", str(args.rows), "--sql", args.sql])
    else:
        ap.print_help()
if __name__ == "__main__": main()
