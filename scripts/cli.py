#!/usr/bin/env python3
import argparse, os, sys, subprocess

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
VENV = os.path.join(ROOT, ".venv")
VENV_PY = os.path.join(VENV, "Scripts", "python.exe") if os.name == "nt" else os.path.join(VENV, "bin", "python")

def run(cmd, check=True):
    print(">", " ".join(cmd))
    return subprocess.run(cmd, check=check)

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
    p_gen.add_argument("--rows", type=int, default=1_000_000)
    p_gen.add_argument("--parts", type=int, default=1)
    p_gen.add_argument("--out", default="lake/blob")

    p_arrow = sub.add_parser("to-arrow", help="convert Blob -> Arrow")
    p_arrow.add_argument("--blob", required=True)
    p_arrow.add_argument("--out", required=True)
    p_arrow.add_argument("--chunk", type=int, default=200_000)

    p_tapar = sub.add_parser("to-arrow-par", help="parallel convert many Blobs -> Arrow")
    p_tapar.add_argument("--pattern", default="lake/blob_*.blob.gz")
    p_tapar.add_argument("--outdir", default="lake/hot")
    p_tapar.add_argument("--workers", type=int, default=4)
    p_tapar.add_argument("--chunk", type=int, default=200_000)
    p_tapar.add_argument("--skip-exists", action="store_true")

    p_sqlite = sub.add_parser("to-sqlite", help="load Blob -> SQLite")
    p_sqlite.add_argument("--blob", required=True)
    p_sqlite.add_argument("--db", default="lake/sqlite.db")
    p_sqlite.add_argument("--reset", action="store_true")
    p_sqlite.add_argument("--mode", choices=["append","upsert"], default="append")

    p_q1 = sub.add_parser("q-arrow", help="query Arrow with DuckDB")
    p_q1.add_argument("--sql", required=True)
    p_q1.add_argument("--arrow", default="./lake/hot")
    p_q1.add_argument("--engine", choices=["auto","ext","bridge"], default="bridge")

    p_q2 = sub.add_parser("q-sqlite", help="query SQLite")
    p_q2.add_argument("--sql", required=True)
    p_q2.add_argument("--db", default="lake/sqlite.db")

    p_benchupd = sub.add_parser("bench-upd", help="parallel bench with updates (control update ratio and hotset)")
    p_benchupd.add_argument("--rows", type=int, default=500000)
    p_benchupd.add_argument("--parts", type=int, default=4)
    p_benchupd.add_argument("--update-ratio", type=float, default=0.5)
    p_benchupd.add_argument("--hot-frac", type=float, default=0.05)
    p_benchupd.add_argument("--hot-amp", type=float, default=20.0)
    p_benchupd.add_argument("--workers", type=int, default=4)
    p_benchupd.add_argument("--chunk", type=int, default=200000)
    p_benchupd.add_argument("--engine", choices=["auto","ext","bridge"], default="bridge")
    p_benchupd.add_argument("--out", help="write metrics JSON")

    p_sweep = sub.add_parser("bench-upd-sweep", help="run update-ratio sweep and write CSV")
    p_sweep.add_argument("--ratios", default="0.1,0.25,0.5")
    p_sweep.add_argument("--rows", type=int, default=500000)
    p_sweep.add_argument("--parts", type=int, default=4)
    p_sweep.add_argument("--workers", type=int, default=4)
    p_sweep.add_argument("--chunk", type=int, default=200000)
    p_sweep.add_argument("--engine", choices=["auto","ext","bridge"], default="bridge")
    p_sweep.add_argument("--sqlite-mode", choices=["append","upsert"], default="append")
    p_sweep.add_argument("--out-csv", default="bench_upd_sweep.csv")

    args = ap.parse_args()

    if args.cmd == "setup":
        ensure_venv()
        print("[setup] done."); 
        if os.name == "nt":
            print(r'Activate later:  CMD:   .venv\Scripts\activate'); 
            print(r'                 PowerShell: .\.venv\Scripts\Activate.ps1 (may need: Set-ExecutionPolicy -Scope Process Bypass)')
        else:
            print('Activate later:  source .venv/bin/activate')
        return

    if args.cmd == "gen":
        py([os.path.join("scripts", "gen_blob.py"),
            "--rows", str(args.rows), "--parts", str(args.parts), "--out", args.out])
    elif args.cmd == "to-arrow":
        py([os.path.join("scripts","blob_to_arrow.py"),
            "--blob", args.blob, "--out", args.out, "--chunk", str(args.chunk)])
    elif args.cmd == "to-arrow-par":
        cmd=[os.path.join("scripts","blob_to_arrow_many.py"),
             "--pattern", args.pattern, "--outdir", args.outdir, "--workers", str(args.workers),
             "--chunk", str(args.chunk)]
        if args.skip_exists: cmd.append("--skip-exists")
        py(cmd)
    elif args.cmd == "to-sqlite":
        cmd=[os.path.join("scripts", "blob_to_sqlite.py"),
             "--blob", args.blob, "--db", args.db, "--mode", args.mode]
        if args.reset: cmd.append("--reset")
        py(cmd)
    elif args.cmd == "q-arrow":
        py([os.path.join("scripts","query_arrow.py"),
            "--sql", args.sql, "--arrow", args.arrow, "--engine", args.engine])
    elif args.cmd == "q-sqlite":
        py([os.path.join("scripts","query_arrow.py"),
            "--sql", args.sql, "--sqlite", args.db])
    elif args.cmd == "bench-upd":
        py([os.path.join("scripts","bench_updates_par.py"),
            "--rows", str(args.rows), "--parts", str(args.parts),
            "--update-ratio", str(args.update_ratio),
            "--hot-frac", str(args.hot_frac), "--hot-amp", str(args.hot_amp),
            "--workers", str(args.workers), "--chunk", str(args.chunk),
            "--engine", args.engine] + (["--out","upd_metrics.json"] if args.out is None else ["--out", args.out]))
    elif args.cmd == "bench-upd-sweep":
        py([os.path.join("scripts","bench_upd_sweep.py"),
            "--ratios", args.ratios, "--rows", str(args.rows), "--parts", str(args.parts),
            "--workers", str(args.workers), "--chunk", str(args.chunk),
            "--engine", args.engine, "--sqlite-mode", args.sqlite_mode, "--out-csv", args.out_csv])
    else:
        ap.print_help()

if __name__ == "__main__":
    main()
