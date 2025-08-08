#!/usr/bin/env python3
import subprocess, sys, time, os

def run(cmd):
    t0=time.time()
    p = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return time.time()-t0, p.stdout

# Arrow MOR
SQL_ARROW_MOR = (
"WITH latest AS ( "
" SELECT address, key, value, timestamp, position, "
"        ROW_NUMBER() OVER (PARTITION BY address, key ORDER BY timestamp DESC, position DESC) AS rn "
" FROM state ) "
"SELECT COUNT(*) FROM latest WHERE rn=1;"
)

# SQLite MOR
SQL_SQLITE_MOR = (
"SELECT COUNT(*) FROM state s "
"WHERE NOT EXISTS ( "
"  SELECT 1 FROM state s2 "
"  WHERE s2.address=s.address AND s2.key=s.key "
"    AND (s2.timestamp>s.timestamp OR "
"         (s2.timestamp=s.timestamp AND s2.position>s.position)) "
");"
)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--ratios", default="0,0.25,0.5,0.75,1.0",
                    help="comma-separated update ratios")
    ap.add_argument("--mode", choices=["append","upsert"], default="upsert",
                    help="SQLite ingest mode")
    ap.add_argument("--keys", type=int, help="optional fixed unique key count")
    args = ap.parse_args()

    ratios = [float(x) for x in args.ratios.split(",")]

    print("rows\tmode\tratio\tgen(s)\tblob->arrow(s)\tq_arrow_MOR(s)\tblob->sqlite(s)\tq_sqlite(s)")
    for r in ratios:
        # 1) gen blob with update ratio
        gen_cmd = [sys.executable, "scripts/gen_blob.py",
                   "--rows", str(args.rows), "--parts", "1", "--out", "lake/blob",
                   "--update-ratio", str(r)]
        if args.keys: gen_cmd += ["--keys", str(args.keys)]
        dt,_ = run(gen_cmd); t_gen = dt

        # 2) blob -> arrow
        dt,_ = run([sys.executable, "scripts/blob_to_arrow.py",
                    "--blob", "lake/blob_000001.blob.gz",
                    "--out",  "lake/hot/blob_000001.arrow"])
        t_b2a = dt

        # 3) query Arrow (DuckDB) with MOR
        dt,_ = run([sys.executable, "scripts/query_arrow.py",
                    "--arrow", "lake/hot", "--duckdb", "--sql", SQL_ARROW_MOR])
        t_qA = dt

        # 4) blob -> sqlite (reset)
        if os.path.exists("lake/sqlite.db"): os.remove("lake/sqlite.db")
        dt,_ = run([sys.executable, "scripts/blob_to_sqlite.py",
                    "--blob", "lake/blob_000001.blob.gz",
                    "--db", "lake/sqlite.db", "--reset", "--mode", args.mode])
        t_b2s = dt

        # 5) query SQLite
        sql = "SELECT COUNT(*) FROM state;" if args.mode=="upsert" else SQL_SQLITE_MOR
        dt,_ = run([sys.executable, "scripts/query_arrow.py",
                    "--sqlite", "lake/sqlite.db", "--sql", sql])
        t_qS = dt

        print(f"{args.rows}\t{args.mode}\t{r:.2f}\t{t_gen:.2f}\t{t_b2a:.2f}\t{t_qA:.2f}\t{t_b2s:.2f}\t{t_qS:.2f}")

if __name__ == "__main__":
    main()
