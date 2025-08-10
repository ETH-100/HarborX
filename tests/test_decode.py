import pathlib, subprocess, sys
from poc_e2e_blob_sql.packages.ingestor.harborx_ingestor.decoder import parse_blob

def test_parse_blob_roundtrip(tmp_path):
    repo = pathlib.Path(__file__).resolve().parents[1]
    gen = repo / "bench" / "gen_blob.py"
    outdir = tmp_path / "blobs"; outdir.mkdir()
    blob = outdir / "blob_000001.blob.gz"
    subprocess.check_call([sys.executable, str(gen), "--out", str(outdir/'blob'), "--rows", "500", "--parts", "1", "--seed", "1"])
    batches = list(parse_blob(str(blob), chunk=128))
    assert sum(b.num_rows for b in batches) == 500
    assert batches[0].schema.names[:5] == ["type","address","key","value","tx_hash"]
