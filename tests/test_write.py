import pathlib, subprocess, sys, pyarrow.parquet as pq

def test_ingest_writes_parquet(tmp_path):
    repo = pathlib.Path(__file__).resolve().parents[1]
    blobs = tmp_path / "blobs"; out = tmp_path / "parquet"
    blobs.mkdir()
    gen = repo / "bench" / "gen_blob.py"
    subprocess.check_call([sys.executable, str(gen), "--out", str(blobs/'blob'), "--rows", "2000", "--parts", "1", "--seed", "2"])
    subprocess.check_call([sys.executable, "-m", "harborx_ingestor.cli", "ingest", "--source", str(blobs), "--chain", "167001", "--out", str(out), "--row-group", "512"])
    files = list(out.rglob("*.parquet"))
    assert files, "no parquet files written"
    pf = pq.ParquetFile(str(files[0]))
    cols = pf.schema_arrow.names
    for c in ["timestamp","blob_index","position","tx_hash","chain_id","date","topic"]:
        assert c in cols
