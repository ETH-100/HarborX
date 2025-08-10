import pathlib, subprocess, sys, pytest
@pytest.mark.skipif(__import__('importlib').util.find_spec('duckdb') is None, reason='duckdb not installed')
def test_duckdb_reads_parquet(tmp_path):
    import duckdb
    repo = pathlib.Path(__file__).resolve().parents[1]
    blobs = tmp_path / 'blobs'; out = tmp_path / 'parquet'
    blobs.mkdir()
    subprocess.check_call([sys.executable, str(repo/'bench'/'gen_blob.py'), '--out', str(blobs/'blob'), '--rows', '5000', '--parts', '1', '--seed', '3'])
    subprocess.check_call([sys.executable, '-m', 'harborx_ingestor.cli', 'ingest', '--source', str(blobs), '--chain', '167001', '--out', str(out), '--row-group', '1024'])
    con = duckdb.connect()
    con.execute(f"CREATE VIEW state AS SELECT * FROM read_parquet('{out}/**/*.parquet');")
    cnt = con.execute('SELECT COUNT(*) FROM state').fetchone()[0]
    assert cnt > 0
