# file: minhash_diff.py
import duckdb, json, time, pathlib, sys

LOG_TXT = "/scratch/minhash_diff_report.txt"
LOG_JSON = "/scratch/minhash_diff_report.json"

db1_path = "/scratch/shared_data_new/Logan_yacht_data/processed_data/database_all.db"
db2_path = "/scratch/dmk333_new/GenBank_WGS_analysis/analysis/wgs_database.db"
out1 = "/scratch/parquet/minhash31_db1"
out2 = "/scratch/parquet/minhash31_db2"
tmp  = "/scratch/duckdb_tmp"

B = 10
mask = (1 << B) - 1

def main():
    t0 = time.time()
    con = duckdb.connect()
    con.execute("PRAGMA threads=128")
    con.execute("SET memory_limit='3500GB'")
    con.execute(f"SET temp_directory='{tmp}/'")
    con.execute("PRAGMA enable_object_cache")
    con.execute("PRAGMA enable_progress_bar")

    con.execute(f"ATTACH '{db1_path}' AS db1 (READ_ONLY)")
    con.execute(f"ATTACH '{db2_path}' AS db2 (READ_ONLY)")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _params AS SELECT {B}::INTEGER AS B, {mask}::BIGINT AS mask")

    # Phase 1: exports (DISTINCT per bucket)
    con.execute(f"""
      COPY (
        SELECT (min_hash & (SELECT mask FROM _params)) AS bucket,
               min_hash AS hash
        FROM db1.sigs_dna.signature_mins
        WHERE ksize = 31
        GROUP BY bucket, hash
      ) TO '{out1}'
      (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (bucket),
       PER_THREAD_OUTPUT, ROW_GROUP_SIZE 4000000);
    """)

    con.execute(f"""
      COPY (
        SELECT (hash & (SELECT mask FROM _params)) AS bucket,
               hash
        FROM db2.hashes.hashes_31
        GROUP BY bucket, hash
      ) TO '{out2}'
      (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (bucket),
       PER_THREAD_OUTPUT, ROW_GROUP_SIZE 4000000);
    """)

    # Phase 2: counts
    a_not_b, b_not_a = con.execute("""
      WITH
      db1u AS (
        SELECT bucket, hash FROM read_parquet($p1, hive_partitioning=true)
      ),
      db2u AS (
        SELECT bucket, hash FROM read_parquet($p2, hive_partitioning=true)
      ),
      a_not_b_by_bucket AS (
        SELECT bucket, COUNT(*) AS cnt
        FROM db1u a ANTI JOIN db2u b USING (bucket, hash)
        GROUP BY bucket
      ),
      b_not_a_by_bucket AS (
        SELECT bucket, COUNT(*) AS cnt
        FROM db2u a ANTI JOIN db1u b USING (bucket, hash)
        GROUP BY bucket
      )
      SELECT
        (SELECT SUM(cnt) FROM a_not_b_by_bucket) AS a_not_b,
        (SELECT SUM(cnt) FROM b_not_a_by_bucket) AS b_not_a
    """, [
      f"{out1}/bucket=*/*.parquet",
      f"{out2}/bucket=*/*.parquet",
    ]).fetchone()

    elapsed = time.time() - t0
    report = {
        "bucket_bits": B,
        "mask": mask,
        "a_not_b": int(a_not_b),
        "b_not_a": int(b_not_a),
        "paths": {"db1": db1_path, "db2": db2_path, "out1": out1, "out2": out2},
        "tmp": tmp,
        "elapsed_sec": elapsed,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "duckdb_version": con.execute("PRAGMA version").fetchone()[0],
    }

    txt = (
        f"[{report['timestamp']}] B={B} mask={mask}\n"
        f"A \\ B (db1 not in db2): {report['a_not_b']}\n"
        f"B \\ A (db2 not in db1): {report['b_not_a']}\n"
        f"Elapsed: {elapsed/3600:.2f} h\n"
        f"db1: {db1_path}\n"
        f"db2: {db2_path}\n"
        f"out1: {out1}\nout2: {out2}\n"
        f"tmp:  {tmp}\n"
        f"DuckDB: {report['duckdb_version']}\n"
    )
    pathlib.Path(LOG_TXT).write_text(txt)
    pathlib.Path(LOG_JSON).write_text(json.dumps(report, indent=2))
    print(txt)

if __name__ == "__main__":
    sys.exit(main())
