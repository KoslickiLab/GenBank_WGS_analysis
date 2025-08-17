# file: diff_bet_wgs_and_logan.py
import duckdb, json, time, pathlib, sys, os, threading

LOG_DIR = "/scratch/wgs_logan_diff"
LOG_TXT = f"{LOG_DIR}/minhash_diff_report.txt"
LOG_JSON = f"{LOG_DIR}/minhash_diff_report.json"

db1_path = "/scratch/shared_data_new/Logan_yacht_data/processed_data/database_all.db"
db2_path = "/scratch/dmk333_new/GenBank_WGS_analysis/analysis/wgs_database.db"
out1 = f"{LOG_DIR}/parquet/minhash31_db1"
out2 = f"{LOG_DIR}/parquet/minhash31_db2"
tmp  = f"{LOG_DIR}/duckdb_tmp"

B = 9
mask = (1 << B) - 1

MONITOR_INTERVAL_SEC = 30

def dir_stats(root):
    total = 0
    files = 0
    by_bucket = {}
    for p, dnames, fnames in os.walk(root):
        for f in fnames:
            if f.endswith(".parquet"):
                fp = os.path.join(p, f)
                try:
                    sz = os.path.getsize(fp)
                except FileNotFoundError:
                    continue
                total += sz
                files += 1
                # hive partition path like .../bucket=123/...
                parts = p.split(os.sep)
                for part in parts[::-1]:
                    if part.startswith("bucket="):
                        b = int(part.split("=",1)[1])
                        by_bucket[b] = by_bucket.get(b, 0) + 1
                        break
    return total, files, by_bucket

def monitor_outputs(stop_evt, paths):
    while not stop_evt.is_set():
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        lines = [f"[{ts}] progress snapshot"]
        for label, path in paths.items():
            if os.path.isdir(path):
                total, files, by_bucket = dir_stats(path)
                lines.append(f"  {label}: {files} files, {total/1e9:.2f} GB written")
            else:
                lines.append(f"  {label}: (dir missing yet)")
        print("\n".join(lines), flush=True)
        stop_evt.wait(MONITOR_INTERVAL_SEC)

def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(out1, exist_ok=True)
    os.makedirs(out2, exist_ok=True)
    os.makedirs(tmp, exist_ok=True)

    # ---- Start progress monitor thread (non-blocking) ----
    stop_evt = threading.Event()
    mon = threading.Thread(
        target=monitor_outputs,
        args=(stop_evt, {"db1_out": out1, "db2_out": out2}),
        daemon=True,
    )
    mon.start()

    t0 = time.time()
    con = duckdb.connect(config={"progress_bar_time": 1000})
    # Global settings
    con.execute("PRAGMA enable_object_cache")
    con.execute("PRAGMA enable_progress_bar")
    con.execute(f"SET temp_directory='{tmp}/'")
    con.execute("SET preserve_insertion_order=false")
    # OPTIONAL: enable DuckDB logging to stdout (very verbose if fully enabled)
    # con.execute("PRAGMA enable_logging")
    # con.execute("SET logging_storage='stdout'")

    con.execute(f"ATTACH '{db1_path}' AS db1 (READ_ONLY)")
    con.execute(f"ATTACH '{db2_path}' AS db2 (READ_ONLY)")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _params AS SELECT {B}::INTEGER AS B, {mask}::BIGINT AS mask")

    # ---------- Phase 1: Exports (distinct per bucket) ----------
    # To avoid a small-file explosion:
    #  * Lower threads during export
    #  * Raise the partitioned open-file cap
    #  * Avoid FILE_SIZE_BYTES/PER_THREAD_OUTPUT with PARTITION_BY
    con.execute("SET threads=32")
    con.execute("SET partitioned_write_max_open_files=8192")  # raise if ulimit allows

    print("Exporting db1 (ksize=31) -> bucketed Parquet ...", flush=True)
    con.execute(f"""
      COPY (
        SELECT (min_hash & (SELECT mask FROM _params)) AS bucket,
               min_hash AS hash
        FROM db1.sigs_dna.signature_mins
        WHERE ksize = 31
        GROUP BY bucket, hash
      ) TO '{out1}'
      (FORMAT parquet, COMPRESSION zstd,
       PARTITION_BY (bucket),
       ROW_GROUP_SIZE 1000000,            -- 1M rows/row group
       ROW_GROUPS_PER_FILE 64);           -- ~64 row groups per file
    """)

    print("Exporting db2 (ksize=31) -> bucketed Parquet ...", flush=True)
    con.execute(f"""
      COPY (
        SELECT (hash & (SELECT mask FROM _params)) AS bucket,
               hash
        FROM db2.hashes.hashes_31
        GROUP BY bucket, hash
      ) TO '{out2}'
      (FORMAT parquet, COMPRESSION zstd,
       PARTITION_BY (bucket),
       ROW_GROUP_SIZE 1000000,
       ROW_GROUPS_PER_FILE 64);
    """)

    # Restore higher parallelism for the compute-heavy difference
    con.execute("SET threads=128")

    # ---------- Phase 2: counts ----------
    print("Counting A\\B and B\\A via ANTI JOIN ...", flush=True)
    a_not_b, b_not_a = con.execute("""
      WITH
      db1u AS (
        SELECT bucket, hash
        FROM read_parquet($p1, hive_partitioning=true)
      ),
      db2u AS (
        SELECT bucket, hash
        FROM read_parquet($p2, hive_partitioning=true)
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

    # Stop monitor and summarize
    stop_evt.set()
    mon.join(timeout=5)

    elapsed = time.time() - t0
    report = {
        "duckdb_version": con.execute("PRAGMA version").fetchone()[0],
        "bucket_bits": B,
        "mask": mask,
        "a_not_b": int(a_not_b),
        "b_not_a": int(b_not_a),
        "paths": {"db1": db1_path, "db2": db2_path, "out1": out1, "out2": out2},
        "tmp": tmp,
        "elapsed_sec": elapsed,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "settings": {
            "threads_after_export": con.execute("SELECT current_setting('threads')").fetchone()[0],
            "memory_limit": con.execute("SELECT current_setting('memory_limit')").fetchone()[0],
            "preserve_insertion_order": con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0],
            "partitioned_write_max_open_files": con.execute("SELECT current_setting('partitioned_write_max_open_files')").fetchone()[0],
        }
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
        f"settings: {report['settings']}\n"
    )
    pathlib.Path(LOG_TXT).write_text(txt)
    pathlib.Path(LOG_JSON).write_text(json.dumps(report, indent=2))
    print(txt, flush=True)

if __name__ == "__main__":
    sys.exit(main())
