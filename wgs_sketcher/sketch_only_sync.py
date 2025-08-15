import os, sys, time, sqlite3, urllib.request, shutil, subprocess, argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple, Optional

# --- config loader reused from your scheduler ---
def load_config(path: str) -> dict:
    import yaml, os
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cfg.setdefault("sourmash_params", "k=15,k=31,k=33,scaled=1000,noabund")
    cfg.setdefault("sourmash_threads", 1)
    cfg.setdefault("request_timeout_seconds", 3600)
    cfg.setdefault("error_retry_cooldown_seconds", 1800)
    cfg.setdefault("error_max_total_tries", 20)
    for key in ("output_root","tmp_root","state_db"):
        if key not in cfg:
            raise ValueError(f"Missing required config key: {key}")
        cfg[key] = os.path.abspath(os.path.expanduser(cfg[key]))
    return cfg

def pick_candidates(db_path: str, include_errors_after: int, error_cap: int, limit: Optional[int]=None) -> List[Tuple[int,str,str,str]]:
    """Return a static list of (id, subdir, filename, url) to process."""
    cutoff = time.time() - include_errors_after
    sql = (
        "SELECT id, subdir, filename, url FROM files "
        "WHERE status='PENDING' "
        "UNION ALL "
        "SELECT id, subdir, filename, url FROM files "
        "WHERE status='ERROR' AND tries < ? AND (updated_at IS NULL OR updated_at <= ?) "
        "ORDER BY id"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    with sqlite3.connect(db_path, timeout=60.0) as conn:
        cur = conn.execute(sql, (error_cap, cutoff))
        return cur.fetchall()

def ensure_dirs(*paths):
    for p in paths:
        if p:
            os.makedirs(p, exist_ok=True)

def http_download(url: str, dest_path: str, ua: Optional[str], timeout: int):
    tmp = dest_path + ".part"
    ensure_dirs(os.path.dirname(dest_path))
    headers = {"User-Agent": ua or "wgs-sketcher-sync/1.0"}
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        with open(tmp, "wb") as out, resp as r:
            shutil.copyfileobj(r, out, length=16*1024*1024)
    os.replace(tmp, dest_path)

def run_sourmash_sync(input_path: str, output_path: str, params: str, rayon_threads: int):
    ensure_dirs(os.path.dirname(output_path))
    env = os.environ.copy()
    env["RAYON_NUM_THREADS"] = str(rayon_threads)
    cmd = ["sourmash","sketch","dna","-p",params,"-o",output_path,input_path]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True)
    return proc.returncode, proc.stdout

def process_one(task: Tuple[int,str,str,str], cfg: dict) -> Tuple[int, str]:
    """Download + sketch one item. Returns (id, 'DONE'|'ERROR'|'GONE')."""
    file_id, subdir, filename, url = task
    out_dir = os.path.join(cfg["output_root"], subdir)
    tmp_dir = os.path.join(cfg["tmp_root"], subdir)
    local_tmp = os.path.join(tmp_dir, filename)
    local_out = os.path.join(out_dir, filename + ".sig.zip")

    # already produced?
    if os.path.exists(local_out):
        return (file_id, "DONE")

    try:
        http_download(url, local_tmp, cfg.get("user_agent"), int(cfg.get("request_timeout_seconds",3600)))
    except Exception as e:
        # detect permanent 404s & mark GONE so we don't thrash
        msg = str(e)
        status = "GONE" if "HTTP 404" in msg else "ERROR"
        with sqlite3.connect(cfg["state_db"], timeout=60.0) as conn:
            conn.execute("UPDATE files SET status=?, last_error=?, tries=tries+1, updated_at=? WHERE id=?",
                         (status, msg[:500], time.time(), file_id))
        # ensure tmp cleanup
        try:
            if os.path.exists(local_tmp):
                os.remove(local_tmp)
        except Exception:
            pass
        return (file_id, status)

    # sketch
    rc, out = run_sourmash_sync(local_tmp, local_out, cfg["sourmash_params"], int(cfg.get("sourmash_threads",1)))
    if rc != 0:
        with sqlite3.connect(cfg["state_db"], timeout=60.0) as conn:
            conn.execute("UPDATE files SET status='ERROR', last_error=?, tries=tries+1, updated_at=? WHERE id=?",
                         (out[:500], time.time(), file_id))
        try:
            if os.path.exists(local_tmp):
                os.remove(local_tmp)
        except Exception:
            pass
        return (file_id, "ERROR")

    # success
    try:
        os.remove(local_tmp)
    except Exception:
        pass
    with sqlite3.connect(cfg["state_db"], timeout=60.0) as conn:
        conn.execute("UPDATE files SET status='DONE', out_path=?, updated_at=? WHERE id=?",
                     (local_out, time.time(), file_id))
    return (file_id, "DONE")

def main():
    ap = argparse.ArgumentParser(description="Sketch-only synchronous runner")
    ap.add_argument("--config","-c", required=True)
    ap.add_argument("--workers","-w", type=int, default=os.cpu_count() or 8, help="number of parallel processes")
    ap.add_argument("--limit","-n", type=int, default=None, help="optional cap on number of tasks to run")
    args = ap.parse_args()

    cfg = load_config(args.config)
    ensure_dirs(cfg["output_root"], cfg["tmp_root"], os.path.dirname(cfg["state_db"]))

    # build a static worklist
    tasks = pick_candidates(cfg["state_db"],
                            int(cfg.get("error_retry_cooldown_seconds",1800)),
                            int(cfg.get("error_max_total_tries",20)),
                            args.limit)
    if not tasks:
        print("No PENDING/eligible ERROR tasks found; exiting.")
        sys.exit(0)

    print(f"Sketch-only: {len(tasks)} tasks, workers={args.workers}")
    done = 0; err = 0; gone = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(process_one, t, cfg) for t in tasks]
        for f in as_completed(futs):
            fid, status = f.result()
            if status == "DONE": done += 1
            elif status == "GONE": gone += 1
            else: err += 1
            if (done + err + gone) % 100 == 0:
                print(f"... progress: DONE={done}, GONE={gone}, ERROR={err}")

    print(f"Finished: DONE={done}, GONE={gone}, ERROR={err}")

if __name__ == "__main__":
    main()
