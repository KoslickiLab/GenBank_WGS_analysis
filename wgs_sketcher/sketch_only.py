import asyncio, os, signal, aiohttp
from .db import DB
from .utils import LOG, build_logger, RateLimiter
from .worker import download_file, run_sourmash
from .scheduler import DEFAULT_PARAMS  # reuse defaults

class SketchOnly:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        build_logger(self.cfg.get("log_path"))
        self.db = DB(self.cfg["state_db"])
        self.stop_flag = False

    async def worker(self, session, net_sem, rate):
        params = self.cfg.get("sourmash_params", DEFAULT_PARAMS)
        rayon_threads = int(self.cfg.get("sourmash_threads", 1))
        tmp_root = self.cfg["tmp_root"]
        out_root = self.cfg["output_root"]
        retry_max = int(self.cfg.get("max_retries", 6))
        timeout = int(self.cfg.get("request_timeout_seconds", 3600))

        while not self.stop_flag:
            claim = self.db.claim_next()
            if not claim:
                await asyncio.sleep(2.0)
                continue
            file_id, subdir, filename, url = claim
            rel_dir = subdir or filename.split(".")[1][:3]
            local_tmp = os.path.join(tmp_root, rel_dir, filename)
            local_out = os.path.join(out_root, rel_dir, filename + ".sig.zip")

            if os.path.exists(local_out):
                self.db.mark_status(file_id, "DONE", out_path=local_out)
                continue

            tries = 0
            while tries <= retry_max and not self.stop_flag:
                try:
                    async with net_sem:
                        await download_file(session, url, local_tmp, rate, timeout=timeout)
                    self.db.mark_status(file_id, "SKETCHING")
                    rc, out = await run_sourmash(local_tmp, local_out, params, rayon_threads, log=LOG)
                    if rc != 0:
                        raise RuntimeError(f"sourmash failed rc={rc}: {out[:500]}")
                    self.db.mark_status(file_id, "DONE", out_path=local_out)
                    try: os.remove(local_tmp)
                    except FileNotFoundError: pass
                    break
                except Exception as e:
                    tries += 1
                    self.db.mark_status(file_id, "ERROR", error=str(e), inc_tries=True)
                    await asyncio.sleep(min(300, 2 ** tries))
                    try:
                        if os.path.exists(local_tmp):
                            os.remove(local_tmp)
                    except Exception:
                        pass

    async def run(self):
        # dirs + requeue stale
        for p in (self.cfg["output_root"], self.cfg["tmp_root"], os.path.dirname(self.cfg["state_db"]), os.path.dirname(self.cfg.get("log_path","/tmp/void.log"))):
            if p: os.makedirs(p, exist_ok=True)
        self.db.reset_stuck(int(self.cfg.get("stale_seconds", 3600)))

        max_dl = int(self.cfg.get("max_concurrent_downloads", 8))
        net_sem = asyncio.Semaphore(max_dl)
        rate_bps = self.cfg.get("rate_limit_bytes_per_sec", None)
        rate = RateLimiter(rate_bps) if rate_bps else None

        conn = aiohttp.TCPConnector(limit_per_host=max_dl, limit=max_dl)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=120, sock_read=3600)
        async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
            total_workers = int(self.cfg.get("max_total_workers", 96))
            workers = [asyncio.create_task(self.worker(session, net_sem, rate)) for _ in range(total_workers)]
            for w in workers:
                w.add_done_callback(lambda t: LOG.exception("Worker crashed: %r", t.exception()) if t.exception() else None)

            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._request_stop)

            # drain until none left
            while True:
                st = self.db.stats()
                pending = (
                    st["by_status"].get("PENDING", 0) +
                    st["by_status"].get("ERROR", 0) +
                    st["by_status"].get("DOWNLOADING", 0) +
                    st["by_status"].get("SKETCHING", 0)
                )
                if pending == 0:
                    self.stop_flag = True
                    break
                await asyncio.sleep(10)

            await asyncio.gather(*workers, return_exceptions=True)

    def _request_stop(self):
        LOG.warning("Stop requested; will finish current tasks then exit.")
        self.stop_flag = True
