# GenBank WGS Sketcher

> **Download, sketch, and archive sourmash signatures for NCBI GenBank WGS**

A restartable, fault-tolerant, and **polite** async pipeline that crawls
the GenBank *Whole Genome Shotgun* (WGS) area, downloads `*.fsa_nt.gz`
assemblies, and produces compressed sourmash signatures (`.sig.zip`) for each file.
State is tracked in a local SQLite database so you can **pause, resume, and recover** safely.

---

## Highlights

- **Polite crawler** over `https://ftp.ncbi.nlm.nih.gov/genbank/wgs/` with bounded concurrency and optional rate-limiting.
- **Resumable**: every file has a status in SQLite (`PENDING, DOWNLOADING, SKETCHING, DONE/ERROR`).
- **Efficient & parallel**: asyncio + aiohttp; separate concurrency controls for crawling and downloads.
- **Sourmash integration**: runs `sourmash sketch dna` with configurable parameters (default `k=15,31,33; scaled=1000; noabund`).  
- **Structured outputs**: signatures are sharded by the first 3 letters of the WGS prefix (e.g., `AAS/`), keeping directories manageable.
- **Simple ops**: one YAML config, one command, one state DB, one log file.

---

## Quick start

```bash
# 1) Create and activate a virtualenv
python -m venv .venv
source .venv/bin/activate

# 2) Install Python deps
pip install -r requirements.txt

# 3) Install sourmash (required runtime dependency)
#    Choose one:
# conda install -c conda-forge sourmash
# pipx install sourmash

# 4) Edit config.yaml to point to your paths (see Config below)

# 5) Run
python -m wgs_sketcher --config config.yaml
# or use the helper script:
./run.sh
```

> **Tip**: This is an async pipeline. Throughput depends on your network, disk, and the concurrency parameters you choose.

---

## Configuration

All settings are provided via a YAML file. Four keys are **required**; others have sensible defaults.

### Minimal example

```yaml
# config.yaml
base_url: "https://ftp.ncbi.nlm.nih.gov/genbank/wgs"
output_root: "/path/to/wgs_sketches"          # REQUIRED
tmp_root: "/path/to/wgs_tmp"                  # REQUIRED
state_db: "/path/to/wgs_state/wgs.sqlite"     # REQUIRED
log_path: "/path/to/wgs_logs/wgs_sketcher.log"# REQUIRED

# Filter which files to process (default shown)
include_regex: '^wgs\.[A-Z0-9]+(?:\.\d+)?\.fsa_nt\.gz$'

# Concurrency & politeness
max_crawl_concurrency: 4          # default: 4
max_concurrent_downloads: 8       # default: 8
max_total_workers: 96             # upper bound for inâ€‘flight tasks (default: 96)
rate_limit_bytes_per_sec: null    # e.g., 50_000_000 for ~50 MB/s; null = unlimited
user_agent: "WGS Sketcher/1.0 (+admin@example.org)"

# Sourmash
sourmash_params: "k=15,k=31,k=33,scaled=1000,noabund"  # default
sourmash_threads: 1                                    # sets RAYON_NUM_THREADS for sourmash

# Networking & retries
request_timeout_seconds: 3600   # default
max_retries: 6                  # default
```

**Required keys**: `output_root`, `tmp_root`, `state_db`, `log_path`  
**Defaults** (if omitted):  
- `base_url`: `https://ftp.ncbi.nlm.nih.gov/genbank/wgs`  
- `include_regex`: `^wgs\.[A-Z0-9]+(?:\.\d+)?\.fsa_nt\.gz$`  
- `max_crawl_concurrency`: `4`  
- `max_concurrent_downloads`: `8`  
- `max_total_workers`: `96`  
- `sourmash_params`: `"k=15,k=31,k=33,scaled=1000,noabund"`  
- `sourmash_threads`: `1`  
- `request_timeout_seconds`: `3600`  
- `max_retries`: `6`  
- `rate_limit_bytes_per_sec`: `null` (no cap)  
- `user_agent`: `"wgs-sketcher/1.0"`

> The example `config.yaml` in the repo shows a realistic HPC layout using `/scratch/...`. Adjust paths for your system.

---

## How it works

1. **Crawl**: Enumerates top-level WGS subdirectories (e.g., `AAS/`, `CAD/`), requests their listings, and filters entries with `include_regex`.
2. **Index**: Each target file is upserted into SQLite (`files` table) with a unique `(subdir, filename)` key.
3. **Download**: Files are streamed via `aiohttp` in 4 MiB chunks to `{tmp_root}/{SHARD}/{filename}.part`, honoring the optional global **RateLimiter**.
4. **Sketch**: On successful download, the pipeline runs:

   ```bash
   sourmash sketch dna -p "<sourmash_params>" -o "{output_root}/{SHARD}/{filename}.sig.zip" "{tmp_root}/{SHARD}/{filename}"
   ```

5. **Commit**: On success, the DB entry moves to `DONE`, and the finished artifact lives under `output_root` (sharded by `SHARD = first 3 letters of the WGS token`, e.g., `{AAS, CAD, ...}`). On failures the error is recorded and retried up to `max_retries`.

---

## Output layout

```
{output_root}/
  AAS/
    wgs.AASDTR.1.fsa_nt.gz.sig.zip
  CAD/
    wgs.CADASZ.1.fsa_nt.gz.sig.zip
{tmp_root}/
  AAS/
    wgs.AASDTR.1.fsa_nt.gz         # transient; removed/overwritten as work proceeds
{state_db}                        # SQLite file holding pipeline state
{log_path}                        # Rotating run log
```

File statuses are tracked in the `files` table (schema excerpt):

```sql
CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subdir TEXT NOT NULL,
  filename TEXT NOT NULL,
  url TEXT NOT NULL,
  size INTEGER,
  mtime TEXT,
  status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING, DOWNLOADING, SKETCHING, DONE, ERROR
  tries INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  out_path TEXT,
  updated_at REAL,
  created_at REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_file ON files(subdir, filename);
```

---

## Operations

### Start / resume
The pipeline is **idempotent** for finished outputs. If a target `.sig.zip` already exists, the file is marked `DONE` without work. Re-running the command will resume from the last unfinished item.

### Monitoring progress
Use `sqlite3` to query status counts:

```bash
sqlite3 /path/to/wgs_state/wgs.sqlite "SELECT status, COUNT(*) FROM files GROUP BY status;"
```

Tail logs during a run:

```bash
tail -f /path/to/wgs_logs/wgs_sketcher.log
```

### Tuning throughput
- Increase `max_concurrent_downloads` to use more bandwidth.  
- Increase `sourmash_threads` to set `RAYON_NUM_THREADS` for sourmash.  
- Set `rate_limit_bytes_per_sec` to keep friendly behavior on shared links.  
- Keep `tmp_root` and `output_root` on **fast local disks** when possible.

---

## Testing

There are basic tests for path sharding and filename selection:

```bash
pip install pytest
pytest -q
```

---

## Requirements

- Python 3.9+
- [`sourmash`](https://sourmash.readthedocs.io/) available on `PATH`
- Python packages: `aiohttp`, `PyYAML` (installed via `requirements.txt`)

---

## Acknowledgements

- Relies on the NCBI GenBank WGS FTP service and the `sourmash` toolkit.
- Originally authored by the Koslicki Lab with assistane from ChatGPT 5 Pro. Project version: **1.0.2**.
- Last updated: **2025-08-12**.

---

## License

MIT
