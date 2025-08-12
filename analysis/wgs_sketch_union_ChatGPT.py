#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
wgs_sketch_union.py

A parallel pipeline to:
  (1) Extract 64-bit FracMinHash values from sourmash .sig.gz files inside .sig.zip archives,
      writing to partitioned on-disk spool files by k-mer size and a partition function over the hash.
  (2) Reduce each partition to exact unique values and write a partitioned Parquet (or NPY) dataset.
  (3) Optionally compute "rhs minus lhs" set-difference counts between two reduced datasets.

Updates:
- Partitioning is now robust to `scaled` truncation by default using a SplitMix64 mixer
  prior to selecting partition bits. This avoids skew when only the smallest hashes are kept.
- JSON parser handles top-level lists containing sourmash_signature objects with a 'signatures' list.
- Fault tolerance improved: errors are logged per-archive and per-member; processing continues.
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import gzip
import json
import logging
import os
from pathlib import Path
import sys
import time
import zipfile
from typing import Dict, Iterator, List, Optional, Sequence, Set, Tuple

# Optional faster JSON
try:
    import orjson as fastjson
except ImportError:
    fastjson = None

import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:
    pa = None
    pq = None


# -----------------------
# Helpers / parsing
# -----------------------

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _hex_width(bits: int) -> int:
    return (bits + 3) // 4


def _iter_sigzip_files(root: Path) -> Iterator[Path]:
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith(".sig.zip"):
                yield Path(dirpath) / fn


def _load_json_bytes(b: bytes):
    if fastjson is not None:
        return fastjson.loads(b)
    return json.loads(b.decode("utf-8"))


def _iter_raw_signatures(obj) -> Iterator[dict]:
    """
    Iterate inner signature dicts from sourmash JSON:
      - dict with 'signatures': [...]
      - list of dicts, each possibly with 'signatures': [...]
      - direct signature dicts (fallback)
    """
    if isinstance(obj, dict):
        if "signatures" in obj and isinstance(obj["signatures"], list):
            for s in obj["signatures"]:
                if isinstance(s, dict):
                    yield s
        else:
            yield obj
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict) and "signatures" in item and isinstance(item["signatures"], list):
                for s in item["signatures"]:
                    if isinstance(s, dict):
                        yield s
            elif isinstance(item, dict):
                yield item


def _extract_hashes_from_signature_json_bytes(b: bytes,
                                              ksizes: Set[int]) -> Dict[int, np.ndarray]:
    """
    Return dict: ksize -> np.unique(np.uint64 array of hash values) from a single .sig(.gz) JSON.
    Supports 'mins' or 'hashes' (dict or list forms).
    """
    obj = _load_json_bytes(b)
    out: Dict[int, List[np.ndarray]] = {}
    for sig in _iter_raw_signatures(obj):
        k = sig.get("ksize")
        if k not in ksizes:
            continue

        hashes = None
        if "mins" in sig and sig["mins"]:
            hashes = sig["mins"]
        elif "hashes" in sig and sig["hashes"]:
            h = sig["hashes"]
            if isinstance(h, dict):
                hashes = list(map(int, h.keys()))
            else:
                hashes = h
        else:
            continue

        arr = np.asarray(hashes, dtype=np.uint64)
        if arr.size:
            arr = np.unique(arr)
            out.setdefault(int(k), []).append(arr)

    combined: Dict[int, np.ndarray] = {}
    for k, arrs in out.items():
        combined[k] = arrs[0] if len(arrs) == 1 else np.unique(np.concatenate(arrs))
    return combined


def _splitmix64(arr: np.ndarray) -> np.ndarray:
    """SplitMix64 mixer; keeps dtype uint64 and is vectorized."""
    mask = np.uint64(0xFFFFFFFFFFFFFFFF)
    z = (arr + np.uint64(0x9E3779B97F4A7C15)) & mask
    z ^= (z >> np.uint64(30))
    z = (z * np.uint64(0xBF58476D1CE4E5B9)) & mask
    z ^= (z >> np.uint64(27))
    z = (z * np.uint64(0x94D049BB133111EB)) & mask
    z ^= (z >> np.uint64(31))
    return z


def _partition_ids_for(arr: np.ndarray, partition_bits: int, mode: str = "mix") -> np.ndarray:
    """
    Map each 64-bit hash to a partition id in [0, 2^partition_bits).
      - "mix" (default): SplitMix64(arr) then take top bits (robust to scaled).
      - "low": take low bits (fast).
      - "high": take high bits (avoid with scaled).
    """
    if partition_bits <= 0:
        raise ValueError("partition_bits must be > 0")
    if mode not in ("mix", "low", "high"):
        raise ValueError("mode must be one of: mix, low, high")

    if mode == "mix":
        arr2 = _splitmix64(arr)
        shift = np.uint64(64 - partition_bits)
        return (arr2 >> shift).astype(np.uint32)
    elif mode == "low":
        mask = np.uint64((1 << partition_bits) - 1)
        return (arr & mask).astype(np.uint32)
    else:
        shift = np.uint64(64 - partition_bits)
        return (arr >> shift).astype(np.uint32)


def _pid_tag() -> str:
    return f"w{os.getpid()}"


# -----------------------
# EXTRACT stage
# -----------------------

def _process_one_sigzip(path: Path,
                        ksizes: Set[int],
                        spool_dir: Path,
                        partition_bits: int,
                        partition_mode: str) -> Tuple[int, int, int, int]:
    """
    Stream a .sig.zip, harvest hashes for given ksizes, and append to spool files.
    Returns: (archives_seen, siggz_seen, members_ok, members_err).
    """
    archives_seen, siggz_seen, members_ok, members_err = 1, 0, 0, 0
    try:
        with zipfile.ZipFile(path, "r") as zf:
            for zinfo in zf.infolist():
                name = zinfo.filename
                if not name.endswith(".sig.gz"):
                    continue
                if "/signatures/" not in name and not name.startswith("signatures/"):
                    continue
                try:
                    with zf.open(zinfo, "r") as zf_fp:
                        with gzip.GzipFile(fileobj=zf_fp, mode="rb") as gz:
                            raw = gz.read()
                    siggz_seen += 1
                    by_k = _extract_hashes_from_signature_json_bytes(raw, ksizes)
                    if not by_k:
                        continue

                    for k, arr in by_k.items():
                        if arr.size == 0:
                            continue
                        parts = _partition_ids_for(arr, partition_bits, mode=partition_mode)
                        order = np.argsort(parts, kind="mergesort")
                        arr_sorted = arr[order]
                        parts_sorted = parts[order]

                        uniq_parts, idx = np.unique(parts_sorted, return_index=True)
                        idx = list(idx) + [arr_sorted.size]
                        k_dir = spool_dir / f"ksize={k}"
                        for i, pid_ in enumerate(uniq_parts):
                            start, end = idx[i], idx[i+1]
                            chunk = arr_sorted[start:end]
                            p_hex = format(int(pid_), f"0{_hex_width(partition_bits)}x")
                            part_dir = k_dir / f"part={p_hex}"
                            _ensure_dir(part_dir)
                            with open(part_dir / f"{_pid_tag()}.bin", "ab") as f:
                                chunk.tofile(f)
                    members_ok += 1
                except (OSError, gzip.BadGzipFile, json.JSONDecodeError, UnicodeDecodeError) as e:
                    logging.warning(f"Skipping bad member in {path}: {name} ({e})")
                    members_err += 1
                except Exception as e:
                    logging.exception(f"Unexpected error for member {name} in {path}: {e}")
                    members_err += 1
    except zipfile.BadZipFile as e:
        logging.warning(f"Skipping corrupted zip archive {path}: {e}")
    except Exception as e:
        logging.exception(f"ERROR opening zip {path}: {e}")

    return (archives_seen, siggz_seen, members_ok, members_err)


def cmd_extract(args: argparse.Namespace) -> None:
    root = Path(args.input).resolve()
    spool_dir = Path(args.out).resolve() / "spool"
    _ensure_dir(spool_dir)

    ksizes = set(args.ksizes) if args.ksizes else {15, 31, 33}

    paths = list(_iter_sigzip_files(root))
    logging.info(f"Found {len(paths):,} .sig.zip files under {root}")

    start = time.time()
    archives = siggz = ok = err = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.processes) as ex:
        fn = lambda p: _process_one_sigzip(p, ksizes, spool_dir, args.partition_bits, args.partition_mode)
        for a, s, m_ok, m_err in ex.map(fn, paths, chunksize=8):
            archives += a; siggz += s; ok += m_ok; err += m_err
            if archives % 1000 == 0:
                elapsed = time.time() - start
                logging.info(f"Processed {archives:,} zips / {siggz:,} sig.gz; "
                             f"members ok={ok:,}, bad={err:,} in {elapsed:,.1f}s")

    logging.info("DONE extract: processed %s zips, %s sig.gz; members ok=%s bad=%s",
                 f"{archives:,}", f"{siggz:,}", f"{ok:,}", f"{err:,}")
    logging.info(f"SPOOL at {spool_dir}")


# -----------------------
# REDUCE stage
# -----------------------

def _read_uint64_bins(bin_files: List[Path]) -> np.ndarray:
    arrays: List[np.ndarray] = []
    for p in bin_files:
        if p.stat().st_size == 0:
            continue
        try:
            arr = np.fromfile(p, dtype=np.uint64)
        except Exception as e:
            logging.warning(f"Skipping unreadable bin file {p}: {e}")
            continue
        if arr.size:
            arrays.append(arr)
    if not arrays:
        return np.zeros((0,), dtype=np.uint64)
    return np.concatenate(arrays, axis=0)


def _write_parquet_unique(out_dir: Path,
                          k: int,
                          part_hex: str,
                          uniq: np.ndarray,
                          compression: str = "zstd") -> None:
    if pa is None or pq is None:
        raise RuntimeError("pyarrow is required for --format parquet")
    k_dir = out_dir / f"ksize={k}" / f"part={part_hex}"
    _ensure_dir(k_dir)
    table = pa.table({"hash": pa.array(uniq, type=pa.uint64())})
    pq.write_table(table, k_dir / "part.parquet", compression=compression)


def _write_npy_unique(out_dir: Path,
                      k: int,
                      part_hex: str,
                      uniq: np.ndarray) -> None:
    k_dir = out_dir / f"ksize={k}" / f"part={part_hex}"
    _ensure_dir(k_dir)
    np.save(k_dir / "part.npy", uniq, allow_pickle=False)


def _reduce_one_partition(spool_dir: Path,
                          out_dir: Path,
                          k: int,
                          part_hex: str,
                          fmt: str,
                          mem_limit_gb: float) -> int:
    part_dir = spool_dir / f"ksize={k}" / f"part={part_hex}"
    if not part_dir.exists():
        return 0
    bin_files = sorted(p for p in part_dir.glob("*.bin") if p.is_file())
    if not bin_files:
        return 0

    total_bytes = sum(p.stat().st_size for p in bin_files)
    if total_bytes > mem_limit_gb * (1024**3) * 0.6:
        raise MemoryError(
            f"Partition k={k} part={part_hex} total_bytes={total_bytes:,} "
            f"exceeds ~60% of mem-limit; re-run with larger --partition-bits or bigger --mem-limit-gb."
        )

    all_arr = _read_uint64_bins(bin_files)
    uniq = np.unique(all_arr) if all_arr.size else all_arr

    if fmt == "parquet":
        _write_parquet_unique(out_dir, k, part_hex, uniq)
    else:
        _write_npy_unique(out_dir, k, part_hex, uniq)

    return int(uniq.size)


def cmd_reduce(args: argparse.Namespace) -> None:
    spool_dir = Path(args.spool).resolve()
    out_dir = Path(args.out).resolve()
    _ensure_dir(out_dir)

    k_dirs = sorted(d for d in spool_dir.glob("ksize=*") if d.is_dir())
    ksizes = sorted(int(d.name.split("=")[1]) for d in k_dirs)
    if args.ksizes:
        ksizes = [k for k in ksizes if k in set(args.ksizes)]
    if not ksizes:
        logging.error("No ksize directories found under spool.")
        sys.exit(2)

    any_part = next((p for p in spool_dir.rglob("part=*") if p.is_dir()), None)
    if not any_part:
        logging.error("No partitions found under spool.")
        sys.exit(2)
    part_name = any_part.name.split("=")[1]
    partition_bits = len(part_name) * 4

    fmt = args.format.lower()
    if fmt not in ("parquet", "npy"):
        logging.error("--format must be parquet or npy")
        sys.exit(2)
    if fmt == "parquet" and (pa is None or pq is None):
        logging.error("pyarrow is required for parquet output.")
        sys.exit(2)

    manifest = {
        "format": fmt,
        "partition_bits": partition_bits,
        "partition_mode": args.partition_mode,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "ksizes": {},
        "total_distinct": 0
    }

    for k in ksizes:
        parts = sorted(d for d in (spool_dir / f"ksize={k}").glob("part=*") if d.is_dir())
        counts = 0
        written_parts = 0
        for pdir in parts:
            part_hex = pdir.name.split("=")[1]
            try:
                n = _reduce_one_partition(
                    spool_dir=spool_dir,
                    out_dir=out_dir,
                    k=k,
                    part_hex=part_hex,
                    fmt=fmt,
                    mem_limit_gb=args.mem_limit_gb
                )
                counts += n
                written_parts += 1
            except MemoryError as me:
                logging.error(str(me))
                sys.exit(3)

        manifest["ksizes"][str(k)] = {
            "partitions": written_parts,
            "distinct_count": counts
        }
        manifest["total_distinct"] += counts
        logging.info(f"k={k}: distinct={counts:,} across {written_parts} partitions")

    with open(out_dir / "_MANIFEST.json", "w") as f:
        json.dump(manifest, f, indent=2)
    logging.info(f"Wrote manifest to {out_dir / '_MANIFEST.json'}")


# -----------------------
# DIFF stage
# -----------------------

def _load_unique_partition(dir_: Path, k: int, part_hex: str) -> np.ndarray:
    p_parquet = dir_ / f"ksize={k}" / f"part={part_hex}" / "part.parquet"
    p_npy = dir_ / f"ksize={k}" / f"part={part_hex}" / "part.npy"
    if p_parquet.exists():
        if pa is None or pq is None:
            raise RuntimeError("pyarrow required to read parquet")
        table = pq.read_table(p_parquet, columns=["hash"])
        arr = table.column("hash").to_numpy(zero_copy_only=False)
        return np.asarray(arr, dtype=np.uint64)
    elif p_npy.exists():
        return np.load(p_npy)
    else:
        return np.zeros((0,), dtype=np.uint64)


def _read_manifest_if_exists(dir_: Path) -> Optional[dict]:
    man = dir_ / "_MANIFEST.json"
    if man.exists():
        try:
            return json.load(open(man, "r"))
        except Exception:
            return None
    return None


def cmd_diff(args: argparse.Namespace) -> None:
    lhs = Path(args.lhs).resolve()
    rhs = Path(args.rhs).resolve()
    out_dir = Path(args.out).resolve() if args.write_diff.lower() == "yes" else None
    if out_dir:
        _ensure_dir(out_dir)

    k_dirs = sorted(d for d in rhs.glob("ksize=*") if d.is_dir())
    ksizes = sorted(int(d.name.split("=")[1]) for d in k_dirs)
    if args.ksizes:
        ksizes = [k for k in ksizes if k in set(args.ksizes)]
    if not ksizes:
        logging.error("No ksizes detected in rhs dataset.")
        sys.exit(2)

    any_part = next((p for p in rhs.rglob("part=*") if p.is_dir()), None)
    if not any_part:
        logging.error("No partitions found under rhs.")
        sys.exit(2)

    lhs_man = _read_manifest_if_exists(lhs)
    rhs_man = _read_manifest_if_exists(rhs)
    if lhs_man and rhs_man:
        if lhs_man.get("partition_bits") != rhs_man.get("partition_bits") \
           or lhs_man.get("partition_mode") != rhs_man.get("partition_mode"):
            logging.error("Partition config mismatch between datasets. "
                          "lhs: bits=%s mode=%s; rhs: bits=%s mode=%s",
                          lhs_man.get("partition_bits"), lhs_man.get("partition_mode"),
                          rhs_man.get("partition_bits"), rhs_man.get("partition_mode"))
            sys.exit(4)

    totals = {}
    grand = 0

    for k in ksizes:
        parts = sorted(d for d in (rhs / f"ksize={k}").glob("part=*") if d.is_dir())
        subtotal = 0
        for pdir in parts:
            part_hex = pdir.name.split("=")[1]
            rhs_arr = _load_unique_partition(rhs, k, part_hex)
            lhs_arr = _load_unique_partition(lhs, k, part_hex)
            if rhs_arr.size == 0:
                continue
            if lhs_arr.size == 0:
                diff_arr = rhs_arr
            else:
                diff_arr = np.setdiff1d(rhs_arr, lhs_arr, assume_unique=True)

            subtotal += int(diff_arr.size)

            if out_dir and diff_arr.size:
                if pa is not None and pq is not None:
                    _write_parquet_unique(out_dir, k, part_hex, diff_arr)
                else:
                    _write_npy_unique(out_dir, k, part_hex, diff_arr)

        totals[str(k)] = subtotal
        grand += subtotal
        logging.info(f"k={k}: rhs-not-in-lhs = {subtotal:,}")

    print(json.dumps({"rhs_not_in_lhs": totals, "total_rhs_not_in_lhs": grand}, indent=2))


# -----------------------
# CLI
# -----------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Collect and count distinct FracMinHash hashes from sourmash .sig.gz inside .sig.zip at scale."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    pe = sub.add_parser("extract", help="Extract hash values into partitioned spool files.")
    pe.add_argument("--input", required=True, help="Root directory containing .sig.zip files.")
    pe.add_argument("--out", required=True, help="Output root directory (will create 'spool').")
    pe.add_argument("--processes", type=int, default=os.cpu_count() or 8, help="Parallel processes.")
    pe.add_argument("--partition-bits", type=int, default=14,
                    help="Number of bits to partition on (default 14 => 16384 partitions).")
    pe.add_argument("--partition-mode", default="mix", choices=["mix", "low", "high"],
                    help="Partition function over 64-bit hashes. Default 'mix' uses SplitMix64+top-bits (robust with scaled).")
    pe.add_argument("--ksizes", type=int, nargs="*", default=[15,31,33], help="k-mer sizes to include.")
    pe.set_defaults(func=cmd_extract)

    pr = sub.add_parser("reduce", help="Reduce spool partitions to unique and write Parquet or NPY dataset.")
    pr.add_argument("--spool", required=True, help="Spool directory created by 'extract' (…/out/spool).")
    pr.add_argument("--out", required=True, help="Output dataset directory for unique partitions.")
    pr.add_argument("--format", default="parquet", help="parquet | npy (default parquet).")
    pr.add_argument("--mem-limit-gb", type=float, default=8.0, help="Approx RAM budget per partition.")
    pr.add_argument("--ksizes", type=int, nargs="*", help="Optionally restrict to these ksizes.")
    pr.add_argument("--partition-mode", default="mix", choices=["mix", "low", "high"],
                    help="Echoed into manifest for later compatibility checks.")
    pr.set_defaults(func=cmd_reduce)

    pdiff = sub.add_parser("diff", help="Compute counts of hashes in RHS not in LHS (both reduced datasets).")
    pdiff.add_argument("--lhs", required=True, help="Left-hand (e.g., GenBank-WGS) reduced dataset (parquet/npy).")
    pdiff.add_argument("--rhs", required=True, help="Right-hand reduced dataset to compare against LHS.")
    pdiff.add_argument("--out", required=False, help="Output directory to write difference partitions.")
    pdiff.add_argument("--write-diff", default="no", choices=["yes", "no"],
                       help="Also write per-partition difference dataset (default no).")
    pdiff.add_argument("--ksizes", type=int, nargs="*", help="Optionally restrict to these ksizes.")
    pdiff.set_defaults(func=cmd_diff)

    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )
    args.func(args)


if __name__ == "__main__":
    main()
