#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
wgs_sketch_union.py

A parallel pipeline to:
  (1) Extract 64-bit FracMinHash values from sourmash .sig.gz files inside .sig.zip archives,
      writing to partitioned on-disk spool files by k-mer size and top bits of the hash.
  (2) Reduce each partition to exact unique values and write a partitioned Parquet (or NPY) dataset.
  (3) Optionally compute "rhs minus lhs" set-difference counts between two reduced datasets.

Design highlights:
- Streams zip->gzip->json (no clobbering; no unpacking to dirs).
- Handles both sourmash JSON styles: 'mins' list or 'hashes' dict/array.
- Partitioning by the top N bits allows external dedup with bounded memory.
- Parquet output is ready for Polars/DuckDB; NPY output is fastest and mmap-friendly.

Author: (you)
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import gzip
import io
import json
import logging
import math
import os
from pathlib import Path
import sys
import time
import zipfile
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

# Optional faster JSON
try:
    import orjson as fastjson
except ImportError:
    fastjson = None

import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:
    pa = None
    pq = None


# -----------------------
# Helpers / parsing
# -----------------------

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _hex_width(bits: int) -> int:
    # number of hex digits to represent 'bits' bits
    return (bits + 3) // 4


def _iter_sigzip_files(root: Path) -> Iterator[Path]:
    # Walk root and yield *.sig.zip files
    for dirpath, dirnames, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith(".sig.zip"):
                yield Path(dirpath) / fn


def _load_json_bytes(b: bytes):
    if fastjson is not None:
        return fastjson.loads(b)
    return json.loads(b.decode("utf-8"))


def _extract_hashes_from_signature_json_bytes(b: bytes,
                                              ksizes: Set[int]) -> Dict[int, np.ndarray]:
    """
    Return dict: ksize -> np.unique(np.uint64 array of hash values) from a single .sig(.gz) JSON.
    Supports sourmash 'mins' or 'hashes'.
    """
    obj = _load_json_bytes(b)
    if isinstance(obj, dict) and "signatures" in obj:
        sigs = obj["signatures"]
    elif isinstance(obj, list):
        sigs = obj
    else:
        sigs = [obj]

    out: Dict[int, List[np.ndarray]] = {}
    for sig in sigs:
        k = sig.get("ksize")
        if k not in ksizes:
            continue
        hashes = None
        if "mins" in sig and sig["mins"]:
            hashes = sig["mins"]
        elif "hashes" in sig and sig["hashes"]:
            h = sig["hashes"]
            if isinstance(h, dict):
                # keys are strings of integers
                hashes = list(map(int, h.keys()))
            else:
                hashes = h
        else:
            continue

        arr = np.asarray(hashes, dtype=np.uint64)
        if arr.size:
            arr = np.unique(arr)
            out.setdefault(int(k), []).append(arr)

    # combine per k
    combined: Dict[int, np.ndarray] = {}
    for k, arrs in out.items():
        if len(arrs) == 1:
            combined[k] = arrs[0]
        else:
            combined[k] = np.unique(np.concatenate(arrs))
    return combined


def _partition_ids_for(arr: np.ndarray, partition_bits: int) -> np.ndarray:
    # top 'partition_bits' bits; arr is uint64
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
                        partition_bits: int) -> Tuple[int, int]:
    """
    Stream a .sig.zip, harvest hashes for given ksizes, and append to spool files.

    Returns: (files_seen, siggz_seen) for basic stats.
    """
    files_seen = 1
    siggz_seen = 0
    try:
        with zipfile.ZipFile(path, "r") as zf:
            for zinfo in zf.infolist():
                name = zinfo.filename
                # we only care about .../signatures/*.sig.gz
                if not name.endswith(".sig.gz"):
                    continue
                if "/signatures/" not in name and not name.startswith("signatures/"):
                    continue
                siggz_seen += 1
                with zf.open(zinfo, "r") as zf_fp:
                    with gzip.GzipFile(fileobj=zf_fp, mode="rb") as gz:
                        raw = gz.read()
                by_k = _extract_hashes_from_signature_json_bytes(raw, ksizes)
                if not by_k:
                    continue

                # For each k, bucket by top bits and append
                for k, arr in by_k.items():
                    if arr.size == 0:
                        continue
                    parts = _partition_ids_for(arr, partition_bits)
                    # sort by partition for grouped writes
                    order = np.argsort(parts, kind="mergesort")
                    arr_sorted = arr[order]
                    parts_sorted = parts[order]

                    # group boundaries
                    uniq_parts, idx = np.unique(parts_sorted, return_index=True)
                    idx = list(idx) + [arr_sorted.size]
                    k_dir = spool_dir / f"ksize={k}"
                    for i, pid_ in enumerate(uniq_parts):
                        start = idx[i]
                        end = idx[i+1]
                        chunk = arr_sorted[start:end]
                        p_hex = format(int(pid_), f"0{_hex_width(partition_bits)}x")
                        part_dir = k_dir / f"part={p_hex}"
                        _ensure_dir(part_dir)
                        # append raw uint64 little-endian
                        out_path = part_dir / f"{_pid_tag()}.bin"
                        with open(out_path, "ab") as f:
                            chunk.tofile(f)

        return (files_seen, siggz_seen)
    except Exception as e:
        logging.exception(f"ERROR processing zip {path}: {e}")
        return (files_seen, siggz_seen)


def cmd_extract(args: argparse.Namespace) -> None:
    root = Path(args.input).resolve()
    spool_dir = Path(args.out).resolve() / "spool"
    _ensure_dir(spool_dir)

    ksizes = set(args.ksizes)
    if not ksizes:
        ksizes = {15, 31, 33}

    paths = list(_iter_sigzip_files(root))
    logging.info(f"Found {len(paths):,} .sig.zip files under {root}")

    start = time.time()
    files = 0
    siggz = 0

    # Reasonable chunksize to cut IPC overhead
    chunksize = 8
    with futures.ProcessPoolExecutor(max_workers=args.processes) as ex:
        fn = lambda p: _process_one_sigzip(p, ksizes, spool_dir, args.partition_bits)
        for nfiles, nsz in ex.map(fn, paths, chunksize=chunksize):
            files += nfiles
            siggz += nsz
            if files % 1000 == 0:
                elapsed = time.time() - start
                logging.info(f"Processed {files:,} zips / {siggz:,} sig.gz in {elapsed:,.1f}s")

    logging.info("DONE extract: processed %s zips, %s sig.gz", f"{files:,}", f"{siggz:,}")
    logging.info(f"SPOOL at {spool_dir}")


# -----------------------
# REDUCE stage
# -----------------------

def _read_uint64_bins(bin_files: List[Path]) -> np.ndarray:
    """Read multiple .bin files (raw uint64) and concatenate to one array."""
    arrays: List[np.ndarray] = []
    for p in bin_files:
        if p.stat().st_size == 0:
            continue
        arr = np.fromfile(p, dtype=np.uint64)
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
    pq.write_table(
        table,
        k_dir / "part.parquet",
        compression=compression
    )


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
    """
    Return the number of unique hashes written for this partition.
    """
    part_dir = spool_dir / f"ksize={k}" / f"part={part_hex}"
    if not part_dir.exists():
        return 0
    bin_files = sorted(p for p in part_dir.glob("*.bin") if p.is_file())
    if not bin_files:
        return 0

    total_bytes = sum(p.stat().st_size for p in bin_files)
    # crude memory check; unique needs ~2x space worst-case
    if total_bytes > mem_limit_gb * (1024**3) * 0.6:
        raise MemoryError(
            f"Partition k={k} part={part_hex} total_bytes={total_bytes:,} "
            f"exceeds ~60% of mem-limit; re-run with larger --partition-bits or bigger --mem-limit-gb."
        )

    all_arr = _read_uint64_bins(bin_files)
    if all_arr.size == 0:
        uniq = all_arr
    else:
        uniq = np.unique(all_arr)

    if fmt == "parquet":
        _write_parquet_unique(out_dir, k, part_hex, uniq)
    else:
        _write_npy_unique(out_dir, k, part_hex, uniq)

    return int(uniq.size)


def cmd_reduce(args: argparse.Namespace) -> None:
    spool_dir = Path(args.spool).resolve()
    out_dir = Path(args.out).resolve()
    _ensure_dir(out_dir)

    # infer ksizes present
    k_dirs = sorted(d for d in spool_dir.glob("ksize=*") if d.is_dir())
    ksizes = sorted(int(d.name.split("=")[1]) for d in k_dirs)
    if args.ksizes:
        ksizes = [k for k in ksizes if k in set(args.ksizes)]
    if not ksizes:
        logging.error("No ksize directories found under spool.")
        sys.exit(2)

    # infer partition-bits from directory names
    # assume uniform width
    any_part = next((p for p in spool_dir.rglob("part=*") if p.is_dir()), None)
    if not any_part:
        logging.error("No partitions found under spool.")
        sys.exit(2)
    part_name = any_part.name.split("=")[1]
    partition_bits = len(part_name) * 4
    part_hex_width = len(part_name)

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
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "ksizes": {},
        "total_distinct": 0
    }

    for k in ksizes:
        # enumerate partitions present under this k
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

    # write manifest
    man_path = out_dir / "_MANIFEST.json"
    with open(man_path, "w") as f:
        json.dump(manifest, f, indent=2)
    logging.info(f"Wrote manifest to {man_path}")


# -----------------------
# DIFF stage
# -----------------------

def _load_unique_partition(dir_: Path, k: int, part_hex: str) -> np.ndarray:
    """
    Load unique partition either from parquet (hash column) or npy. Returns sorted unique np.uint64.
    """
    p_parquet = dir_ / f"ksize={k}" / f"part={part_hex}" / "part.parquet"
    p_npy = dir_ / f"ksize={k}" / f"part={part_hex}" / "part.npy"
    if p_parquet.exists():
        if pa is None or pq is None:
            raise RuntimeError("pyarrow required to read parquet")
        table = pq.read_table(p_parquet, columns=["hash"])
        col = table.column("hash")
        arr = col.to_numpy(zero_copy_only=False)  # numpy view
        return np.asarray(arr, dtype=np.uint64)
    elif p_npy.exists():
        return np.load(p_npy)
    else:
        return np.zeros((0,), dtype=np.uint64)


def cmd_diff(args: argparse.Namespace) -> None:
    lhs = Path(args.lhs).resolve()
    rhs = Path(args.rhs).resolve()
    out_dir = Path(args.out).resolve() if args.write_diff.lower() == "yes" else None
    if out_dir:
        _ensure_dir(out_dir)

    # detect ksizes from rhs (what we want to subtract)
    k_dirs = sorted(d for d in rhs.glob("ksize=*") if d.is_dir())
    ksizes = sorted(int(d.name.split("=")[1]) for d in k_dirs)
    if args.ksizes:
        ksizes = [k for k in ksizes if k in set(args.ksizes)]
    if not ksizes:
        logging.error("No ksizes detected in rhs dataset.")
        sys.exit(2)

    # infer partition-bits / width from rhs
    any_part = next((p for p in rhs.rglob("part=*") if p.is_dir()), None)
    if not any_part:
        logging.error("No partitions found under rhs.")
        sys.exit(2)
    part_hex_width = len(any_part.name.split("=")[1])

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
                diff_arr = rhs_arr  # everything is "not in lhs"
            else:
                # both are sorted unique
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

    # summary
    summary = {
        "rhs_not_in_lhs": totals,
        "total_rhs_not_in_lhs": grand
    }
    print(json.dumps(summary, indent=2))


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
                    help="Number of top bits to partition on (default 14 => 16384 partitions).")
    pe.add_argument("--ksizes", type=int, nargs="*", default=[15,31,33], help="k-mer sizes to include.")
    pe.set_defaults(func=cmd_extract)

    pr = sub.add_parser("reduce", help="Reduce spool partitions to unique and write Parquet or NPY dataset.")
    pr.add_argument("--spool", required=True, help="Spool directory created by 'extract' (…/out/spool).")
    pr.add_argument("--out", required=True, help="Output dataset directory for unique partitions.")
    pr.add_argument("--format", default="parquet", help="parquet | npy (default parquet).")
    pr.add_argument("--mem-limit-gb", type=float, default=8.0, help="Approx RAM budget per partition.")
    pr.add_argument("--ksizes", type=int, nargs="*", help="Optionally restrict to these ksizes.")
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
