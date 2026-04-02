#!/usr/bin/env python3
"""
Recompute perf summary CSV from metadata + raw events, ignoring timeout semantics.

Definition used by this script:
- delivered/received message: inbound_recv_ns > 0
- timeout fields are intentionally ignored and emitted as 0

Usage examples:
  python3 scripts/generate_summary_no_timeout.py \
    --meta-dir results/meta \
    --raw-dir results/raw \
    --output results/summary/summary_no_timeout.csv

  python3 scripts/generate_summary_no_timeout.py \
    --meta-file results/meta/run_20260402_163108_baseline_meta.json \
    --raw-dir results/raw \
    --output results/summary/summary_no_timeout_single.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Dict, Iterable, List, Optional


SUMMARY_HEADER = [
    "run_id",
    "scenario_id",
    "namespace",
    "target",
    "is_prod",
    "pairs",
    "arrival_pattern",
    "rate_per_sender",
    "warmup_sec",
    "measure_sec",
    "drain_sec",
    "cooldown_sec",
    "payload_bytes",
    "ack_timeout_ms",
    "e2e_timeout_ms",
    "attempted_messages",
    "acked_messages",
    "received_messages",
    "ack_fail_count",
    "ack_timeout_count",
    "e2e_timeout_count",
    "attempted_rate_per_sec",
    "ack_success_rate",
    "receive_rate",
    "ack_timeout_rate",
    "e2e_timeout_rate",
    "ack_latency_samples",
    "e2e_latency_samples",
    "ack_p50_ms",
    "ack_p75_ms",
    "ack_p85_ms",
    "ack_p95_ms",
    "ack_p99_ms",
    "e2e_p50_ms",
    "e2e_p95_ms",
    "e2e_p99_ms",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate timeout-agnostic perf summary CSV.")
    p.add_argument("--meta-dir", type=Path, help="Directory containing *_meta.json files.")
    p.add_argument("--meta-file", type=Path, help="Single *_meta.json file.")
    p.add_argument("--raw-dir", type=Path, required=True, help="Directory containing *_events.csv files.")
    p.add_argument("--output", type=Path, required=True, help="Output summary CSV path.")
    p.add_argument(
        "--namespace",
        default="",
        help="Fallback namespace if metadata does not include one (default: empty).",
    )
    p.add_argument(
        "--drain-sec",
        default="0",
        help="Fallback drain_sec because metadata does not contain it (default: 0).",
    )
    p.add_argument(
        "--cooldown-sec",
        default="0",
        help="Fallback cooldown_sec because metadata does not contain it (default: 0).",
    )
    return p.parse_args()


def discover_meta_files(meta_dir: Optional[Path], meta_file: Optional[Path]) -> List[Path]:
    files: List[Path] = []
    if meta_file:
        files.append(meta_file)
    if meta_dir:
        files.extend(sorted(meta_dir.glob("*_meta.json")))
    unique = sorted(set(files))
    if not unique:
        raise ValueError("No metadata input found. Provide --meta-file or --meta-dir.")
    return unique


def load_json(path: Path) -> Dict[str, str]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # keep values as strings to mirror original metadata behavior
    return {str(k): "" if v is None else str(v) for k, v in data.items()}


def safe_int(v: str) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return 0


def safe_float(v: str) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return -1.0


def safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return -1.0
    return float(numerator) / float(denominator)


def fmt_double(value: float) -> str:
    if value < 0:
        return ""
    return f"{value:.3f}"


def percentile(values: List[float], p: int) -> float:
    if not values:
        return -1.0
    sorted_vals = sorted(values)
    idx = math.ceil((p / 100.0) * len(sorted_vals)) - 1
    idx = max(0, min(len(sorted_vals) - 1, idx))
    return sorted_vals[idx]


def iter_raw_rows(raw_file: Path) -> Iterable[Dict[str, str]]:
    with raw_file.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def summarize_raw(rows: List[Dict[str, str]], measure_sec: int) -> Dict[str, str]:
    ack_latencies: List[float] = []
    e2e_latencies: List[float] = []

    attempted = len(rows)
    acked = 0
    delivered = 0
    ack_fail = 0

    for row in rows:
        ack_recv_ns = safe_int(row.get("ack_recv_ns", "0"))
        inbound_recv_ns = safe_int(row.get("inbound_recv_ns", "0"))
        ack_success = str(row.get("ack_success", "")).strip().lower() == "true"
        ack_error_code = str(row.get("ack_error_code", "")).strip()
        ack_error_reason = str(row.get("ack_error_reason", "")).strip()

        if ack_recv_ns > 0:
            acked += 1
            ack_ms = safe_float(row.get("ack_latency_ms", ""))
            if ack_ms >= 0:
                ack_latencies.append(ack_ms)

        # Timeout-agnostic delivery definition requested by user.
        if inbound_recv_ns > 0:
            delivered += 1
            e2e_ms = safe_float(row.get("e2e_latency_ms", ""))
            if e2e_ms >= 0:
                e2e_latencies.append(e2e_ms)

        if (not ack_success) and (ack_recv_ns > 0 or ack_error_code or ack_error_reason):
            ack_fail += 1

    return {
        "attempted_messages": str(attempted),
        "acked_messages": str(acked),
        "received_messages": str(delivered),
        "ack_fail_count": str(ack_fail),
        # Intentionally ignored for this summary variant.
        "ack_timeout_count": "0",
        "e2e_timeout_count": "0",
        "attempted_rate_per_sec": fmt_double(safe_rate(attempted, measure_sec)),
        "ack_success_rate": fmt_double(safe_rate(acked, attempted)),
        "receive_rate": fmt_double(safe_rate(delivered, attempted)),
        "ack_timeout_rate": "0.000",
        "e2e_timeout_rate": "0.000",
        "ack_latency_samples": str(len(ack_latencies)),
        "e2e_latency_samples": str(len(e2e_latencies)),
        "ack_p50_ms": fmt_double(percentile(ack_latencies, 50)),
        "ack_p75_ms": fmt_double(percentile(ack_latencies, 75)),
        "ack_p85_ms": fmt_double(percentile(ack_latencies, 85)),
        "ack_p95_ms": fmt_double(percentile(ack_latencies, 95)),
        "ack_p99_ms": fmt_double(percentile(ack_latencies, 99)),
        "e2e_p50_ms": fmt_double(percentile(e2e_latencies, 50)),
        "e2e_p95_ms": fmt_double(percentile(e2e_latencies, 95)),
        "e2e_p99_ms": fmt_double(percentile(e2e_latencies, 99)),
    }


def build_row(meta: Dict[str, str], metrics: Dict[str, str], fallback_namespace: str, fallback_drain: str, fallback_cooldown: str) -> Dict[str, str]:
    row = {
        "run_id": meta.get("run_id", ""),
        "scenario_id": meta.get("scenario_id", ""),
        "namespace": meta.get("namespace", fallback_namespace),
        "target": meta.get("target", ""),
        "is_prod": meta.get("is_prod", ""),
        "pairs": meta.get("pairs", ""),
        "arrival_pattern": meta.get("arrival_pattern", ""),
        "rate_per_sender": meta.get("rate_per_sender", ""),
        "warmup_sec": meta.get("warmup_sec", ""),
        "measure_sec": meta.get("measure_sec", ""),
        "drain_sec": meta.get("drain_sec", fallback_drain),
        "cooldown_sec": meta.get("cooldown_sec", fallback_cooldown),
        "payload_bytes": meta.get("payload_bytes", ""),
        "ack_timeout_ms": meta.get("ack_timeout_ms", ""),
        "e2e_timeout_ms": meta.get("e2e_timeout_ms", ""),
    }
    row.update(metrics)
    return row


def main() -> None:
    args = parse_args()

    meta_files = discover_meta_files(args.meta_dir, args.meta_file)
    rows: List[Dict[str, str]] = []

    for meta_file in meta_files:
        meta = load_json(meta_file)
        run_id = meta.get("run_id", "")
        if not run_id:
            print(f"[skip] missing run_id in metadata: {meta_file}")
            continue

        raw_file = args.raw_dir / f"{run_id}_events.csv"
        if not raw_file.exists():
            print(f"[skip] raw file not found for run_id={run_id}: {raw_file}")
            continue

        raw_rows = list(iter_raw_rows(raw_file))
        measure_sec = safe_int(meta.get("measure_sec", "0"))
        metrics = summarize_raw(raw_rows, measure_sec)
        out_row = build_row(meta, metrics, args.namespace, str(args.drain_sec), str(args.cooldown_sec))
        rows.append(out_row)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_HEADER, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"[done] wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
