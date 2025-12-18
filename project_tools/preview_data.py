#!/usr/bin/env python3
"""Create lightweight text previews of CSV files under data/."""

from __future__ import annotations

import argparse
from collections import deque
from itertools import islice
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_PREVIEW_DIRNAME = "preview_data"
DEFAULT_HEAD_ROWS = 10
DEFAULT_TAIL_ROWS = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate txt previews for data/index_weight, data/index_weight_daily, "
            "and the first/last stock_st CSVs."
        )
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Path to the data directory (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--preview-dirname",
        default=DEFAULT_PREVIEW_DIRNAME,
        help="Name of the folder (inside data/) where previews are written.",
    )
    parser.add_argument(
        "--head-rows",
        type=int,
        default=DEFAULT_HEAD_ROWS,
        help=f"Rows to include from the top of each file (default: {DEFAULT_HEAD_ROWS}).",
    )
    parser.add_argument(
        "--tail-rows",
        type=int,
        default=DEFAULT_TAIL_ROWS,
        help=(
            "Rows to include from the bottom for stock_st previews "
            f"(default: {DEFAULT_TAIL_ROWS})."
        ),
    )
    return parser.parse_args()


def read_header_and_head_rows(csv_path: Path, limit: int) -> tuple[str | None, list[str]]:
    header = None
    rows: list[str] = []
    with csv_path.open("r", encoding="utf-8", errors="replace") as fh:
        header_raw = fh.readline()
        if header_raw:
            header = header_raw.rstrip("\n")
        for line in islice(fh, limit):
            rows.append(line.rstrip("\n"))
    return header, rows


def read_tail_rows(csv_path: Path, limit: int) -> list[str]:
    tail = deque[str](maxlen=limit)
    with csv_path.open("r", encoding="utf-8", errors="replace") as fh:
        next(fh, None)  # skip header
        for line in fh:
            tail.append(line.rstrip("\n"))
    return list(tail)


def write_preview(preview_dir: Path, csv_path: Path, lines: Iterable[str]) -> Path:
    preview_path = preview_dir / f"preview_{csv_path.stem}.txt"
    content = "\n".join(lines).rstrip("\n") + "\n"
    preview_path.write_text(content, encoding="utf-8")
    return preview_path


def build_head_preview(
    csv_path: Path, header: str | None, rows: list[str], limit: int
) -> list[str]:
    row_count = min(limit, len(rows))
    lines = [
        f"Source: {csv_path.name}",
        f"Header + first {row_count} row(s)",
    ]
    if header:
        lines += ["", "Header:", header]
    if rows:
        lines += ["", "Rows:"]
        lines.extend(rows[:row_count])
    return lines


def build_head_tail_preview(
    csv_path: Path,
    header: str | None,
    head_rows: list[str],
    tail_rows: list[str],
    head_limit: int,
    tail_limit: int,
) -> list[str]:
    head_count = min(head_limit, len(head_rows))
    tail_count = min(tail_limit, len(tail_rows))
    lines = [
        f"Source: {csv_path.name}",
        f"Header + first {head_count} row(s) and last {tail_count} row(s)",
    ]
    if header:
        lines += ["", "Header:", header]
    if head_rows:
        lines += ["", f"First {head_count} row(s):"]
        lines.extend(head_rows[:head_count])
    if tail_rows:
        lines += ["", f"Last {tail_count} row(s):"]
        lines.extend(tail_rows[-tail_count:])
    return lines


def preview_directory_head_only(source_dir: Path, preview_dir: Path, head_rows: int) -> int:
    if not source_dir.is_dir():
        print(f"Skipping missing directory {source_dir}")
        return 0

    count = 0
    for csv_path in sorted(source_dir.glob("*.csv")):
        header, rows = read_header_and_head_rows(csv_path, head_rows)
        lines = build_head_preview(csv_path, header, rows, head_rows)
        write_preview(preview_dir, csv_path, lines)
        count += 1
    return count


def preview_stock_st(
    source_dir: Path, preview_dir: Path, head_rows: int, tail_rows: int
) -> int:
    if not source_dir.is_dir():
        print(f"Skipping missing directory {source_dir}")
        return 0

    csv_files = sorted(source_dir.glob("*.csv"))
    if not csv_files:
        return 0

    targets = [csv_files[0]]
    if len(csv_files) > 1:
        targets.append(csv_files[-1])

    count = 0
    for csv_path in targets:
        header, head = read_header_and_head_rows(csv_path, head_rows)
        tail = read_tail_rows(csv_path, tail_rows)
        lines = build_head_tail_preview(csv_path, header, head, tail, head_rows, tail_rows)
        write_preview(preview_dir, csv_path, lines)
        count += 1
    return count


def main() -> None:
    args = parse_args()
    if args.head_rows < 1:
        raise SystemExit("head-rows must be >= 1")
    if args.tail_rows < 1:
        raise SystemExit("tail-rows must be >= 1")

    data_dir = args.data_dir.resolve()
    if not data_dir.is_dir():
        raise SystemExit(f"Data directory not found: {data_dir}")

    preview_dir = (data_dir / args.preview_dirname).resolve()
    preview_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    total += preview_directory_head_only(data_dir / "index_weight", preview_dir, args.head_rows)
    total += preview_directory_head_only(
        data_dir / "index_weight_daily", preview_dir, args.head_rows
    )
    total += preview_stock_st(data_dir / "stock_st", preview_dir, args.head_rows, args.tail_rows)

    print(f"Wrote {total} preview file(s) to {preview_dir}")


if __name__ == "__main__":
    main()
