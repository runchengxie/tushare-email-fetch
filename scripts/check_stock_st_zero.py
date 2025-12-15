#!/usr/bin/env python3
"""列出 data/stock_st 目录下只有表头、没有数据行的 CSV。"""

from __future__ import annotations

import csv
from pathlib import Path

from fetch_tushare import DATA_DIR


def has_zero_data_rows(path: Path) -> bool:
    """Return True if file exists but has no data rows beyond the header."""
    if not path.exists() or path.stat().st_size == 0:
        return False

    with path.open(newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        return next(reader, None) is None


def main() -> None:
    stock_dir = DATA_DIR / "stock_st"
    if not stock_dir.exists():
        raise SystemExit(f"目录不存在：{stock_dir}")

    candidates = sorted(stock_dir.glob("stock_st_*.csv"))
    zero_rows: list[Path] = []
    errors: list[tuple[Path, str]] = []

    for path in candidates:
        if not path.is_file():
            continue
        try:
            if has_zero_data_rows(path):
                zero_rows.append(path)
        except Exception as exc:  # pragma: no cover - defensive logging
            errors.append((path, str(exc)))

    print(f"共检查 {len(candidates)} 个文件（仅包含 stock_st_*.csv）。")
    if zero_rows:
        print("发现 0 行数据的文件：")
        for path in zero_rows:
            print(f"- {path.relative_to(DATA_DIR)}")
    else:
        print("未发现 0 行数据的文件。")

    if errors:
        print("\n读取失败的文件：")
        for path, msg in errors:
            print(f"- {path.relative_to(DATA_DIR)} -> {msg}")


if __name__ == "__main__":
    main()
