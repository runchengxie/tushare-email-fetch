#!/usr/bin/env python3
"""列出 data/stock_st 目录下只有表头、没有数据行的 CSV。"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from fetch_tushare import DATA_DIR, init_tushare, load_trade_dates


def has_zero_data_rows(path: Path) -> bool:
    """Return True if file exists but has no data rows beyond the header."""
    if not path.exists() or path.stat().st_size == 0:
        return False

    with path.open(newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        return next(reader, None) is None


def _extract_trade_date(path: Path) -> datetime.date | None:
    stem = path.stem  # e.g. stock_st_20160101
    if "_" not in stem:
        return None
    candidate = stem.split("_")[-1]
    try:
        return datetime.strptime(candidate, "%Y%m%d").date()
    except ValueError:
        return None


def main() -> None:
    stock_dir = DATA_DIR / "stock_st"
    if not stock_dir.exists():
        raise SystemExit(f"目录不存在：{stock_dir}")

    candidates = sorted(stock_dir.glob("stock_st_*.csv"))
    dated_candidates: list[tuple[Path, str]] = []
    for path in candidates:
        parsed = _extract_trade_date(path)
        if parsed:
            dated_candidates.append((path, parsed.strftime("%Y%m%d")))

    if not dated_candidates:
        print(f"共检查 0 个文件（仅包含 stock_st_*.csv）。")
        return

    min_date = min(datetime.strptime(dt, "%Y%m%d").date() for _, dt in dated_candidates)
    max_date = max(datetime.strptime(dt, "%Y%m%d").date() for _, dt in dated_candidates)
    pro = init_tushare()
    trading_days = load_trade_dates(pro, min_date, max_date)
    trading_set = {d.strftime("%Y%m%d") for d in trading_days}

    filtered = [(path, dt) for path, dt in dated_candidates if dt in trading_set]
    skipped_non_trading = [path for path, dt in dated_candidates if dt not in trading_set]

    zero_rows: list[Path] = []
    errors: list[tuple[Path, str]] = []

    for path, trade_date in filtered:
        if not path.is_file():
            continue
        try:
            if has_zero_data_rows(path):
                zero_rows.append(path)
        except Exception as exc:  # pragma: no cover - defensive logging
            errors.append((path, str(exc)))

    print(
        f"共检查 {len(filtered)} 个交易日文件（仅包含 stock_st_*.csv，跳过非交易日 {len(skipped_non_trading)} 个）。"
    )
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
