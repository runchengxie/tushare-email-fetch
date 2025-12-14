import os
from datetime import date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from fetch_tushare import (
    DATA_DIR,
    fetch_index_weight,
    fetch_stock_st,
    init_tushare,
    parse_index_codes,
)  # 直接复用

BJT = ZoneInfo("Asia/Shanghai")

def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def already_downloaded(path: Path) -> bool:
    """Return True if file already exists and is non-empty."""
    return path.exists() and path.stat().st_size > 0

def main():
    pro = init_tushare()
    start = date(2016, 1, 1)
    end = date(2025, 12, 11)  # 或今天

    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")
    backfill_index_weight = (
        os.getenv("BACKFILL_INDEX_WEIGHT", "false").lower()
        not in {"", "false", "0", "no"}
    )

    for d in daterange(start, end):
        trade_date = d.strftime("%Y%m%d")
        output_path = DATA_DIR / "stock_st" / f"stock_st_{trade_date}.csv"
        if already_downloaded(output_path):
            print(f"=== {trade_date} === 跳过（已存在）")
            continue

        print(f"=== {trade_date} === 拉取中")
        fetch_stock_st(pro, trade_date=trade_date)

    if backfill_index_weight:
        index_codes = parse_index_codes()
        for code in index_codes:
            safe_code = code.replace(".", "_")
            output_path = (
                DATA_DIR
                / "index_weight"
                / f"index_weight_{safe_code}_{start_str}_{end_str}.csv"
            )
            if already_downloaded(output_path):
                print(f"指数 {code} 跳过（已存在）")
                continue

            print(f"指数 {code} 拉取中：{start_str} -> {end_str}")
            fetch_index_weight(
                pro,
                index_code=code,
                start_date=start_str,
                end_date=end_str,
            )

if __name__ == "__main__":
    main()
