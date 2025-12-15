import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

from fetch_tushare import (
    DATA_DIR,
    bool_env,
    refresh_index_weight,
    fetch_stock_st,
    init_tushare,
    parse_index_codes,
    fetch_with_retry,
)  # 直接复用
from index_weight_utils import has_data_rows

BJT = ZoneInfo("Asia/Shanghai")

def parse_date_env(var_name: str, default: date) -> date:
    raw = os.getenv(var_name)
    if not raw:
        return default
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError as exc:
        raise SystemExit(f"{var_name} 格式错误（需 YYYYMMDD）：{exc}")

def load_trade_dates(pro, start: date, end: date) -> list[date]:
    """Fetch trading days to avoid hitting non-trading days."""
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")
    cal = fetch_with_retry(
        lambda: pro.trade_cal(exchange="SSE", start_date=start_str, end_date=end_str),
        label=f"trade_cal {start_str}->{end_str}",
    )
    trading_days = cal[cal["is_open"] == 1]["cal_date"]
    return [
        datetime.strptime(str(cal_date), "%Y%m%d").date() for cal_date in trading_days
    ]

def main():
    pro = init_tushare()
    today = datetime.now(tz=BJT).date()
    default_start = date(2016, 1, 1)
    default_end = today

    start = parse_date_env("BACKFILL_START_DATE", default_start)
    end = parse_date_env("BACKFILL_END_DATE", default_end)
    if start > end:
        raise SystemExit("BACKFILL_START_DATE 不得晚于 BACKFILL_END_DATE")

    if not os.getenv("BACKFILL_START_DATE") or not os.getenv("BACKFILL_END_DATE"):
        print(
            "未指定日期范围，使用默认值：2016-01-01 起（按交易日过滤，从当年第一个交易日起算）直到今天。"
        )

    trade_dates = load_trade_dates(pro, start, end)
    total_days = len(trade_dates)
    if total_days == 0:
        print("未找到交易日，检查日期范围或交易所代码。")
        return

    backfill_env = os.getenv("BACKFILL_INDEX_WEIGHT")
    backfill_index_weight = (
        True
        if backfill_env is None
        else backfill_env.lower() not in {"", "false", "0", "no"}
    )
    daily_index_weight_env = os.getenv("INDEX_WEIGHT_DAILY")
    backfill_index_weight_daily = (
        True
        if daily_index_weight_env is None
        else daily_index_weight_env.lower() not in {"", "false", "0", "no"}
    )
    force_full_refresh = bool_env("INDEX_FULL_REFRESH", False)

    for idx, d in enumerate(trade_dates, start=1):
        trade_date = d.strftime("%Y%m%d")
        output_path = DATA_DIR / "stock_st" / f"stock_st_{trade_date}.csv"
        if has_data_rows(output_path):
            print(f"[{idx}/{total_days}] {trade_date} 跳过（已存在）")
            continue

        if output_path.exists():
            print(f"[{idx}/{total_days}] {trade_date} 发现空/无数据文件，重拉")
        else:
            print(f"[{idx}/{total_days}] {trade_date} 拉取中")
        fetch_stock_st(pro, trade_date=trade_date)

    if backfill_index_weight:
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        index_codes = parse_index_codes()
        total_index = len(index_codes)
        for idx, code in enumerate(index_codes, start=1):
            print(
                f"[{idx}/{total_index}] 指数 {code} 回填/增量：{start_str} -> {end_str}"
            )
            refresh_index_weight(
                pro,
                index_code=code,
                default_start=start_str,
                end_date=end_str,
                force_full_refresh=force_full_refresh,
                generate_daily=backfill_index_weight_daily,
            )

if __name__ == "__main__":
    main()
