#!/usr/bin/env python3
"""Fetch daily Tushare data and optionally email a summary."""

from __future__ import annotations

import os
import smtplib
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable, List
from zoneinfo import ZoneInfo

import pandas as pd
import tushare as ts
from index_weight_utils import expand_index_weight_daily, has_data_rows

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
BJT = ZoneInfo("Asia/Shanghai")
DEFAULT_INDEX_START_DATE = "20160101"


@dataclass
class FetchResult:
    label: str
    path: Path
    rows: int


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def optional_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value else default


def bool_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.lower() not in {"", "false", "0", "no"}


def ensure_data_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_yyyymmdd(raw: str) -> date:
    return datetime.strptime(raw, "%Y%m%d").date()


def trade_date_today() -> str:
    today = datetime.now(tz=BJT).date()
    return today.strftime("%Y%m%d")


def index_weight_raw_path(index_code: str) -> Path:
    safe_code = index_code.replace(".", "_")
    return DATA_DIR / "index_weight" / f"index_weight_{safe_code}.csv"


def index_weight_daily_path(index_code: str) -> Path:
    safe_code = index_code.replace(".", "_")
    return DATA_DIR / "index_weight_daily" / f"index_weight_daily_{safe_code}.csv"


def _min_max_trade_date(df: pd.DataFrame) -> tuple[date, date]:
    trade_dates = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d").dt.date
    return trade_dates.min(), trade_dates.max()


def init_tushare() -> ts.pro_api:
    token = required_env("TUSHARE_TOKEN")
    ts.set_token(token)
    return ts.pro_api()

def fetch_with_retry(fn, *, label: str, retries: int = 3, base_delay: float = 1.0):
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt == retries:
                print(f"{label} failed after {retries} attempts; re-raising.")
                raise
            delay = base_delay * attempt
            print(
                f"{label} failed (attempt {attempt}/{retries}): {exc}. "
                f"Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)


def load_trade_dates(pro: ts.pro_api, start: date, end: date) -> list[date]:
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


def fetch_stock_st(pro: ts.pro_api, trade_date: str) -> FetchResult:
    print(f"Fetching stock_st for trade_date={trade_date}")
    df = fetch_with_retry(
        lambda: pro.stock_st(trade_date=trade_date),
        label=f"stock_st {trade_date}",
    )
    output_path = DATA_DIR / "stock_st" / f"stock_st_{trade_date}.csv"
    ensure_data_dir(output_path)
    df.to_csv(output_path, index=False)
    print(f"Saved stock_st to {output_path} ({len(df)} rows)")
    return FetchResult(label="stock_st", path=output_path, rows=len(df))


def fetch_index_weight(
    pro: ts.pro_api,
    index_code: str,
    start_date: str,
    end_date: str,
    output_path: Path | None = None,
) -> FetchResult:
    print(
        f"Fetching index_weight for index_code={index_code} "
        f"start_date={start_date} end_date={end_date}"
    )
    df = fetch_with_retry(
        lambda: pro.index_weight(
            index_code=index_code, start_date=start_date, end_date=end_date
        ),
        label=f"index_weight {index_code} {start_date}->{end_date}",
    )
    safe_code = index_code.replace(".", "_")
    if output_path is None:
        output_path = (
            DATA_DIR
            / "index_weight"
            / f"index_weight_{safe_code}_{start_date}_{end_date}.csv"
        )
    ensure_data_dir(output_path)
    df.to_csv(output_path, index=False)
    print(f"Saved index_weight to {output_path} ({len(df)} rows)")
    return FetchResult(
        label=f"index_weight {index_code}", path=output_path, rows=len(df)
    )


def refresh_index_weight(
    pro: ts.pro_api,
    index_code: str,
    *,
    default_start: str,
    end_date: str,
    force_full_refresh: bool,
    generate_daily: bool,
) -> list[FetchResult]:
    """Incrementally pull index_weight and optionally expand to daily."""
    raw_path = index_weight_raw_path(index_code)
    ensure_data_dir(raw_path)

    existing_df: pd.DataFrame | None = None
    if not force_full_refresh and has_data_rows(raw_path):
        existing_df = pd.read_csv(raw_path)
    elif force_full_refresh and has_data_rows(raw_path):
        print(f"INDEX_FULL_REFRESH=true，忽略已存在文件 {raw_path}")

    start_date = default_start
    last_trade: date | None = None
    if existing_df is not None and not existing_df.empty:
        _, last_trade = _min_max_trade_date(existing_df)
        start_date = (last_trade + timedelta(days=1)).strftime("%Y%m%d")

    start_dt = parse_yyyymmdd(start_date)
    end_dt = parse_yyyymmdd(end_date)

    if start_dt > end_dt and existing_df is None:
        print(
            f"index_weight {index_code} 未找到抓取区间，start_date {start_date} 晚于 end_date {end_date}"
        )
        return []

    results: list[FetchResult] = []
    rows_added = 0
    final_df: pd.DataFrame

    if start_dt > end_dt and existing_df is not None:
        print(
            f"index_weight {index_code} 已是最新，最后 trade_date="
            f"{last_trade.strftime('%Y%m%d') if last_trade else 'unknown'}"
        )
        final_df = existing_df
    else:
        print(
            f"Fetching index_weight for {index_code} start_date={start_date} end_date={end_date}"
        )
        df_new = fetch_with_retry(
            lambda: pro.index_weight(
                index_code=index_code, start_date=start_date, end_date=end_date
            ),
            label=f"index_weight {index_code} {start_date}->{end_date}",
        )

        if existing_df is not None and not force_full_refresh:
            rows_added = len(df_new)
            if df_new.empty:
                print(
                    f"index_weight {index_code} 无新增数据（起始 {start_date}），保持现有文件。"
                )
                final_df = existing_df
            else:
                final_df = pd.concat([existing_df, df_new], ignore_index=True)
                final_df.drop_duplicates(
                    subset=["index_code", "con_code", "trade_date"], inplace=True
                )
        else:
            final_df = df_new.copy()
            rows_added = len(df_new)

        if not final_df.empty:
            final_df = final_df.sort_values(
                ["trade_date", "index_code", "con_code"]
            ).reset_index(drop=True)

        should_write = force_full_refresh or existing_df is None or rows_added > 0
        if should_write:
            final_df.to_csv(raw_path, index=False)
            rows_for_result = (
                rows_added if existing_df is not None and not force_full_refresh else len(final_df)
            )
            print(
                f"Saved index_weight to {raw_path} "
                f"({len(final_df)} rows total, +{rows_added} new)"
            )
            results.append(
                FetchResult(
                    label=f"index_weight {index_code}",
                    path=raw_path,
                    rows=rows_for_result,
                )
            )

    if generate_daily:
        daily_path = index_weight_daily_path(index_code)
        needs_daily = force_full_refresh or rows_added > 0 or not has_data_rows(
            daily_path
        )
        if not needs_daily:
            return results

        if final_df.empty:
            daily_start = parse_yyyymmdd(default_start)
            daily_end = end_dt
        else:
            min_trade, max_trade = _min_max_trade_date(final_df)
            daily_start = min(min_trade, parse_yyyymmdd(default_start))
            daily_end = max(max_trade, end_dt)

        if daily_start <= daily_end:
            trade_dates = load_trade_dates(pro, daily_start, daily_end)
            rows_daily = expand_index_weight_daily(final_df, trade_dates, daily_path)
            results.append(
                FetchResult(
                    label=f"index_weight_daily {index_code}",
                    path=daily_path,
                    rows=rows_daily,
                )
            )

    return results

def summarize_results(results: Iterable[FetchResult]) -> str:
    lines = ["Tushare fetch summary:"]
    for result in results:
        relative_path = result.path.relative_to(PROJECT_ROOT)
        lines.append(f"- {result.label}: {result.rows} rows -> {relative_path}")
    return "\n".join(lines)


def parse_index_codes() -> List[str]:
    raw = optional_env("INDEX_CODES")
    if not raw:
        return ["000300.SH", "000905.SH"]
    return [code.strip() for code in raw.split(",") if code.strip()]


def send_email(summary: str, attachments: List[Path]) -> None:
    to_addr = optional_env("EMAIL_TO")
    smtp_server = optional_env("SMTP_SERVER")
    smtp_user = optional_env("SMTP_USERNAME")
    smtp_password = optional_env("SMTP_PASSWORD")

    if not to_addr or not smtp_server:
        print("EMAIL_TO or SMTP_SERVER not provided; skipping email sending.")
        return

    from_addr = optional_env("EMAIL_FROM", smtp_user or "tushare-bot@example.com")
    smtp_port = int(optional_env("SMTP_PORT", "587"))
    use_starttls = optional_env("SMTP_STARTTLS", "true").lower() != "false"
    subject_prefix = optional_env("EMAIL_SUBJECT_PREFIX", "[tushare]")

    msg = EmailMessage()
    msg["Subject"] = f"{subject_prefix} Daily Tushare fetch"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(summary)

    for path in attachments:
        data = path.read_bytes()
        msg.add_attachment(
            data,
            maintype="text",
            subtype="csv",
            filename=path.name,
        )

    print(f"Sending email to {to_addr} via {smtp_server}:{smtp_port}")
    with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as smtp:
        if use_starttls:
            smtp.starttls()
        if smtp_user:
            if not smtp_password:
                raise SystemExit("SMTP_USERNAME provided but SMTP_PASSWORD missing.")
            smtp.login(smtp_user, smtp_password)
        smtp.send_message(msg)
    print("Email sent successfully.")


def main() -> None:
    today_str = trade_date_today()
    trade_date = optional_env("TRADE_DATE", today_str)
    index_start_date = optional_env("INDEX_START_DATE", DEFAULT_INDEX_START_DATE)
    end_date = optional_env("INDEX_END_DATE", today_str)
    force_full_refresh = bool_env("INDEX_FULL_REFRESH", False)
    generate_daily = bool_env("INDEX_WEIGHT_DAILY", True)

    pro = init_tushare()
    results: List[FetchResult] = []

    results.append(fetch_stock_st(pro, trade_date=trade_date))

    index_codes = parse_index_codes()
    for code in index_codes:
        results.extend(
            refresh_index_weight(
                pro,
                index_code=code,
                default_start=index_start_date,
                end_date=end_date,
                force_full_refresh=force_full_refresh,
                generate_daily=generate_daily,
            )
        )

    summary = summarize_results(results)
    print(summary)
    send_email(summary, [result.path for result in results])


if __name__ == "__main__":
    main()
