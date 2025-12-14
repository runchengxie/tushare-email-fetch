#!/usr/bin/env python3
"""Fetch daily Tushare data and optionally email a summary."""

from __future__ import annotations

import os
import smtplib
import time
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable, List
from zoneinfo import ZoneInfo

import pandas as pd
import tushare as ts

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


def ensure_data_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def trade_date_today() -> str:
    today = datetime.now(tz=BJT).date()
    return today.strftime("%Y%m%d")


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
    pro: ts.pro_api, index_code: str, start_date: str, end_date: str
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
    start_date = optional_env("INDEX_START_DATE", DEFAULT_INDEX_START_DATE)
    end_date = optional_env("INDEX_END_DATE", today_str)

    pro = init_tushare()
    results: List[FetchResult] = []

    results.append(fetch_stock_st(pro, trade_date=trade_date))

    index_codes = parse_index_codes()
    for code in index_codes:
        results.append(
            fetch_index_weight(
                pro, index_code=code, start_date=start_date, end_date=end_date
            )
        )

    summary = summarize_results(results)
    print(summary)
    send_email(summary, [result.path for result in results])


if __name__ == "__main__":
    main()
