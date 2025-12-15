"""Shared helpers for index_weight processing."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pandas as pd


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def has_data_rows(path: Path) -> bool:
    """Return True if file exists and has at least one data row (beyond header)."""
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with path.open(newline="") as f:
            reader = csv.reader(f)
            next(reader, None)  # header
            return next(reader, None) is not None
    except Exception as exc:
        print(f"读取 {path} 失败，视为缺失：{exc}")
        return False


def expand_index_weight_daily(
    df: pd.DataFrame, trade_dates: list[date], output_path: Path
) -> int:
    """Expand index_weight snapshots into daily as-of rows (forward-fill until next rebalance)."""
    _ensure_parent_dir(output_path)

    if df.empty:
        pd.DataFrame(columns=["trade_date", "snapshot_date"]).to_csv(
            output_path, index=False
        )
        return 0

    df = df.copy()
    df["trade_date"] = (
        pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d").dt.date
    )
    trade_dates = sorted(trade_dates)
    base_cols = [col for col in df.columns if col != "trade_date"]

    frames = []
    for _, g in df.groupby("index_code"):
        g = g.sort_values("trade_date")
        snap_dates = g["trade_date"].drop_duplicates().tolist()
        for i, snap_date in enumerate(snap_dates):
            next_snap = snap_dates[i + 1] if i + 1 < len(snap_dates) else None
            segment_days = [
                d
                for d in trade_dates
                if d >= snap_date and (next_snap is None or d < next_snap)
            ]
            if not segment_days:
                continue
            snap_rows = g[g["trade_date"] == snap_date][base_cols].copy()
            snap_rows.insert(0, "snapshot_date", snap_date)
            repeated = pd.concat(
                [snap_rows.assign(trade_date=d) for d in segment_days],
                ignore_index=True,
            )
            frames.append(repeated)

    if frames:
        out_df = pd.concat(frames, ignore_index=True)
    else:
        out_df = pd.DataFrame(columns=["snapshot_date"] + base_cols + ["trade_date"])

    out_cols = ["trade_date", "snapshot_date"] + [
        col for col in base_cols if col != "snapshot_date"
    ]
    out_df = out_df[out_cols]
    out_df.to_csv(output_path, index=False)
    return len(out_df)
