from datetime import date, timedelta
from zoneinfo import ZoneInfo

from fetch_tushare import init_tushare, fetch_stock_st, DATA_DIR  # 直接复用

BJT = ZoneInfo("Asia/Shanghai")

def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def main():
    pro = init_tushare()
    start = date(2016, 1, 1)
    end = date(2025, 12, 11)  # 或今天

    for d in daterange(start, end):
        trade_date = d.strftime("%Y%m%d")
        print(f"=== {trade_date} ===")
        fetch_stock_st(pro, trade_date=trade_date)

if __name__ == "__main__":
    main()
