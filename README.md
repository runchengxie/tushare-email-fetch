# Tushare 数据搬运（电子邮箱推送版）

通过 GitHub Actions 定时拉取 Tushare 数据，存入仓库，顺手给你发一封带 CSV 附件的汇总邮件。

## 功能简介

* 抓 ST 股 (`stock_st`)：基于指定的 `trade_date`（默认是北京时间当天）。*帮你看看今天谁在雷区蹦迪。*

* 抓指数权重 (`index_weight`)：针对可配置的指数代码（默认套餐：沪深300 `000300.SH` 和中证500 `000905.SH`）。默认会读取 `data/index_weight/index_weight_<code>.csv` 的最后一个 `trade_date`，从下一天开始增量拉取；首次没有文件时，从2016-01-01 至今（北京时间） 抓全量。

* 需要无视本地缓存可设 `INDEX_FULL_REFRESH=true`。会同时生成日频展开版到 `data/index_weight_daily/index_weight_daily_<code>.csv`（若不需要可设 `INDEX_WEIGHT_DAILY=false`）。

## 参数设置

如果要在本地运行（或者调试），请按以下步骤操作：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 设置Tushare Token
export TUSHARE_TOKEN=your_token_here
python scripts/fetch_tushare.py
```

本地测试和Github Action运行时的可选环境变量：

* `TRADE_DATE=YYYYMMDD`

* `INDEX_START_DATE=YYYYMMDD`（默认 `20160101`，首次建库或 `INDEX_FULL_REFRESH=true` 时使用）

* `INDEX_END_DATE=YYYYMMDD`（默认对齐到北京时间“今天”）

* `INDEX_FULL_REFRESH=true`（可选，强制从 `INDEX_START_DATE` 开始全量重抓并覆盖本地文件）

* `INDEX_WEIGHT_DAILY=false`（可选，跳过生成日频展开）

* `INDEX_CODES=000300.SH,000905.SH` 设置想要抓取的指数成分的指数代码列表，逗号分隔（默认是沪深300和中证500）

* 邮件配置 (可选)：`EMAIL_TO`, `EMAIL_FROM`, `SMTP_SERVER`, `SMTP_PORT` (默认 587), `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_STARTTLS` (设为 `false` 可禁用)。

## 本地运行批量下载历史数据

脚本：`scripts/backfill.py`

* 断点续跑：目标 CSV 已存在且非空就跳过，避免重复下载。

* 交易日过滤：先拉取交易日历，只对交易日请求 `stock_st`，减少空数据浪费配额。

* 空文件重拉：若已有 CSV 只有表头（0 行数据），会视为未下载并重新抓取一次（该功能的设计思路是，当加入重试机制后，该该能可以填补那些因为网络抖动而下载失败的数据，并进行一次新的下载尝试）

* 自定义日期范围：通过环境变量覆盖默认区间（默认 `2016-01-01` 起算到脚本运行的当天）：

* `BACKFILL_START_DATE=YYYYMMDD`

* `BACKFILL_END_DATE=YYYYMMDD`

  * 未指定时脚本会提示并使用默认值。

  * 指数权重：

* 默认会抓 `index_weight`；如不需要，可设置 `BACKFILL_INDEX_WEIGHT=false`。

* 默认同时生成日频展开版（按交易日向前填充到下一个调仓日），输出到 `data/index_weight_daily/`；如不需要，可设置 `INDEX_WEIGHT_DAILY=false`。

示例：

```bash
# 只补 ST，默认 2016-01-01 到今天，已存在文件跳过
TUSHARE_TOKEN=... BACKFILL_INDEX_WEIGHT=false python scripts/backfill.py

# 补 2020-2022 的 ST + 指数权重 + 日频展开（默认就会抓指数+日频）
TUSHARE_TOKEN=... BACKFILL_START_DATE=20200101 BACKFILL_END_DATE=20221231 \
INDEX_CODES=000300.SH,000905.SH python scripts/backfill.py
```

## GitHub Actions 使用指南

1.提供 Secrets (配置密钥)：进入 Settings → Secrets and variables → Actions，添加以下内容：

* 你的 Tushare Token `TUSHARE_TOKEN` 。

* 邮件通知对应参数：`EMAIL_TO`, `EMAIL_FROM`, `SMTP_SERVER`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_STARTTLS` 

* 设置想要抓取的指数成分的指数代码列表 `INDEX_CODES`。

2.工作流配置 (`.github/workflows/tushare.yml`)：

* 定时任务：周一到周五 UTC 01:25 (也就是北京时间 09:25，对应A股当日开盘前)。

* 手动触发：Actions → `tushare-daily` → `Run workflow` (支持手动覆盖日期，以此来修补历史数据)。

3.自动存档：工作流会利用 `GITHUB_TOKEN` 把抓到的 `data/` 自动 Commit 回仓库。

## 备注

* 关于积分：Tushare 积分需要3000分或以上，其中，`stock_st` 需要 3000 分，`index_weight` 需要 2000 分。

* 关于邮件：如果 `EMAIL_TO` 或 `SMTP_SERVER` 没填，脚本依然会运行并抓取数据，但是无法转发运行结果。

* 关于产出：CSV 会放在 `data/stock_st/stock_st_<date>.csv`、`data/index_weight/index_weight_<code>.csv`，以及日频的 `data/index_weight_daily/index_weight_daily_<code>.csv`。

* 抓取增加了简单重试/回退逻辑，碰到网络抖动会自动再试几次。
