# Tushare 数据搬运工 (Email Fetch)

让 GitHub Actions 给你当免费劳动力：定时拉取 Tushare 数据，存入仓库，顺手给你发一封带 CSV 附件的汇总邮件。

## 它能干啥 (What it grabs)

- **抓 ST 股 (`stock_st`)**：基于指定的 `trade_date`（默认是北京时间当天）。*帮你看看今天谁在雷区蹦迪。*
- **抓指数权重 (`index_weight`)**：针对可配置的指数代码（默认套餐：沪深300 `000300.SH` 和中证500 `000905.SH`），默认抓 **2016-01-01 至今（北京时间）** 的成分股权重，可用环境变量覆盖。

## 本地手动挡 (Local run)

如果你非要在本地跑（或者调试），请按以下步骤操作：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 别忘了先把 Token 设好
export TUSHARE_TOKEN=your_token_here
python scripts/fetch_tushare.py
```

本地测试时的可选环境变量（以此来假装是 Actions 在跑）：

- `TRADE_DATE=YYYYMMDD`
- `INDEX_START_DATE=YYYYMMDD`（默认 `20160101`）
- `INDEX_END_DATE=YYYYMMDD`（默认对齐到北京时间“今天”）
- `INDEX_CODES=000300.SH,000905.SH`
- 邮件配置 (可选)：`EMAIL_TO`, `EMAIL_FROM`, `SMTP_SERVER`, `SMTP_PORT` (默认 587), `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_STARTTLS` (设为 `false` 可禁用)。

## 补历史数据 (Backfill)

脚本：`scripts/backfill.py`

- **断点续跑**：目标 CSV 已存在且非空就跳过，避免重复下载。
- **交易日过滤**：先拉取交易日历，只对交易日请求 `stock_st`，减少空数据浪费配额。
- **空文件重拉**：若已有 CSV 只有表头（0 行数据），会视为未下载并重新抓取一次。
- **自定义日期范围**：通过环境变量覆盖默认区间（默认 `2016-01-01` 起算到“今天”）：
  - `BACKFILL_START_DATE=YYYYMMDD`
  - `BACKFILL_END_DATE=YYYYMMDD`
  未指定时脚本会提示并使用默认值。
- **是否顺带补指数权重**：默认会抓 `index_weight`；如不需要，可设置 `BACKFILL_INDEX_WEIGHT=false`。时间范围与 `INDEX_CODES` 同 backfill，共用跳过逻辑。

示例：

```bash
# 只补 ST，默认 2016-01-01 到今天，已存在文件跳过
TUSHARE_TOKEN=... python scripts/backfill.py

# 补 2020-2022 的 ST + 指数权重（默认就会抓指数；如不需要可设 BACKFILL_INDEX_WEIGHT=false）
TUSHARE_TOKEN=... BACKFILL_START_DATE=20200101 BACKFILL_END_DATE=20221231 \
INDEX_CODES=000300.SH,000905.SH python scripts/backfill.py
```

## GitHub Actions 调教指南

1. **喂点 Secrets (配置密钥)**：
   进入 Settings → Secrets and variables → Actions，添加以下内容：
   - `TUSHARE_TOKEN` (**必填**，没这个啥也干不了)
   - 想要邮件通知？那就把这些也填上：`EMAIL_TO`, `EMAIL_FROM`, `SMTP_SERVER`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_STARTTLS` (可选)。
   - 想换换口味抓别的指数？配置 `INDEX_CODES`。

2. **工作流配置** (`.github/workflows/tushare.yml`)：
   - **定时任务**：周一到周五 UTC 01:25 (也就是北京时间 09:25，刚好赶在开盘前喝口水的时候)。
   - **手动触发**：Actions → `tushare-daily` → `Run workflow` (支持手动覆盖日期，以此来修补历史数据)。

3. **自动存档**：
   工作流会利用 `GITHUB_TOKEN` 把抓到的 `data/` 自动 Commit 回仓库。*数据落袋为安。*

## 碎碎念 (Notes)

- **关于积分**：Tushare 不是慈善家。根据文档，你的 Token 积分得够格才行：`stock_st` 需要 3000 分，`index_weight` 需要 2000 分。*积分不够，神仙难救。*
- **关于邮件**：如果 `EMAIL_TO` 或 `SMTP_SERVER` 没填，脚本会很识趣地跳过发邮件步骤，只抓数据。
- **关于产出**：CSV 文件会乖乖躺在 `data/stock_st/` 和 `data/index_weight/` 目录下，文件名会以此日期和代码范围命名，强迫症友好。
- **稳一点**：抓取增加了简单重试/回退逻辑，碰到网络抖动会自动再试几次。
