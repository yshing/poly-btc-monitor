# Polymarket BTC Up/Down 5m On-Chain PnL Analyzer

分析 Polymarket 上「BTC Up or Down 5m」市場中，哪個錢包在一段時間內贏最多。

資料直接來自 Polygon 鏈上 `OrderFilled` 事件，解析每個市場的 Up/Down token 交易，計算每個 proxy wallet 的盈虧（PnL）。

## 專案結構

```
.
├── src/
│   ├── config.py              # 合約地址、API endpoint、Market 模型
│   ├── models.py              # Trade / WalletMarketPnl 資料模型
│   ├── analyzer.py            # 錢包盈虧計算核心
│   ├── main.py                # CLI 入口
│   └── fetchers/
│       ├── gamma_api.py       # 從 Gamma API 取得市場 metadata
│       ├── data_api.py        # 從 Polymarket Data API 取得最近 public trades
│       └── chain_rpc.py       # 從 Polygon RPC 抓取 OrderFilled 事件
├── data/                      # 輸出目錄（markets.json、wallet_pnl.csv）
├── requirements.txt
├── .env.example
└── README.md
```

## 安裝

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 設定（可選）

預設使用公開 RPC `https://polygon.drpc.org`。若你有自己的 RPC（Alchemy/Infura/QuickNode），可建立 `.env`：

```bash
cp .env.example .env
# 編輯 .env
POLYGON_RPC_URL=https://your-rpc-endpoint
```

如果設定的 RPC 連線失敗，程式會自動 fallback 到公開 RPC。

## 使用方式

### 1. 抓取市場 metadata

指定時間範圍，抓取所有已結算的 BTC Up/Down 5m 市場。

```bash
python -m src.main fetch --start "2026-03-25T00:00:00Z" --end "2026-03-25T06:00:00Z"
```

輸出：`data/markets.json`

時間參數支援：
- ISO-8601：`2026-03-25T00:00:00Z`
- Unix epoch seconds：`1774416000`

### 2. 分析鏈上盈虧

```bash
python -m src.main analyze --markets data/markets.json --source chain --top 20
```

輸出：
- 終端機顯示 Top 20 贏家
- `data/wallet_pnl.csv`

常用參數：
- `--source chain`：使用鏈上 OrderFilled 事件（推薦，完整歷史）
- `--source api`：使用 Polymarket Data API `/trades`（**無法按市場過濾**，只會拿到最近公開交易，通常不適合歷史分析）
- `--buffer 100`：每個市場結束時間前後多掃 100 個 blocks（預設 300）
- `--timestamps`：為每筆交易抓取 block timestamp（較慢，預設關閉）
- `--top 10`：顯示前幾名錢包

### 快速範例

```bash
# 抓取 1 小時的市場
python -m src.main fetch --start "2026-03-25T02:00:00Z" --end "2026-03-25T03:00:00Z" --output markets_hour.json

# 分析這些市場
python -m src.main analyze --markets data/markets_hour.json --source chain --top 10 --buffer 100
```

## 盈虧計算邏輯

對每個市場：
- 獲勝方 token 結算價值 = 1 USDC / share
- 落敗方 token 結算價值 = 0 USDC

對每個錢包：
```
PnL = (淨持有獲勝 token × 1)
      + 出售獲勝/落敗 token 的收入
      - 買入獲勝/落敗 token 的成本
```

注意：
- 分析的是 **proxy wallet**（Polymarket 為每個用戶建立的 Gnosis Safe proxy），不是用戶的 EOA。
- 已排除 Polymarket 合約地址（CTF Exchange、NegRiskAdapter 等）。
- 只計算已結算（resolved）市場。

## 資料來源

- 市場 metadata：`https://gamma-api.polymarket.com/events?slug=btc-updown-5m-{timestamp}`
- 鏈上交易：Polygon `OrderFilled` events
  - V1 CTF Exchange：`0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`（2026-04-28 前）
  - V2 CTF Exchange：`0xe111180000d2663c0091e4f400237545b87b996b`（2026-04-28 後）

## 效能提示

- 每個 5 分鐘市場約需 30–90 秒抓取與解碼。
- 分析大量市場時，建議先用較小的 `--buffer`（例如 50–100）。
- 抓取區塊 timestamp（`--timestamps`）會顯著變慢，因為需要額外 RPC 呼叫。

## 進階：使用 Envio HyperSync

如果你有 HyperSync API token，可設定：

```bash
HYPERSYNC_API_TOKEN=your-token
```

目前程式以 RPC 模式為主；未來可擴充 HyperSync 模式以大幅提升大量市場的抓取速度。

## 免責聲明

這是教育與研究用途的工具。計算結果基於鏈上公開數據與市場結算價格，不保證完全精確（例如未考慮手續費、部分贖回、轉帳等複雜情況），請自行驗證後使用。
