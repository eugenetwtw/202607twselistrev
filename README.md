# 台股每月營收 · PEAD 做多異常掃描

抓取上市／上櫃每月營收，回補歷史，計算**只做多**異常訊號（極端成長 + 驚喜／轉折），網頁檢視並可下載 CSV。

## 快速開始

```bash
./start.sh
```

第一次會自動建立 `.venv`、安裝依賴並啟動；之後同一指令即可。

開啟 <http://127.0.0.1:5051>（預設埠；也可用 `PORT=5050 ./start.sh`）

1. 按 **回補 24 個月歷史**（首次建議；約數分鐘）
2. 或 **抓取最新營收**（OpenAPI + 缺邊用 MOPS 補齊）
3. 在 **做多異常** 分頁看 E／S 名單，可下載 CSV

### 常用選項

| 變數 | 預設 | 說明 |
|------|------|------|
| `PORT` | `5051` | 監聽埠 |
| `HOST` | `127.0.0.1` | 綁定位址（對外可設 `0.0.0.0`） |
| `OPEN_BROWSER` | `1` | 設 `0` 則不自動開瀏覽器 |
| `TWSE_KILL_PORT` | `1` | 埠被佔用時是否結束舊行程 |

範例：

```bash
PORT=8080 OPEN_BROWSER=0 ./start.sh
HOST=0.0.0.0 ./start.sh
```

手動方式（不經腳本）：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

`./run.sh` 等同 `./start.sh`。

### 訪問分析（GLM + Brave + Firecrawl）

在 Long PEAD 名單列上按 **訪問分析**（每次呼叫會重新搜尋網頁並寫入 DB，含時間戳）：

| 權重 | 意義 |
|------|------|
| PEAD 權重 | 月營收訊號有多像「可延續的 PEAD 驚喜」（建設交屋、影視一次性事件等會被降權） |
| 供應鏈權重 | 是否在近期走強之美股 S&P/Nasdaq 電子／AI 供應鏈 |
| 綜合權重 | 兩者綜合可操作權重 |

金鑰放在本機 `.env`（見 `.env.example`，勿提交 git）：

```
GLM_API_KEY=…
BRAVE_API_KEY=…
FIRECRAWL_API_KEY=…
GLM_MODEL=glm-4.6
```

完整報告在第三分頁 **訪問分析報告**；名單上另有權重欄與評估摘要。

## 資料來源

| 來源 | 用途 |
|------|------|
| 證交所 OpenAPI `t187ap05_L` | 上市最新月 |
| 櫃買 OpenAPI `mopsfin_t187ap05_O` | 上櫃最新月 |
| 公開資訊觀測站 MOPS 彙總 HTML | 指定年月歷史、補齊雙市場 |

金額單位：**新台幣千元**。

## 做多異常（v1）

### E · 極端成長

- 同產業 ≥ 5 家  
- 產業內 YoY 百分位 ≥ 90  
- YoY &gt; 0  
- 當月營收 ≥ 門檻（預設 50,000 千元）

### S · 驚喜／轉折（S1 + S2）

- **S1** 預期 = 同產業當月 YoY **中位數**；驚喜 = 實際 − S1  
- **S2** 預期 = 該公司近 12 個月 YoY **中位數**（至少 6 個月歷史）；驚喜 = 實際 − S2  
- 綜合驚喜平均 ≥ 15 百分點，或驚喜百分位 ≥ 85  
- 或 **由負轉正**／**連兩月 YoY 加速**（且 YoY &gt; 0）

**不做空。**

## 主要 API

| 方法 | 路徑 | 說明 |
|------|------|------|
| POST | `/api/fetch` | 最新月 + 補齊 |
| POST | `/api/backfill` | 回補 N 個月（預設 24） |
| GET | `/api/signals` | 做多異常 JSON |
| GET | `/api/signals.csv` | 異常 CSV（`which=extreme_growth\|surprise_turnaround\|all_long`） |
| GET | `/api/revenue` | 營收明細 |
| GET | `/api/download-db` | 下載 SQLite |

## 專案結構

```
start.sh              # 一鍵啟動（venv + 依賴 + 網頁）
run.sh                # start.sh 別名
app.py
analysis_service.py   # Brave + Firecrawl + GLM 訪問分析
templates/index.html
requirements.txt
.env                  # 本機 API 金鑰（不進 git）
data/revenue.db       # 本機產生，不進 git
```
