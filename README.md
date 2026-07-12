# 台股每月營收 · PEAD 做多異常掃描

抓取上市／上櫃每月營收，回補歷史，計算**只做多**異常訊號（極端成長 + 驚喜／轉折），網頁檢視並可下載 CSV。

## 快速開始

```bash
cd /path/to/202607twselistrev
pip3 install -r requirements.txt
python3 app.py
```

開啟 <http://127.0.0.1:5051>（預設埠；也可用 `PORT=5050 python3 app.py`）

1. 按 **回補 24 個月歷史**（首次建議；約數分鐘）
2. 或 **抓取最新營收**（OpenAPI + 缺邊用 MOPS 補齊）
3. 在 **做多異常** 分頁看 E／S 名單，可下載 CSV

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
app.py
templates/index.html
requirements.txt
data/revenue.db   # 本機產生，不進 git
```
