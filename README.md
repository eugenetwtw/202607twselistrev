# 台股每月營收 · PEAD 掃描（MVP）

用證交所 / 櫃買 OpenAPI 抓**最新一個月**上市、上櫃營收，寫入本機 SQLite，用網頁依月份檢視與簡單異常榜。

## 快速開始

```bash
cd /Users/pe/Downloads/twselistrev
pip3 install -r requirements.txt
python3 app.py
```

瀏覽器開啟：<http://127.0.0.1:5050>

## 功能

| 功能 | 說明 |
|------|------|
| **抓取最新營收** | 呼叫上市 + 上櫃 API，upsert 進 `data/revenue.db` |
| **選月份** | 檢視**已存入 DB** 的月份（官方 API 本身不提供歷史月份參數） |
| **異常榜** | 全市場 YoY top/bottom；產業內百分位領先/落後 |
| **全部資料** | 可篩市場 / 產業、欄位排序 |
| **下載資料庫** | 下載 `revenue.db` 備份 |

## 資料來源

- 上市：`https://openapi.twse.com.tw/v1/opendata/t187ap05_L`
- 上櫃：`https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O`

> 官方 OpenAPI **只回傳最新月**。要累積歷史，請每月公告後按一次「抓取」；DB 會依 `(市場, 年月, 公司代號)` 累積多個月份。

## SQLite 表

`monthly_revenue`：營收金額、MoM%、YoY%、累計 YoY%、產業、備註、抓取時間等。

## 異常規則（MVP）

1. **全市場**：YoY 最高 / 最低 30 名（可設最低營收過濾微型股）
2. **產業內**：同產業 ≥ 5 家時，YoY 百分位 ≥ 90 或 ≤ 10

後續可加：連續月加速、由負轉正、公告後股價反應等（PEAD 本體）。
