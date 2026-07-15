"""
台股每月營收瀏覽（PEAD 研究用 MVP）
- 從證交所 / 櫃買 OpenAPI 抓最新營收
- 若某月只有單一邊市場，自動用公開資訊觀測站彙總表補齊另一邊
  → 同一「資料年月」可同時看到上市 + 上櫃
- 寫入 SQLite，可依月份檢視 / 下載資料庫
"""

from __future__ import annotations

import csv
import json
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd
import requests
from flask import Flask, Response, g, jsonify, render_template, request, send_file

from analysis_service import (
    analyze_company,
    batch_score_weights,
    config_status as analysis_config_status,
    load_dotenv,
)
from company_service import company_service
import job_progress
from price_service import price_service
from trend_service import (
    build_theme_pack,
    rank_score_row,
    score_companies_parallel,
    trend_concurrency,
)

load_dotenv()

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
DB_PATH = DATA_DIR / "revenue.db"

# 上市 / 上櫃 當月營收 OpenAPI（僅最新一個月）
API_URLS = {
    "L": "https://openapi.twse.com.tw/v1/opendata/t187ap05_L",
    "O": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O",
}
# 公開資訊觀測站歷史彙總（可指定年月，補齊落後的一邊）
# sii=上市, otc=上櫃；民國年_月
MOPS_MONTH_URL = (
    "https://mopsov.twse.com.tw/nas/t21/{seg}/t21sc03_{roc_y}_{month}{suffix}.html"
)
MARKET_SEG = {"L": "sii", "O": "otc"}
MARKET_LABEL = {"L": "上市", "O": "上櫃"}

HTTP_HEADERS = {"User-Agent": "twselistrev/0.2 (PEAD research; +local)"}

app = Flask(__name__)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_exc: BaseException | None = None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS monthly_revenue (
            market TEXT NOT NULL,
            year_month TEXT NOT NULL,          -- YYYYMM (西元)
            roc_year_month TEXT,               -- 民國 YYYMM，對應 API 原文
            company_id TEXT NOT NULL,
            company_name TEXT,
            industry TEXT,
            revenue_current REAL,
            revenue_prev_month REAL,
            revenue_prev_year REAL,
            mom_pct REAL,
            yoy_pct REAL,
            revenue_ytd REAL,
            revenue_ytd_prev REAL,
            ytd_yoy_pct REAL,
            note TEXT,
            report_date TEXT,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (market, year_month, company_id)
        );

        CREATE INDEX IF NOT EXISTS idx_revenue_ym
            ON monthly_revenue(year_month);
        CREATE INDEX IF NOT EXISTS idx_revenue_industry
            ON monthly_revenue(year_month, industry);
        CREATE INDEX IF NOT EXISTS idx_revenue_yoy
            ON monthly_revenue(year_month, yoy_pct);

        -- 訪問分析：Brave+Firecrawl+GLM，每次按一下寫一筆（含時間）
        CREATE TABLE IF NOT EXISTS visit_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id TEXT NOT NULL,
            company_name TEXT,
            market TEXT,
            industry TEXT,
            year_month TEXT NOT NULL,
            analyzed_at TEXT NOT NULL,
            pead_quality_weight REAL,
            supply_chain_weight REAL,
            composite_weight REAL,
            evaluation TEXT,
            risk_flags TEXT,
            us_related_tickers TEXT,
            confidence REAL,
            sources_json TEXT,
            model TEXT,
            raw_json TEXT,
            status TEXT,
            error_message TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_visit_company_ym
            ON visit_analysis(company_id, year_month, analyzed_at DESC);
        CREATE INDEX IF NOT EXISTS idx_visit_ym
            ON visit_analysis(year_month, analyzed_at DESC);

        CREATE TABLE IF NOT EXISTS trend_run (
            run_id TEXT PRIMARY KEY,
            year_month TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT,
            theme_summary_json TEXT,
            company_count INTEGER,
            ok_count INTEGER,
            fail_count INTEGER,
            concurrency INTEGER,
            app_version TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_trend_run_ym
            ON trend_run(year_month, started_at DESC);

        CREATE TABLE IF NOT EXISTS company_trend (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            company_id TEXT NOT NULL,
            company_name TEXT,
            market TEXT,
            industry TEXT,
            year_month TEXT NOT NULL,
            revision INTEGER NOT NULL DEFAULT 1,
            trend_weight REAL,
            hot_now REAL,
            persist REAL,
            supply_chain_weight REAL,
            rationale TEXT,
            us_related_tickers TEXT,
            sources_json TEXT,
            analyzed_at TEXT NOT NULL,
            model TEXT,
            status TEXT,
            error_message TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_company_trend_ym_w
            ON company_trend(year_month, trend_weight DESC);
        CREATE INDEX IF NOT EXISTS idx_company_trend_run
            ON company_trend(run_id);
        CREATE INDEX IF NOT EXISTS idx_company_trend_code_ym
            ON company_trend(company_id, year_month, revision DESC);
        """
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    s = str(value).strip().replace(",", "").replace("%", "")
    if s in ("", "-", "NA", "n/a", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def roc_ym_to_ad(roc_ym: str) -> str:
    """民國年月 '11505' / '1155' → 西元 '202505'。"""
    s = re.sub(r"\D", "", str(roc_ym or ""))
    if len(s) == 5:  # YYYMM
        roc_y, m = int(s[:3]), int(s[3:])
    elif len(s) == 4:  # YYMM (少見)
        roc_y, m = int(s[:2]), int(s[2:])
    elif len(s) == 6 and s.startswith("1"):  # already maybe mixed
        roc_y, m = int(s[:3]), int(s[3:])
    else:
        raise ValueError(f"無法解析資料年月: {roc_ym!r}")
    if not 1 <= m <= 12:
        raise ValueError(f"月份不合法: {roc_ym!r}")
    return f"{roc_y + 1911:04d}{m:02d}"


def format_ym_display(ym: str) -> str:
    """'202505' → '2025-05'。"""
    s = re.sub(r"\D", "", ym)
    if len(s) == 6:
        return f"{s[:4]}-{s[4:]}"
    return ym


def row_from_api(item: dict, market: str, fetched_at: str) -> dict | None:
    company_id = str(item.get("公司代號") or "").strip()
    if not company_id:
        return None
    roc_ym = str(item.get("資料年月") or "").strip()
    try:
        year_month = roc_ym_to_ad(roc_ym)
    except ValueError:
        return None

    return {
        "market": market,
        "year_month": year_month,
        "roc_year_month": roc_ym,
        "company_id": company_id,
        "company_name": str(item.get("公司名稱") or "").strip(),
        "industry": str(item.get("產業別") or "").strip() or "未分類",
        "revenue_current": _to_float(item.get("營業收入-當月營收")),
        "revenue_prev_month": _to_float(item.get("營業收入-上月營收")),
        "revenue_prev_year": _to_float(item.get("營業收入-去年當月營收")),
        "mom_pct": _to_float(item.get("營業收入-上月比較增減(%)")),
        "yoy_pct": _to_float(item.get("營業收入-去年同月增減(%)")),
        "revenue_ytd": _to_float(item.get("累計營業收入-當月累計營收")),
        "revenue_ytd_prev": _to_float(item.get("累計營業收入-去年累計營收")),
        "ytd_yoy_pct": _to_float(item.get("累計營業收入-前期比較增減(%)")),
        "note": str(item.get("備註") or "").strip(),
        "report_date": str(item.get("出表日期") or "").strip(),
        "fetched_at": fetched_at,
    }


def fetch_market(market: str, timeout: int = 60) -> list[dict]:
    url = API_URLS[market]
    resp = requests.get(
        url,
        timeout=timeout,
        headers=HTTP_HEADERS,
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"{market} API 回傳格式非 list")
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    for item in data:
        if not isinstance(item, dict):
            continue
        row = row_from_api(item, market, fetched_at)
        if row:
            rows.append(row)
    return rows


def _flatten_html_cols(cols) -> list[str]:
    out: list[str] = []
    for c in cols:
        if isinstance(c, tuple):
            parts = [
                str(x).strip()
                for x in c
                if str(x).strip() and not str(x).startswith("Unnamed")
            ]
            out.append(re.sub(r"\s+", "", "".join(parts) if parts else str(c[-1])))
        else:
            out.append(re.sub(r"\s+", "", str(c).strip()))
    return out


def _ad_ym_parts(year_month: str) -> tuple[int, int, int]:
    """'202605' → (2026, 5, 115 roc year)."""
    s = re.sub(r"\D", "", year_month)
    if len(s) != 6:
        raise ValueError(f"year_month 格式錯誤: {year_month!r}")
    y, m = int(s[:4]), int(s[4:6])
    if not 1 <= m <= 12:
        raise ValueError(f"月份不合法: {year_month!r}")
    return y, m, y - 1911


def fetch_mops_month(market: str, year_month: str, timeout: int = 90) -> list[dict]:
    """
    從公開資訊觀測站抓指定年月、指定市場的營收彙總。
    market: 'L' | 'O'；year_month: 'YYYYMM'
    """
    if market not in MARKET_SEG:
        raise ValueError(f"未知 market: {market}")
    y, m, roc_y = _ad_ym_parts(year_month)
    seg = MARKET_SEG[market]
    roc_ym = f"{roc_y}{m:02d}"
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    text: str | None = None
    used_url = ""
    # 無 suffix 的頁面通常較完整（含較多家數）
    for suffix in ("", "_0"):
        url = MOPS_MONTH_URL.format(seg=seg, roc_y=roc_y, month=m, suffix=suffix)
        resp = requests.get(url, timeout=timeout, headers=HTTP_HEADERS)
        if resp.status_code == 200 and len(resp.content) > 5000:
            text = resp.content.decode("big5", errors="replace")
            used_url = url
            break
    if not text:
        raise ValueError(
            f"MOPS 無法取得 {MARKET_LABEL[market]} {format_ym_display(year_month)} 彙總表"
        )

    dfs = pd.read_html(StringIO(text))
    industry = "未分類"
    rows: list[dict] = []

    def pick_col(ncol: list[str], *needles: str) -> str | None:
        for n in needles:
            for k in ncol:
                if n in k:
                    return k
        return None

    def cell_float(raw: Any) -> float | None:
        if raw is None:
            return None
        s = str(raw).replace(",", "").replace("%", "").strip()
        if s in ("", "-", "nan", "None", "NaN"):
            return None
        try:
            return float(s)
        except ValueError:
            return None

    for df in dfs:
        raw_join = " ".join(
            map(str, list(df.columns) + list(df.astype(str).values.flatten()[:8]))
        )
        im = re.search(r"產業別[：:]\s*([^\s單位　]+)", raw_join)
        if im and df.shape[0] <= 3 and df.shape[1] <= 4:
            industry = im.group(1).strip()
            continue

        ncol = _flatten_html_cols(df.columns)
        id_col = pick_col(ncol, "公司代號")
        if not id_col:
            continue
        name_c = pick_col(ncol, "公司名稱")
        rev_c = pick_col(ncol, "當月營收")
        prev_c = pick_col(ncol, "上月營收")
        # 避免「去年當月」被「當月」誤匹配：先找去年當月
        py_c = pick_col(ncol, "去年當月營收", "去年當月")
        mom_c = pick_col(ncol, "上月比較")
        yoy_c = pick_col(ncol, "去年同月")
        ytd_c = pick_col(ncol, "當月累計")
        ytdp_c = pick_col(ncol, "去年累計")
        ytdy_c = pick_col(ncol, "前期比較")
        note_c = pick_col(ncol, "備註")

        col_index = {c: i for i, c in enumerate(ncol)}

        def col_series(key: str | None):
            if not key:
                return None
            return df.iloc[:, col_index[key]]

        id_s = col_series(id_col)
        name_s = col_series(name_c)
        for i in range(len(df)):
            cid = str(id_s.iloc[i]).strip()
            if not re.fullmatch(r"\d{3,6}", cid):
                continue

            def v(series) -> float | None:
                if series is None:
                    return None
                return cell_float(series.iloc[i])

            note_val = ""
            if note_c and col_series(note_c) is not None:
                note_val = str(col_series(note_c).iloc[i]).strip()
                if note_val in ("nan", "None", "-"):
                    note_val = "-"

            rows.append(
                {
                    "market": market,
                    "year_month": year_month,
                    "roc_year_month": roc_ym,
                    "company_id": cid,
                    "company_name": (
                        str(name_s.iloc[i]).strip() if name_s is not None else ""
                    ),
                    "industry": industry or "未分類",
                    "revenue_current": v(col_series(rev_c)),
                    "revenue_prev_month": v(col_series(prev_c)),
                    "revenue_prev_year": v(col_series(py_c)),
                    "mom_pct": v(col_series(mom_c)),
                    "yoy_pct": v(col_series(yoy_c)),
                    "revenue_ytd": v(col_series(ytd_c)),
                    "revenue_ytd_prev": v(col_series(ytdp_c)),
                    "ytd_yoy_pct": v(col_series(ytdy_c)),
                    "note": note_val or "-",
                    "report_date": "",
                    "fetched_at": fetched_at,
                }
            )

    if not rows:
        raise ValueError(
            f"MOPS 解析後 0 筆：{MARKET_LABEL[market]} {format_ym_display(year_month)} ({used_url})"
        )
    return rows


def market_counts(db: sqlite3.Connection, year_month: str) -> dict[str, int]:
    cur = db.execute(
        """
        SELECT market, COUNT(*) AS n FROM monthly_revenue
        WHERE year_month = ?
        GROUP BY market
        """,
        (year_month,),
    )
    out = {"L": 0, "O": 0}
    for r in cur.fetchall():
        out[r["market"]] = int(r["n"])
    return out


def ensure_both_markets(
    db: sqlite3.Connection,
    year_month: str,
    *,
    min_count: int = 50,
    force: bool = False,
    markets: tuple[str, ...] | list[str] = ("L", "O"),
) -> dict[str, Any]:
    """
    確保某年月有上市 / 上櫃營收。
    - force=False：僅在家數 < min_count 時用 MOPS 補
    - force=True：強制用 MOPS 整包 upsert（覆蓋／補齊晚公告公司，如台積電）
    """
    counts = market_counts(db, year_month)
    result: dict[str, Any] = {
        "year_month": year_month,
        "label": format_ym_display(year_month),
        "before": dict(counts),
        "filled": [],
        "errors": [],
        "force": force,
    }
    for market in markets:
        if market not in ("L", "O"):
            continue
        need = force or counts.get(market, 0) < min_count
        if not need:
            continue
        try:
            rows = fetch_mops_month(market, year_month)
            n = upsert_rows(db, rows)
            # force 後更新 counts，避免同 loop 誤判
            counts = market_counts(db, year_month)
            result["filled"].append(
                {
                    "market": market,
                    "market_label": MARKET_LABEL[market],
                    "fetched": n,
                    "source": "mops",
                    "forced": force,
                }
            )
        except Exception as exc:  # noqa: BLE001
            result["errors"].append(
                {"market": market, "error": str(exc), "source": "mops"}
            )
    result["after"] = market_counts(db, year_month)
    # 完整度：上市家數接近全市場（避免 900 家就當齊、漏掉晚申報）
    full_l = 1000
    full_o = 750
    result["complete"] = (
        result["after"].get("L", 0) >= full_l
        and result["after"].get("O", 0) >= full_o
    )
    return result


def prev_year_month(year_month: str) -> str:
    y, m = int(year_month[:4]), int(year_month[4:6])
    m -= 1
    if m == 0:
        y -= 1
        m = 12
    return f"{y:04d}{m:02d}"


def year_month_range_ending(end_ym: str, n_months: int) -> list[str]:
    """含 end_ym 在內、往前共 n_months 個 YYYYMM（舊→新）。"""
    cur = end_ym
    out = [cur]
    for _ in range(n_months - 1):
        cur = prev_year_month(cur)
        out.append(cur)
    return list(reversed(out))


def latest_data_year_month(db: sqlite3.Connection) -> str | None:
    row = db.execute(
        "SELECT year_month FROM monthly_revenue ORDER BY year_month DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def default_backfill_end_ym() -> str:
    """預設回補終點：上個月（營收資料月）。"""
    now = datetime.now()
    y, m = now.year, now.month - 1
    if m <= 0:
        y -= 1
        m = 12
    return f"{y:04d}{m:02d}"


def backfill_history(
    db: sqlite3.Connection,
    *,
    months: int = 24,
    end_ym: str | None = None,
    sleep_s: float = 0.6,
    skip_complete: bool = True,
    min_count: int = 50,
) -> dict[str, Any]:
    """
    從 MOPS 回補近 months 個月、上市+上櫃。
    已齊月份預設跳過；force 式可 skip_complete=False。
    """
    end = end_ym or latest_data_year_month(db) or default_backfill_end_ym()
    yms = year_month_range_ending(end, months)
    steps: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for i, ym in enumerate(yms):
        counts = market_counts(db, ym)
        complete = counts.get("L", 0) >= min_count and counts.get("O", 0) >= min_count
        if skip_complete and complete:
            steps.append(
                {
                    "year_month": ym,
                    "label": format_ym_display(ym),
                    "skipped": True,
                    "reason": "already_complete",
                    "after": counts,
                }
            )
            continue

        # 兩邊都抓（缺才抓）；force 已齊時仍可不重抓
        fr = ensure_both_markets(db, ym, min_count=min_count, force=False)
        step = {
            "year_month": ym,
            "label": format_ym_display(ym),
            "skipped": False,
            "filled": fr["filled"],
            "before": fr["before"],
            "after": fr["after"],
            "complete": fr["complete"],
            "progress": f"{i + 1}/{len(yms)}",
        }
        if fr["errors"]:
            step["errors"] = fr["errors"]
            errors.extend(
                {"year_month": ym, **e} for e in fr["errors"]
            )
        steps.append(step)
        if sleep_s > 0 and i < len(yms) - 1:
            time.sleep(sleep_s)

    months_out = list_months(db)
    complete_n = sum(1 for m in months_out if m["complete"])
    return {
        "end_ym": end,
        "months_requested": months,
        "year_months": yms,
        "steps": steps,
        "errors": errors,
        "complete_months": complete_n,
        "months": months_out,
        "ok": len(errors) == 0,
    }


def _safe_median(vals: list[float]) -> float | None:
    clean = [float(v) for v in vals if v is not None]
    if not clean:
        return None
    return float(median(clean))


def _percentile_rank(sorted_vals: list[float], value: float) -> float:
    """0~100，值越大百分位越高。"""
    n = len(sorted_vals)
    if n <= 1:
        return 50.0
    # 有多少嚴格小於 value
    less = sum(1 for x in sorted_vals if x < value)
    equal = sum(1 for x in sorted_vals if x == value)
    # mid-rank for ties
    return 100.0 * (less + 0.5 * equal) / n


def compute_long_signals(
    db: sqlite3.Connection,
    year_month: str,
    *,
    min_revenue: float = 50_000,
    market: str | None = None,
    # 極端成長
    extreme_industry_pctile: float = 90.0,
    extreme_min_yoy: float = 0.0,
    # 驚喜：相對 S1/S2 的超出幅度（百分點）
    surprise_min_pp: float = 15.0,
    # 或落在 surprise 自身分布的高百分位
    surprise_pctile_min: float = 85.0,
    min_history_for_s2: int = 6,
) -> dict[str, Any]:
    """
    只做多異常：
    - E 極端成長：產業 YoY 百分位高 + YoY 為正 + 營收門檻
    - S 驚喜/轉折：S1 產業中位、S2 自身近12月中位；轉折=由負轉正或連月加速
    """
    clauses = ["year_month = ?", "yoy_pct IS NOT NULL"]
    params: list[Any] = [year_month]
    if market in ("L", "O"):
        clauses.append("market = ?")
        params.append(market)
    if min_revenue > 0:
        clauses.append("revenue_current IS NOT NULL AND revenue_current >= ?")
        params.append(min_revenue)

    rows = [
        dict(r)
        for r in db.execute(
            f"SELECT * FROM monthly_revenue WHERE {' AND '.join(clauses)}",
            params,
        ).fetchall()
    ]
    if not rows:
        return {
            "year_month": year_month,
            "label": format_ym_display(year_month),
            "universe": 0,
            "extreme_growth": [],
            "surprise_turnaround": [],
            "all_long": [],
            "params": {},
        }

    # --- S1: 產業當月 YoY 中位 ---
    by_ind: dict[str, list[float]] = {}
    for r in rows:
        ind = r.get("industry") or "未分類"
        by_ind.setdefault(ind, []).append(float(r["yoy_pct"]))
    ind_median = {ind: _safe_median(vs) for ind, vs in by_ind.items()}
    ind_sorted = {ind: sorted(vs) for ind, vs in by_ind.items()}

    # --- 產業百分位 ---
    for r in rows:
        ind = r.get("industry") or "未分類"
        r["industry_yoy_median"] = ind_median.get(ind)
        r["industry_n"] = len(by_ind.get(ind, []))
        r["industry_yoy_pctile"] = _percentile_rank(
            ind_sorted.get(ind, [float(r["yoy_pct"])]), float(r["yoy_pct"])
        )
        # S1 surprise
        em = r["industry_yoy_median"]
        r["expected_yoy_s1"] = em
        r["surprise_s1"] = (
            float(r["yoy_pct"]) - em if em is not None else None
        )

    # --- S2: 自身近 12 個月 YoY 中位（不含當月）+ 上月 YoY（轉折）---
    hist_yms = []
    cur = year_month
    for _ in range(12):
        cur = prev_year_month(cur)
        hist_yms.append(cur)
    prev_ym = hist_yms[0] if hist_yms else None
    prev2_ym = hist_yms[1] if len(hist_yms) > 1 else None

    company_ids = [r["company_id"] for r in rows]
    # 一次查出相關歷史
    placeholders = ",".join("?" * len(hist_yms)) if hist_yms else "''"
    id_ph = ",".join("?" * len(company_ids))
    hist_map: dict[str, list[tuple[str, float | None]]] = {}
    if hist_yms and company_ids:
        q = f"""
            SELECT company_id, year_month, yoy_pct
            FROM monthly_revenue
            WHERE year_month IN ({placeholders})
              AND company_id IN ({id_ph})
        """
        for hr in db.execute(q, [*hist_yms, *company_ids]).fetchall():
            hist_map.setdefault(hr["company_id"], []).append(
                (hr["year_month"], hr["yoy_pct"])
            )

    for r in rows:
        cid = r["company_id"]
        series = hist_map.get(cid, [])
        by_ym = {ym: yoy for ym, yoy in series}
        hist_yoys = [
            float(by_ym[ym])
            for ym in hist_yms
            if ym in by_ym and by_ym[ym] is not None
        ]
        r["hist_yoy_n"] = len(hist_yoys)
        r["expected_yoy_s2"] = (
            _safe_median(hist_yoys) if len(hist_yoys) >= min_history_for_s2 else None
        )
        if r["expected_yoy_s2"] is not None:
            r["surprise_s2"] = float(r["yoy_pct"]) - r["expected_yoy_s2"]
        else:
            r["surprise_s2"] = None

        r["prev_yoy"] = float(by_ym[prev_ym]) if prev_ym and prev_ym in by_ym and by_ym[prev_ym] is not None else None
        r["prev2_yoy"] = (
            float(by_ym[prev2_ym])
            if prev2_ym and prev2_ym in by_ym and by_ym[prev2_ym] is not None
            else None
        )

        # 轉折：由負轉正
        r["is_turnaround"] = bool(
            r["prev_yoy"] is not None
            and r["prev_yoy"] < 0
            and float(r["yoy_pct"]) > 0
        )
        # 連兩月加速（YoY 遞增）
        r["is_accelerating"] = bool(
            r["prev_yoy"] is not None
            and r["prev2_yoy"] is not None
            and float(r["yoy_pct"]) > r["prev_yoy"] > r["prev2_yoy"]
        )

        # 綜合 surprise：S1+S2 有則平均，否則用有的那個
        s_parts = [x for x in (r["surprise_s1"], r["surprise_s2"]) if x is not None]
        r["surprise_avg"] = sum(s_parts) / len(s_parts) if s_parts else None

    # surprise 分布百分位（用 surprise_avg，缺則 s1）
    surp_vals = []
    for r in rows:
        s = r["surprise_avg"] if r["surprise_avg"] is not None else r["surprise_s1"]
        r["_surp_for_rank"] = s
        if s is not None:
            surp_vals.append(s)
    surp_sorted = sorted(surp_vals)
    for r in rows:
        s = r["_surp_for_rank"]
        r["surprise_pctile"] = (
            _percentile_rank(surp_sorted, s) if s is not None and surp_sorted else None
        )

    # --- 標籤：只做多 ---
    for r in rows:
        yoy = float(r["yoy_pct"])
        r["is_extreme_growth"] = bool(
            yoy > extreme_min_yoy
            and (r.get("industry_yoy_pctile") or 0) >= extreme_industry_pctile
            and (r.get("industry_n") or 0) >= 5
        )
        s_ok = False
        if r.get("surprise_avg") is not None and r["surprise_avg"] >= surprise_min_pp:
            s_ok = True
        if r.get("surprise_pctile") is not None and r["surprise_pctile"] >= surprise_pctile_min:
            s_ok = True
        # 轉折或加速也算驚喜/動能類做多訊號（需 YoY 為正）
        if yoy > 0 and (r["is_turnaround"] or r["is_accelerating"]):
            s_ok = True
        r["is_surprise_long"] = bool(s_ok and yoy > extreme_min_yoy)

        # 綜合分（做多）：百分位 + surprise 貢獻 + 轉折加成
        score = 0.0
        score += 0.45 * float(r.get("industry_yoy_pctile") or 0)
        score += 0.35 * float(r.get("surprise_pctile") or 50)
        if r["is_turnaround"]:
            score += 12
        if r["is_accelerating"]:
            score += 8
        if r["is_extreme_growth"]:
            score += 5
        r["anomaly_score"] = round(score, 2)
        r["signal_types"] = []
        if r["is_extreme_growth"]:
            r["signal_types"].append("extreme_growth")
        if r["is_surprise_long"]:
            r["signal_types"].append("surprise_turnaround")

    extreme = [r for r in rows if r["is_extreme_growth"]]
    surprise = [r for r in rows if r["is_surprise_long"]]
    all_long = [r for r in rows if r["is_extreme_growth"] or r["is_surprise_long"]]

    extreme.sort(key=lambda x: (x.get("anomaly_score") or 0, x.get("yoy_pct") or 0), reverse=True)
    surprise.sort(key=lambda x: (x.get("anomaly_score") or 0, x.get("surprise_avg") or 0), reverse=True)
    all_long.sort(key=lambda x: (x.get("anomaly_score") or 0), reverse=True)

    return {
        "year_month": year_month,
        "label": format_ym_display(year_month),
        "universe": len(rows),
        "extreme_growth": extreme,
        "surprise_turnaround": surprise,
        "all_long": all_long,
        "params": {
            "min_revenue": min_revenue,
            "extreme_industry_pctile": extreme_industry_pctile,
            "extreme_min_yoy": extreme_min_yoy,
            "surprise_min_pp": surprise_min_pp,
            "surprise_pctile_min": surprise_pctile_min,
            "min_history_for_s2": min_history_for_s2,
            "expected": "S1=產業當月YoY中位; S2=自身近12月YoY中位",
            "direction": "long_only",
        },
    }


def serialize_signal_row(r: dict) -> dict:
    out = serialize_row(r)
    for k in (
        "industry_yoy_pctile",
        "industry_yoy_median",
        "industry_n",
        "expected_yoy_s1",
        "expected_yoy_s2",
        "surprise_s1",
        "surprise_s2",
        "surprise_avg",
        "surprise_pctile",
        "prev_yoy",
        "prev2_yoy",
        "hist_yoy_n",
        "anomaly_score",
        "last_close",
        "change_pct",
        "change",
        "change_6m_pct",
    ):
        if k in r and r[k] is not None:
            try:
                out[k] = float(r[k])
            except (TypeError, ValueError):
                out[k] = r[k]
        elif k in r:
            out[k] = r[k]
    out["is_extreme_growth"] = bool(r.get("is_extreme_growth"))
    out["is_surprise_long"] = bool(r.get("is_surprise_long"))
    out["is_turnaround"] = bool(r.get("is_turnaround"))
    out["is_accelerating"] = bool(r.get("is_accelerating"))
    out["signal_types"] = r.get("signal_types") or []
    out["chart_url"] = r.get("chart_url")
    # 前端 SVG 軸標用：精簡點位
    sp = r.get("spark_points") or []
    out["spark_points"] = [
        {"date": str(p.get("date") or "")[:10], "close": float(p["close"])}
        for p in sp
        if p.get("close") is not None
    ][:48]
    out["quote_as_of"] = r.get("quote_as_of")
    out["quote_source"] = r.get("quote_source")
    out["history_points"] = r.get("history_points")
    # 訪問分析 / 趨勢權重
    for k in (
        "pead_quality_weight",
        "supply_chain_weight",
        "composite_weight",
        "evaluation",
        "analyzed_at",
        "trend_weight",
        "hot_now",
        "persist",
        "trend_rationale",
        "trend_run_id",
        "trend_revision",
        "trend_analyzed_at",
        "rank_score",
    ):
        if k in r:
            out[k] = r.get(k)
    if r.get("us_related_tickers") is not None:
        out["us_related_tickers"] = r.get("us_related_tickers")
    if r.get("visit_analysis"):
        out["visit_analysis"] = r["visit_analysis"]
    return out


def upsert_rows(db: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO monthly_revenue (
            market, year_month, roc_year_month, company_id, company_name,
            industry, revenue_current, revenue_prev_month, revenue_prev_year,
            mom_pct, yoy_pct, revenue_ytd, revenue_ytd_prev, ytd_yoy_pct,
            note, report_date, fetched_at
        ) VALUES (
            :market, :year_month, :roc_year_month, :company_id, :company_name,
            :industry, :revenue_current, :revenue_prev_month, :revenue_prev_year,
            :mom_pct, :yoy_pct, :revenue_ytd, :revenue_ytd_prev, :ytd_yoy_pct,
            :note, :report_date, :fetched_at
        )
        ON CONFLICT(market, year_month, company_id) DO UPDATE SET
            roc_year_month = excluded.roc_year_month,
            company_name = excluded.company_name,
            industry = excluded.industry,
            revenue_current = excluded.revenue_current,
            revenue_prev_month = excluded.revenue_prev_month,
            revenue_prev_year = excluded.revenue_prev_year,
            mom_pct = excluded.mom_pct,
            yoy_pct = excluded.yoy_pct,
            revenue_ytd = excluded.revenue_ytd,
            revenue_ytd_prev = excluded.revenue_ytd_prev,
            ytd_yoy_pct = excluded.ytd_yoy_pct,
            note = excluded.note,
            report_date = excluded.report_date,
            fetched_at = excluded.fetched_at
    """
    db.executemany(sql, rows)
    db.commit()
    return len(rows)


def list_months(db: sqlite3.Connection) -> list[dict]:
    cur = db.execute(
        """
        SELECT year_month,
               COUNT(*) AS n,
               SUM(CASE WHEN market = 'L' THEN 1 ELSE 0 END) AS n_listed,
               SUM(CASE WHEN market = 'O' THEN 1 ELSE 0 END) AS n_otc,
               MAX(fetched_at) AS last_fetched
        FROM monthly_revenue
        GROUP BY year_month
        ORDER BY year_month DESC
        """
    )
    # 完整度門檻：避免 OpenAPI 僅 ~900 家就當齊（漏台積電等晚申報）
    FULL_L, FULL_O = 1000, 750
    out = []
    for r in cur.fetchall():
        n_l = int(r["n_listed"] or 0)
        n_o = int(r["n_otc"] or 0)
        complete = n_l >= FULL_L and n_o >= FULL_O
        out.append(
            {
                "year_month": r["year_month"],
                "label": format_ym_display(r["year_month"]),
                "n": r["n"],
                "n_listed": n_l,
                "n_otc": n_o,
                "complete": complete,
                "last_fetched": r["last_fetched"],
            }
        )
    return out


def query_rows(
    db: sqlite3.Connection,
    year_month: str,
    market: str | None = None,
    industry: str | None = None,
    sort: str = "yoy_pct",
    order: str = "desc",
    limit: int = 5000,
) -> list[dict]:
    allowed_sort = {
        "yoy_pct",
        "mom_pct",
        "ytd_yoy_pct",
        "revenue_current",
        "company_id",
        "industry",
    }
    if sort not in allowed_sort:
        sort = "yoy_pct"
    order_sql = "ASC" if order.lower() == "asc" else "DESC"

    clauses = ["year_month = ?"]
    params: list[Any] = [year_month]
    if market in ("L", "O"):
        clauses.append("market = ?")
        params.append(market)
    if industry:
        clauses.append("industry = ?")
        params.append(industry)

    # NULLS last-ish for DESC: put nulls at bottom by COALESCE trick
    null_sentinel = -1e18 if order_sql == "DESC" else 1e18
    sql = f"""
        SELECT * FROM monthly_revenue
        WHERE {' AND '.join(clauses)}
        ORDER BY COALESCE({sort}, {null_sentinel}) {order_sql}, company_id ASC
        LIMIT ?
    """
    params.append(limit)
    cur = db.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def list_industries(db: sqlite3.Connection, year_month: str) -> list[str]:
    cur = db.execute(
        """
        SELECT DISTINCT industry FROM monthly_revenue
        WHERE year_month = ?
        ORDER BY industry
        """,
        (year_month,),
    )
    return [r[0] for r in cur.fetchall() if r[0]]


def compute_anomalies(
    db: sqlite3.Connection,
    year_month: str,
    direction: str = "both",
    top_n: int = 30,
    min_revenue: float = 50_000,  # API 數字單位為新台幣「千元」；50000 ≈ 5千萬元
    market: str | None = None,
) -> dict:
    """
    簡單異常：
    1) 全市場 YoY top/bottom
    2) 產業內 YoY 排名（同產業至少 5 家才算百分位）
    """
    clauses = ["year_month = ?", "yoy_pct IS NOT NULL"]
    params: list[Any] = [year_month]
    if market in ("L", "O"):
        clauses.append("market = ?")
        params.append(market)
    if min_revenue > 0:
        clauses.append("revenue_current IS NOT NULL AND revenue_current >= ?")
        params.append(min_revenue)

    rows = [
        dict(r)
        for r in db.execute(
            f"SELECT * FROM monthly_revenue WHERE {' AND '.join(clauses)}",
            params,
        ).fetchall()
    ]

    # industry percentile
    by_ind: dict[str, list[dict]] = {}
    for r in rows:
        by_ind.setdefault(r["industry"] or "未分類", []).append(r)

    for ind, group in by_ind.items():
        group_sorted = sorted(group, key=lambda x: x["yoy_pct"])
        n = len(group_sorted)
        for i, r in enumerate(group_sorted):
            # 0~100, higher = stronger YoY
            r["industry_yoy_pctile"] = round(100.0 * i / (n - 1), 2) if n > 1 else 50.0
            r["industry_n"] = n

    pos = sorted(rows, key=lambda x: x["yoy_pct"], reverse=True)
    neg = sorted(rows, key=lambda x: x["yoy_pct"])

    # industry leaders: top within industry (n>=5) and pctile >= 90
    ind_pos = [
        r
        for r in rows
        if r.get("industry_n", 0) >= 5 and r.get("industry_yoy_pctile", 0) >= 90
    ]
    ind_pos.sort(key=lambda x: x["yoy_pct"], reverse=True)

    ind_neg = [
        r
        for r in rows
        if r.get("industry_n", 0) >= 5 and r.get("industry_yoy_pctile", 100) <= 10
    ]
    ind_neg.sort(key=lambda x: x["yoy_pct"])

    result = {
        "year_month": year_month,
        "label": format_ym_display(year_month),
        "count": len(rows),
        "min_revenue": min_revenue,
        "top_yoy": pos[:top_n] if direction in ("both", "up") else [],
        "bottom_yoy": neg[:top_n] if direction in ("both", "down") else [],
        "industry_leaders": ind_pos[:top_n] if direction in ("both", "up") else [],
        "industry_laggards": ind_neg[:top_n] if direction in ("both", "down") else [],
    }
    return result


def serialize_row(r: dict) -> dict:
    """前端友善格式。"""
    out = dict(r)
    out["market_label"] = MARKET_LABEL.get(r.get("market"), r.get("market"))
    out["year_month_label"] = format_ym_display(r.get("year_month") or "")
    # 官網（點名稱連外）
    code = str(out.get("company_id") or "").strip()
    if not out.get("website") and code:
        out["website"] = company_service.get_website(code)
    for k in (
        "revenue_current",
        "revenue_prev_month",
        "revenue_prev_year",
        "mom_pct",
        "yoy_pct",
        "revenue_ytd",
        "revenue_ytd_prev",
        "ytd_yoy_pct",
        "industry_yoy_pctile",
    ):
        if k in out and out[k] is not None:
            try:
                out[k] = float(out[k])
            except (TypeError, ValueError):
                pass
    return out


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

APP_VERSION = "2026-07-15-dock-fix"


def save_visit_analysis(db: sqlite3.Connection, result: dict[str, Any]) -> int:
    cur = db.execute(
        """
        INSERT INTO visit_analysis (
            company_id, company_name, market, industry, year_month, analyzed_at,
            pead_quality_weight, supply_chain_weight, composite_weight,
            evaluation, risk_flags, us_related_tickers, confidence,
            sources_json, model, raw_json, status, error_message
        ) VALUES (
            :company_id, :company_name, :market, :industry, :year_month, :analyzed_at,
            :pead_quality_weight, :supply_chain_weight, :composite_weight,
            :evaluation, :risk_flags, :us_related_tickers, :confidence,
            :sources_json, :model, :raw_json, :status, :error_message
        )
        """,
        {
            "company_id": result["company_id"],
            "company_name": result.get("company_name"),
            "market": result.get("market"),
            "industry": result.get("industry"),
            "year_month": result["year_month"],
            "analyzed_at": result["analyzed_at"],
            "pead_quality_weight": result.get("pead_quality_weight"),
            "supply_chain_weight": result.get("supply_chain_weight"),
            "composite_weight": result.get("composite_weight"),
            "evaluation": result.get("evaluation"),
            "risk_flags": json.dumps(result.get("risk_flags") or [], ensure_ascii=False),
            "us_related_tickers": json.dumps(
                result.get("us_related_tickers") or [], ensure_ascii=False
            ),
            "confidence": result.get("confidence"),
            "sources_json": json.dumps(result.get("sources_json") or {}, ensure_ascii=False),
            "model": result.get("model"),
            "raw_json": json.dumps(result.get("raw_json") or {}, ensure_ascii=False),
            "status": result.get("status") or "ok",
            "error_message": result.get("error_message"),
        },
    )
    db.commit()
    return int(cur.lastrowid)


def _parse_json_field(raw: Any, default: Any) -> Any:
    if raw is None or raw == "":
        return default
    if isinstance(raw, (list, dict)):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return default


def serialize_visit_analysis(row: sqlite3.Row | dict, *, include_sources: bool = True) -> dict:
    r = dict(row)
    out = {
        "id": r.get("id"),
        "company_id": r.get("company_id"),
        "company_name": r.get("company_name"),
        "market": r.get("market"),
        "industry": r.get("industry"),
        "year_month": r.get("year_month"),
        "analyzed_at": r.get("analyzed_at"),
        "pead_quality_weight": r.get("pead_quality_weight"),
        "supply_chain_weight": r.get("supply_chain_weight"),
        "composite_weight": r.get("composite_weight"),
        "evaluation": r.get("evaluation"),
        "risk_flags": _parse_json_field(r.get("risk_flags"), []),
        "us_related_tickers": _parse_json_field(r.get("us_related_tickers"), []),
        "confidence": r.get("confidence"),
        "model": r.get("model"),
        "status": r.get("status"),
        "error_message": r.get("error_message"),
    }
    if include_sources:
        out["sources"] = _parse_json_field(r.get("sources_json"), {})
    return out


def fetch_latest_analyses(
    db: sqlite3.Connection,
    year_month: str,
    company_ids: list[str] | None = None,
) -> dict[str, dict]:
    """Return latest visit_analysis per company_id for a year_month."""
    if company_ids is not None and not company_ids:
        return {}
    if company_ids:
        # SQLite variable limit — chunk if needed
        out: dict[str, dict] = {}
        chunk = 400
        for i in range(0, len(company_ids), chunk):
            part = company_ids[i : i + chunk]
            ph = ",".join("?" * len(part))
            rows = db.execute(
                f"""
                SELECT v.*
                FROM visit_analysis v
                INNER JOIN (
                    SELECT company_id, MAX(id) AS max_id
                    FROM visit_analysis
                    WHERE year_month = ? AND company_id IN ({ph})
                    GROUP BY company_id
                ) t ON v.id = t.max_id
                """,
                [year_month, *part],
            ).fetchall()
            for row in rows:
                s = serialize_visit_analysis(row, include_sources=False)
                out[str(s["company_id"])] = s
        return out
    rows = db.execute(
        """
        SELECT v.*
        FROM visit_analysis v
        INNER JOIN (
            SELECT company_id, MAX(id) AS max_id
            FROM visit_analysis
            WHERE year_month = ?
            GROUP BY company_id
        ) t ON v.id = t.max_id
        """,
        (year_month,),
    ).fetchall()
    return {
        str(serialize_visit_analysis(row, include_sources=False)["company_id"]): serialize_visit_analysis(
            row, include_sources=False
        )
        for row in rows
    }


def attach_analysis_to_rows(db: sqlite3.Connection, ym: str, rows: list[dict]) -> None:
    ids = [str(r.get("company_id") or "") for r in rows if r.get("company_id")]
    latest = fetch_latest_analyses(db, ym, ids)
    for r in rows:
        a = latest.get(str(r.get("company_id") or ""))
        if not a:
            r["visit_analysis"] = None
            r["pead_quality_weight"] = None
            r["supply_chain_weight"] = None
            r["composite_weight"] = None
            r["evaluation"] = None
            r["analyzed_at"] = None
            continue
        r["visit_analysis"] = a
        r["pead_quality_weight"] = a.get("pead_quality_weight")
        r["supply_chain_weight"] = a.get("supply_chain_weight")
        r["composite_weight"] = a.get("composite_weight")
        r["evaluation"] = a.get("evaluation")
        r["analyzed_at"] = a.get("analyzed_at")


@app.route("/")
def index():
    return render_template(
        "index.html",
        server_version=APP_VERSION,
        api_routes=sorted(
            r.rule for r in app.url_map.iter_rules() if str(r.rule).startswith("/api")
        ),
    )


@app.get("/api/months")
def api_months():
    db = get_db()
    return jsonify({"months": list_months(db)})


@app.get("/api/industries")
def api_industries():
    ym = request.args.get("year_month", "").strip()
    if not ym:
        return jsonify({"error": "缺少 year_month"}), 400
    db = get_db()
    return jsonify({"industries": list_industries(db, ym)})


@app.get("/api/revenue")
def api_revenue():
    ym = request.args.get("year_month", "").strip()
    if not ym:
        return jsonify({"error": "缺少 year_month"}), 400
    market = request.args.get("market") or None
    industry = request.args.get("industry") or None
    sort = request.args.get("sort", "yoy_pct")
    order = request.args.get("order", "desc")
    try:
        limit = min(int(request.args.get("limit", 2000)), 10000)
    except ValueError:
        limit = 2000

    db = get_db()
    rows = query_rows(db, ym, market=market, industry=industry, sort=sort, order=order, limit=limit)
    try:
        company_service.ensure()
        company_service.attach(rows)
    except Exception:
        pass
    return jsonify(
        {
            "year_month": ym,
            "label": format_ym_display(ym),
            "count": len(rows),
            "rows": [serialize_row(r) for r in rows],
        }
    )


@app.get("/api/anomalies")
def api_anomalies():
    ym = request.args.get("year_month", "").strip()
    if not ym:
        return jsonify({"error": "缺少 year_month"}), 400
    market = request.args.get("market") or None
    direction = request.args.get("direction", "both")
    try:
        # top_n=0 或很大：回傳全部（前端兩張表完整列出）
        top_n = int(request.args.get("top_n", 0))
        if top_n < 0:
            top_n = 0
        if top_n == 0:
            top_n = 100_000
        else:
            top_n = min(top_n, 100_000)
    except ValueError:
        top_n = 100_000
    try:
        # API 數字單位為新台幣「千元」
        min_revenue = float(request.args.get("min_revenue", 50_000))
    except ValueError:
        min_revenue = 50_000

    db = get_db()
    result = compute_anomalies(
        db, ym, direction=direction, top_n=top_n, min_revenue=min_revenue, market=market
    )
    for key in ("top_yoy", "bottom_yoy", "industry_leaders", "industry_laggards"):
        result[key] = [serialize_row(r) for r in result[key]]
    return jsonify(result)


@app.post("/api/fetch")
def api_fetch():
    """
    1) 抓上市 + 上櫃 OpenAPI 最新月（可能不完整，會漏晚公告公司）
    2) **強制**對 OpenAPI 出現的每個資料年月，用 MOPS 彙總整包覆蓋上市+上櫃
       → 補上如台積電等已公告但尚未進 OpenAPI 的公司
    3) （可選）DB 內其他「未齊」舊月份再輕量補齊（不 force）
    """
    body = request.get_json(silent=True) or {}
    markets = body.get("markets") or ["L", "O"]
    markets = [m for m in markets if m in API_URLS]
    if not markets:
        return jsonify({"error": "markets 無效"}), 400
    # 是否對「已存在但未齊」的舊月份也補齊（預設 True；這些不 force）
    fill_all_incomplete = body.get("fill_all_incomplete", True)
    # 抓取最新後是否強制 MOPS 覆蓋當月（預設 True）
    force_mops_latest = body.get("force_mops_latest", True)

    db = get_db()
    summary = []
    errors = []
    all_ym: set[str] = set()
    openapi_yms: set[str] = set()

    for market in markets:
        try:
            rows = fetch_market(market)
            n = upsert_rows(db, rows)
            yms = sorted({r["year_month"] for r in rows})
            all_ym.update(yms)
            openapi_yms.update(yms)
            summary.append(
                {
                    "market": market,
                    "market_label": MARKET_LABEL[market],
                    "fetched": n,
                    "year_months": yms,
                    "labels": [format_ym_display(y) for y in yms],
                    "source": "openapi",
                }
            )
        except Exception as exc:  # noqa: BLE001 — surface to UI
            errors.append({"market": market, "error": str(exc), "source": "openapi"})

    fill_results = []

    # 關鍵：OpenAPI 當月一律強制 MOPS 覆蓋（上市+上櫃）
    if force_mops_latest and openapi_yms:
        for ym in sorted(openapi_yms):
            fr = ensure_both_markets(
                db, ym, min_count=50, force=True, markets=("L", "O")
            )
            fr["reason"] = "force_mops_after_openapi"
            fill_results.append(fr)
            if fr["errors"]:
                errors.extend(
                    {
                        "market": e["market"],
                        "error": f"{fr['label']}: {e['error']}",
                        "source": "mops",
                    }
                    for e in fr["errors"]
                )
            all_ym.add(ym)

    # 其他未齊舊月：只補缺，不 force（避免每次掃 24 個月）
    if fill_all_incomplete:
        for m in list_months(db):
            ym = m["year_month"]
            if ym in openapi_yms:
                continue
            if m["complete"]:
                continue
            fr = ensure_both_markets(db, ym, min_count=50, force=False)
            fr["reason"] = "fill_incomplete"
            fill_results.append(fr)
            if fr["errors"]:
                errors.extend(
                    {
                        "market": e["market"],
                        "error": f"{fr['label']}: {e['error']}",
                        "source": "mops",
                    }
                    for e in fr["errors"]
                )
            all_ym.add(ym)

    months = list_months(db)
    # 預設建議：優先 OpenAPI 最新月，否則最新已齊月
    preferred = None
    if openapi_yms:
        preferred = sorted(openapi_yms)[-1]
    if not preferred:
        preferred = next((m["year_month"] for m in months if m["complete"]), None)
    if not preferred and months:
        preferred = months[0]["year_month"]

    return jsonify(
        {
            "ok": len(errors) == 0,
            "summary": summary,
            "fill": fill_results,
            "force_mops_latest": force_mops_latest,
            "errors": errors,
            "year_months": sorted(all_ym),
            "preferred_year_month": preferred,
            "months": months,
        }
    )


@app.post("/api/fill-month")
def api_fill_month():
    """
    對指定年月用 MOPS 補齊上市+上櫃。
    - skip_if_complete=true：已齊則跳過（回補進度條）
    - force=true：即使已齊也重抓兩邊
    預設 force=true（維持舊行為：手動補單月會重抓）
    """
    body = request.get_json(silent=True) or {}
    ym = str(body.get("year_month") or "").strip()
    if not re.fullmatch(r"\d{6}", ym):
        return jsonify({"error": "year_month 須為 YYYYMM"}), 400
    skip_if_complete = bool(body.get("skip_if_complete", False))
    force = bool(body.get("force", True))
    min_count = 50
    try:
        min_count = int(body.get("min_count", 50))
    except (TypeError, ValueError):
        min_count = 50

    db = get_db()
    if skip_if_complete and not force:
        payload = _backfill_step_payload(
            ym, skip_if_complete=True, min_count=min_count
        )
    else:
        fr = ensure_both_markets(db, ym, min_count=min_count, force=force)
        payload = {
            "ok": fr["complete"] and not fr["errors"],
            "skipped": False,
            "year_month": ym,
            "label": fr["label"],
            "before": fr["before"],
            "after": fr["after"],
            "complete": fr["complete"],
            "filled": fr["filled"],
            "errors": fr["errors"],
        }

    return jsonify(
        {
            **payload,
            "result": {
                "year_month": payload.get("year_month"),
                "label": payload.get("label"),
                "before": payload.get("before"),
                "after": payload.get("after"),
                "complete": payload.get("complete"),
                "filled": payload.get("filled"),
                "errors": payload.get("errors"),
            },
            "months": list_months(db),
        }
    )


def _backfill_plan_payload(
    *,
    months: int = 24,
    end_ym: str | None = None,
    min_count: int = 50,
) -> dict[str, Any]:
    db = get_db()
    end = end_ym or latest_data_year_month(db) or default_backfill_end_ym()
    yms = year_month_range_ending(end, months)
    items = []
    need = 0
    for ym in yms:
        counts = market_counts(db, ym)
        complete = counts.get("L", 0) >= min_count and counts.get("O", 0) >= min_count
        if not complete:
            need += 1
        items.append(
            {
                "year_month": ym,
                "label": format_ym_display(ym),
                "n_listed": counts.get("L", 0),
                "n_otc": counts.get("O", 0),
                "complete": complete,
            }
        )
    return {
        "end_ym": end,
        "months_requested": months,
        "total": len(items),
        "need_fetch": need,
        "already_complete": len(items) - need,
        "items": items,
    }


def _backfill_step_payload(
    ym: str,
    *,
    skip_if_complete: bool = True,
    min_count: int = 50,
) -> dict[str, Any]:
    db = get_db()
    counts = market_counts(db, ym)
    complete = counts.get("L", 0) >= min_count and counts.get("O", 0) >= min_count
    if skip_if_complete and complete:
        return {
            "ok": True,
            "skipped": True,
            "year_month": ym,
            "label": format_ym_display(ym),
            "before": counts,
            "after": counts,
            "complete": True,
            "filled": [],
            "errors": [],
        }

    fr = ensure_both_markets(db, ym, min_count=min_count, force=False)
    return {
        "ok": fr["complete"] and not fr["errors"],
        "skipped": False,
        "year_month": ym,
        "label": fr["label"],
        "before": fr["before"],
        "after": fr["after"],
        "complete": fr["complete"],
        "filled": fr["filled"],
        "errors": fr["errors"],
    }


@app.get("/api/backfill_plan")
@app.get("/api/backfill/plan")
def api_backfill_plan():
    """
    回補計畫：近 N 個月清單 + 是否已齊。
    前端依此逐月呼叫 /api/backfill_step 以顯示進度條。
    """
    try:
        months = int(request.args.get("months", 24))
    except (TypeError, ValueError):
        months = 24
    months = max(1, min(months, 36))
    end_ym = request.args.get("end_ym") or None
    if end_ym:
        end_ym = str(end_ym).strip()
        if not re.fullmatch(r"\d{6}", end_ym):
            return jsonify({"error": "end_ym 須為 YYYYMM"}), 400
    min_count = 50
    try:
        min_count = int(request.args.get("min_count", 50))
    except (TypeError, ValueError):
        min_count = 50

    return jsonify(
        _backfill_plan_payload(months=months, end_ym=end_ym, min_count=min_count)
    )


@app.post("/api/backfill_step")
@app.post("/api/backfill/step")
def api_backfill_step():
    """回補單一資料年月（上市+上櫃）。供前端進度條逐月呼叫。"""
    body = request.get_json(silent=True) or {}
    ym = str(body.get("year_month") or "").strip()
    if not re.fullmatch(r"\d{6}", ym):
        return jsonify({"error": "year_month 須為 YYYYMM"}), 400
    skip_if_complete = body.get("skip_if_complete", True)
    min_count = 50
    try:
        min_count = int(body.get("min_count", 50))
    except (TypeError, ValueError):
        min_count = 50

    return jsonify(
        _backfill_step_payload(
            ym, skip_if_complete=bool(skip_if_complete), min_count=min_count
        )
    )


@app.post("/api/backfill")
def api_backfill():
    """
    一次回補近 N 個月（無進度串流；建議改用 plan + step）。
    已齊月份預設跳過。
    """
    body = request.get_json(silent=True) or {}
    try:
        months = int(body.get("months", 24))
    except (TypeError, ValueError):
        months = 24
    months = max(1, min(months, 36))
    end_ym = body.get("end_ym") or None
    if end_ym is not None:
        end_ym = str(end_ym).strip()
        if not re.fullmatch(r"\d{6}", end_ym):
            return jsonify({"error": "end_ym 須為 YYYYMM"}), 400
    skip_complete = body.get("skip_complete", True)
    try:
        sleep_s = float(body.get("sleep_s", 0.6))
    except (TypeError, ValueError):
        sleep_s = 0.6

    db = get_db()
    result = backfill_history(
        db,
        months=months,
        end_ym=end_ym,
        sleep_s=sleep_s,
        skip_complete=bool(skip_complete),
    )
    preferred = next((m["year_month"] for m in result["months"] if m["complete"]), None)
    result["preferred_year_month"] = preferred
    return jsonify(result)


def _attach_prices(rows: list[dict], *, allow_network_history: bool = False) -> list[dict]:
    """掛上收盤／漲跌／近六月／QuickChart（歷史優先用磁碟快取）。"""
    try:
        company_service.ensure()
        company_service.attach(rows)
    except Exception:
        pass
    try:
        price_service.ensure_quotes()
        price_service.enrich_rows(
            rows, allow_network_history=allow_network_history, max_network=0
        )
    except Exception:
        for r in rows:
            r.setdefault("last_close", None)
            r.setdefault("change_pct", None)
            r.setdefault("change_6m_pct", None)
            r.setdefault("chart_url", None)
    return rows


def fetch_latest_trends_map(
    db: sqlite3.Connection, year_month: str
) -> dict[str, dict[str, Any]]:
    """最新一筆 company_trend（依 id 最大） per company_id。"""
    rows = db.execute(
        """
        SELECT t.*
        FROM company_trend t
        INNER JOIN (
            SELECT company_id, MAX(id) AS max_id
            FROM company_trend
            WHERE year_month = ?
            GROUP BY company_id
        ) x ON t.id = x.max_id
        """,
        (year_month,),
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        r = dict(row)
        code = str(r.get("company_id") or "")
        out[code] = {
            "trend_weight": r.get("trend_weight"),
            "hot_now": r.get("hot_now"),
            "persist": r.get("persist"),
            "supply_chain_weight": r.get("supply_chain_weight"),
            "trend_rationale": r.get("rationale"),
            "us_related_tickers": _parse_json_field(r.get("us_related_tickers"), []),
            "trend_run_id": r.get("run_id"),
            "trend_revision": r.get("revision"),
            "trend_analyzed_at": r.get("analyzed_at"),
        }
    return out


def attach_trends_to_rows(
    db: sqlite3.Connection, ym: str, rows: list[dict]
) -> None:
    m = fetch_latest_trends_map(db, ym)
    for r in rows:
        t = m.get(str(r.get("company_id") or ""))
        if not t:
            r["trend_weight"] = None
            r["supply_chain_weight"] = None
            r["hot_now"] = None
            r["persist"] = None
            continue
        r.update(t)
        r["rank_score"] = rank_score_row(
            anomaly_score=r.get("anomaly_score"),
            trend_weight=t.get("trend_weight"),
            supply_chain_weight=t.get("supply_chain_weight"),
            is_e=bool(r.get("is_extreme_growth")),
            is_s=bool(r.get("is_surprise_long")),
        )


def _filter_by_trend(
    rows: list[dict],
    *,
    min_trend: float | None,
    min_supply: float | None,
    require_trend: bool,
) -> list[dict]:
    if not require_trend and min_trend is None and min_supply is None:
        return rows
    out = []
    for r in rows:
        tw = r.get("trend_weight")
        sw = r.get("supply_chain_weight")
        if require_trend and tw is None and sw is None:
            continue
        if min_trend is not None:
            if tw is None or float(tw) < min_trend:
                continue
        if min_supply is not None:
            if sw is None or float(sw) < min_supply:
                continue
        out.append(r)
    return out


def _signals_json_response(
    ym: str,
    market: str | None,
    min_revenue: float,
    *,
    min_trend_weight: float | None = 0.45,
    min_supply_weight: float | None = None,
    apply_trend_filter: bool = True,
):
    db = get_db()
    raw = compute_long_signals(db, ym, min_revenue=min_revenue, market=market)
    for key in ("extreme_growth", "surprise_turnaround", "all_long"):
        _attach_prices(raw[key], allow_network_history=False)
        attach_analysis_to_rows(db, ym, raw[key])
        attach_trends_to_rows(db, ym, raw[key])

    filtered = {}
    for key in ("extreme_growth", "surprise_turnaround", "all_long"):
        rows = raw[key]
        if apply_trend_filter:
            rows = _filter_by_trend(
                rows,
                min_trend=min_trend_weight,
                min_supply=min_supply_weight,
                require_trend=True,
            )
        filtered[key] = rows

    return {
        "version": APP_VERSION,
        "year_month": raw["year_month"],
        "label": raw["label"],
        "universe": raw["universe"],
        "params": {
            **raw["params"],
            "min_trend_weight": min_trend_weight,
            "min_supply_weight": min_supply_weight,
            "apply_trend_filter": apply_trend_filter,
        },
        "quote_as_of": price_service.quote_as_of,
        "counts": {
            "extreme_growth": len(filtered["extreme_growth"]),
            "surprise_turnaround": len(filtered["surprise_turnaround"]),
            "all_long": len(filtered["all_long"]),
            "extreme_growth_raw": len(raw["extreme_growth"]),
            "surprise_turnaround_raw": len(raw["surprise_turnaround"]),
            "all_long_raw": len(raw["all_long"]),
        },
        "extreme_growth": [serialize_signal_row(r) for r in filtered["extreme_growth"]],
        "surprise_turnaround": [
            serialize_signal_row(r) for r in filtered["surprise_turnaround"]
        ],
        "all_long": [serialize_signal_row(r) for r in filtered["all_long"]],
        "analysis_config": analysis_config_status(),
    }


@app.get("/api/signals")
@app.get("/api/long_pead")
@app.get("/api/pead")
def api_signals():
    """Long PEAD：預設先過趨勢過濾再回 E/S。"""
    ym = request.args.get("year_month", "").strip()
    if not ym:
        return jsonify({"error": "缺少 year_month", "version": APP_VERSION}), 400
    market = request.args.get("market") or None
    try:
        min_revenue = float(request.args.get("min_revenue", 50_000))
    except ValueError:
        min_revenue = 50_000
    apply_filter = str(request.args.get("apply_trend_filter", "1")).lower() not in (
        "0",
        "false",
        "no",
    )
    min_trend = None
    min_supply = None
    if request.args.get("min_trend_weight") not in (None, ""):
        try:
            min_trend = float(request.args.get("min_trend_weight"))
        except ValueError:
            min_trend = 0.45
    elif apply_filter:
        min_trend = 0.45
    if request.args.get("min_supply_weight") not in (None, ""):
        try:
            min_supply = float(request.args.get("min_supply_weight"))
        except ValueError:
            min_supply = None
    return jsonify(
        _signals_json_response(
            ym,
            market,
            min_revenue,
            min_trend_weight=min_trend,
            min_supply_weight=min_supply,
            apply_trend_filter=apply_filter,
        )
    )


@app.get("/api/signals.csv")
@app.get("/api/long_pead.csv")
def api_signals_csv():
    """下載指定月份 Long PEAD CSV。"""
    ym = request.args.get("year_month", "").strip()
    if not ym:
        return jsonify({"error": "缺少 year_month"}), 400
    market = request.args.get("market") or None
    which = request.args.get("which", "all_long")  # extreme_growth | surprise_turnaround | all_long
    try:
        min_revenue = float(request.args.get("min_revenue", 50_000))
    except ValueError:
        min_revenue = 50_000

    db = get_db()
    raw = compute_long_signals(db, ym, min_revenue=min_revenue, market=market)
    rows = raw.get(which) or raw["all_long"]

    buf = StringIO()
    fields = [
        "year_month",
        "company_id",
        "company_name",
        "market",
        "industry",
        "revenue_current",
        "yoy_pct",
        "mom_pct",
        "industry_yoy_pctile",
        "industry_yoy_median",
        "expected_yoy_s1",
        "expected_yoy_s2",
        "surprise_s1",
        "surprise_s2",
        "surprise_avg",
        "surprise_pctile",
        "prev_yoy",
        "is_turnaround",
        "is_accelerating",
        "is_extreme_growth",
        "is_surprise_long",
        "anomaly_score",
        "signal_types",
        "note",
    ]
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(
            {
                "year_month": r.get("year_month"),
                "company_id": r.get("company_id"),
                "company_name": r.get("company_name"),
                "market": r.get("market"),
                "industry": r.get("industry"),
                "revenue_current": r.get("revenue_current"),
                "yoy_pct": r.get("yoy_pct"),
                "mom_pct": r.get("mom_pct"),
                "industry_yoy_pctile": r.get("industry_yoy_pctile"),
                "industry_yoy_median": r.get("industry_yoy_median"),
                "expected_yoy_s1": r.get("expected_yoy_s1"),
                "expected_yoy_s2": r.get("expected_yoy_s2"),
                "surprise_s1": r.get("surprise_s1"),
                "surprise_s2": r.get("surprise_s2"),
                "surprise_avg": r.get("surprise_avg"),
                "surprise_pctile": r.get("surprise_pctile"),
                "prev_yoy": r.get("prev_yoy"),
                "is_turnaround": r.get("is_turnaround"),
                "is_accelerating": r.get("is_accelerating"),
                "is_extreme_growth": r.get("is_extreme_growth"),
                "is_surprise_long": r.get("is_surprise_long"),
                "anomaly_score": r.get("anomaly_score"),
                "signal_types": "|".join(r.get("signal_types") or []),
                "note": r.get("note"),
            }
        )

    filename = f"signals_{ym}_{which}.csv"
    return Response(
        "\ufeff" + buf.getvalue(),  # BOM for Excel
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/download-db")
def api_download_db():
    if not DB_PATH.exists():
        return jsonify({"error": "資料庫尚不存在，請先抓取資料"}), 404
    # Ensure connection flushed
    db = get_db()
    db.commit()
    return send_file(
        DB_PATH,
        as_attachment=True,
        download_name="revenue.db",
        mimetype="application/x-sqlite3",
    )


@app.post("/api/quotes/refresh")
def api_quotes_refresh():
    """重新抓最近交易日收盤（上市 MI_INDEX + 上櫃）。"""
    result = price_service.refresh_quotes()
    return jsonify({"version": APP_VERSION, **result})


@app.post("/api/companies/refresh")
def api_companies_refresh():
    """重新抓上市/上櫃基本資料（含官網網址）。"""
    result = company_service.refresh()
    return jsonify({"version": APP_VERSION, **result})


@app.get("/api/price/<code>")
def api_price_one(code: str):
    """單一股票：收盤、漲跌%、近六月漲跌%、QuickChart URL。"""
    code = str(code or "").strip()
    if not re.fullmatch(r"\d{3,6}", code):
        return jsonify({"error": "invalid code"}), 400
    market = request.args.get("market")  # L | O | None
    allow_network = request.args.get("network", "1") != "0"
    price_service.ensure_quotes()
    q = price_service.get_quote(code) or {}
    hist = price_service.get_history(
        code, market=market, allow_network=allow_network
    )
    spark = hist.get("spark_points") or price_service.downsample_points(
        hist.get("points") or [], max_n=48
    )
    return jsonify(
        {
            "version": APP_VERSION,
            "code": code,
            "last_close": q.get("close"),
            "change": q.get("change"),
            "change_pct": q.get("change_pct"),
            "prev_close": q.get("prev_close"),
            "quote_as_of": price_service.quote_as_of,
            "quote_source": q.get("source"),
            "change_6m_pct": hist.get("change_6m_pct"),
            "chart_url": hist.get("chart_url"),
            "spark_points": spark,
            "history_points": len(hist.get("points") or []),
            "history_source": hist.get("source"),
        }
    )


# ---------------------------------------------------------------------------
# 趨勢權重 + Ranking 30
# ---------------------------------------------------------------------------

_db_write_lock = threading.Lock()


def _next_revision(conn: sqlite3.Connection, company_id: str, year_month: str) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(MAX(revision), 0) AS m
        FROM company_trend
        WHERE company_id = ? AND year_month = ?
        """,
        (company_id, year_month),
    ).fetchone()
    return int(row["m"] or 0) + 1


def _save_company_trend(conn: sqlite3.Connection, run_id: str, result: dict[str, Any]) -> int:
    rev = _next_revision(conn, str(result["company_id"]), str(result["year_month"]))
    cur = conn.execute(
        """
        INSERT INTO company_trend (
            run_id, company_id, company_name, market, industry, year_month, revision,
            trend_weight, hot_now, persist, supply_chain_weight, rationale,
            us_related_tickers, sources_json, analyzed_at, model, status, error_message
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            result.get("company_id"),
            result.get("company_name"),
            result.get("market"),
            result.get("industry"),
            result.get("year_month"),
            rev,
            result.get("trend_weight"),
            result.get("hot_now"),
            result.get("persist"),
            result.get("supply_chain_weight"),
            result.get("rationale"),
            json.dumps(result.get("us_related_tickers") or [], ensure_ascii=False),
            json.dumps(result.get("sources_json") or {}, ensure_ascii=False),
            result.get("analyzed_at"),
            result.get("model"),
            result.get("status") or "ok",
            result.get("error_message"),
        ),
    )
    return int(cur.lastrowid)


def _run_monthly_trend_job(
    *,
    ym: str,
    min_revenue: float,
    concurrency: int,
) -> None:
    step = job_progress.make_step_callback()
    conn = _db_connect()
    run_id = ""
    try:
        j0 = job_progress.get_job() or {}
        run_id = str(j0.get("id") or f"tr{int(time.time())}")
        started = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

        job_progress.set_progress(pct=1, phase="load", message=f"載入 {ym} 有營收公司…")
        rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT * FROM monthly_revenue
                WHERE year_month = ?
                  AND revenue_current IS NOT NULL
                  AND (? <= 0 OR revenue_current >= ?)
                """,
                (ym, min_revenue, min_revenue),
            ).fetchall()
        ]
        # 同 code 多 market 取一列
        by_code: dict[str, dict] = {}
        for r in rows:
            cid = str(r.get("company_id") or "")
            if cid and cid not in by_code:
                by_code[cid] = r
        companies = list(by_code.values())
        step("ok", f"宇宙 {len(companies)} 家（min_revenue={min_revenue}）")

        conn.execute(
            """
            INSERT INTO trend_run (
                run_id, year_month, started_at, status, company_count,
                concurrency, app_version
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (run_id, ym, started, "running", len(companies), concurrency, APP_VERSION),
        )
        conn.commit()

        theme = build_theme_pack(on_step=step)
        conn.execute(
            "UPDATE trend_run SET theme_summary_json = ? WHERE run_id = ?",
            (json.dumps(theme, ensure_ascii=False), run_id),
        )
        conn.commit()

        job_progress.set_progress(
            pct=8,
            phase="score",
            total=len(companies),
            current=0,
            message=f"【2/3】並行打趨勢+供應鏈權重 · 並發 {concurrency}",
        )
        step("phase", f"並行評分 {len(companies)} 家 · concurrency={concurrency}")

        ok_n = 0
        fail_n = 0

        def on_done(result: dict, done: int, total: int) -> None:
            nonlocal ok_n, fail_n
            with _db_write_lock:
                try:
                    _save_company_trend(conn, run_id, result)
                    conn.commit()
                except Exception as e:
                    step("error", f"寫入失敗 {result.get('company_id')}: {e}")
                    fail_n += 1
                    job_progress.bump_fail()
                    return
            if (result.get("status") or "") == "error":
                fail_n += 1
                job_progress.bump_fail()
            else:
                ok_n += 1
                job_progress.bump_ok()
            pct = 8 + 90 * done / max(total, 1)
            code = result.get("company_id")
            job_progress.set_progress(
                pct=pct,
                phase="score",
                current=done,
                total=total,
                company_id=str(code or ""),
                company_name=str(result.get("company_name") or ""),
                message=None,
            )
            # 節流 log：每 10 家或前 5 家
            if done <= 5 or done % 10 == 0 or done == total:
                step(
                    "ok" if result.get("status") != "error" else "warn",
                    f"[{done}/{total}] {code} T={result.get('trend_weight')} "
                    f"SC={result.get('supply_chain_weight')} "
                    f"{(result.get('rationale') or '')[:60]}",
                )

        score_companies_parallel(
            companies,
            theme,
            concurrency=concurrency,
            should_cancel=job_progress.should_cancel,
            on_company_done=on_done,
        )

        if job_progress.should_cancel():
            conn.execute(
                """
                UPDATE trend_run SET status=?, finished_at=?, ok_count=?, fail_count=?
                WHERE run_id=?
                """,
                (
                    "cancelled",
                    datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
                    ok_n,
                    fail_n,
                    run_id,
                ),
            )
            conn.commit()
            job_progress.finish_job(
                "cancelled",
                message=f"已取消 · 成功 {ok_n} · 失敗 {fail_n}",
                result={"run_id": run_id, "year_month": ym, "ok": ok_n, "fail": fail_n},
            )
            return

        finished = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
        conn.execute(
            """
            UPDATE trend_run SET status=?, finished_at=?, ok_count=?, fail_count=?
            WHERE run_id=?
            """,
            ("done", finished, ok_n, fail_n, run_id),
        )
        conn.commit()
        step("phase", "【3/3】可開啟 Ranking 30 / PEAD 過濾")
        job_progress.finish_job(
            "done",
            message=f"趨勢 run 完成 {run_id} · 成功 {ok_n} · 失敗 {fail_n} · 宇宙 {len(companies)}",
            result={
                "run_id": run_id,
                "year_month": ym,
                "ok": ok_n,
                "fail": fail_n,
                "company_count": len(companies),
                "theme_summary": theme.get("summary_zh"),
            },
        )
    except Exception as e:
        step("error", f"趨勢 job 例外：{e}")
        try:
            if run_id:
                conn.execute(
                    "UPDATE trend_run SET status=?, finished_at=? WHERE run_id=?",
                    (
                        "error",
                        datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
                        run_id,
                    ),
                )
                conn.commit()
        except Exception:
            pass
        job_progress.finish_job("error", message=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/api/trend/run")
def api_trend_run():
    """背景：Theme Pack + 並行趨勢/供應鏈權重（當月有營收公司）。"""
    if job_progress.is_busy():
        return jsonify(
            {"ok": False, "error": "已有任務進行中", "job": job_progress.get_job(), "version": APP_VERSION}
        ), 409
    payload = request.get_json(silent=True) or {}
    ym = str(payload.get("year_month") or "").strip()
    if not ym:
        return jsonify({"error": "需要 year_month", "version": APP_VERSION}), 400
    try:
        min_revenue = float(payload.get("min_revenue", 0))
    except (TypeError, ValueError):
        min_revenue = 0.0
    try:
        concurrency = int(payload.get("concurrency") or trend_concurrency())
    except (TypeError, ValueError):
        concurrency = trend_concurrency()
    concurrency = max(1, min(concurrency, 1000))

    try:
        job_id = job_progress.start_job(
            "trend_monthly",
            meta={"year_month": ym, "min_revenue": min_revenue, "concurrency": concurrency},
        )
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e), "version": APP_VERSION}), 409

    # job id 當 run_id 前綴對齊
    t = threading.Thread(
        target=_run_monthly_trend_job,
        kwargs={"ym": ym, "min_revenue": min_revenue, "concurrency": concurrency},
        daemon=True,
        name=f"trend-{job_id}",
    )
    # 讓 job id 寫入 DB：在 worker 內用 get_job id
    t.start()
    return jsonify(
        {
            "ok": True,
            "job_id": job_id,
            "version": APP_VERSION,
            "message": "趨勢權重背景任務已啟動（平行）",
            "concurrency": concurrency,
        }
    )


@app.get("/api/trend")
def api_trend_list():
    """列出某月最新趨勢權重（每公司最新 revision）。"""
    ym = (request.args.get("year_month") or "").strip()
    if not ym:
        return jsonify({"error": "需要 year_month", "version": APP_VERSION}), 400
    try:
        min_w = float(request.args.get("min_trend_weight", 0) or 0)
    except ValueError:
        min_w = 0.0
    limit = min(int(request.args.get("limit") or 5000), 10000)
    db = get_db()
    run = db.execute(
        """
        SELECT * FROM trend_run
        WHERE year_month = ? AND status = 'done'
        ORDER BY started_at DESC LIMIT 1
        """,
        (ym,),
    ).fetchone()
    rows = db.execute(
        """
        SELECT t.*
        FROM company_trend t
        INNER JOIN (
            SELECT company_id, MAX(id) AS max_id
            FROM company_trend WHERE year_month = ?
            GROUP BY company_id
        ) x ON t.id = x.max_id
        WHERE (? <= 0 OR t.trend_weight >= ?)
        ORDER BY t.trend_weight DESC, t.supply_chain_weight DESC
        LIMIT ?
        """,
        (ym, min_w, min_w, limit),
    ).fetchall()
    items = []
    for row in rows:
        r = dict(row)
        items.append(
            {
                "id": r.get("id"),
                "run_id": r.get("run_id"),
                "company_id": r.get("company_id"),
                "company_name": r.get("company_name"),
                "market": r.get("market"),
                "industry": r.get("industry"),
                "year_month": r.get("year_month"),
                "revision": r.get("revision"),
                "trend_weight": r.get("trend_weight"),
                "hot_now": r.get("hot_now"),
                "persist": r.get("persist"),
                "supply_chain_weight": r.get("supply_chain_weight"),
                "rationale": r.get("rationale"),
                "us_related_tickers": _parse_json_field(r.get("us_related_tickers"), []),
                "analyzed_at": r.get("analyzed_at"),
                "model": r.get("model"),
                "status": r.get("status"),
            }
        )
    theme = None
    run_info = None
    if run:
        run_info = dict(run)
        theme = _parse_json_field(run_info.get("theme_summary_json"), {})
    return jsonify(
        {
            "ok": True,
            "version": APP_VERSION,
            "year_month": ym,
            "run": run_info,
            "theme": theme,
            "items": items,
            "count": len(items),
        }
    )


@app.get("/api/ranking")
def api_ranking():
    """Top 30：anomaly + trend + supply_chain；須有 E 或 S。"""
    ym = (request.args.get("year_month") or "").strip()
    if not ym:
        return jsonify({"error": "需要 year_month", "version": APP_VERSION}), 400
    market = request.args.get("market") or None
    try:
        min_revenue = float(request.args.get("min_revenue", 50_000))
    except ValueError:
        min_revenue = 50_000
    try:
        min_trend = float(request.args.get("min_trend_weight", 0.45))
    except ValueError:
        min_trend = 0.45
    try:
        top_n = int(request.args.get("top_n", 30))
    except ValueError:
        top_n = 30
    top_n = max(1, min(top_n, 100))

    db = get_db()
    raw = compute_long_signals(db, ym, min_revenue=min_revenue, market=market)
    pool = raw.get("all_long") or []
    attach_trends_to_rows(db, ym, pool)
    # 必須有趨勢資料且過門檻，且本來就是 E∪S
    cand = []
    for r in pool:
        tw = r.get("trend_weight")
        if tw is None or float(tw) < min_trend:
            continue
        if not (r.get("is_extreme_growth") or r.get("is_surprise_long")):
            continue
        r["rank_score"] = rank_score_row(
            anomaly_score=r.get("anomaly_score"),
            trend_weight=r.get("trend_weight"),
            supply_chain_weight=r.get("supply_chain_weight"),
            is_e=bool(r.get("is_extreme_growth")),
            is_s=bool(r.get("is_surprise_long")),
        )
        cand.append(r)
    cand.sort(
        key=lambda x: (
            float(x.get("rank_score") or 0),
            float(x.get("trend_weight") or 0),
            float(x.get("supply_chain_weight") or 0),
            float(x.get("anomaly_score") or 0),
        ),
        reverse=True,
    )
    top = cand[:top_n]
    for i, r in enumerate(top, 1):
        r["rank"] = i

    run = db.execute(
        """
        SELECT run_id, started_at, finished_at, status FROM trend_run
        WHERE year_month = ? ORDER BY started_at DESC LIMIT 1
        """,
        (ym,),
    ).fetchone()

    return jsonify(
        {
            "ok": True,
            "version": APP_VERSION,
            "year_month": ym,
            "label": raw.get("label"),
            "top_n": top_n,
            "min_trend_weight": min_trend,
            "candidate_count": len(cand),
            "all_long_count": len(pool),
            "trend_run": dict(run) if run else None,
            "weights": {
                "anomaly": 0.30,
                "trend": 0.35,
                "supply_chain": 0.35,
            },
            "ranking": [serialize_signal_row(r) for r in top],
        }
    )


def _enrich_company_for_analysis(
    db: sqlite3.Connection, company: dict[str, Any]
) -> dict[str, Any]:
    """Attach PEAD feature fields if the name is in the long-signal lists."""
    code = str(company.get("company_id") or "")
    ym = str(company.get("year_month") or "")
    if not code or not ym:
        return company
    try:
        full = compute_long_signals(db, ym, min_revenue=0, market=None)
        for lst in (
            full.get("all_long") or [],
            full.get("extreme_growth") or [],
            full.get("surprise_turnaround") or [],
        ):
            for r in lst:
                if str(r.get("company_id")) == code:
                    for k, v in r.items():
                        if k in ("spark_points", "chart_url"):
                            continue
                        company[k] = v
                    return company
    except Exception:
        pass
    company.setdefault("is_extreme_growth", False)
    company.setdefault("is_surprise_long", False)
    return company


def _db_connect() -> sqlite3.Connection:
    """Thread-safe standalone connection (not Flask g)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _analysis_payload(result: dict[str, Any], rid: int) -> dict[str, Any]:
    return {
        "id": rid,
        "company_id": result["company_id"],
        "company_name": result.get("company_name"),
        "market": result.get("market"),
        "industry": result.get("industry"),
        "year_month": result["year_month"],
        "analyzed_at": result["analyzed_at"],
        "pead_quality_weight": result.get("pead_quality_weight"),
        "supply_chain_weight": result.get("supply_chain_weight"),
        "composite_weight": result.get("composite_weight"),
        "evaluation": result.get("evaluation"),
        "risk_flags": result.get("risk_flags") or [],
        "us_related_tickers": result.get("us_related_tickers") or [],
        "confidence": result.get("confidence"),
        "model": result.get("model"),
        "status": result.get("status"),
        "error_message": result.get("error_message"),
        "sources": result.get("sources_json") or {},
    }


def _run_batch_top_job(
    *,
    ym: str,
    market: str | None,
    min_revenue: float,
    top_n: int,
    use_glm: bool,
) -> None:
    """Background worker: score all → analyze top N with live job_progress logs."""
    step = job_progress.make_step_callback()
    conn = _db_connect()
    try:
        job_progress.set_progress(
            pct=2, phase="signals", message=f"載入 {ym} Long PEAD 訊號…"
        )
        raw = compute_long_signals(conn, ym, min_revenue=min_revenue, market=market)
        universe = raw.get("all_long") or []
        step("ok", f"E∪S 名單 {len(universe)} 家（universe 計算完成）")

        job_progress.set_progress(
            pct=5, phase="weights", message="階段 1/2：計算全部權重…"
        )
        scored = batch_score_weights(universe, use_glm=use_glm, on_step=step)
        top = scored[:top_n]
        job_progress.set_progress(
            pct=12,
            phase="analyze",
            total=len(top),
            current=0,
            message=f"權重完成 {len(scored)} 家 → 深入前 {len(top)} 家",
        )
        step("phase", f"階段 2/2：完整訪問分析前 {len(top)} 家")

        analyses: list[dict] = []
        for i, item in enumerate(top):
            if job_progress.should_cancel():
                jsnap = job_progress.get_job() or {}
                job_progress.finish_job(
                    "cancelled",
                    message=f"已取消（成功 {jsnap.get('ok', 0)} · 失敗 {jsnap.get('fail', 0)}）",
                    result={
                        "weights": scored,
                        "top": top,
                        "analyses": analyses,
                        "year_month": ym,
                    },
                )
                return

            code = str(item.get("company_id") or "")
            name = str(item.get("company_name") or "")
            pct = 12 + (88 * i) / max(len(top), 1)
            job_progress.set_progress(
                pct=pct,
                phase="analyze",
                current=i + 1,
                total=len(top),
                company_id=code,
                company_name=name,
                message=f"分析 {i + 1}/{len(top)}：{code} {name}",
            )
            step("phase", f"━━ {i + 1}/{len(top)} {code} {name} ━━")

            row = conn.execute(
                """
                SELECT * FROM monthly_revenue
                WHERE company_id = ? AND year_month = ?
                ORDER BY market ASC LIMIT 1
                """,
                (code, ym),
            ).fetchone()
            if not row:
                step("error", f"{code} 找不到營收列，跳過")
                job_progress.bump_fail()
                continue

            company = _enrich_company_for_analysis(conn, dict(row))
            # keep rank from batch
            company["rank_score"] = item.get("rank_score")
            try:
                result = analyze_company(company, on_step=step)
                rid = save_visit_analysis(conn, result)
                payload = _analysis_payload(result, rid)
                payload["rank_score"] = item.get("rank_score")
                payload["weight_rank"] = item.get("rank")
                analyses.append(payload)
                job_progress.bump_ok()
                step(
                    "ok",
                    f"✓ {code} 已寫入 DB · PEAD={result.get('pead_quality_weight')} "
                    f"供應鏈={result.get('supply_chain_weight')}",
                )
            except Exception as e:
                job_progress.bump_fail()
                step("error", f"✗ {code} 失敗：{e}")

            done_pct = 12 + (88 * (i + 1)) / max(len(top), 1)
            job_progress.set_progress(pct=done_pct)

        j = job_progress.get_job() or {}
        job_progress.finish_job(
            "done",
            message=f"批次完成：成功 {j.get('ok', 0)} · 失敗 {j.get('fail', 0)} · 報告 {len(analyses)} 筆",
            result={
                "weights": scored,
                "top": top,
                "top_ids": [r["company_id"] for r in top],
                "analyses": analyses,
                "year_month": ym,
                "label": raw.get("label"),
                "scored": len(scored),
            },
        )
    except Exception as e:
        job_progress.log("error", f"任務例外：{e}")
        job_progress.finish_job("error", message=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/api/analysis/weights")
def api_analysis_weights():
    """為當月全部 Long PEAD（E∪S）估算權重並排序（無完整網頁；可選 GLM 批次）。"""
    payload = request.get_json(silent=True) or {}
    ym = str(payload.get("year_month") or request.args.get("year_month") or "").strip()
    if not ym:
        return jsonify({"error": "需要 year_month", "version": APP_VERSION}), 400
    market = payload.get("market") or request.args.get("market") or None
    if market == "":
        market = None
    try:
        min_revenue = float(
            payload.get("min_revenue", request.args.get("min_revenue", 50_000))
        )
    except (TypeError, ValueError):
        min_revenue = 50_000
    use_glm = str(payload.get("use_glm", "1")).lower() not in ("0", "false", "no")
    try:
        top_n = int(payload.get("top_n") or request.args.get("top_n") or 30)
    except (TypeError, ValueError):
        top_n = 30
    top_n = max(1, min(top_n, 100))

    db = get_db()
    raw = compute_long_signals(db, ym, min_revenue=min_revenue, market=market)
    universe = raw.get("all_long") or []
    scored = batch_score_weights(universe, use_glm=use_glm)
    top = scored[:top_n]
    return jsonify(
        {
            "ok": True,
            "version": APP_VERSION,
            "year_month": ym,
            "label": raw.get("label"),
            "universe": len(universe),
            "scored": len(scored),
            "top_n": top_n,
            "use_glm": use_glm,
            "weights": scored,
            "top": top,
            "top_ids": [r["company_id"] for r in top],
        }
    )


@app.post("/api/analysis/batch/start")
def api_analysis_batch_start():
    """背景執行：全部權重 → 前 N 完整分析。前端輪詢 /api/analysis/job。"""
    if job_progress.is_busy():
        j = job_progress.get_job()
        return jsonify(
            {
                "ok": False,
                "error": "已有任務進行中",
                "job": j,
                "version": APP_VERSION,
            }
        ), 409

    payload = request.get_json(silent=True) or {}
    ym = str(payload.get("year_month") or "").strip()
    if not ym:
        return jsonify({"error": "需要 year_month", "version": APP_VERSION}), 400
    market = payload.get("market") or None
    if market == "":
        market = None
    try:
        min_revenue = float(payload.get("min_revenue", 50_000))
    except (TypeError, ValueError):
        min_revenue = 50_000
    use_glm = str(payload.get("use_glm", "1")).lower() not in ("0", "false", "no")
    try:
        top_n = int(payload.get("top_n") or 30)
    except (TypeError, ValueError):
        top_n = 30
    top_n = max(1, min(top_n, 100))

    try:
        job_id = job_progress.start_job(
            "batch_top",
            meta={
                "year_month": ym,
                "market": market,
                "min_revenue": min_revenue,
                "top_n": top_n,
                "use_glm": use_glm,
            },
        )
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e), "version": APP_VERSION}), 409

    t = threading.Thread(
        target=_run_batch_top_job,
        kwargs={
            "ym": ym,
            "market": market,
            "min_revenue": min_revenue,
            "top_n": top_n,
            "use_glm": use_glm,
        },
        daemon=True,
        name=f"batch-{job_id}",
    )
    t.start()
    return jsonify(
        {
            "ok": True,
            "job_id": job_id,
            "version": APP_VERSION,
            "message": "背景任務已啟動，請輪詢 /api/analysis/job",
        }
    )


@app.get("/api/analysis/job")
def api_analysis_job():
    """目前／最近一次任務進度 + 活動日誌（給前端小視窗）。"""
    j = job_progress.get_job()
    return jsonify({"ok": True, "job": j, "version": APP_VERSION, "busy": job_progress.is_busy()})


@app.post("/api/analysis/job/cancel")
def api_analysis_job_cancel():
    ok = job_progress.request_cancel()
    return jsonify({"ok": ok, "version": APP_VERSION, "job": job_progress.get_job()})


@app.post("/api/analysis/job/reset")
def api_analysis_job_reset():
    """強制清掉卡住的任務狀態（例如 cancel 後 thread 已死）。"""
    j = job_progress.get_job()
    if j and j.get("status") == "running":
        job_progress.request_cancel()
        job_progress.finish_job("cancelled", message="強制 reset")
    elif j:
        job_progress.finish_job(j.get("status") or "cancelled", message="reset")
    # hard clear
    with job_progress._lock:  # noqa: SLF001
        job_progress._job = None  # type: ignore[attr-defined]
    return jsonify({"ok": True, "version": APP_VERSION, "job": None})


@app.post("/api/analysis/run")
def api_analysis_run():
    """執行一次訪問分析（Brave + Firecrawl + GLM），寫入 DB 含時間戳。"""
    payload = request.get_json(silent=True) or {}
    code = str(
        payload.get("company_id") or request.args.get("company_id") or ""
    ).strip()
    ym = str(payload.get("year_month") or request.args.get("year_month") or "").strip()
    if not code or not ym:
        return jsonify({"error": "需要 company_id 與 year_month", "version": APP_VERSION}), 400

    # optional: log into job console even for single run if not busy
    use_console = not job_progress.is_busy()
    if use_console:
        try:
            job_progress.start_job("single", meta={"company_id": code, "year_month": ym})
        except RuntimeError:
            use_console = False

    step = job_progress.make_step_callback() if use_console else None
    if use_console:
        job_progress.set_progress(
            pct=5,
            phase="analyze",
            current=1,
            total=1,
            company_id=code,
            message=f"單家訪問分析 {code}",
        )

    db = get_db()
    row = db.execute(
        """
        SELECT * FROM monthly_revenue
        WHERE company_id = ? AND year_month = ?
        ORDER BY market ASC
        LIMIT 1
        """,
        (code, ym),
    ).fetchone()
    if not row:
        if use_console:
            job_progress.finish_job("error", message=f"找不到 {code} @ {ym}")
        return jsonify({"error": f"找不到 {code} @ {ym} 營收列", "version": APP_VERSION}), 404

    company = _enrich_company_for_analysis(db, dict(row))

    try:
        result = analyze_company(company, on_step=step)
        rid = save_visit_analysis(db, result)
        analysis = _analysis_payload(result, rid)
        if use_console:
            job_progress.bump_ok()
            job_progress.finish_job(
                "done",
                message=f"{code} 分析完成",
                result={"analysis": analysis},
            )
        return jsonify({"ok": True, "version": APP_VERSION, "analysis": analysis})
    except Exception as e:
        if use_console:
            job_progress.finish_job("error", message=str(e))
        return jsonify({"ok": False, "error": str(e), "version": APP_VERSION}), 500


@app.get("/api/analysis")
def api_analysis_get():
    """查詢訪問分析：latest=1 取最新，或 list 該月。"""
    db = get_db()
    code = (request.args.get("company_id") or "").strip()
    ym = (request.args.get("year_month") or "").strip()
    latest = (request.args.get("latest") or "1") == "1"
    limit = min(int(request.args.get("limit") or 50), 200)

    if code and ym and latest:
        row = db.execute(
            """
            SELECT * FROM visit_analysis
            WHERE company_id = ? AND year_month = ?
            ORDER BY id DESC LIMIT 1
            """,
            (code, ym),
        ).fetchone()
        if not row:
            return jsonify({"ok": True, "analysis": None, "version": APP_VERSION})
        return jsonify(
            {
                "ok": True,
                "analysis": serialize_visit_analysis(row, include_sources=True),
                "version": APP_VERSION,
            }
        )

    clauses: list[str] = []
    params: list[Any] = []
    if code:
        clauses.append("company_id = ?")
        params.append(code)
    if ym:
        clauses.append("year_month = ?")
        params.append(ym)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = db.execute(
        f"""
        SELECT id, company_id, company_name, market, industry, year_month, analyzed_at,
               pead_quality_weight, supply_chain_weight, composite_weight,
               evaluation, risk_flags, us_related_tickers, confidence,
               model, status, error_message
        FROM visit_analysis
        {where}
        ORDER BY id DESC
        LIMIT ?
        """,
        [*params, limit],
    ).fetchall()
    items = [serialize_visit_analysis(r, include_sources=False) for r in rows]
    return jsonify({"ok": True, "items": items, "count": len(items), "version": APP_VERSION})


@app.get("/api/analysis/<int:analysis_id>")
def api_analysis_detail(analysis_id: int):
    db = get_db()
    row = db.execute(
        "SELECT * FROM visit_analysis WHERE id = ?", (analysis_id,)
    ).fetchone()
    if not row:
        return jsonify({"error": "not found", "version": APP_VERSION}), 404
    return jsonify(
        {
            "ok": True,
            "analysis": serialize_visit_analysis(row, include_sources=True),
            "version": APP_VERSION,
        }
    )


@app.get("/api/status")
def api_status():
    db = get_db()
    months = list_months(db)
    total = db.execute("SELECT COUNT(*) FROM monthly_revenue").fetchone()[0]
    n_analysis = 0
    try:
        n_analysis = db.execute("SELECT COUNT(*) FROM visit_analysis").fetchone()[0]
    except sqlite3.OperationalError:
        pass
    return jsonify(
        {
            "version": APP_VERSION,
            "db_path": str(DB_PATH),
            "db_exists": DB_PATH.exists(),
            "db_size_bytes": DB_PATH.stat().st_size if DB_PATH.exists() else 0,
            "total_rows": total,
            "visit_analysis_rows": n_analysis,
            "months": months,
            "apis": API_URLS,
            "analysis_config": analysis_config_status(),
            "features": [
                "fill-month",
                "backfill_plan",
                "backfill_step",
                "signals",
                "long_pead",
                "pead",
                "signals.csv",
                "price",
                "quotes/refresh",
                "analysis/run",
                "analysis/weights",
                "analysis/batch/start",
                "analysis/job",
                "analysis",
                "trend/run",
                "trend",
                "ranking",
            ],
            "quote_as_of": price_service.quote_as_of,
            "quote_count": len(price_service.quotes),
            "company_count": len(company_service.by_code),
            "company_with_website": sum(
                1 for v in company_service.by_code.values() if v.get("website")
            ),
            "api_routes": sorted(
                r.rule for r in app.url_map.iter_rules() if str(r.rule).startswith("/api")
            ),
            "note": (
                "OpenAPI 僅最新月；歷史可用「回補 24 個月」（逐月 fill-month）。"
                "Long PEAD：極端成長 + S1/S2 驚喜轉折（只做多）。"
                "訪問分析：Brave+Firecrawl+GLM 權重寫入 DB。"
                "請用本程式開啟的網址，勿用 Live Server 直接開 HTML。"
            ),
        }
    )


def main() -> None:
    import os
    import socket

    init_db()
    # 清掉上次殘留的 job 狀態，避免前端誤以為「還在跑」
    try:
        live = DATA_DIR / "job_live.json"
        if live.is_file():
            live.unlink()
        with job_progress._lock:  # noqa: SLF001
            job_progress._job = None  # type: ignore[attr-defined]
    except Exception:
        pass
    host = os.environ.get("HOST", "127.0.0.1")
    # 本機預設 5051（5050 常被舊行程佔用造成「有網頁但 API 404」）
    port = int(os.environ.get("PORT", "5051"))

    # 若預設埠被佔用，自動往後找空埠（避免 Address already in use）
    def port_free(p: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind((host, p))
                return True
            except OSError:
                return False

    if not port_free(port):
        # 釋放目標埠上的殘留行程（避免舊 app 佔埠 → 網頁有、API 404）
        if os.environ.get("TWSE_KILL_PORT", "1") == "1":
            import subprocess

            try:
                out = subprocess.check_output(["lsof", "-ti", f":{port}"], text=True)
                for pid in out.split():
                    print(f"Killing stale process on :{port} pid={pid}")
                    subprocess.call(["kill", "-9", pid])
            except Exception:
                pass
            import time as _time

            _time.sleep(0.4)
        if not port_free(port):
            for candidate in range(port + 1, port + 20):
                if port_free(candidate):
                    print(f"Port {port} in use, switching to {candidate}")
                    port = candidate
                    break
            else:
                raise SystemExit(f"No free port in range {port}-{port + 19}")

    print(f"=== TWSE Long PEAD {APP_VERSION} ===")
    print(f"Open http://{host}:{port}")
    print("API routes:", ", ".join(
        sorted(r.rule for r in app.url_map.iter_rules() if str(r.rule).startswith("/api"))
    ))
    # use_reloader=False：避免 debug reloader 再開第二個 process 佔埠
    # threaded=True：背景批次分析時前端仍可輪詢 /api/analysis/job
    app.run(host=host, port=port, debug=True, use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
