"""
台股每月營收瀏覽（PEAD 研究用 MVP）
- 從證交所 / 櫃買 OpenAPI 抓最新營收
- 若某月只有單一邊市場，自動用公開資訊觀測站彙總表補齊另一邊
  → 同一「資料年月」可同時看到上市 + 上櫃
- 寫入 SQLite，可依月份檢視 / 下載資料庫
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from flask import Flask, g, jsonify, render_template, request, send_file

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
) -> dict[str, Any]:
    """
    確保某年月同時有上市 + 上櫃。
    若某一邊缺資料（或筆數過少），用 MOPS 彙總表補抓。
    """
    counts = market_counts(db, year_month)
    result: dict[str, Any] = {
        "year_month": year_month,
        "label": format_ym_display(year_month),
        "before": dict(counts),
        "filled": [],
        "errors": [],
    }
    for market in ("L", "O"):
        need = force or counts.get(market, 0) < min_count
        if not need:
            continue
        try:
            rows = fetch_mops_month(market, year_month)
            n = upsert_rows(db, rows)
            result["filled"].append(
                {
                    "market": market,
                    "market_label": MARKET_LABEL[market],
                    "fetched": n,
                    "source": "mops",
                }
            )
        except Exception as exc:  # noqa: BLE001
            result["errors"].append(
                {"market": market, "error": str(exc), "source": "mops"}
            )
    result["after"] = market_counts(db, year_month)
    result["complete"] = (
        result["after"].get("L", 0) >= min_count
        and result["after"].get("O", 0) >= min_count
    )
    return result


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
    out = []
    for r in cur.fetchall():
        n_l = int(r["n_listed"] or 0)
        n_o = int(r["n_otc"] or 0)
        complete = n_l > 0 and n_o > 0
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

@app.route("/")
def index():
    return render_template("index.html")


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
    1) 抓上市 + 上櫃 OpenAPI 最新月
    2) 對出現的每個資料年月，若缺另一邊市場，用 MOPS 彙總表補齊
       → 同一月份可同時有上市 + 上櫃
    """
    body = request.get_json(silent=True) or {}
    markets = body.get("markets") or ["L", "O"]
    markets = [m for m in markets if m in API_URLS]
    if not markets:
        return jsonify({"error": "markets 無效"}), 400
    # 是否對「已存在但未齊」的舊月份也補齊（預設 True）
    fill_all_incomplete = body.get("fill_all_incomplete", True)

    db = get_db()
    summary = []
    errors = []
    all_ym: set[str] = set()

    for market in markets:
        try:
            rows = fetch_market(market)
            n = upsert_rows(db, rows)
            yms = sorted({r["year_month"] for r in rows})
            all_ym.update(yms)
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

    # 補齊：本次抓到的月份 +（可選）DB 內所有未齊月份
    ym_to_fill = set(all_ym)
    if fill_all_incomplete:
        for m in list_months(db):
            if not m["complete"]:
                ym_to_fill.add(m["year_month"])

    fill_results = []
    for ym in sorted(ym_to_fill):
        fr = ensure_both_markets(db, ym, min_count=50, force=False)
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
    # 預設建議：最新「已齊」月份，否則最新月份
    preferred = next((m["year_month"] for m in months if m["complete"]), None)
    if not preferred and months:
        preferred = months[0]["year_month"]

    return jsonify(
        {
            "ok": len(errors) == 0,
            "summary": summary,
            "fill": fill_results,
            "errors": errors,
            "year_months": sorted(all_ym),
            "preferred_year_month": preferred,
            "months": months,
        }
    )


@app.post("/api/fill-month")
def api_fill_month():
    """對指定年月強制用 MOPS 補齊上市+上櫃。"""
    body = request.get_json(silent=True) or {}
    ym = str(body.get("year_month") or "").strip()
    if not re.fullmatch(r"\d{6}", ym):
        return jsonify({"error": "year_month 須為 YYYYMM"}), 400
    db = get_db()
    result = ensure_both_markets(db, ym, min_count=50, force=True)
    return jsonify(
        {
            "ok": result["complete"] and not result["errors"],
            "result": result,
            "months": list_months(db),
        }
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


@app.get("/api/status")
def api_status():
    db = get_db()
    months = list_months(db)
    total = db.execute("SELECT COUNT(*) FROM monthly_revenue").fetchone()[0]
    return jsonify(
        {
            "db_path": str(DB_PATH),
            "db_exists": DB_PATH.exists(),
            "db_size_bytes": DB_PATH.stat().st_size if DB_PATH.exists() else 0,
            "total_rows": total,
            "months": months,
            "apis": API_URLS,
            "note": "官方 OpenAPI 僅提供最新一個月；選月份是檢視已寫入 DB 的資料。",
        }
    )


def main() -> None:
    import os
    import socket

    init_db()
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "5050"))

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
        for candidate in range(port + 1, port + 20):
            if port_free(candidate):
                print(f"Port {port} in use, switching to {candidate}")
                port = candidate
                break
        else:
            raise SystemExit(f"No free port in range {port}-{port + 19}")

    print(f"Open http://{host}:{port}")
    # use_reloader=False：避免 debug reloader 再開第二個 process 佔埠
    app.run(host=host, port=port, debug=True, use_reloader=False)


if __name__ == "__main__":
    main()
