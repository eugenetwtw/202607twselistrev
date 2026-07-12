"""股價：最近交易日收盤／漲跌、近六月漲跌%、QuickChart 折線。

參考 eugenetwtw/202607twselist 的 data_service，改為 Flask 同步 requests 版。
"""

from __future__ import annotations

import json
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

TZ_TW = timezone(timedelta(hours=8))
APP_DIR = Path(__file__).resolve().parent
CACHE_DIR = APP_DIR / "data" / "price_cache"
HISTORY_DIR = CACHE_DIR / "history"
QUICKCHART_BASE = "https://quickchart.io/chart"
HISTORY_LOOKBACK_DAYS = 183

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}


def _now_tw() -> datetime:
    return datetime.now(TZ_TW)


def _to_roc_slash(dt: datetime) -> str:
    return f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"


def _parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s or s in {"-", "--", "---", "X", "N/A"}:
        return None
    s = s.replace(",", "").replace(" ", "")
    s = re.sub(r"<[^>]+>", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def _parse_signed_change(sign_html: Any, diff: Any) -> float | None:
    amount = _parse_number(diff)
    if amount is None:
        return None
    if amount == 0:
        return 0.0
    sign_raw = re.sub(r"<[^>]+>", "", str(sign_html or "")).strip()
    if "green" in str(sign_html) or sign_raw in {"-", "－", "▼"}:
        return -abs(amount)
    if "red" in str(sign_html) or sign_raw in {"+", "＋", "▲"}:
        return abs(amount)
    return amount


def _parse_roc_date_str(s: str) -> date | None:
    m = re.match(r"(\d{2,3})/(\d{1,2})/(\d{1,2})", str(s or "").strip())
    if not m:
        return None
    try:
        return date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


class PriceService:
    def __init__(self) -> None:
        self.quotes: dict[str, dict[str, Any]] = {}
        self.quote_as_of: str | None = None
        self.last_quote_refresh: float | None = None
        self._lock = threading.Lock()
        self._hist_locks: dict[str, threading.Lock] = {}
        self._last_official = 0.0
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    # ── quotes ──────────────────────────────────────────────────────────

    def ensure_quotes(self, max_age_sec: float = 3600 * 6) -> dict[str, Any]:
        with self._lock:
            if (
                self.quotes
                and self.last_quote_refresh
                and time.time() - self.last_quote_refresh < max_age_sec
            ):
                return {
                    "ok": True,
                    "cached": True,
                    "count": len(self.quotes),
                    "as_of": self.quote_as_of,
                }
            return self.refresh_quotes()

    def refresh_quotes(self) -> dict[str, Any]:
        twse, twse_d = self._fetch_twse_quotes()
        tpex, tpex_d = self._fetch_tpex_quotes()
        merged = {**twse, **tpex}
        self.quotes = merged
        self.last_quote_refresh = time.time()
        parts = [p for p in [twse_d, tpex_d] if p]
        self.quote_as_of = " / ".join(dict.fromkeys(parts)) if parts else None
        # disk snapshot
        try:
            (CACHE_DIR / "quotes.json").write_text(
                json.dumps(
                    {"as_of": self.quote_as_of, "quotes": merged, "ts": self.last_quote_refresh},
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass
        return {
            "ok": bool(merged),
            "cached": False,
            "count": len(merged),
            "as_of": self.quote_as_of,
            "twse": len(twse),
            "tpex": len(tpex),
        }

    def load_quotes_disk(self) -> bool:
        path = CACHE_DIR / "quotes.json"
        if not path.exists():
            return False
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            self.quotes = data.get("quotes") or {}
            self.quote_as_of = data.get("as_of")
            self.last_quote_refresh = data.get("ts") or time.time()
            return bool(self.quotes)
        except Exception:
            return False

    def get_quote(self, code: str) -> dict[str, Any] | None:
        return self.quotes.get(str(code).strip())

    def _fetch_twse_quotes(self) -> tuple[dict[str, dict[str, Any]], str | None]:
        base = (
            "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
            "?response=json&type=ALLBUT0999"
        )
        for offset in range(0, 8):
            dt = _now_tw() - timedelta(days=offset)
            date_param = dt.strftime("%Y%m%d")
            try:
                resp = requests.get(
                    f"{base}&date={date_param}", headers=HEADERS, timeout=45
                )
                data = resp.json()
            except Exception:
                continue
            if not data or data.get("stat") != "OK":
                continue
            target = None
            for table in data.get("tables") or []:
                fields = table.get("fields") or []
                if fields and fields[0] == "證券代號" and "收盤價" in fields:
                    target = table
                    break
            if not target or not target.get("data"):
                continue
            fields = target["fields"]
            idx = {name: i for i, name in enumerate(fields)}
            out: dict[str, dict[str, Any]] = {}
            for row in target["data"]:
                code = str(row[idx["證券代號"]]).strip()
                if not re.fullmatch(r"\d{4,6}", code):
                    continue
                close = _parse_number(row[idx["收盤價"]])
                change = _parse_signed_change(
                    row[idx.get("漲跌(+/-)", -1)] if "漲跌(+/-)" in idx else None,
                    row[idx["漲跌價差"]] if "漲跌價差" in idx else None,
                )
                prev = None
                change_pct = None
                if close is not None and change is not None:
                    prev = close - change
                    if prev:
                        change_pct = (change / prev) * 100
                out[code] = {
                    "code": code,
                    "close": close,
                    "change": change,
                    "change_pct": change_pct,
                    "prev_close": prev,
                    "source": "TWSE",
                }
            if out:
                return out, str(data.get("date") or date_param)
        return {}, None

    def _fetch_tpex_quotes(self) -> tuple[dict[str, dict[str, Any]], str | None]:
        base = (
            "https://www.tpex.org.tw/web/stock/aftertrading/"
            "otc_quotes_no1430/stk_wn1430_result.php"
            "?l=zh-tw&o=json&se=EW"
        )
        for offset in range(0, 8):
            dt = _now_tw() - timedelta(days=offset)
            d = _to_roc_slash(dt)
            try:
                resp = requests.get(f"{base}&d={d}", headers=HEADERS, timeout=45)
                data = resp.json()
            except Exception:
                continue
            tables = data.get("tables") or []
            if not tables:
                continue
            rows = tables[0].get("data") or []
            if not rows:
                continue
            out: dict[str, dict[str, Any]] = {}
            for row in rows:
                if len(row) < 4:
                    continue
                code = str(row[0]).strip()
                if not re.fullmatch(r"\d{4,6}", code):
                    continue
                close = _parse_number(row[2])
                change = _parse_number(row[3])
                prev = None
                change_pct = None
                if close is not None and change is not None:
                    prev = close - change
                    if prev:
                        change_pct = (change / prev) * 100
                out[code] = {
                    "code": code,
                    "close": close,
                    "change": change,
                    "change_pct": change_pct,
                    "prev_close": prev,
                    "source": "TPEx",
                }
            if out:
                as_of = tables[0].get("date") or data.get("date") or d
                return out, str(as_of)
        return {}, None

    # ── history + quickchart ────────────────────────────────────────────

    @staticmethod
    def downsample_points(
        points: list[dict[str, Any]], max_n: int = 48
    ) -> list[dict[str, Any]]:
        """Downsample {date, close} for charts / API payload."""
        bars = [
            {"date": str(p.get("date") or "")[:10], "close": float(p["close"])}
            for p in points
            if p.get("close") is not None and float(p["close"]) > 0
        ]
        if len(bars) <= max_n:
            return bars
        if max_n <= 1:
            return bars[-1:]
        step = (len(bars) - 1) / (max_n - 1)
        return [bars[round(i * step)] for i in range(max_n)]

    @staticmethod
    def build_quickchart_url(
        code: str,
        points: list[dict[str, Any]],
        *,
        width: int = 280,
        height: int = 96,
    ) -> str | None:
        """QuickChart 折線：含 X（日期）/ Y（股價）軸標 — 對齊 202607twselist。"""
        spark = PriceService.downsample_points(points, max_n=48)
        if len(spark) < 2:
            return None
        closes = [float(p["close"]) for p in spark]
        labels = []
        for p in spark:
            d = str(p.get("date") or "")
            labels.append(d[5:10] if len(d) >= 10 else d)
        first, last = closes[0], closes[-1]
        # 台股慣例：紅漲綠跌
        if last > first:
            color = "rgb(248,113,113)"
        elif last < first:
            color = "rgb(74,222,128)"
        else:
            color = "rgb(148,163,184)"
        config = {
            "type": "line",
            "data": {
                "labels": labels,
                "datasets": [
                    {
                        "label": code,
                        "data": closes,
                        "fill": False,
                        "borderColor": color,
                        "borderWidth": 1.75,
                        "pointRadius": 0,
                        "pointHoverRadius": 0,
                        "tension": 0.25,
                    }
                ],
            },
            "options": {
                "responsive": False,
                "legend": {"display": False},
                "tooltips": {"enabled": False},
                "layout": {
                    "padding": {"left": 2, "right": 2, "top": 4, "bottom": 2}
                },
                "scales": {
                    "xAxes": [
                        {
                            "display": True,
                            "gridLines": {"display": False, "drawBorder": False},
                            "ticks": {
                                "fontSize": 8,
                                "fontColor": "rgba(148,163,184,0.85)",
                                "maxTicksLimit": 4,
                                "maxRotation": 0,
                                "autoSkip": True,
                                "padding": 2,
                            },
                        }
                    ],
                    "yAxes": [
                        {
                            "display": True,
                            "gridLines": {
                                "color": "rgba(148,163,184,0.15)",
                                "drawBorder": False,
                                "zeroLineColor": "rgba(148,163,184,0.15)",
                            },
                            "ticks": {
                                "fontSize": 8,
                                "fontColor": "rgba(148,163,184,0.85)",
                                "maxTicksLimit": 3,
                                "padding": 2,
                            },
                        }
                    ],
                },
            },
        }
        chart_json = json.dumps(config, ensure_ascii=False, separators=(",", ":"))
        params = (
            f"w={width}&h={height}&devicePixelRatio=2"
            f"&bkg=transparent&f=png&c={quote(chart_json, safe='')}"
        )
        return f"{QUICKCHART_BASE}?{params}"

    @staticmethod
    def compute_change_6m_pct(points: list[dict[str, Any]]) -> float | None:
        closes = [
            float(p["close"])
            for p in points
            if p.get("close") is not None and float(p["close"]) > 0
        ]
        if len(closes) < 2:
            return None
        first, last = closes[0], closes[-1]
        if first == 0:
            return None
        return (last - first) / first * 100.0

    def _hist_lock(self, code: str) -> threading.Lock:
        with self._lock:
            if code not in self._hist_locks:
                self._hist_locks[code] = threading.Lock()
            return self._hist_locks[code]

    def _history_path(self, code: str) -> Path:
        return HISTORY_DIR / f"{code}.json"

    def _load_history_disk(self, code: str) -> dict[str, Any] | None:
        path = self._history_path(code)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            # stale if older than 1 day
            ts = data.get("fetched_at") or 0
            if time.time() - float(ts) > 86400 * 2:
                # still usable as cache but mark
                data["_stale"] = True
            return data
        except Exception:
            return None

    def _save_history_disk(self, code: str, payload: dict[str, Any]) -> None:
        try:
            payload = dict(payload)
            payload["fetched_at"] = time.time()
            self._history_path(code).write_text(
                json.dumps(payload, ensure_ascii=False), encoding="utf-8"
            )
        except Exception:
            pass

    def _throttle_official(self) -> None:
        now = time.time()
        wait = 0.25 - (now - self._last_official)
        if wait > 0:
            time.sleep(wait)
        self._last_official = time.time()

    def _fetch_yahoo(self, code: str, market: str | None) -> list[dict[str, Any]]:
        suffixes = [".TWO", ".TW"] if market == "O" else [".TW", ".TWO"]
        hosts = (
            "https://query2.finance.yahoo.com/v8/finance/chart",
            "https://query1.finance.yahoo.com/v8/finance/chart",
        )
        end = _now_tw().date()
        start = end - timedelta(days=HISTORY_LOOKBACK_DAYS)
        for host in hosts:
            for suffix in suffixes:
                symbol = f"{code}{suffix}"
                try:
                    resp = requests.get(
                        f"{host}/{symbol}",
                        params={"range": "6mo", "interval": "1d", "events": "history"},
                        headers=HEADERS,
                        timeout=25,
                    )
                    if resp.status_code != 200:
                        continue
                    payload = resp.json()
                except Exception:
                    continue
                results = ((payload.get("chart") or {}).get("result")) or []
                if not results:
                    continue
                result = results[0]
                timestamps = result.get("timestamp") or []
                quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
                closes = quote.get("close") or []
                points: list[dict[str, Any]] = []
                for ts, close in zip(timestamps, closes):
                    if ts is None or close is None:
                        continue
                    try:
                        c = float(close)
                    except (TypeError, ValueError):
                        continue
                    if c <= 0:
                        continue
                    d = datetime.fromtimestamp(int(ts), tz=TZ_TW).date()
                    if d < start or d > end:
                        continue
                    points.append({"date": d.isoformat(), "close": c})
                if len(points) >= 2:
                    return points
        return []

    def _fetch_twse_month(self, code: str, month: date) -> list[dict[str, Any]]:
        urls = (
            "https://www.twse.com.tw/exchangeReport/STOCK_DAY",
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY",
        )
        params = {
            "response": "json",
            "date": month.strftime("%Y%m%d"),
            "stockNo": code,
        }
        for url in urls:
            self._throttle_official()
            try:
                resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
                if resp.status_code != 200:
                    continue
                payload = resp.json()
            except Exception:
                continue
            if str(payload.get("stat") or "") != "OK":
                continue
            points: list[dict[str, Any]] = []
            for row in payload.get("data") or []:
                if not row or len(row) < 7:
                    continue
                d = _parse_roc_date_str(str(row[0]))
                close = _parse_number(row[6])
                if d is None or close is None or close <= 0:
                    continue
                points.append({"date": d.isoformat(), "close": close})
            if points:
                return points
        return []

    def _fetch_tpex_month(self, code: str, month: date) -> list[dict[str, Any]]:
        url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
        self._throttle_official()
        try:
            resp = requests.get(
                url,
                params={
                    "code": code,
                    "date": f"{month.year:04d}/{month.month:02d}/01",
                    "response": "json",
                },
                headers=HEADERS,
                timeout=30,
            )
            if resp.status_code != 200:
                return []
            payload = resp.json()
        except Exception:
            return []
        tables = payload.get("tables") or []
        if not tables:
            return []
        points: list[dict[str, Any]] = []
        for row in tables[0].get("data") or []:
            if not row or len(row) < 7:
                continue
            d = _parse_roc_date_str(str(row[0]))
            close = _parse_number(row[6])
            if d is None or close is None or close <= 0:
                continue
            points.append({"date": d.isoformat(), "close": close})
        return points

    def _fetch_history_official(
        self, code: str, market: str | None
    ) -> list[dict[str, Any]]:
        end = _now_tw().date()
        start = end - timedelta(days=HISTORY_LOOKBACK_DAYS)
        # month starts
        months: list[date] = []
        y, m = start.year, start.month
        while date(y, m, 1) <= end:
            months.append(date(y, m, 1))
            m += 1
            if m > 12:
                m = 1
                y += 1
        by_date: dict[str, float] = {}
        for month in months:
            if market == "O":
                pts = self._fetch_tpex_month(code, month) or self._fetch_twse_month(
                    code, month
                )
            else:
                pts = self._fetch_twse_month(code, month) or self._fetch_tpex_month(
                    code, month
                )
            for p in pts:
                d = str(p.get("date") or "")[:10]
                c = p.get("close")
                if d and c is not None:
                    by_date[d] = float(c)
        return [{"date": d, "close": by_date[d]} for d in sorted(by_date)]

    def get_history(
        self,
        code: str,
        *,
        market: str | None = None,
        allow_network: bool = True,
    ) -> dict[str, Any]:
        code = str(code).strip()
        with self._hist_lock(code):
            def _normalize_hist(payload: dict[str, Any]) -> dict[str, Any]:
                pts = payload.get("points") or []
                if pts and not payload.get("chart_url"):
                    # 強制用有軸標的新版 QuickChart
                    payload["chart_url"] = self.build_quickchart_url(code, pts)
                if pts and not payload.get("spark_points"):
                    payload["spark_points"] = self.downsample_points(pts, max_n=48)
                # 舊快取可能是無軸標 URL — 重建
                if pts:
                    payload["chart_url"] = self.build_quickchart_url(code, pts)
                return payload

            cached = self._load_history_disk(code)
            if cached and cached.get("points") and not cached.get("_stale"):
                return _normalize_hist(cached)
            if not allow_network:
                if cached and cached.get("points"):
                    return _normalize_hist(cached)
                return {
                    "code": code,
                    "points": [],
                    "spark_points": [],
                    "chart_url": None,
                    "change_6m_pct": None,
                }

            points = self._fetch_yahoo(code, market)
            source = "yahoo" if points else None
            if len(points) < 2:
                points = self._fetch_history_official(code, market)
                source = "official" if points else source
            if len(points) > 140:
                points = points[-140:]
            chart_url = self.build_quickchart_url(code, points) if points else None
            change_6m = self.compute_change_6m_pct(points) if points else None
            spark_points = self.downsample_points(points, max_n=48) if points else []
            payload = {
                "code": code,
                "points": points,
                "spark_points": spark_points,
                "chart_url": chart_url,
                "change_6m_pct": change_6m,
                "source": source,
                "as_of": _now_tw().date().isoformat(),
            }
            if points:
                self._save_history_disk(code, payload)
            elif cached and cached.get("points"):
                # network failed — fall back to stale
                return cached
            return payload

    def enrich_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        allow_network_history: bool = False,
        max_network: int = 0,
    ) -> list[dict[str, Any]]:
        """Attach quote + optional cached history to signal rows."""
        self.ensure_quotes()
        networked = 0
        for r in rows:
            code = str(r.get("company_id") or "").strip()
            q = self.get_quote(code) or {}
            r["last_close"] = q.get("close")
            r["change_pct"] = q.get("change_pct")
            r["change"] = q.get("change")
            r["quote_source"] = q.get("source")
            r["quote_as_of"] = self.quote_as_of

            allow_net = allow_network_history and networked < max_network
            hist = self.get_history(
                code,
                market=r.get("market"),
                allow_network=allow_net,
            )
            if allow_net and hist.get("points"):
                networked += 1
            r["change_6m_pct"] = hist.get("change_6m_pct")
            r["chart_url"] = hist.get("chart_url")
            r["spark_points"] = hist.get("spark_points") or []
            r["history_points"] = len(hist.get("points") or [])
        return rows


# singleton
price_service = PriceService()
price_service.load_quotes_disk()
