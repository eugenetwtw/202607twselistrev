"""公司基本資料：官網網址（對齊 202607twselist）。

來源：
- 上市 t187ap03_L「網址」
- 上櫃 mopsfin_t187ap03_O「WebAddress」
"""

from __future__ import annotations

import json
import re
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

APP_DIR = Path(__file__).resolve().parent
CACHE_FILE = APP_DIR / "data" / "company_websites.json"

PROFILE_URLS = {
    "L": "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
    "O": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
}

HEADERS = {
    "User-Agent": "twselistrev/0.3 (company profiles)",
    "Accept": "application/json",
}


def normalize_website(raw: Any) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip().replace("\u3000", "").replace("　", "").strip()
    if not s or s in {"-", "－", "—", "N/A", "n/a", "None"}:
        return None
    if not re.match(r"^https?://", s, re.I):
        s = "https://" + s
    # strip trailing junk
    s = s.rstrip(" \t/;，,")
    try:
        p = urlparse(s)
        if not p.netloc:
            return None
    except Exception:
        return None
    return s


class CompanyService:
    def __init__(self) -> None:
        # code -> {website, company_name, market, ...}
        self.by_code: dict[str, dict[str, Any]] = {}
        self.last_refresh: float | None = None
        self._lock = threading.Lock()
        self.load_disk()

    def load_disk(self) -> bool:
        if not CACHE_FILE.exists():
            return False
        try:
            data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            self.by_code = data.get("by_code") or {}
            self.last_refresh = data.get("ts")
            return bool(self.by_code)
        except Exception:
            return False

    def save_disk(self) -> None:
        try:
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            CACHE_FILE.write_text(
                json.dumps(
                    {"ts": self.last_refresh, "by_code": self.by_code},
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass

    def ensure(self, max_age_sec: float = 86400 * 7) -> dict[str, Any]:
        with self._lock:
            if (
                self.by_code
                and self.last_refresh
                and time.time() - self.last_refresh < max_age_sec
            ):
                return {
                    "ok": True,
                    "cached": True,
                    "count": len(self.by_code),
                    "with_website": sum(
                        1 for v in self.by_code.values() if v.get("website")
                    ),
                }
            return self.refresh()

    def refresh(self) -> dict[str, Any]:
        by_code: dict[str, dict[str, Any]] = {}
        stats = {"L": 0, "O": 0, "errors": []}

        # Listed
        try:
            resp = requests.get(PROFILE_URLS["L"], headers=HEADERS, timeout=90)
            resp.raise_for_status()
            for row in resp.json():
                code = str(row.get("公司代號") or "").strip()
                if not re.fullmatch(r"\d{3,6}", code):
                    continue
                website = normalize_website(row.get("網址"))
                by_code[code] = {
                    "company_id": code,
                    "company_name": str(row.get("公司簡稱") or row.get("公司名稱") or "").strip(),
                    "market": "L",
                    "website": website,
                }
                stats["L"] += 1
        except Exception as exc:  # noqa: BLE001
            stats["errors"].append(f"L: {exc}")

        # OTC
        try:
            resp = requests.get(PROFILE_URLS["O"], headers=HEADERS, timeout=90)
            resp.raise_for_status()
            for row in resp.json():
                code = str(
                    row.get("SecuritiesCompanyCode") or row.get("公司代號") or ""
                ).strip()
                if not re.fullmatch(r"\d{3,6}", code):
                    continue
                website = normalize_website(
                    row.get("WebAddress") or row.get("網址")
                )
                name = str(
                    row.get("CompanyAbbreviation")
                    or row.get("CompanyName")
                    or row.get("公司簡稱")
                    or ""
                ).strip()
                # prefer keep L if duplicate (rare)
                if code in by_code and by_code[code].get("market") == "L":
                    continue
                by_code[code] = {
                    "company_id": code,
                    "company_name": name,
                    "market": "O",
                    "website": website,
                }
                stats["O"] += 1
        except Exception as exc:  # noqa: BLE001
            stats["errors"].append(f"O: {exc}")

        if by_code:
            self.by_code = by_code
            self.last_refresh = time.time()
            self.save_disk()

        return {
            "ok": bool(by_code),
            "cached": False,
            "count": len(by_code),
            "with_website": sum(1 for v in by_code.values() if v.get("website")),
            "listed": stats["L"],
            "otc": stats["O"],
            "errors": stats["errors"],
        }

    def get_website(self, code: str) -> str | None:
        rec = self.by_code.get(str(code).strip())
        if not rec:
            return None
        return rec.get("website")

    def attach(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self.ensure()
        for r in rows:
            code = str(r.get("company_id") or "").strip()
            rec = self.by_code.get(code) or {}
            r["website"] = rec.get("website")
        return rows


company_service = CompanyService()
