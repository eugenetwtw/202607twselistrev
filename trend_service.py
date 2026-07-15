"""每月趨勢權重 + 供應鏈權重（Brave / Firecrawl / GLM）。

趨勢權重：現在夯 (hot_now) + 會不會續 (persist) → trend_weight
供應鏈權重：是否連結「在趨勢、在漲、營收成長」的美股 S&P/Nasdaq 公司

並行：ThreadPoolExecutor，並發上限 TREND_CONCURRENCY（受 API 限流）。
"""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from analysis_service import (
    _clamp01,
    _env,
    _extract_json_object,
    brave_search,
    firecrawl_scrape,
    glm_chat,
    load_dotenv,
    now_tw_iso,
)

load_dotenv()

DEFAULT_ANCHOR_HINT = (
    "NVDA AMD AVGO TSM ASML MU AAPL MSFT GOOGL META AMZN "
    "2330 TSMC 2454 2382 2317 2308 3711 3034 6669"
)

OnStep = Callable[..., None] | None
ShouldCancel = Callable[[], bool] | None


def trend_concurrency() -> int:
    try:
        n = int(_env("TREND_CONCURRENCY", "50") or "50")
    except ValueError:
        n = 50
    return max(1, min(n, 1000))


def build_theme_pack(*, on_step: OnStep = None) -> dict[str, Any]:
    """本月美股／台股主題 + 強勢美股錨定（少次查詢，全公司共用）。"""
    if on_step:
        on_step("phase", "【1/3】本月 Theme + 美股錨定 Pack…")

    queries = [
        "S&P 500 Nasdaq AI semiconductor stocks rising revenue growth",
        "NVIDIA AMD Micron TSMC supply chain stocks performance",
        "台股 AI 半導體 供應鏈 本月 熱門",
        "US tech earnings growth AI data center power cooling",
        "HBM 矽晶圓 先進封裝 台股 供應鏈",
    ]
    blocks: list[dict[str, Any]] = []
    urls: list[str] = []
    for q in queries:
        if on_step:
            on_step("api", f"→ Brave：{q[:70]}")
        hits = brave_search(q, count=6, on_step=None)
        blocks.append({"query": q, "results": hits})
        if on_step:
            on_step("ok", f"← Brave {len(hits)} 筆")
        for h in hits:
            u = h.get("url") or ""
            if u and u not in urls and not u.endswith(".pdf"):
                urls.append(u)

    scrapes = []
    for u in urls[:3]:
        if on_step:
            on_step("api", f"→ Firecrawl：{u[:60]}")
        scrapes.append(firecrawl_scrape(u, on_step=None, max_chars=2200))

    web_bits = []
    for b in blocks:
        web_bits.append(f"## {b['query']}")
        for h in (b.get("results") or [])[:4]:
            web_bits.append(f"- {h.get('title')}: {h.get('description')}")
    for s in scrapes:
        if s.get("ok"):
            web_bits.append(f"## {s.get('url')}\n{(s.get('markdown') or '')[:1800]}")
    web_text = "\n".join(web_bits)[:12000]

    system = """你是全球股市主題研究員。輸出本月「Theme + 美股錨定」JSON（只要 JSON）：
{
  "themes": [{"name":"","keywords_zh":[],"keywords_en":[],"structural":true,"note":""}],
  "hot_us_names": [{"ticker":"NVDA","reason":"在漲/營收成長/主題","structural":true}],
  "fading_or_one_off": [{"name":"","note":"例：大型賽事訂閱結束"}],
  "taiwan_supply_chain_hints": [""],
  "summary_zh": "繁中摘要 ≤200字"
}
hot_us_names 只列 S&P/Nasdaq 相關、當前偏強且基本面/主題站得住的錨定。"""

    pack: dict[str, Any] = {
        "themes": [],
        "hot_us_names": [],
        "fading_or_one_off": [],
        "taiwan_supply_chain_hints": [],
        "summary_zh": "",
        "built_at": now_tw_iso(),
        "anchor_hint": DEFAULT_ANCHOR_HINT,
    }
    try:
        if on_step:
            on_step("phase", "GLM 寫 Theme Pack…")
        out = glm_chat(
            [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": f"參考：{DEFAULT_ANCHOR_HINT}\n\n資料：\n{web_text}",
                },
            ],
            temperature=0.3,
            on_step=on_step,
        )
        parsed = _extract_json_object(str(out.get("content") or ""))
        if parsed:
            for k in (
                "themes",
                "hot_us_names",
                "fading_or_one_off",
                "taiwan_supply_chain_hints",
                "summary_zh",
            ):
                if k in parsed:
                    pack[k] = parsed[k]
            pack["model"] = out.get("model")
            if on_step:
                on_step("ok", f"Theme Pack：{(pack.get('summary_zh') or '')[:100]}")
        else:
            pack["summary_zh"] = web_text[:400]
            if on_step:
                on_step("warn", "Theme Pack JSON 解析失敗，用搜尋摘要")
    except Exception as e:
        pack["summary_zh"] = f"Theme 失敗：{e}"
        pack["error"] = str(e)
        if on_step:
            on_step("error", str(e))

    pack["source_titles"] = [
        h.get("title") or ""
        for b in blocks
        for h in (b.get("results") or [])[:2]
    ]
    return pack


def _heuristic_weights(company: dict[str, Any], theme: dict[str, Any]) -> dict[str, Any]:
    ind = str(company.get("industry") or "")
    name = str(company.get("company_name") or "")
    text = ind + name
    blob = json.dumps(theme, ensure_ascii=False)
    hot, persist, sc = 0.25, 0.25, 0.2

    if re.search(r"半導體|電子|電腦|光電|通信|電機|資訊|晶圓|封裝|PCB|設備", ind):
        hot, persist, sc = 0.65, 0.7, 0.55
    if re.search(r"化學|化工|材料", ind) and re.search(
        r"矽|晶圓|HBM|半導體|AI|台積|美光|NVDA", blob + text
    ):
        hot, persist, sc = 0.55, 0.75, 0.6
    if re.search(r"建設|營建|營造|不動產", ind):
        hot, persist, sc = 0.12, 0.1, 0.05
    if re.search(r"影視|媒體|廣播|娛樂", ind):
        hot, persist, sc = 0.35, 0.12, 0.08
    if re.search(r"金融|銀行|證券|保險", ind):
        hot, persist, sc = 0.2, 0.25, 0.1

    for th in theme.get("themes") or []:
        for kw in (th.get("keywords_zh") or []) + (th.get("keywords_en") or []):
            if kw and str(kw) in text:
                hot = max(hot, 0.6)
                if th.get("structural"):
                    persist = max(persist, 0.7)
                    sc = max(sc, 0.45)

    tw = round(0.4 * hot + 0.6 * persist, 3)
    return {
        "hot_now": round(hot, 3),
        "persist": round(persist, 3),
        "trend_weight": tw,
        "supply_chain_weight": round(sc, 3),
        "rationale": f"【後備】產業「{ind}」+ Theme 關鍵字。",
        "us_related_tickers": [],
        "status": "heuristic",
        "model": "heuristic",
    }


def score_company(
    company: dict[str, Any],
    theme: dict[str, Any],
    *,
    light_brave: bool = True,
) -> dict[str, Any]:
    """單家：一次 GLM 同時打趨勢權重 + 供應鏈權重。"""
    code = str(company.get("company_id") or "")
    name = str(company.get("company_name") or "")
    industry = str(company.get("industry") or "")
    ym = str(company.get("year_month") or "")
    analyzed_at = now_tw_iso()
    sources: dict[str, Any] = {
        "theme_summary": (theme.get("summary_zh") or "")[:400],
        "brave": [],
    }

    extra = ""
    if light_brave and _env("BRAVE_API_KEY"):
        q = f"{code} {name} {industry} 供應鏈 OR 客戶 NVIDIA OR TSMC OR 美光 OR AI"
        hits = brave_search(q, count=4, on_step=None)
        sources["brave"] = [
            {
                "title": h.get("title"),
                "url": h.get("url"),
                "description": (h.get("description") or "")[:180],
            }
            for h in hits[:4]
        ]
        extra = "\n".join(f"- {h.get('title')}: {h.get('description')}" for h in hits[:4])

    system = """你是台股主題與美股供應鏈研究員。
為「一家台股公司」打兩個 0~1 權重（不是 PEAD 營收異常分）。

1) 趨勢權重
- hot_now：現在是否夯（主題/熱度）
- persist：會不會持續？
  低 persist：世足/賽事 OTT（愛爾達類）、交屋認列、一次性促銷
  高 persist：先進製程材料、HBM、矽晶圓、長期 AI 基建（即使產業是化學）
- trend_weight ≈ 0.4*hot_now + 0.6*persist

2) 供應鏈權重 supply_chain_weight
- 是否連結「目前在趨勢上、股價偏強、營收/需求在成長」的美股 S&P/Nasdaq 公司
- 僅有鬆散概念、美方已轉弱 → 低分
- 直接/關鍵二階供貨給強勢美股錨定 → 高分

只輸出 JSON：
{
  "hot_now": 0,
  "persist": 0,
  "trend_weight": 0,
  "supply_chain_weight": 0,
  "us_related_tickers": ["NVDA"],
  "rationale": "繁中 80-160 字",
  "flags": []
}"""

    theme_compact = {
        "summary_zh": theme.get("summary_zh"),
        "themes": theme.get("themes"),
        "hot_us_names": theme.get("hot_us_names"),
        "fading_or_one_off": theme.get("fading_or_one_off"),
        "taiwan_supply_chain_hints": theme.get("taiwan_supply_chain_hints"),
    }
    user = json.dumps(
        {
            "company_id": code,
            "company_name": name,
            "industry": industry,
            "year_month": ym,
            "revenue_current": company.get("revenue_current"),
            "yoy_pct": company.get("yoy_pct"),
            "theme_and_us_anchors": theme_compact,
            "company_snippets": extra,
        },
        ensure_ascii=False,
    )

    base = {
        "company_id": code,
        "company_name": name,
        "market": company.get("market"),
        "industry": industry,
        "year_month": ym,
        "analyzed_at": analyzed_at,
        "sources_json": sources,
    }

    try:
        out = glm_chat(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.25,
            on_step=None,
        )
        parsed = _extract_json_object(str(out.get("content") or ""))
        if not parsed:
            h = _heuristic_weights(company, theme)
            return {**base, **h, "error_message": "glm_json_parse"}

        hot = _clamp01(parsed.get("hot_now"), 0.4)
        persist = _clamp01(parsed.get("persist"), 0.4)
        tw = _clamp01(parsed.get("trend_weight"), 0.4 * hot + 0.6 * persist)
        # 略強制 persist 偏重
        tw_calc = 0.4 * hot + 0.6 * persist
        if abs(tw - tw_calc) > 0.25:
            tw = tw_calc
        sc = _clamp01(parsed.get("supply_chain_weight"), 0.3)

        return {
            **base,
            "hot_now": round(hot, 3),
            "persist": round(persist, 3),
            "trend_weight": round(float(tw), 3),
            "supply_chain_weight": round(float(sc), 3),
            "rationale": str(parsed.get("rationale") or "")[:800],
            "us_related_tickers": parsed.get("us_related_tickers") or [],
            "flags": parsed.get("flags") or [],
            "model": out.get("model") or _env("GLM_MODEL", "glm-4.6"),
            "status": "ok",
            "error_message": None,
        }
    except Exception as e:
        h = _heuristic_weights(company, theme)
        return {**base, **h, "status": "error", "error_message": str(e)}


def score_companies_parallel(
    companies: list[dict[str, Any]],
    theme: dict[str, Any],
    *,
    concurrency: int | None = None,
    should_cancel: ShouldCancel = None,
    on_company_done: Callable[[dict[str, Any], int, int], None] | None = None,
) -> list[dict[str, Any]]:
    workers = concurrency or trend_concurrency()
    total = len(companies)
    results: list[dict[str, Any]] = []
    if not total:
        return results

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(score_company, c, theme): c for c in companies}
        for fut in as_completed(futs):
            if should_cancel and should_cancel():
                break
            try:
                r = fut.result()
            except Exception as e:
                c = futs[fut]
                r = {
                    "company_id": c.get("company_id"),
                    "company_name": c.get("company_name"),
                    "market": c.get("market"),
                    "industry": c.get("industry"),
                    "year_month": c.get("year_month"),
                    "analyzed_at": now_tw_iso(),
                    "hot_now": 0.2,
                    "persist": 0.2,
                    "trend_weight": 0.2,
                    "supply_chain_weight": 0.15,
                    "rationale": f"例外：{e}",
                    "us_related_tickers": [],
                    "status": "error",
                    "error_message": str(e),
                    "sources_json": {},
                    "model": "",
                }
            results.append(r)
            done += 1
            if on_company_done:
                on_company_done(r, done, total)
    return results


def rank_score_row(
    *,
    anomaly_score: float | None,
    trend_weight: float | None,
    supply_chain_weight: float | None,
    is_e: bool = False,
    is_s: bool = False,
) -> float:
    """合成分 → Top30。0.30 anomaly + 0.35 trend + 0.35 supply。"""
    try:
        a = float(anomaly_score) if anomaly_score is not None else 40.0
    except (TypeError, ValueError):
        a = 40.0
    a_n = _clamp01(a / 100.0, 0.4)
    t = _clamp01(trend_weight, 0.0)
    s = _clamp01(supply_chain_weight, 0.0)
    bonus = 0.03 if is_e else 0.0
    bonus += 0.02 if is_s else 0.0
    return round(0.30 * a_n + 0.35 * t + 0.35 * s + bonus, 4)
