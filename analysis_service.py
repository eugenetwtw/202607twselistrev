"""訪問分析：Brave 搜尋 + Firecrawl 擷取 + GLM 評估 PEAD 品質／美股供應鏈權重。

權重語意（0~1）：
- pead_quality_weight：月營收訊號有多像「可延續的 PEAD 驚喜」，而非交屋／一次性事件
- supply_chain_weight：是否在近期走強之 S&P/Nasdaq 電子供應鏈內
- composite_weight：綜合可操作權重（模型給出，並有本機後備合成）
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import requests

from job_progress import WaitTicker

APP_DIR = Path(__file__).resolve().parent
TZ_TW = timezone(timedelta(hours=8))

# 產業／情境先驗：給 GLM 當硬提示，也當後備降權
INDUSTRY_RISK_HINTS: list[tuple[re.Pattern[str], str, float]] = [
    (re.compile(r"建設|營建|不動產|建築|營造|土建"), "construction_delivery", 0.35),
    (re.compile(r"影視|媒體|廣播|電視|娛樂|內容|數位|廣告"), "media_one_off_event", 0.40),
    (re.compile(r"觀光|旅遊|飯店|餐飲|航運.*客"), "seasonal_demand", 0.45),
    (re.compile(r"金融保險|銀行|證券|金控"), "financial_non_pead", 0.50),
]


def load_dotenv(path: Path | None = None) -> None:
    """Minimal .env loader (no python-dotenv dependency)."""
    env_path = path or (APP_DIR / ".env")
    if not env_path.is_file():
        return
    try:
        text = env_path.read_text(encoding="utf-8")
    except OSError:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


load_dotenv()


def now_tw_iso() -> str:
    return datetime.now(TZ_TW).isoformat(timespec="seconds")


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def config_status() -> dict[str, Any]:
    return {
        "glm_configured": bool(_env("GLM_API_KEY")),
        "brave_configured": bool(_env("BRAVE_API_KEY")),
        "firecrawl_configured": bool(_env("FIRECRAWL_API_KEY")),
        "glm_model": _env("GLM_MODEL", "glm-4.6"),
        "glm_base_url": _env(
            "GLM_BASE_URL", "https://api.z.ai/api/paas/v4/chat/completions"
        ),
    }


def industry_prior(industry: str | None) -> dict[str, Any]:
    ind = industry or ""
    flags: list[str] = []
    ceiling = 1.0
    for pat, flag, cap in INDUSTRY_RISK_HINTS:
        if pat.search(ind):
            flags.append(flag)
            ceiling = min(ceiling, cap)
    return {"risk_flags": flags, "pead_quality_ceiling": ceiling, "industry": ind}


def brave_search(
    query: str,
    *,
    count: int = 6,
    on_step: Any = None,
) -> list[dict[str, str]]:
    key = _env("BRAVE_API_KEY")
    if not key:
        if on_step:
            on_step("warn", "Brave：未設定 BRAVE_API_KEY，跳過搜尋")
        return []
    url = "https://api.search.brave.com/res/v1/web/search"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": key,
    }
    if on_step:
        on_step("api", f"→ Brave Search：{query[:80]}{'…' if len(query) > 80 else ''}")
    try:
        with WaitTicker(f"Brave「{query[:40]}」"):
            r = requests.get(
                url,
                headers=headers,
                params={"q": query, "count": count, "search_lang": "zh-hant", "country": "TW"},
                timeout=25,
            )
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        if on_step:
            on_step("error", f"← Brave 失敗：{e}")
        return [{"title": "Brave error", "url": "", "description": str(e)}]
    out: list[dict[str, str]] = []
    for item in (data.get("web") or {}).get("results") or []:
        out.append(
            {
                "title": str(item.get("title") or "")[:200],
                "url": str(item.get("url") or ""),
                "description": str(item.get("description") or "")[:500],
            }
        )
    if on_step:
        on_step("ok", f"← Brave 回傳 {len(out)} 筆結果")
        for i, h in enumerate(out[:3], 1):
            on_step("data", f"  [{i}] {h.get('title') or '(無標題)'}")
            desc = (h.get("description") or "").strip()
            if desc:
                on_step("data", f"      {desc[:140]}{'…' if len(desc) > 140 else ''}")
    return out


def firecrawl_scrape(
    url: str,
    *,
    max_chars: int = 3500,
    on_step: Any = None,
) -> dict[str, Any]:
    key = _env("FIRECRAWL_API_KEY")
    if not key or not url:
        return {"url": url, "ok": False, "markdown": ""}
    if on_step:
        short = url if len(url) <= 70 else url[:67] + "…"
        on_step("api", f"→ Firecrawl scrape：{short}")
    try:
        with WaitTicker(f"Firecrawl {url[:50]}"):
            r = requests.post(
                "https://api.firecrawl.dev/v1/scrape",
                headers={
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                },
                json={
                    "url": url,
                    "formats": ["markdown"],
                    "onlyMainContent": True,
                },
                timeout=45,
            )
            r.raise_for_status()
            payload = r.json()
        data = payload.get("data") or payload
        md = str(data.get("markdown") or data.get("content") or "")
        if len(md) > max_chars:
            md = md[:max_chars] + "\n…(truncated)"
        if on_step:
            on_step("ok", f"← Firecrawl 成功（{len(md)} 字）")
            preview = " ".join(md.split())[:180]
            if preview:
                on_step("data", f"  摘錄：{preview}{'…' if len(md) > 180 else ''}")
        return {"url": url, "ok": True, "markdown": md, "title": data.get("metadata", {}).get("title")}
    except Exception as e:
        if on_step:
            on_step("error", f"← Firecrawl 失敗：{e}")
        return {"url": url, "ok": False, "markdown": "", "error": str(e)}


def gather_web_context(
    company: dict[str, Any],
    *,
    on_step: Any = None,
) -> dict[str, Any]:
    code = str(company.get("company_id") or "")
    name = str(company.get("company_name") or "")
    industry = str(company.get("industry") or "")
    ym = str(company.get("year_month") or "")

    queries = [
        f"{code} {name} 月營收 年增 原因 {ym or '2025'} {industry}",
        f"{code} {name} 營收 交屋 OR 專案認列 OR 一次性 OR 賽事 OR 訂閱",
        f"{code} {name} 供應鏈 美股 OR NVIDIA OR Apple OR AMD OR TSMC OR AI 客戶",
        f"Taiwan {code} {name} supply chain S&P Nasdaq AI semiconductor",
        f"S&P 500 Nasdaq AI semiconductor stocks rising suppliers Taiwan {industry}",
    ]
    search_blocks: list[dict[str, Any]] = []
    urls: list[str] = []
    if on_step:
        on_step("phase", f"網頁調查 {code} {name}（Brave ×{len(queries)}）")
    for q in queries:
        hits = brave_search(q, count=5, on_step=on_step)
        search_blocks.append({"query": q, "results": hits})
        for h in hits:
            u = h.get("url") or ""
            if u and u not in urls and not u.endswith(".pdf"):
                urls.append(u)

    scrapes: list[dict[str, Any]] = []
    for u in urls[:3]:
        scrapes.append(firecrawl_scrape(u, on_step=on_step))

    if on_step:
        ok_n = sum(1 for s in scrapes if s.get("ok"))
        on_step("info", f"調查結束：搜尋 {len(search_blocks)} 組 · 擷取成功 {ok_n}/{len(scrapes)}")

    return {
        "searches": search_blocks,
        "scrapes": scrapes,
        "prior": industry_prior(industry),
    }


def _clip_web_for_prompt(web: dict[str, Any], *, max_chars: int = 12000) -> str:
    parts: list[str] = []
    prior = web.get("prior") or {}
    parts.append(f"[產業先驗] {json.dumps(prior, ensure_ascii=False)}")
    for block in web.get("searches") or []:
        parts.append(f"\n## 搜尋: {block.get('query')}")
        for i, h in enumerate(block.get("results") or [], 1):
            parts.append(
                f"{i}. {h.get('title')}\n   {h.get('url')}\n   {h.get('description')}"
            )
    for sc in web.get("scrapes") or []:
        if not sc.get("ok"):
            parts.append(f"\n## 擷取失敗 {sc.get('url')}: {sc.get('error')}")
            continue
        parts.append(f"\n## 網頁擷取 {sc.get('url')}\n{sc.get('markdown') or ''}")
    text = "\n".join(parts)
    if len(text) > max_chars:
        return text[:max_chars] + "\n…(truncated)"
    return text


def _extract_json_object(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    s = text.strip()
    # strip ```json fences
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", s)
    if fence:
        s = fence.group(1).strip()
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass
    # find first { ... last }
    i = s.find("{")
    j = s.rfind("}")
    if i >= 0 and j > i:
        try:
            obj = json.loads(s[i : j + 1])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            return None
    return None


def glm_chat(
    messages: list[dict[str, str]],
    *,
    temperature: float = 0.3,
    on_step: Any = None,
) -> dict[str, Any]:
    key = _env("GLM_API_KEY")
    if not key:
        raise RuntimeError("缺少 GLM_API_KEY（請寫入 .env）")
    base = _env("GLM_BASE_URL", "https://api.z.ai/api/paas/v4/chat/completions")
    preferred = _env("GLM_MODEL", "glm-4.6")
    # 最好可用模型：依序嘗試
    models = []
    for m in (preferred, "glm-4.6", "glm-4.5", "glm-4-plus", "glm-4"):
        if m and m not in models:
            models.append(m)

    last_err: Exception | None = None
    for model in models:
        body: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 4096,
        }
        # thinking mode for stronger reasoning (supported on glm-4.5+)
        if model.startswith("glm-4.5") or model.startswith("glm-4.6") or model.startswith("glm-5"):
            body["thinking"] = {"type": "enabled"}
        if on_step:
            on_step("api", f"→ GLM chat/completions model={model}")
        try:
            with WaitTicker(f"GLM {model}"):
                r = requests.post(
                    base,
                    headers={
                        "Authorization": f"Bearer {key}",
                        "Content-Type": "application/json",
                    },
                    json=body,
                    timeout=120,
                )
            if r.status_code >= 400:
                # try China endpoint once if international fails auth/path
                last_err = RuntimeError(f"{model} HTTP {r.status_code}: {r.text[:400]}")
                if on_step:
                    on_step("warn", f"← GLM {model} HTTP {r.status_code}，嘗試下一個…")
                    on_step("data", f"  錯誤摘要：{(r.text or '')[:200]}")
                if r.status_code in (401, 403, 404) and "api.z.ai" in base:
                    alt = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
                    if on_step:
                        on_step("api", f"→ GLM 改打中國端點 model={model}")
                    with WaitTicker(f"GLM-CN {model}"):
                        r2 = requests.post(
                            alt,
                            headers={
                                "Authorization": f"Bearer {key}",
                                "Content-Type": "application/json",
                            },
                            json=body,
                            timeout=120,
                        )
                    if r2.status_code < 400:
                        data = r2.json()
                        content = (
                            ((data.get("choices") or [{}])[0].get("message") or {}).get(
                                "content"
                            )
                            or ""
                        )
                        if on_step:
                            on_step("ok", f"← GLM {model} 回應 {len(content)} 字（CN）")
                            preview = " ".join(str(content).split())[:280]
                            if preview:
                                on_step("data", f"  GLM 原文：{preview}{'…' if len(content) > 280 else ''}")
                        return {"model": model, "content": content, "raw": data, "endpoint": alt}
                    last_err = RuntimeError(
                        f"{model} CN HTTP {r2.status_code}: {r2.text[:400]}"
                    )
                    continue
                continue
            data = r.json()
            msg = ((data.get("choices") or [{}])[0].get("message") or {})
            content = msg.get("content") or ""
            # some GLM responses put final answer after reasoning
            if not content and msg.get("reasoning_content"):
                content = str(msg.get("reasoning_content"))
            if on_step:
                on_step("ok", f"← GLM {model} 回應 {len(content)} 字")
                preview = " ".join(str(content).split())[:280]
                if preview:
                    on_step("data", f"  GLM 原文：{preview}{'…' if len(content) > 280 else ''}")
            return {"model": model, "content": content, "raw": data, "endpoint": base}
        except Exception as e:
            last_err = e
            if on_step:
                on_step("error", f"← GLM {model} 例外：{e}")
            continue
    raise RuntimeError(f"GLM 呼叫失敗: {last_err}")


def _clamp01(v: Any, default: float = 0.5) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, x))


def _fallback_composite(pead_w: float, sc_w: float) -> float:
    # 品質權重主導；供應鏈為加分項
    return round(0.65 * pead_w + 0.35 * sc_w, 3)


def heuristic_weights(company: dict[str, Any]) -> dict[str, Any]:
    """快速為全部名單估權重（無網頁）。用於排序，篩出前 N 再做完整訪問分析。"""
    prior = industry_prior(company.get("industry"))
    ceiling = float(prior.get("pead_quality_ceiling") or 1.0)
    ind = str(company.get("industry") or "")
    yoy = company.get("yoy_pct")
    try:
        yoy_f = float(yoy) if yoy is not None else None
    except (TypeError, ValueError):
        yoy_f = None

    # 基礎 PEAD 品質：產業百分位 + 驚喜 + 轉折加速
    base = 0.42
    pctile = company.get("industry_yoy_pctile")
    if pctile is not None:
        try:
            base += 0.35 * (float(pctile) / 100.0)
        except (TypeError, ValueError):
            pass
    surp = company.get("surprise_avg")
    if surp is not None:
        try:
            # 15pp ≈ 中性偏上；再夾在合理範圍
            base += 0.15 * _clamp01((float(surp) + 5.0) / 40.0, 0.5)
        except (TypeError, ValueError):
            pass
    if company.get("is_turnaround"):
        base += 0.08
    if company.get("is_accelerating"):
        base += 0.06
    if company.get("is_extreme_growth"):
        base += 0.05

    # 極端 YoY 但高風險產業：更像交屋／一次性
    if yoy_f is not None and yoy_f > 200 and ceiling < 0.5:
        base -= 0.25
    elif yoy_f is not None and yoy_f > 500:
        base -= 0.08  # 極端暴衝略降（可能一次性）

    pead_w = min(_clamp01(base, 0.4), ceiling)

    # 供應鏈：電子相關產業先驗較高
    if re.search(r"半導體|IC|晶圓|封測|光罩|設備", ind):
        sc_w = 0.72
    elif re.search(r"電子|光電|通信網路|電腦及週邊|資訊服務|電機|電器電纜", ind):
        sc_w = 0.55
    elif re.search(r"化學|塑膠|鋼鐵|橡膠", ind):
        sc_w = 0.28
    else:
        sc_w = 0.18
    if prior.get("risk_flags"):
        sc_w = min(sc_w, 0.25)

    composite = _fallback_composite(pead_w, sc_w)
    rank = rank_score(
        anomaly_score=company.get("anomaly_score"),
        composite_weight=composite,
        pead_quality_weight=pead_w,
    )
    return {
        "pead_quality_weight": round(pead_w, 3),
        "supply_chain_weight": round(sc_w, 3),
        "composite_weight": composite,
        "rank_score": rank,
        "risk_flags": list(prior.get("risk_flags") or []),
        "weight_source": "heuristic",
        "confidence": 0.45,
    }


def rank_score(
    *,
    anomaly_score: Any = None,
    composite_weight: Any = None,
    pead_quality_weight: Any = None,
) -> float:
    """排序分：綜合權重為主，量化 anomaly 為輔。0~1 左右。"""
    try:
        a = float(anomaly_score) if anomaly_score is not None else 50.0
    except (TypeError, ValueError):
        a = 50.0
    a_n = _clamp01(a / 100.0, 0.5)
    try:
        c = float(composite_weight) if composite_weight is not None else 0.4
    except (TypeError, ValueError):
        c = 0.4
    try:
        p = float(pead_quality_weight) if pead_quality_weight is not None else c
    except (TypeError, ValueError):
        p = c
    # 建設等低 pead 品質應明顯掉出前段
    return round(0.55 * c + 0.25 * p + 0.20 * a_n, 4)


def batch_score_weights(
    companies: list[dict[str, Any]],
    *,
    use_glm: bool = True,
    chunk_size: int = 35,
    on_step: Any = None,
) -> list[dict[str, Any]]:
    """為全部公司估權重並算 rank_score。先啟發式，可選 GLM 批次微調。"""
    if on_step:
        on_step("phase", f"啟發式權重：{len(companies)} 家")
    scored: list[dict[str, Any]] = []
    by_code: dict[str, dict[str, Any]] = {}
    for c in companies:
        code = str(c.get("company_id") or "")
        h = heuristic_weights(c)
        row = {
            "company_id": code,
            "company_name": c.get("company_name"),
            "market": c.get("market"),
            "industry": c.get("industry"),
            "year_month": c.get("year_month"),
            "anomaly_score": c.get("anomaly_score"),
            "yoy_pct": c.get("yoy_pct"),
            "is_extreme_growth": c.get("is_extreme_growth"),
            "is_surprise_long": c.get("is_surprise_long"),
            **h,
        }
        scored.append(row)
        by_code[code] = row
    if on_step:
        on_step("ok", f"啟發式完成 {len(scored)} 家")

    if use_glm and _env("GLM_API_KEY") and scored:
        try:
            if on_step:
                on_step("phase", "GLM 批次微調權重…")
            _glm_refine_weights(
                scored, by_code, chunk_size=chunk_size, on_step=on_step
            )
        except Exception as e:
            if on_step:
                on_step("warn", f"GLM 微調略過：{e}")

    scored.sort(
        key=lambda r: (
            float(r.get("rank_score") or 0),
            float(r.get("composite_weight") or 0),
            float(r.get("anomaly_score") or 0),
        ),
        reverse=True,
    )
    for i, r in enumerate(scored, 1):
        r["rank"] = i
    if on_step and scored:
        head = ", ".join(
            f"{r['company_id']}({r['rank_score']:.2f})" for r in scored[:5]
        )
        on_step("ok", f"排序完成 · 前5：{head}")
    return scored


def _glm_refine_weights(
    scored: list[dict[str, Any]],
    by_code: dict[str, dict[str, Any]],
    *,
    chunk_size: int = 35,
    on_step: Any = None,
) -> None:
    """用 GLM 一次批改一組公司的三權重（無網頁，便宜）。"""
    system = """你是台股 PEAD 研究員。依產業與量化訊號，為每家公司打 0~1 權重。
注意：建設/營建交屋、影視/賽事訂閱等一次性事件 pead_quality_weight 必須很低；
半導體/電子供應鏈 supply_chain_weight 可較高。
只輸出 JSON 陣列，每項：
{"company_id":"2330","pead_quality_weight":0.0,"supply_chain_weight":0.0,"composite_weight":0.0,"risk_flags":[]}
不要 markdown。"""

    n_chunks = (len(scored) + chunk_size - 1) // chunk_size
    for i in range(0, len(scored), chunk_size):
        chunk = scored[i : i + chunk_size]
        ci = i // chunk_size + 1
        if on_step:
            on_step(
                "api",
                f"→ GLM 權重批次 {ci}/{n_chunks}（{len(chunk)} 家）",
            )
        compact = [
            {
                "company_id": r["company_id"],
                "company_name": r.get("company_name"),
                "industry": r.get("industry"),
                "yoy_pct": r.get("yoy_pct"),
                "anomaly_score": r.get("anomaly_score"),
                "heuristic_pead": r.get("pead_quality_weight"),
                "heuristic_sc": r.get("supply_chain_weight"),
                "E": bool(r.get("is_extreme_growth")),
                "S": bool(r.get("is_surprise_long")),
            }
            for r in chunk
        ]
        messages = [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": "請為下列公司打權重：\n"
                + json.dumps(compact, ensure_ascii=False),
            },
        ]
        out = glm_chat(messages, temperature=0.2, on_step=on_step)
        content = str(out.get("content") or "")
        # parse array
        arr = None
        fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", content)
        text = fence.group(1).strip() if fence else content.strip()
        try:
            arr = json.loads(text)
        except json.JSONDecodeError:
            a0, a1 = text.find("["), text.rfind("]")
            if a0 >= 0 and a1 > a0:
                try:
                    arr = json.loads(text[a0 : a1 + 1])
                except json.JSONDecodeError:
                    arr = None
        if not isinstance(arr, list):
            continue
        for item in arr:
            if not isinstance(item, dict):
                continue
            code = str(item.get("company_id") or "")
            row = by_code.get(code)
            if not row:
                continue
            prior = industry_prior(row.get("industry"))
            pead_w = min(
                _clamp01(item.get("pead_quality_weight"), row["pead_quality_weight"]),
                float(prior.get("pead_quality_ceiling") or 1.0),
            )
            sc_w = _clamp01(item.get("supply_chain_weight"), row["supply_chain_weight"])
            comp = item.get("composite_weight")
            composite = (
                _clamp01(comp, _fallback_composite(pead_w, sc_w))
                if comp is not None
                else _fallback_composite(pead_w, sc_w)
            )
            row["pead_quality_weight"] = round(pead_w, 3)
            row["supply_chain_weight"] = round(sc_w, 3)
            row["composite_weight"] = round(composite, 3)
            row["rank_score"] = rank_score(
                anomaly_score=row.get("anomaly_score"),
                composite_weight=composite,
                pead_quality_weight=pead_w,
            )
            flags = list(item.get("risk_flags") or [])
            for f in prior.get("risk_flags") or []:
                if f not in flags:
                    flags.append(f)
            row["risk_flags"] = flags
            row["weight_source"] = "glm+heuristic"
            row["confidence"] = 0.7
            row["model"] = out.get("model")


def build_glm_prompt(company: dict[str, Any], web_text: str) -> list[dict[str, str]]:
    system = """你是台股 PEAD（Post-Earnings Announcement Drift）研究員。
任務：依「月營收訊號 + 網頁調查」評估該公司本次成長是否像可延續的 PEAD 驚喜，以及是否在近期走強美股（S&P500/Nasdaq 電子／AI）供應鏈內。

嚴格注意偽訊號：
- 建設／營建：交屋、完工認列常造成單月營收暴衝，通常不是可持續 PEAD。
- 影視／媒體：大型賽事（如世足）、熱播、一次性訂閱潮（例：愛爾達）常為短期事件，通常不是 PEAD。
- 電子供應鏈：若為 NVDA/AMD/AVGO/AAPL/MSFT/TSM 等近期強勢鏈的直接或關鍵二階供應，可給較高 supply_chain_weight。

只輸出一個 JSON 物件（不要 markdown、不要前言），欄位：
{
  "pead_quality_weight": 0.0到1.0,
  "supply_chain_weight": 0.0到1.0,
  "composite_weight": 0.0到1.0,
  "risk_flags": ["字串標籤"],
  "us_related_tickers": ["美股代號若有"],
  "evaluation": "繁體中文評估，約200-400字，說明為何像或不像PEAD、供應鏈位置、主要風險",
  "confidence": 0.0到1.0
}
權重語意：1=高度可信／高度相關；0=幾乎不可信／無關。"""

    signal_bits = {
        "company_id": company.get("company_id"),
        "company_name": company.get("company_name"),
        "market": company.get("market"),
        "industry": company.get("industry"),
        "year_month": company.get("year_month"),
        "revenue_current_千元": company.get("revenue_current"),
        "yoy_pct": company.get("yoy_pct"),
        "mom_pct": company.get("mom_pct"),
        "industry_yoy_pctile": company.get("industry_yoy_pctile"),
        "surprise_s1": company.get("surprise_s1"),
        "surprise_s2": company.get("surprise_s2"),
        "surprise_avg": company.get("surprise_avg"),
        "is_extreme_growth": company.get("is_extreme_growth"),
        "is_surprise_long": company.get("is_surprise_long"),
        "is_turnaround": company.get("is_turnaround"),
        "is_accelerating": company.get("is_accelerating"),
        "anomaly_score": company.get("anomaly_score"),
        "note": company.get("note"),
    }
    user = (
        "【公司與 Long PEAD 量化訊號】\n"
        + json.dumps(signal_bits, ensure_ascii=False, indent=2)
        + "\n\n【Brave + Firecrawl 網頁調查摘要】\n"
        + web_text
        + "\n\n請依證據給權重與評估 JSON。"
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def analyze_company(
    company: dict[str, Any],
    *,
    on_step: Any = None,
) -> dict[str, Any]:
    """Full visit-analysis pipeline. Returns a dict ready for DB insert."""
    analyzed_at = now_tw_iso()
    code = str(company.get("company_id") or "")
    name = str(company.get("company_name") or "")
    if on_step:
        on_step("phase", f"開始完整分析 {code} {name}")
    prior = industry_prior(company.get("industry"))
    web: dict[str, Any] = {"searches": [], "scrapes": [], "prior": prior}
    errors: list[str] = []

    try:
        web = gather_web_context(company, on_step=on_step)
    except Exception as e:
        errors.append(f"web: {e}")
        if on_step:
            on_step("error", f"網頁調查例外：{e}")

    web_text = _clip_web_for_prompt(web)
    glm_out: dict[str, Any] | None = None
    parsed: dict[str, Any] | None = None
    model_used = _env("GLM_MODEL", "glm-4.6")

    try:
        if on_step:
            on_step("phase", f"GLM 綜合評估 {code}（PEAD 品質 + 供應鏈）")
        glm_out = glm_chat(
            build_glm_prompt(company, web_text), on_step=on_step
        )
        model_used = glm_out.get("model") or model_used
        parsed = _extract_json_object(str(glm_out.get("content") or ""))
        if not parsed:
            errors.append("GLM 回傳無法解析 JSON")
            if on_step:
                on_step("warn", "GLM 回傳無法解析 JSON，改用後備權重")
        elif on_step:
            on_step(
                "ok",
                f"權重 PEAD={parsed.get('pead_quality_weight')} "
                f"供應鏈={parsed.get('supply_chain_weight')} "
                f"綜合={parsed.get('composite_weight')}",
            )
    except Exception as e:
        errors.append(f"glm: {e}")
        if on_step:
            on_step("error", f"GLM 例外：{e}")

    if parsed:
        pead_w = _clamp01(parsed.get("pead_quality_weight"), 0.5)
        sc_w = _clamp01(parsed.get("supply_chain_weight"), 0.3)
        # 產業天花板：建設／影視等硬上限
        pead_w = min(pead_w, float(prior.get("pead_quality_ceiling") or 1.0))
        comp = parsed.get("composite_weight")
        composite = _clamp01(comp, _fallback_composite(pead_w, sc_w)) if comp is not None else _fallback_composite(pead_w, sc_w)
        risk_flags = list(parsed.get("risk_flags") or [])
        for f in prior.get("risk_flags") or []:
            if f not in risk_flags:
                risk_flags.append(f)
        evaluation = str(parsed.get("evaluation") or "").strip()
        us_tickers = parsed.get("us_related_tickers") or []
        confidence = _clamp01(parsed.get("confidence"), 0.5)
        status = "ok"
        err_msg = None
    else:
        # 後備：僅產業先驗 + 是否電子業
        pead_w = float(prior.get("pead_quality_ceiling") or 0.55)
        ind = str(company.get("industry") or "")
        sc_w = 0.45 if re.search(r"電子|半導體|光電|通信|電腦|資訊", ind) else 0.2
        composite = _fallback_composite(pead_w, sc_w)
        risk_flags = list(prior.get("risk_flags") or []) + ["glm_unavailable"]
        evaluation = (
            f"【後備評估 {analyzed_at}】未能完成完整 GLM 解析。"
            f"產業「{ind or '未分類'}」先驗 PEAD 品質上限約 {pead_w:.2f}。"
            f"網頁調查筆數：搜尋區塊 {len(web.get('searches') or [])}、"
            f"擷取成功 {sum(1 for s in (web.get('scrapes') or []) if s.get('ok'))}。"
            f"錯誤：{'; '.join(errors) if errors else '無'}。"
            "請確認 .env 的 GLM/Brave/Firecrawl 金鑰後重試「訪問分析」。"
        )
        us_tickers = []
        confidence = 0.2
        status = "degraded" if not errors else "error"
        err_msg = "; ".join(errors) if errors else None

    if evaluation and "分析時間" not in evaluation[:40]:
        evaluation = f"【分析時間 {analyzed_at}】\n{evaluation}"

    return {
        "company_id": str(company.get("company_id") or ""),
        "company_name": company.get("company_name"),
        "market": company.get("market"),
        "industry": company.get("industry"),
        "year_month": str(company.get("year_month") or ""),
        "analyzed_at": analyzed_at,
        "pead_quality_weight": round(pead_w, 3),
        "supply_chain_weight": round(sc_w, 3),
        "composite_weight": round(composite, 3),
        "evaluation": evaluation,
        "risk_flags": risk_flags,
        "us_related_tickers": us_tickers,
        "confidence": confidence,
        "sources_json": {
            "searches": web.get("searches"),
            "scrapes": [
                {
                    "url": s.get("url"),
                    "ok": s.get("ok"),
                    "title": s.get("title"),
                    "error": s.get("error"),
                    "markdown_len": len(s.get("markdown") or ""),
                }
                for s in (web.get("scrapes") or [])
            ],
            "prior": web.get("prior"),
        },
        "model": model_used,
        "raw_json": {
            "glm_content": (glm_out or {}).get("content"),
            "parsed": parsed,
            "errors": errors,
        },
        "status": status,
        "error_message": err_msg,
    }
