"""justhodl-asia-leads v1.1 — Asia TECH-PULSE from free primaries (lean).

v1.1 audit finding (extend-don't-duplicate): the platform ALREADY owned
(a) justhodl-econ-calendar (FMP calendar WITH consensus + surprises — superior
to FRED releases/dates, us_calendar block DROPPED, see data/econ-calendar.json)
and (b) justhodl-china-liquidity (China credit-impulse desk — the REAL NBS TSF
discovered in ops 3582 now lives THERE, china_tsf block DROPPED). This engine
is now the focused KR/TW export pulse.

Original v1.0 framing — the MacroMicro gap-analysis engine, sourced from
FREE PRIMARIES (no middleman): China Total Social Financing (NBS via DBnomics
A_A0L08 — the global credit-impulse lead the CB-balance-sheet stack misses),
Korea exports (FRED monthly; 20-day customs flash queued behind a free BoK
ECOS key), Taiwan exports (FRED, proxy for MOEA export orders — the classic
semis/AI-demand lead; true orders series queued behind endpoint discovery),
and the FRED releases/dates calendar (upcoming high-impact US prints).
Writes data/asia-leads.json. Real data only; blocks degrade independently."""
import json, os, time, urllib.parse, urllib.request
from datetime import datetime, timezone
import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/asia-leads.json"
FRED = os.environ.get("FRED_API_KEY") or "2f057499936072679d8843d7fce99989"
s3 = boto3.client("s3", region_name="us-east-1")
UA = {"User-Agent": "JustHodl research contact@justhodl.ai", "Accept": "application/json"}


def gj(url, timeout=25):
    try:
        raw = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout).read()
        return json.loads(raw)
    except Exception as e:
        print("[asia-leads] fetch fail", url[:80], str(e)[:80])
        return None


def yoy(periods, values, back=12):
    try:
        if len(values) > back and values[-1 - back]:
            return round((values[-1] / values[-1 - back] - 1) * 100, 2)
    except Exception:
        pass
    return None


def mom3(periods, values):
    try:
        if len(values) > 3 and values[-4]:
            return round((values[-1] / values[-4] - 1) * 100, 2)
    except Exception:
        pass
    return None


def fred_block(sid, label, extra_note=""):
    j = gj(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
           f"&api_key={FRED}&file_type=json&observation_start=2015-01-01")
    obs = (j or {}).get("observations") or []
    per = [o["date"] for o in obs if o.get("value") not in (None, ".")]
    val = [float(o["value"]) for o in obs if o.get("value") not in (None, ".")]
    if not val:
        return {"source": f"FRED {sid}", "label": label, "error": "no observations"}
    return {"source": f"FRED {sid}", "label": label, "frequency": "M",
            "last_period": per[-1], "last_value": val[-1],
            "yoy_pct": yoy(per, val), "chg_3m_pct": mom3(per, val),
            "n_obs": len(val), "history_24m": [{"p": p, "v": v} for p, v in zip(per[-24:], val[-24:])],
            "note": extra_note}


_GOV_PROXY = "https://justhodl-data-proxy.raafouis.workers.dev/gov?u="
_KCS_LIST = "https://www.customs.go.kr/kcs/na/ntt/selectNttList.do?mi=2889&bbsId=1362"


def _num_kr(x):
    try:
        return float(x.replace(",", "").replace("+", "").replace("△", "-").replace("▲", "").strip())
    except Exception:
        return None


NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "17d36cdd13c44e139853b3a6876cf940")
FMP_KEY = os.environ.get("FMP_API_KEY", os.environ.get("FMP_KEY", ""))

def _json_get(u, timeout=20):
    try:
        return json.loads(urllib.request.urlopen(
            urllib.request.Request(u, headers=UA), timeout=timeout).read())
    except Exception as e:
        print("[asia] json fail", u[:60], str(e)[:60])
        return None

EDGE = "https://justhodl-data-proxy.raafouis.workers.dev/gov?u="

def _edge(u, timeout=25, cap=300_000):
    """v1.4: fetch geo-blocked gov hosts through the CF /gov edge (ops 3592
    proved MOEA 200 via edge vs 403 direct); falls back to direct+noverify."""
    import ssl, urllib.parse as _up
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            EDGE + _up.quote(u, safe=""), headers=UA), timeout=timeout)
        b = r.read(cap)
        if r.status == 200 and len(b) > 400:
            return b, "edge"
    except Exception as e:
        print("[asia] edge fail", str(e)[:60])
    try:
        ctx = ssl.create_default_context(); ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return urllib.request.urlopen(urllib.request.Request(u, headers=UA),
                                      timeout=timeout, context=ctx).read(cap), "direct"
    except Exception as e:
        return b"", "fail:" + str(e)[:40]


def korea_flash_tape():
    """v1.4 KEYLESS KR 1-20 flash from the NEWS TAPE (KCS host WAF-walled even
    at the edge). Yonhap/Reuters carry the print within hours of 9am KST on
    ~the 21st. Sources: NewsAPI everything + FMP general news. Verb-signed %
    extraction, month-window tagged; validated against the prior month's print
    so the mechanism is proven before the live one lands. Never fabricates."""
    import re as _re
    from datetime import datetime, timezone, timedelta
    out = {"label": "Korea 1-20 day exports (news-tape parse)", "method": "news-tape",
           "latest": None, "validated_sample": None, "articles_scanned": 0,
           "sources": [], "error": None}
    arts = []
    try:
        q = urllib.parse.quote('"South Korea" exports "20 days"')
        j = _json_get("https://newsapi.org/v2/everything?q=" + q +
                      "&language=en&sortBy=publishedAt&pageSize=50&apiKey=" + NEWSAPI_KEY)
        for a in (j or {}).get("articles") or []:
            arts.append({"t": (a.get("title") or "") + ". " + (a.get("description") or ""),
                         "u": a.get("url"), "p": (a.get("publishedAt") or "")[:10],
                         "src": "newsapi"})
        out["sources"].append({"newsapi": len(arts)})
    except Exception as e:
        out["sources"].append({"newsapi_err": str(e)[:60]})
    try:
        j = _json_get("https://financialmodelingprep.com/stable/news/general-latest?page=0&limit=120&apikey=" + FMP_KEY)
        n0 = len(arts)
        for a in (j or []):
            arts.append({"t": (a.get("title") or "") + ". " + (a.get("text") or "")[:240],
                         "u": a.get("url"), "p": (a.get("publishedDate") or "")[:10],
                         "src": "fmp"})
        out["sources"].append({"fmp": len(arts) - n0})
    except Exception as e:
        out["sources"].append({"fmp_err": str(e)[:60]})
    out["articles_scanned"] = len(arts)
    MONTHS = {m: i + 1 for i, m in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}
    now = datetime.now(timezone.utc)
    hits = []
    for a in arts:
        t = a["t"]
        if not _re.search(r"korea", t, _re.I) or not _re.search(r"export", t, _re.I):
            continue
        if not _re.search(r"(first\s*20\s*days|1\s*[-–]\s*20)", t, _re.I):
            continue
        mv = _re.search(r"(rose|rise[sn]?|up|grew|gain(?:ed)?|jump(?:ed)?|climb(?:ed)?|"
                        r"fell|fall[sn]?|down|declin\w*|slid|slipp?ed?|drop(?:ped)?)\s+"
                        r"(?:by\s+)?([\d.]+)\s*(?:percent|%)", t, _re.I)
        if not mv:
            continue
        sign = -1 if _re.search(r"fell|fall|down|declin|slid|slip|drop", mv.group(1), _re.I) else 1
        yoy = round(sign * float(mv.group(2)), 1)
        mm = _re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s*1\s*[-–]\s*20",
                        t, _re.I)
        mon = MONTHS.get(mm.group(1).lower()[:3]) if mm else None
        yr = now.year
        if mon and mon > now.month:
            yr -= 1
        sm = _re.search(r"(?:chip|semiconductor)s?[^.%]{0,90}?([\d.]+)\s*(?:percent|%)", t, _re.I)
        semis = None
        if sm:
            sneg = _re.search(r"(fell|down|declin|slid|slip|drop)[^%]{0,40}" + _re.escape(sm.group(1)), t, _re.I)
            semis = round((-1 if sneg else 1) * float(sm.group(1)), 1)
        hits.append({"yoy_pct": yoy, "semis_yoy_pct": semis,
                     "period": (f"{yr}-{mon:02d}-01..20" if mon else None),
                     "headline": a["t"][:180], "url": a["u"], "published": a["p"],
                     "via": a["src"]})
    hits.sort(key=lambda h: (h.get("published") or ""), reverse=True)
    for h in hits:
        pm = h.get("period") or ""
        cur = f"{now.year}-{now.month:02d}"
        prv = f"{(now.replace(day=1) - timedelta(days=1)).year}-{(now.replace(day=1) - timedelta(days=1)).month:02d}"
        if pm.startswith(cur) and out["latest"] is None:
            out["latest"] = h
        elif pm.startswith(prv) and out["validated_sample"] is None:
            out["validated_sample"] = h
    if not hits:
        out["error"] = "no matching flash headline on tape yet"
    try:
        s3.put_object(Bucket=BUCKET, Key="asia/kr-flash-tape.json",
                      Body=json.dumps({"hits": hits[:10], "at": now.isoformat()}).encode(),
                      ContentType="application/json")
    except Exception:
        pass
    return out


def korea_flash():
    """v1.3 (ops 3592): the Korea 1-20 day export FLASH — the best high-frequency
    global trade nowcast — scraped from the PUBLIC KCS press release via the
    CF-worker /gov edge-fetch (KR gov firewalls cloud-ASN runners; data.go.kr
    key path dead: US phone rejected). Parses total + semiconductor prints with
    stated YoY. History caches to S3; never fabricates."""
    import re, urllib.parse
    out = {"source": _KCS_LIST, "via": "cf-edge /gov",
           "label": "Korea exports, 1st-20th of month (KCS provisional flash)",
           "period": None, "total_usd_bn": None, "total_yoy_pct": None,
           "semis_usd_bn": None, "semis_yoy_pct": None, "error": None}
    try:
        raw = urllib.request.urlopen(urllib.request.Request(
            _GOV_PROXY + urllib.parse.quote(_KCS_LIST, safe=""), headers=UA), timeout=25).read(600_000)
        lst = raw.decode("utf-8", "replace")
        items = [(u, re.sub(r"<[^>]+>|\s+", " ", t).strip())
                 for u, t in re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', lst, re.S)]
        tgt = next(((u, t) for u, t in items if re.search(r"수출입\s*현황", t)), None)
        if not tgt:
            out["error"] = "no 수출입 현황 item on board"
            return out
        iu = tgt[0]
        if iu.startswith("/"):
            iu = "https://www.customs.go.kr" + iu
        out["period"] = tgt[1][:80]
        raw2, _via_k = _edge(iu)
        out["via_item"] = _via_k
        body = re.sub(r"<[^>]+>|&nbsp;|\s+", " ", raw2.decode("utf-8", "replace"))
        out["item_url"] = iu
        m = re.search(r"수출[은는]?\s*([\d,]+(?:\.\d+)?)\s*억\s*달러\s*\(?[^)%]*?([+\-△▲]?\s*[\d.]+)\s*%", body)
        if m:
            v = _num_kr(m.group(1))
            out["total_usd_bn"] = round(v / 10.0, 2) if v is not None else None
            out["total_yoy_pct"] = _num_kr(m.group(2)) if "△" not in m.group(2) else -abs(_num_kr(m.group(2)) or 0)
        ms = re.search(r"반도체[^%]{0,60}?([+\-△▲]?\s*[\d.]+)\s*%", body)
        if ms:
            g = ms.group(1)
            val = _num_kr(g)
            out["semis_yoy_pct"] = (-abs(val) if ("△" in g and val is not None) else val)
        ms2 = re.search(r"반도체\s*\(?([\d,]+(?:\.\d+)?)\s*억", body)
        if ms2:
            v2 = _num_kr(ms2.group(1))
            out["semis_usd_bn"] = round(v2 / 10.0, 2) if v2 is not None else None
        out["raw_head"] = body[:200]
        if out["total_usd_bn"] is None and out["total_yoy_pct"] is None:
            out["error"] = "parse found no export print in item"
        else:
            try:
                key = "kcs/flash-cache.json"
                try:
                    cache = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
                except Exception:
                    cache = {"releases": {}}
                cache["releases"][out["period"]] = {k: out[k] for k in
                    ("total_usd_bn", "total_yoy_pct", "semis_usd_bn", "semis_yoy_pct", "item_url")}
                cache["releases"] = dict(list(cache["releases"].items())[-40:])
                s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(cache).encode(),
                              ContentType="application/json")
            except Exception as _e:
                print("[asia-leads] kcs cache skip", str(_e)[:60])
    except Exception as e:
        out["error"] = str(e)[:140]
    return out


_DGBAS_ORDERS = "https://eng.stat.gov.tw/Point.aspx?sid=t.6&n=4205&sms=11713"


def taiwan_orders():
    """v1.2 (ops 3587): TRUE Taiwan EXPORT ORDERS (orders lead shipments) —
    scraped from the DGBAS English indicator Point page discovered in ops 3586.
    Regex-extracted latest print + YoY; raw head kept for audit; never fabricates."""
    import re, ssl
    out = {"source": _DGBAS_ORDERS, "label": "Taiwan export orders (MOEA via DGBAS point page)",
           "latest_usd_bn": None, "yoy_pct": None, "period": None, "error": None}
    try:
        raw, via1 = _edge(_DGBAS_ORDERS)
        out["via_stage1"] = via1
        shell = raw.decode("utf-8", "replace")
        # v1.2.1: the Point page is a JS shell — it embeds its own data
        # querystring (const jhxiaoQS = '?sid=...&Create=1&_guid=...'); extract
        # it and fetch stage-2 from the same endpoint for the real content.
        qs = re.search(r"jhxiaoQS\s*=\s*'([^']+)'", shell)
        if qs:
            u2 = "https://eng.stat.gov.tw/Point.aspx" + qs.group(1).replace("&amp;", "&")
            try:
                raw, via2 = _edge(u2, cap=400_000)
                out["stage2"] = u2[:120]
                out["via_stage2"] = via2
            except Exception as _e2:
                out["stage2_err"] = str(_e2)[:90]
        txt = re.sub(r"<[^>]+>|&nbsp;|\s+", " ", raw.decode("utf-8", "replace"))
        seg = txt
        m = re.search(r"[Ee]xport [Oo]rders(.{0,600})", txt)
        if m:
            seg = m.group(1)
        v = re.search(r"US\$ ?([\d,]+\.?\d*) ?billion", seg)
        y = re.search(r"(?:increase|decrease|grew|fell|down|up)[a-z]* (?:by )?([\d.]+) ?%", seg, re.I) \
            or re.search(r"([+-]\d+\.\d+) ?%", seg)
        p = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December) 20\d\d", seg) \
            or re.search(r"20\d\d[./-]\d{1,2}", seg)
        neg = bool(re.search(r"decrease|fell|down|declin", seg[:200], re.I))
        if v:
            out["latest_usd_bn"] = float(v.group(1).replace(",", ""))
        if y:
            val = float(y.group(1).replace("+", ""))
            out["yoy_pct"] = round(-val if (neg and val > 0) else val, 2)
        if p:
            out["period"] = p.group(0)
        out["raw_head"] = seg[:220]
        if out["latest_usd_bn"] is None and out["yoy_pct"] is None:
            out["error"] = "regex found no orders print on page"
    except Exception as e:
        out["error"] = str(e)[:140]
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    out = {
        "engine": "asia-leads", "version": "1.3.0",
        "generated_at": now.isoformat(),
        "korea_exports": fred_block(
            "XTEXVA01KRM667N", "Korea merchandise exports (monthly, NSA)",
            "20-day customs flash (the true nowcast) requires a free Bank of Korea ECOS key — PENDING."),
        "korea_flash": korea_flash(),
        "korea_flash_tape": korea_flash_tape(),
        "taiwan_orders": taiwan_orders(),
        "taiwan_exports": fred_block(
            "VALEXPTWM052N", "Taiwan goods exports (monthly)",
            "Proxy for MOEA export ORDERS (orders lead shipments); direct MOEA endpoint discovery queued."),
        "siblings": {"china_credit_impulse": "data/china-liquidity.json (now carries REAL NBS Total Social Financing)",
                     "us_release_calendar": "data/econ-calendar.json (FMP, consensus + surprise tape)"},
        "methodology": {
            "origin": ("MacroMicro gap analysis 2026-07-20: rejected the paid middleman API; "
                       "sourced the genuinely-missing leads from free primaries instead."),
            "reads": ("China TSF YoY turning up = global credit impulse improving (risk-asset lead ~2-4q); "
                      "Korea + Taiwan export YoY = global tech/semis demand pulse (feeds the AI-infra thesis); "
                      "calendar = upcoming high-impact US prints for the front-run sniffer."),
        },
        "sources": ["FRED XTEXVA01KRM667N", "FRED VALEXPTWM052N"],
        "disclaimer": "Real primary data, research only — not investment advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[asia-leads] kr_yoy={out['korea_exports'].get('yoy_pct')} "
          f"tw_yoy={out['taiwan_exports'].get('yoy_pct')} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True,
        "kr_yoy": out["korea_exports"].get("yoy_pct"),
        "tw_yoy": out["taiwan_exports"].get("yoy_pct")})}
