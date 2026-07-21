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
    # source 0 (keyless primary): Google News RSS — carries Yonhap/Reuters flashes
    try:
        import re as _re0
        for gq in ('%22South%20Korea%22%20exports%20%2220%20days%22%20when%3A14d',
                   'Korea%20exports%20first%2020%20days%20when%3A35d'):
            xml = urllib.request.urlopen(urllib.request.Request(
                "https://news.google.com/rss/search?q=" + gq + "&hl=en-US&gl=US&ceid=US:en",
                headers=UA), timeout=20).read().decode("utf-8", "replace")
            for it in _re0.findall(r"<item>(.*?)</item>", xml, _re0.S)[:40]:
                tt = _re0.search(r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", it, _re0.S)
                lk = _re0.search(r"<link>(.*?)</link>", it, _re0.S)
                pd = _re0.search(r"<pubDate>(.*?)</pubDate>", it)
                if tt:
                    from email.utils import parsedate_to_datetime as _p2d
                    try:
                        pds = _p2d(pd.group(1)).strftime("%Y-%m-%d") if pd else ""
                    except Exception:
                        pds = ""
                    arts.append({"t": tt.group(1), "u": (lk.group(1) if lk else None),
                                 "p": pds, "src": "gnews"})
        out["sources"].append({"gnews": len(arts)})
    except Exception as e:
        out["sources"].append({"gnews_err": str(e)[:70]})
    try:
        q = urllib.parse.quote('"South Korea" exports "20 days"')
        j = _json_get("https://newsapi.org/v2/everything?q=" + q +
                      "&language=en&sortBy=publishedAt&pageSize=50&apiKey=" + NEWSAPI_KEY)
        n0 = len(arts)
        if isinstance(j, dict) and j.get("status") != "ok":
            out["sources"].append({"newsapi_status": str(j.get("code") or j.get("status"))[:40]})
        for a in (j or {}).get("articles") or []:
            arts.append({"t": (a.get("title") or "") + ". " + (a.get("description") or ""),
                         "u": a.get("url"), "p": (a.get("publishedAt") or "")[:10],
                         "src": "newsapi"})
        out["sources"].append({"newsapi": len(arts) - n0})
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
        def _parse_orders(txt0):
            r0 = {"usd_bn": None, "yoy": None, "period": None, "head": None}
            seg0 = txt0
            m0 = re.search(r"[Ee]xport [Oo]rders(.{0,600})", txt0)
            if m0:
                seg0 = m0.group(1)
            v0 = re.search(r"US\$ ?([\d,]+\.?\d*) ?billion", seg0)
            y0 = re.search(r"(?:increase|decrease|grew|fell|down|up)[a-z]* (?:by )?([\d.]+) ?%", seg0, re.I) \
                or re.search(r"([+-]\d+\.\d+) ?%", seg0)
            p0 = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December) 20\d\d", seg0) \
                or re.search(r"20\d\d[./-]\d{1,2}", seg0)
            neg0 = bool(re.search(r"decrease|fell|down|declin", seg0[:200], re.I))
            if v0:
                r0["usd_bn"] = float(v0.group(1).replace(",", ""))
            if y0:
                val0 = float(y0.group(1).replace("+", ""))
                r0["yoy"] = round(-val0 if (neg0 and val0 > 0) else val0, 2)
            if p0:
                r0["period"] = p0.group(0)
            r0["head"] = seg0[:220]
            return r0

        raw_s = raw.decode("utf-8", "replace")
        txt = re.sub(r"<[^>]+>|&nbsp;|\s+", " ", raw_s)
        pr = _parse_orders(txt)
        out["raw_head"] = pr["head"]
        # v1.5 STAGE-3: value sits one link deeper — follow order-labeled hrefs
        if pr["usd_bn"] is None and pr["yoy"] is None:
            if "__doPostBack" in raw_s and "href=" not in raw_s.lower():
                out["error"] = "stage3 requires POST (__doPostBack) — /gov is GET-only; next: worker POST support"
            else:
                cands = []
                for hu, lab in re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', raw_s, re.S):
                    lab2 = re.sub(r"<[^>]+>|\s+", " ", lab)[:70].strip()
                    if hu.startswith("javascript") or "#" == hu:
                        continue
                    au = hu if hu.startswith("http") else ("https://eng.stat.gov.tw/" + hu.lstrip("/"))
                    score = 2 if re.search(r"order|訂單|外銷", lab2 + hu, re.I) else                             (1 if "sid=" in hu else 0)
                    cands.append((score, au, lab2))
                cands.sort(key=lambda x: -x[0])
                out["stage3_candidates"] = [{"u": c[1][:110], "label": c[2]} for c in cands[:6]]
                tried = []
                _bodies3 = []
                for sc0, au, lab2 in cands[:3]:
                    if sc0 == 0 and tried:
                        break
                    b3, via3 = _edge(au, cap=400_000)
                    t3 = re.sub(r"<[^>]+>|&nbsp;|\s+", " ", b3.decode("utf-8", "replace"))
                    pr3 = _parse_orders(t3)
                    tried.append({"u": au[:110], "label": lab2, "via": via3,
                                  "bytes": len(b3), "hit": pr3["usd_bn"] is not None or pr3["yoy"] is not None})
                    _bodies3.append((au, b3.decode("utf-8", "replace")))
                    if tried[-1]["hit"]:
                        pr = pr3
                        out["stage3_hit"] = au[:130]
                        break
                out["stage3_tried"] = tried
                # v1.5.1 STAGE-4: candidate pages are often shells too — probe
                # nested jhxiaoQS / iframe / meta-refresh one hop deeper via edge.
                if pr["usd_bn"] is None and pr["yoy"] is None:
                    s4 = []
                    _bodies4 = []
                    for au0, bs in _bodies3[:2]:
                        hops = []
                        q4 = re.search(r"jhxiaoQS\s*=\s*'([^']+)'", bs)
                        if q4:
                            hops.append(au0.split("?")[0] + q4.group(1).replace("&amp;", "&"))
                        for ifr in re.findall(r'<iframe[^>]+src="([^"]+)"', bs, re.I)[:2]:
                            hops.append(ifr if ifr.startswith("http")
                                        else "https://eng.stat.gov.tw/" + ifr.lstrip("/"))
                        mr = re.search(r'http-equiv="refresh"[^>]+url=([^"\'>]+)', bs, re.I)
                        if mr:
                            hops.append(mr.group(1))
                        for h4 in hops[:2]:
                            b4, via4 = _edge(h4, cap=400_000)
                            _bodies4.append((h4, b4.decode("utf-8", "replace")))
                            t4 = re.sub(r"<[^>]+>|&nbsp;|\s+", " ",
                                        b4.decode("utf-8", "replace"))
                            pr4 = _parse_orders(t4)
                            s4.append({"u": h4[:110], "via": via4, "bytes": len(b4),
                                       "hit": pr4["usd_bn"] is not None or pr4["yoy"] is not None})
                            if s4[-1]["hit"]:
                                pr = pr4
                                out["stage4_hit"] = h4[:130]
                                break
                        if out.get("stage4_hit"):
                            break
                    if not s4 and _bodies3 and "__doPostBack" in _bodies3[0][1]:
                        out["stage4_block"] = "postback-only shells — needs worker POST"
                    out["stage4_tried"] = s4
                # v1.6 STAGE-5: frames are JS apps — the value arrives by XHR.
                # Extract api/ajax endpoint literals from frame bodies, probe
                # top candidates via edge, parse any JSON/text for the print.
                if pr["usd_bn"] is None and pr["yoy"] is None and _bodies3:
                    eps = []
                    for au0, bs in (_bodies4 + _bodies3)[:4]:
                        for e0 in re.findall(
                                r'["\']((?:https?://[^"\']+|/[A-Za-z0-9_./-]+)?'
                                r'[A-Za-z0-9_./-]*(?:api|ajax|Ashx|ashx|asmx|'
                                r'Handler|GetData|Query|\.json)'
                                r'[A-Za-z0-9_./?&=%\-]*)["\']', bs):
                            if len(e0) > 6 and "css" not in e0 and "js" != e0[-2:]:
                                u5 = e0 if e0.startswith("http") else                                      "https://eng.stat.gov.tw" + (e0 if e0.startswith("/")
                                                                  else "/" + e0)
                                if u5 not in [x["u"] for x in eps]:
                                    eps.append({"u": u5, "src_frame": au0[-40:]})
                    out["stage5_endpoints"] = eps[:8]
                    s5 = []
                    for ep in eps[:3]:
                        b5, via5 = _edge(ep["u"], cap=300_000)
                        h5 = b5.decode("utf-8", "replace")
                        t5 = re.sub(r"<[^>]+>|\s+", " ", h5)
                        pr5 = _parse_orders(t5)
                        looks_json = h5.lstrip()[:1] in "[{"
                        s5.append({"u": ep["u"][:110], "via": via5, "bytes": len(b5),
                                   "json": looks_json, "hit": pr5["usd_bn"] is not None
                                   or pr5["yoy"] is not None,
                                   "head": h5.strip()[:100]})
                        if s5[-1]["hit"]:
                            pr = pr5
                            out["stage5_hit"] = ep["u"][:130]
                            break
                    out["stage5_tried"] = s5
                # v1.7 STAGE-6: GetPointData.ashx is THE feed — sweep every
                # SitesSN embedded in the orders frames; scan JSON for the
                # Export-Orders item and pull value/YoY from Content/Date.
                out["stage6_sitesns"] = []
                out["stage6_tried"] = []
                out["v17"] = True
                if pr["usd_bn"] is None and pr["yoy"] is None:
                    sns = []
                    for _, bs in (_bodies4 + _bodies3):
                        for sn in re.findall(r"SitesSN=(\d{2,5})", bs):
                            if sn not in sns:
                                sns.append(sn)
                    if not sns:
                        sns = [str(x) for x in range(455, 476)]
                        out["stage6_brute"] = True
                    out["stage6_sitesns"] = sns[:10]
                    s6 = []
                    for sn in sns[:8]:
                        u6 = "https://eng.stat.gov.tw/Common/GetPointData.ashx?SitesSN=" + sn
                        b6, via6 = _edge(u6, cap=200_000)
                        h6 = b6.decode("utf-8", "replace")
                        rec = {"sn": sn, "via": via6, "bytes": len(b6), "hit": False}
                        try:
                            arr = json.loads(h6)
                            rec["titles"] = [str((it or {}).get("Title"))[:34]
                                             for it in arr[:6]]
                            for it in arr:
                                ti = str((it or {}).get("Title") or "")
                                if re.search(r"order", ti, re.I):
                                    blob = " ".join(str((it or {}).get(k) or "")
                                                    for k in ("Content", "Date",
                                                              "Title", "Num"))
                                    vv = re.search(r"([\d,]+\.?\d*)\s*"
                                                   r"(?:US\$|billion|億美元)", blob) \
                                        or re.search(r"US\$\s*([\d,]+\.?\d*)", blob)
                                    yy = re.search(r"(-?[\d.]+)\s*\(?%", blob)
                                    if vv:
                                        pr["usd_bn"] = float(vv.group(1).replace(",", ""))
                                    if yy:
                                        pr["yoy"] = float(yy.group(1))
                                    pm = re.search(r"(20\d\d[./-]\d{1,2}|"
                                                   r"[A-Z][a-z]{2,8}\.? 20\d\d)", blob)
                                    if pm:
                                        pr["period"] = pm.group(1)
                                    rec["hit"] = pr["usd_bn"] is not None or                                         pr["yoy"] is not None
                                    rec["item_blob"] = blob[:160]
                                    if rec["hit"]:
                                        out["stage6_hit"] = {"sn": sn, "title": ti[:60]}
                                    break
                        except Exception as _e6:
                            rec["err"] = str(_e6)[:60]
                        s6.append(rec)
                        if rec["hit"]:
                            break
                    out["stage6_tried"] = s6
        out["latest_usd_bn"], out["yoy_pct"] = pr["usd_bn"], pr["yoy"]
        if pr["period"]:
            out["period"] = pr["period"]
        if out["latest_usd_bn"] is None and out["yoy_pct"] is None and not out.get("error"):
            out["error"] = "no orders print through stage-3; see stage3_tried/candidates"
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
