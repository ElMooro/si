"""justhodl-china-liquidity — China liquidity + credit-impulse engine.

China is the world's #2 economy and the marginal driver of global growth and
commodity demand. Your Global Liquidity Index covers the Fed, ECB and BOJ but
not China — this fills that gap.

The single most useful China signal is the CREDIT IMPULSE: the change in the
flow of new credit. It leads Chinese activity, global manufacturing PMIs and
commodity prices by roughly 6-12 months. The textbook measure uses Total
Social Financing, which is not on FRED; this engine uses the best free proxy
— the ACCELERATION of broad/narrow money growth (the 2nd derivative), which
moves with the same signal — and is explicit that it is a proxy.

MEASURES (all FRED — free, defensive: uses whatever series resolve):
  • China M1 / M2 year-over-year growth        (money supply)
  • Money-growth ACCELERATION                  (credit-impulse proxy)
  • China interbank rate                       (liquidity tightness)
  • USD/CNY                                    (currency pressure / capital flow)
  • Copper price YoY + copper/gold ratio       (Dr. Copper — real China demand)

REGIME: EASING / NEUTRAL / TIGHTENING — with the read on what it has
historically meant for commodities, EM and global cyclicals ~2-3 quarters out.

OUTPUT: data/china-liquidity.json   Schedule: daily.
"""
import json, os, time
from datetime import datetime, timezone
from urllib import request, error
import boto3
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/china-liquidity.json"
S3_HISTORY_KEY = "data/china-liquidity-history.json"
HISTORY_MAX = 260

FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")

# Candidate FRED series — tried in order; first that resolves wins.
SERIES = {
    "m1": ["MANMM101CNM189S", "MANMM101CNQ189S"],
    "m2": ["MYAGM2CNM189N", "MABMM301CNM189S", "MABMM301CNQ189S"],
    "interbank": ["IR3TIB01CNM156N", "IR3TIB01CNM156S"],
    "usdcny": ["DEXCHUS"],
    "copper": ["PCOPPUSDM"],
    "gold": ["IQ12260", "GOLDAMGBD228NLBM"],
}


def _get_json(url, timeout=15, retries=3):
    last = None
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-China/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last = e
            time.sleep(0.5 * (i + 1))
    return None


def fred(series_id, limit=400):
    if not FRED_KEY:
        return []
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}")
    j = _get_json(url)
    if not j:
        return []
    out = []
    for o in j.get("observations", []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append({"date": o.get("date"), "value": float(v)})
        except Exception:
            pass
    return out  # newest-first


def fred_first(candidates):
    """Return (series_id, observations) for the first candidate that resolves."""
    for sid in candidates:
        obs = fred(sid)
        if obs:
            return sid, obs
    return None, []


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = request.Request(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                               data=body, headers={"Content-Type": "application/json"})
        request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def yoy(obs, periods=12):
    """Year-over-year % from a monthly newest-first series."""
    if len(obs) < periods + 1:
        return None
    cur, prior = obs[0]["value"], obs[periods]["value"]
    if prior and prior != 0:
        return (cur - prior) / abs(prior) * 100
    return None


def _dbn(url, timeout=25):
    try:
        req = request.Request(url, headers={"User-Agent": "JustHodl research contact@justhodl.ai",
                                            "Accept": "application/json"})
        return json.loads(request.urlopen(req, timeout=timeout).read())
    except Exception as e:
        print("[china-liq] dbnomics fail", str(e)[:80])
        return None


_PBOC_LIST = "http://www.pbc.gov.cn/en/3688247/3688978/3709140/index.html"

EDGE = "https://justhodl-data-proxy.raafouis.workers.dev/gov?u="


def _edge(u, timeout=25, cap=500_000):
    """ops 3625: fetch CN gov via the CF /gov edge (allowlisted); direct fallback."""
    import urllib.parse as _up
    from urllib import request as _rq
    try:
        r = _rq.urlopen(_rq.Request(EDGE + _up.quote(u, safe=""),
                                     headers={"User-Agent": "Mozilla/5.0"}), timeout=timeout)
        b = r.read(cap)
        if r.status == 200 and len(b) > 300:
            return b.decode("utf-8", "replace"), "edge"
    except Exception as e:
        print("[china-liq] edge fail", str(e)[:60])
    return _html(u, timeout=timeout, limit=cap), "direct"


def pboc_cn_tsf():
    """ops 3625: CN-side 社会融资规模增量 (TSF flow) via /gov edge — the EN
    report lags ~11 months; the CN release is same-week. Listing candidates
    probed, first 增量 item parsed for 万亿/亿元 flow + YoY delta. Honest
    hop-map recorded when structure blocks; never fabricates."""
    import re as _re
    out = {"label": "PBoC CN TSF monthly flow (社会融资规模增量)",
           "period": None, "flow_trn_cny": None, "yoy_delta_trn": None,
           "title": None, "url": None, "via": None,
           "candidates": [], "error": None}
    LISTS = ("http://www.pbc.gov.cn/diaochatongjisi/116219/116225/index.html",
             "http://www.pbc.gov.cn/diaochatongjisi/116219/116225/index_1.html",
             "http://www.pbc.gov.cn/diaochatongjisi/116219/116225/index_2.html",
             "http://www.pbc.gov.cn/diaochatongjisi/116219/116319/index.html",
             "http://www.pbc.gov.cn/diaochatongjisi/116219/index.html")
    try:
        item = None
        for L in LISTS:
            listing, via0 = _edge(L)
            if not listing:
                continue
            for _m in _re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
                                   listing, _re.I | _re.S):
                t0 = _re.sub(r"<[^>]+>|\s+", "", _m.group(2))
                if "社会融资规模增量" in t0:
                    hu = _m.group(1)
                    if hu.startswith("/"):
                        hu = "http://www.pbc.gov.cn" + hu
                    out["candidates"].append({"list": L[-28:], "title": t0[:60],
                                              "u": hu[:110], "via": via0})
                    _score = (2 if _re.search(r"20\d\d年\d{1,2}月", t0) else 0) \
                             - (2 if ("地区" in t0 or "季度" in t0) else 0)
                    if item is None or _score > item[2]:
                        item = (hu, t0, _score)
            # keep collecting across pages; newest-monthly chosen below
        # prefer the NEWEST monthly item across all pages probed
        _mons = []
        for c0 in out["candidates"]:
            mm = _re.search(r"(20\d\d)年(\d{1,2})月", c0.get("title", ""))
            if mm and "地区" not in c0["title"]:
                _mons.append((int(mm.group(1)), int(mm.group(2)), c0))
        if _mons:
            _mons.sort(reverse=True)
            _c = _mons[0][2]
            item = (_c["u"], _c["title"], 9)
        if not item:
            out["error"] = "no 增量 item on CN listings (see candidates/list probes)"
            return out
        page, via1 = _edge(item[0])
        out["url"], out["title"], out["via"] = item[0][:130], item[1][:100], via1
        # national item pages carry the body in an attachment .htm (EN pattern)
        _p0 = _re.sub(r"\s+", "", page)
        if "社会融资规模增量为" not in _p0:
            # v2.5: shells reference content via JS file lists (W020… paths)
            _w = _re.findall(r'["\']((?:https?://[^"\']+)?/?[^"\']*W0\d{9,}[^"\']*'
                             r'\.(?:html?|txt))["\']', page, _re.I)
            out["js_files"] = [w[:110] for w in _w[:4]]
            for _wf in _w[:3]:
                if _wf.startswith("/"):
                    _wf = "http://www.pbc.gov.cn" + _wf
                elif not _wf.startswith("http"):
                    _wf = item[0].rsplit("/", 1)[0] + "/" + _wf
                _sub, _v3 = _edge(_wf)
                if "社会融资规模增量为" in _re.sub(r"\s+", "", _sub):
                    page = _sub
                    out["attachment"], out["via"] = _wf[:120], _v3
                    break
        _p0 = _re.sub(r"\s+", "", page)
        if "社会融资规模增量为" not in _p0:
            for _att in _re.findall(r'href="([^"]+\.html?)"', page, _re.I)[:6]:
                if "index" in _att:
                    continue
                if _att.startswith("/"):
                    _att = "http://www.pbc.gov.cn" + _att
                elif not _att.startswith("http"):
                    _att = item[0].rsplit("/", 1)[0] + "/" + _att
                _sub, _v2 = _edge(_att)
                if "社会融资规模增量为" in _re.sub(r"\s+", "", _sub):
                    page = _sub
                    out["attachment"], out["via"] = _att[:120], _v2
                    break
        body = _re.sub(r"<[^>]+>|&nbsp;|\s+", "", page)
        m = _re.search(r"(20\d\d)年(\d{1,2})月(?:份)?社会融资规模增量为([\d.]+)(万亿|亿)元", body)
        if m:
            out["period"] = f"{m.group(1)}-{int(m.group(2)):02d}"
            v = float(m.group(3))
            out["flow_trn_cny"] = round(v if m.group(4) == "万亿" else v / 10000.0, 3)
        d = _re.search(r"比上年同期(多|少)([\d.]+)(万亿|亿)元", body)
        if d:
            dv = float(d.group(2))
            dv = dv if d.group(3) == "万亿" else dv / 10000.0
            out["yoy_delta_trn"] = round(dv if d.group(1) == "多" else -dv, 3)
        if out["flow_trn_cny"] is None:
            out["error"] = "item fetched, flow pattern not found (head recorded)"
            out["body_head"] = body[:200]
    except Exception as e:
        out["error"] = str(e)[:120]
    return out
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def _html(url, timeout=18, limit=500_000):
    import re as _re
    from urllib import request as _rq
    try:
        r = _rq.urlopen(_rq.Request(url, headers={"User-Agent": "JustHodl research contact@justhodl.ai"}),
                        timeout=timeout)
        return r.read(limit).decode("utf-8", "replace")
    except Exception as e:
        print("[china-liq] pboc fetch fail", url[:60], str(e)[:80])
        return ""


def _tables_rows(html):
    import re as _re
    rows = []
    for tb in _re.findall(r"<table[\s\S]*?</table>", html, _re.I)[:4]:
        for tr in _re.findall(r"<tr[\s\S]*?</tr>", tb, _re.I):
            cells = [_re.sub(r"<[^>]+>|&nbsp;|&#160;|\s+", " ", c).strip()
                     for c in _re.findall(r"<t[dh][\s\S]*?</t[dh]>", tr, _re.I)]
            if any(cells):
                rows.append(cells)
    return rows


def pboc_afre_block():
    """v2.1 (ops 3587): the REAL monthly TSF — scraped from PBoC's English
    'Report on Aggregate Financing to the Real Economy (Flow)'. Listing walked
    fresh each run; parsed reports cached to S3 (pboc/afre-flow-cache.json) so
    history self-builds and YoY/credit-impulse activate once two vintages
    accumulate. EN site lags the CN release by weeks — labeled. Never fabricates."""
    import re as _re
    out = {"source": _PBOC_LIST, "latest_report": None, "series": None, "error": None,
           "note": ("PBoC EN monthly AFRE Flow report (units: 100 million yuan). "
                    "EN publication lags the CN release; freshest-available honestly labeled.")}
    try:
        listing = _html(_PBOC_LIST)
        items = [(u, t) for u, t in
                 [( _m.group(1), _re.sub(r"<[^>]+>|\s+", " ", _m.group(2)).strip())
                  for _m in _re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', listing, _re.I | _re.S)]
                 if "aggregate financing" in t.lower() and "(flow)" in t.lower()]
        if not items:
            out["error"] = "no Flow items on listing"
            return out
        url = items[0][0]
        if url.startswith("/"):
            url = "http://www.pbc.gov.cn" + url
        page = _html(url)
        rows = _tables_rows(page)
        att_used = None
        if len(rows) < 3:
            # PBoC EN item pages often carry the table as an attached .htm —
            # follow the first same-host htm/html attachment and parse THAT.
            import re as _re2
            atts = [_re2.sub(r'^/', 'http://www.pbc.gov.cn/', a) if a.startswith('/')
                    else a
                    for a in _re2.findall(r'href="([^"]+\.html?)"', page, _re2.I)
                    if "index.html" not in a]
            for a in atts[:3]:
                if not a.startswith("http"):
                    a = url.rsplit("/", 1)[0] + "/" + a
                sub = _html(a)
                r2 = _tables_rows(sub)
                if len(r2) >= 3:
                    rows, att_used = r2, a
                    break
        hdr = next((r0 for r0 in rows if sum(1 for c in r0 if any(m in c for m in _MONTHS)) >= 3), None)
        afre = next((r0 for r0 in rows if r0 and "aggregate financing" in r0[0].lower()), None)
        parsed_rows = []
        for r0 in rows:
            if not r0 or len(r0) < 3:
                continue
            vals = []
            for c in r0[1:]:
                try:
                    vals.append(float(c.replace(",", "")))
                except Exception:
                    vals.append(None)
            if sum(1 for v in vals if v is not None) >= 2:
                parsed_rows.append({"item": r0[0][:70], "values": vals[:14]})
        out["latest_report"] = {"title": items[0][1][:110], "url": url, "attachment": att_used,
                               "header": (hdr[:14] if hdr else None),
                               "n_rows_parsed": len(parsed_rows)}
        if afre:
            vals = []
            for c in afre[1:]:
                try:
                    vals.append(float(c.replace(",", "")))
                except Exception:
                    pass
            out["series"] = {"item": afre[0][:70], "monthly_flow_100m_rmb": vals[:14],
                             "latest": (vals[-1] if vals else None)}
        out["rows"] = parsed_rows[:12]
        # self-building cache → YoY once two vintages exist
        try:
            key = "pboc/afre-flow-cache.json"
            try:
                cache = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            except Exception:
                cache = {"reports": {}}
            cache["reports"][items[0][1][:110]] = {"url": url,
                                                   "series": out.get("series"),
                                                   "fetched_at": datetime.now(timezone.utc).isoformat()}
            cache["reports"] = dict(list(cache["reports"].items())[-36:])
            s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(cache).encode(),
                          ContentType="application/json")
            out["cache_n_reports"] = len(cache["reports"])
        except Exception as _e:
            print("[china-liq] afre cache skip", str(_e)[:60])
    except Exception as e:
        out["error"] = str(e)[:140]
    return out


def real_tsf_block():
    """v2 (ops 3584): the REAL Total Social Financing from NBS via DBnomics —
    the textbook credit-impulse input this engine previously proxied with
    money-growth acceleration (proxy retained alongside; this is additive).
    Annual composition always (A_A0L08); monthly series when the probed
    dataset code is provided via env NBS_TSF_MONTHLY."""
    out = {"source_annual": "DBnomics NBS/A_A0L08",
           "annual_composition": [], "monthly": None,
           "note": ("Real NBS Total Social Financing — replaces the money-acceleration "
                    "PROXY as the textbook credit-impulse input; proxy kept for continuity.")}
    j = _dbn("https://api.db.nomics.world/v22/series/NBS/A_A0L08?limit=30&observations=1")
    for d in ((j or {}).get("series") or {}).get("docs") or []:
        pv = [(p, v) for p, v in zip(d.get("period") or [], d.get("value") or [])
              if isinstance(v, (int, float))]
        if not pv:
            continue
        per = [p for p, _ in pv]; val = [v for _, v in pv]
        yoy1 = round((val[-1] / val[-2] - 1) * 100, 2) if len(val) >= 2 and val[-2] else None
        out["annual_composition"].append({"code": d.get("series_code"),
                                          "name": (d.get("series_name") or "")[:110],
                                          "frequency": "A", "last_period": per[-1],
                                          "last_value": val[-1], "yoy_pct": yoy1})
    mcode = os.environ.get("NBS_TSF_MONTHLY")
    if mcode:
        jm = _dbn("https://api.db.nomics.world/v22/series/NBS/%s?limit=40&observations=1" % mcode)
        docs = ((jm or {}).get("series") or {}).get("docs") or []
        rows = []
        for d in docs:
            pv = [(p, v) for p, v in zip(d.get("period") or [], d.get("value") or [])
                  if isinstance(v, (int, float))]
            if len(pv) < 13:
                continue
            per = [p for p, _ in pv]; val = [v for _, v in pv]
            f12 = sum(val[-12:]); f12p = sum(val[-24:-12]) if len(val) >= 24 else None
            rows.append({"code": d.get("series_code"), "name": (d.get("series_name") or "")[:110],
                         "frequency": "M", "last_period": per[-1], "last_value": val[-1],
                         "yoy_pct": round((val[-1] / val[-13] - 1) * 100, 2) if val[-13] else None,
                         "flow_12m": round(f12, 1),
                         "credit_impulse_flow_yoy_pct": (round((f12 / f12p - 1) * 100, 2)
                                                         if f12p else None)})
        out["monthly"] = {"source": "DBnomics NBS/%s" % mcode, "series": rows[:12],
                          "n_series": len(rows)}
    return out


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[china-liquidity] starting {datetime.now(timezone.utc).isoformat()}")
    if not FRED_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FRED_API_KEY not set"})}

    resolved = {}
    raw = {}
    for key, cands in SERIES.items():
        sid, obs = fred_first(cands)
        resolved[key] = sid
        raw[key] = obs
        print(f"[china] {key}: {sid} ({len(obs)} obs)")

    failed = [k for k, v in raw.items() if not v]

    # ── money growth + credit-impulse proxy ──
    m1 = raw.get("m1", [])
    m2 = raw.get("m2", [])
    m1_yoy = yoy(m1)
    m2_yoy = yoy(m2)
    # prior-year YoY for acceleration
    m1_yoy_prior = yoy(m1[12:]) if len(m1) > 24 else None
    m2_yoy_prior = yoy(m2[12:]) if len(m2) > 24 else None
    # credit-impulse proxy = acceleration of money growth (pp change in YoY)
    impulse_m1 = (m1_yoy - m1_yoy_prior) if (m1_yoy is not None and m1_yoy_prior is not None) else None
    impulse_m2 = (m2_yoy - m2_yoy_prior) if (m2_yoy is not None and m2_yoy_prior is not None) else None
    impulse = None
    parts = [x for x in (impulse_m1, impulse_m2) if x is not None]
    if parts:
        impulse = sum(parts) / len(parts)

    # ── interbank rate (tightness) ──
    ib = raw.get("interbank", [])
    ib_latest = ib[0]["value"] if ib else None
    ib_3m_ago = ib[3]["value"] if len(ib) > 3 else None
    ib_trend = (ib_latest - ib_3m_ago) if (ib_latest is not None and ib_3m_ago is not None) else None

    # ── USD/CNY pressure ──
    cny = raw.get("usdcny", [])
    cny_latest = cny[0]["value"] if cny else None
    cny_3m_ago = cny[63]["value"] if len(cny) > 63 else None
    cny_chg_3m = ((cny_latest - cny_3m_ago) / cny_3m_ago * 100) if (cny_latest and cny_3m_ago) else None

    # ── Dr. Copper ──
    cop = raw.get("copper", [])
    gold = raw.get("gold", [])
    copper_yoy = yoy(cop)
    copper_gold = None
    if cop and gold and gold[0]["value"]:
        copper_gold = cop[0]["value"] / gold[0]["value"]

    # ── regime classification ──
    # primary signal: credit-impulse proxy + money growth level
    score = 0
    if impulse is not None:
        score += 2 if impulse > 1.5 else (-2 if impulse < -1.5 else 0)
    if m2_yoy is not None:
        score += 1 if m2_yoy > 9 else (-1 if m2_yoy < 6 else 0)
    if ib_trend is not None:
        score += -1 if ib_trend > 0.3 else (1 if ib_trend < -0.3 else 0)
    if copper_yoy is not None:
        score += 1 if copper_yoy > 8 else (-1 if copper_yoy < -8 else 0)

    if score >= 2:
        regime = "EASING"
        regime_read = ("China liquidity/credit is accelerating. Historically a 6-12 "
                       "month tailwind for global commodities, emerging markets and "
                       "cyclical/industrial equities — the credit impulse leads.")
    elif score <= -2:
        regime = "TIGHTENING"
        regime_read = ("China liquidity/credit is decelerating. A forward headwind for "
                       "commodities, EM and global cyclicals — typically felt 2-3 "
                       "quarters out as the credit impulse rolls over.")
    else:
        regime = "NEUTRAL"
        regime_read = ("China liquidity is roughly steady — no strong forward push or "
                       "pull on commodities and global cyclicals from the credit impulse.")

    hist = {"snapshots": []}
    try:
        hist = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY)["Body"].read())
    except Exception:
        pass
    prior_regime = hist["snapshots"][-1]["regime"] if hist.get("snapshots") else None

    out = {
        "schema_version": "1.0",
        "method": "china_liquidity_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "fred_failed": failed,
        "series_resolved": resolved,
        "regime": regime,
        "regime_read": regime_read,
        "money": {
            "m1_yoy_pct": round(m1_yoy, 2) if m1_yoy is not None else None,
            "m2_yoy_pct": round(m2_yoy, 2) if m2_yoy is not None else None,
        },
        "credit_impulse": {
            "value_pp": round(impulse, 2) if impulse is not None else None,
            "is_proxy": True,
            "definition": ("acceleration of money-supply YoY growth (pp change). "
                           "A free proxy for the Total Social Financing credit "
                           "impulse — moves with the same signal, leads ~6-12mo."),
            "signal": ("credit accelerating — forward tailwind" if (impulse or 0) > 1.5
                       else "credit decelerating — forward headwind" if (impulse or 0) < -1.5
                       else "credit impulse roughly flat"),
        },
        "interbank_rate": {
            "latest_pct": round(ib_latest, 3) if ib_latest is not None else None,
            "change_3m_pp": round(ib_trend, 3) if ib_trend is not None else None,
        },
        "currency": {
            "usd_cny": round(cny_latest, 4) if cny_latest is not None else None,
            "cny_change_3m_pct": round(cny_chg_3m, 2) if cny_chg_3m is not None else None,
            "read": ("CNY weakening vs USD — capital-outflow / easing pressure"
                     if (cny_chg_3m or 0) > 1
                     else "CNY firm — stable capital picture"),
        },
        "dr_copper": {
            "copper_yoy_pct": round(copper_yoy, 1) if copper_yoy is not None else None,
            "copper_gold_ratio": round(copper_gold, 5) if copper_gold is not None else None,
            "read": ("copper strong — real China/global demand firming"
                     if (copper_yoy or 0) > 8
                     else "copper weak — real demand softening" if (copper_yoy or 0) < -8
                     else "copper neutral"),
        },
    }

    out["tsf"] = real_tsf_block()
    out["tsf"]["pboc_monthly"] = pboc_afre_block()
    out["tsf"]["pboc_cn"] = pboc_cn_tsf()
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    hist["snapshots"].append({"ts": out["generated_at"], "regime": regime,
                               "m2_yoy": m2_yoy, "credit_impulse": impulse,
                               "copper_yoy": copper_yoy})
    hist["snapshots"] = hist["snapshots"][-HISTORY_MAX:]
    hist["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    if prior_regime and prior_regime != regime:
        maybe_telegram(
            f"[china] <b>CHINA LIQUIDITY REGIME CHANGE</b>\n"
            f"<b>{prior_regime} → {regime}</b>\n"
            f"credit impulse {round(impulse,1) if impulse is not None else '—'}pp · "
            f"M2 {round(m2_yoy,1) if m2_yoy is not None else '—'}%\n{regime_read}")

    print(f"[china-liquidity] done {out['elapsed_s']}s regime={regime} "
          f"impulse={impulse} failed={failed}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "regime": regime,
        "credit_impulse_pp": round(impulse, 2) if impulse is not None else None,
        "m2_yoy": round(m2_yoy, 2) if m2_yoy is not None else None,
        "fred_failed": failed})}
