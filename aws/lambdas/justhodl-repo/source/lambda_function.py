"""justhodl-repo — THE repo master engine (ops 4763).

Khalid's spec: one engine named repo feeding one page named repo, with
EVERY repo-related series the system holds -- the master inventory's
300 (TRI/TRIV1/DVP/GCF, FNYR rates+underlying volumes, PD fails +
repo/reverse-repo financing) -- PLUS the FRED trade-weighted dollar
complex, each shown with day/week/month/quarter/year percentage
changes, click-through full-history charts, and a combined barometer.

Design:
  - inputs are the BANK, never the wire: series bodies come from the
    warm docs the convergence engines maintain (data/warm/ofr/series/*,
    data/providers/fred-scoped/*), and the scope comes from
    data/repo-master-inventory.json (ops 4761) so anything the
    inventory gains later flows in automatically
  - shape-robust pair extraction (OFR payloads and FRED docs differ);
    the longest [date,value] sequence found anywhere in the doc wins
  - changes: pop = vs previous observation (the honest 'day-to-day'
    for daily series, period-over-period for weekly), plus calendar
    lookbacks 7/30/91/365d against the series' own history
  - outputs: data/repo.json (hot index: groups, 5-change grid,
    barometer) + data/repo-history/{id}.json per series (lean
    dates/values arrays the page lazy-loads for full-history charts)
  - barometer: labeled heuristic, fully transparent -- every component
    (venue spreads, collateral spread, fails momentum, overnight-share
    proxy, dollar impulse) ships with its value, sign, and clipped
    contribution; score = 50 + 10*mean(clipped z), 0..100
Explicit failures per series; a missing doc becomes a skipped row with
a reason, never a fabricated number."""
import gzip
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")

DOLLAR_TITLES = {
 "DTWEXBGS": "Nominal Broad U.S. Dollar Index (daily)",
 "RTWEXBGS": "Real Broad U.S. Dollar Index (monthly)",
 "DTWEXAFEGS": "Nominal Advanced Foreign Economies Dollar Index (daily)",
 "DTWEXEMEGS": "Nominal Emerging Market Economies Dollar Index (daily)",
 "DTWEXM": "Trade Weighted Dollar vs Major Currencies (daily, 1973-2019)",
 "DTWEXB": "Trade Weighted Broad Dollar (daily, 1995-2019)",
 "TWEXBGSMTH": "Nominal Broad Dollar Index (monthly)",
 "TWEXAFEGSMTH": "Nominal AFE Dollar Index (monthly)",
 "TWEXEMEGSMTH": "Nominal EME Dollar Index (monthly)"}
DOLLAR_IDS = ["DTWEXBGS", "RTWEXBGS", "DTWEXAFEGS", "DTWEXEMEGS",
               "DTWEXM", "DTWEXB", "TWEXBGSMTH", "TWEXAFEGSMTH",
               "TWEXEMEGSMTH"]
FRED_META_KEY = "data/_state/fred-series-meta.json"
FRED_WARM_TPL = "data/warm/fred-scoped/{cat}/{id}.json"
DATE_KEYS = ("date", "d", "asofdate", "as_of_date", "observation_date",
              "DATE", "period")
VAL_KEYS = ("value", "v", "val", "rate", "close", "VALUE", "obs_value")


def sread(key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def _num(x):
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        xs = str(x).replace(",", "").strip()
        if xs in ("", ".", "null", "None", "NA", "NaN"):
            return None
        return float(xs)
    except Exception:
        return None


def _isdate(sv):
    return (isinstance(sv, str) and len(sv) >= 10 and sv[:4].isdigit()
             and sv[4] == "-" and sv[7] == "-")


def extract_pairs(obj):
    """Longest [ISO-date, number] sequence anywhere in the doc."""
    best = []

    def walk(o, depth=0):
        nonlocal best
        if depth > 8:
            return
        if isinstance(o, list):
            pairs = []
            for row in o:
                if isinstance(row, (list, tuple)) and len(row) >= 2 \
                        and _isdate(row[0]):
                    v = _num(row[1])
                    if v is not None:
                        pairs.append((row[0][:10], v))
                elif isinstance(row, dict):
                    dv = next((row[k] for k in DATE_KEYS
                                if _isdate(row.get(k))), None)
                    vv = next((_num(row[k]) for k in VAL_KEYS
                                if _num(row.get(k)) is not None), None)
                    if dv is not None and vv is not None:
                        pairs.append((dv[:10], vv))
            if len(pairs) > len(best):
                best = pairs
            for row in o[:50]:
                walk(row, depth + 1)
        elif isinstance(o, dict):
            for v in o.values():
                walk(v, depth + 1)

    walk(obj)
    dd = {}
    for d, v in best:
        dd[d] = v
    return sorted(dd.items())


def pct(last, base):
    if base is None or last is None or base == 0:
        return None
    return round((last / base - 1.0) * 100.0, 3)


def changes(pairs):
    if not pairs:
        return None, None, {}
    dates = [p[0] for p in pairs]
    vals = [p[1] for p in pairs]
    last_d, last_v = dates[-1], vals[-1]
    out = {"pop": pct(last_v, vals[-2]) if len(vals) > 1 else None}
    import bisect
    ld = datetime.strptime(last_d, "%Y-%m-%d")
    for tag, days in (("w", 7), ("m", 30), ("q", 91), ("y", 365)):
        target = (ld - timedelta(days=days)).strftime("%Y-%m-%d")
        i = bisect.bisect_right(dates, target) - 1
        out[tag] = pct(last_v, vals[i]) if i >= 0 else None
    return last_d, last_v, out


def safe_id(m):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", m)


def group_of(e):
    m = e["mnemonic"]
    if m.startswith("REPO-TRIV1"):
        return "Tri-party (TRIV1, ex-Fed)"
    if m.startswith("REPO-TRI_"):
        return "Tri-party (TRI legacy)"
    if m.startswith("REPO-DVP"):
        return "DVP bilateral cleared"
    if m.startswith("REPO-GCF"):
        return "GCF"
    if m.startswith("FNYR"):
        return "Reference rates & volumes"
    if "AFtD" in m or "AFtR" in m:
        return "Primary dealer FAILS"
    if m.startswith("NYPD"):
        return "Primary dealer financing"
    if e.get("family") == "DOLLAR":
        return "Dollar (FRED trade-weighted)"
    if e.get("family") == "ICEBOFA":
        return "ICE BofA option-adjusted spreads (FRED)"
    if e.get("family") == "HAIRCUT":
        return "Tri-party haircuts (NY Fed monthly)"
    if e.get("family") == "DTCCFAILS":
        return "DTCC daily settlement fails (FICC)"
    if e.get("family") == "SFTR":
        return "SFTR EU/UK repo (ICMA weekly)"
    if e.get("family") == "EUROREPO":
        return "European sovereign collateral (ECB MMSR)"
    if e.get("family") == "EUYIELD":
        return "European sovereign yields & spreads"
    if e.get("family") == "FREDREPO":
        return "FRED repo & SOFR complex"
    if e.get("family") == "MMFRP":
        return "MMF repo counterparties (OFR)"
    if e.get("family") == "SPONSORED":
        return "FICC sponsored repo"
    if e.get("family") == "RATESPCT":
        return "Rate percentiles (NY Fed)"
    if e.get("family") == "SRF":
        return "Standing Repo Facility"
    return "Other"


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    inv = sread("data/repo-master-inventory.json")
    # v1.5: readable names straight from OFR metadata (field is
    # series_name -- the inventory stored blanks); 3 tiny calls/day
    names = {}
    try:
        import urllib.request as _ur
        for _ds in ("repo", "nypd", "fnyr", "mmf"):
            _rq = _ur.Request("https://data.financialresearch.gov/v1/"
                               f"metadata/mnemonics?dataset={_ds}",
                               headers={"User-Agent":
                                         "JustHodl raafouis@gmail.com"})
            with _ur.urlopen(_rq, timeout=30) as _r:
                for _x in json.loads(_r.read()):
                    if isinstance(_x, dict) and _x.get("mnemonic"):
                        names[str(_x["mnemonic"])] = str(
                            _x.get("series_name") or "")[:160]
    except Exception:
        pass
    scope = list(inv.get("series") or [])

    # add the dollar complex (v1.2): warm-first at
    # data/warm/fred-scoped/Dollar_TradeWeighted/{ID}.json; if a series
    # was outside the scoped FRED import, self-heal ONCE from the FRED
    # API (key inherited via deploy inherit_env) and bank it to warm
    # permanently -- thereafter it reads from the bank like everything.
    import urllib.request
    dollar_miss = []
    fred_key = os.environ.get("FRED_API_KEY", "")
    DOLLAR_DIR = "data/warm/fred-scoped/Dollar_TradeWeighted"
    for fid in DOLLAR_IDS:
        wkey = f"{DOLLAR_DIR}/{fid}.json"
        ok = False
        try:
            s3.head_object(Bucket=BUCKET, Key=wkey)
            ok = True
        except Exception:
            if not fred_key:
                dollar_miss.append({"id": fid, "reason": "no_api_key"})
                continue
            try:
                u = ("https://api.stlouisfed.org/fred/series/observations"
                     f"?series_id={fid}&api_key={fred_key}"
                     "&file_type=json&observation_start=1970-01-01")
                req = urllib.request.Request(u, headers={
                    "User-Agent": "JustHodl research raafouis@gmail.com"})
                with urllib.request.urlopen(req, timeout=60) as r:
                    doc = json.loads(r.read())
                obs = doc.get("observations") or []
                if not obs:
                    dollar_miss.append({"id": fid,
                                         "reason": "fred_zero_obs"})
                    continue
                s3.put_object(Bucket=BUCKET, Key=wkey,
                               Body=json.dumps(
                                   {"id": fid,
                                    "source": "FRED API via justhodl-repo",
                                    "banked_at": now,
                                    "observations": obs},
                                   separators=(",", ":")).encode(),
                               ContentType="application/json")
                ok = True
            except Exception as ex:
                dollar_miss.append({"id": fid,
                                     "reason": f"{type(ex).__name__}: "
                                               f"{str(ex)[:60]}"})
        if ok:
            scope.append({"mnemonic": fid,
                           "name": DOLLAR_TITLES.get(fid, f"FRED {fid}"),
                           "family": "DOLLAR", "s3_key": wkey,
                           "api_url": ("https://fred.stlouisfed.org/series/"
                                        + fid),
                           "tier": 1 if fid == "DTWEXBGS" else 2})

    # v1.3: PD splice layer -- if ops 4772 published a verified map,
    # NYPD rows prepend older-break history (scaled to OFR units)
    try:
        _spl = sread("data/warm/nyfed-markets/pd-splice-map.json"
                      ).get("verified") or {}
    except Exception:
        _spl = {}

    # v1.6: ICE BofA option-adjusted spread complex. Source of truth =
    # the recovered archive (data/warm/archived-fred/BAML*.json, incl
    # Wayback-restored history); self-heal keeps it CURRENT: when the
    # merged copy is stale >21d and FRED_API_KEY is present, fetch the
    # tail from FRED and bank the merged doc to
    # data/warm/fred-scoped/ICE_BofA_OAS/{ID}.json (read first next runs).
    ICE_DIR = "data/warm/fred-scoped/ICE_BofA_OAS"
    # sweep BOTH sources: the Wayback archive AND every fred-scoped
    # category dir (BAML* prefix per dir); dup ids -> merge later
    ice_src = {}
    _tok = None
    while True:
        _kw = dict(Bucket=BUCKET, Prefix="data/warm/archived-fred/BAML",
                    MaxKeys=1000)
        if _tok:
            _kw["ContinuationToken"] = _tok
        _r = s3.list_objects_v2(**_kw)
        for _o in _r.get("Contents") or []:
            _id = _o["Key"].rsplit("/", 1)[-1].replace(".json", "")
            if _id:
                ice_src.setdefault(_id, []).append(_o["Key"])
        _tok = _r.get("NextContinuationToken")
        if not _tok:
            break
    _r = s3.list_objects_v2(Bucket=BUCKET,
                              Prefix="data/warm/fred-scoped/",
                              Delimiter="/", MaxKeys=1000)
    for _cp in _r.get("CommonPrefixes") or []:
        _dir = _cp["Prefix"]
        if _dir.endswith("ICE_BofA_OAS/"):
            continue
        _r2 = s3.list_objects_v2(Bucket=BUCKET, Prefix=_dir + "BAML",
                                   MaxKeys=1000)
        for _o in _r2.get("Contents") or []:
            _id = _o["Key"].rsplit("/", 1)[-1].replace(".json", "")
            if _id:
                ice_src.setdefault(_id, []).append(_o["Key"])
    ice_ids = sorted(ice_src)
    ice_added = 0
    for _id in ice_ids:
        merged_key = f"{ICE_DIR}/{_id}.json"
        _have_merged = True
        try:
            s3.head_object(Bucket=BUCKET, Key=merged_key)
        except Exception:
            _have_merged = False
        _pairs = []
        _doc = None
        _read_keys = ([merged_key] if _have_merged
                       else ice_src.get(_id, []))
        for _rk in _read_keys:
            try:
                _d0 = sread(_rk)
            except Exception:
                continue
            _doc = _doc or _d0
            _pp = dict(_pairs)
            for _dt, _vv in extract_pairs(_d0):
                _pp.setdefault(_dt, _vv)
            _pairs = sorted(_pp.items())
        if not _pairs:
            continue
        _last = _pairs[-1][0] if _pairs else "1900-01-01"
        _stale = (datetime.now(timezone.utc) -
                   datetime.strptime(_last, "%Y-%m-%d")
                   .replace(tzinfo=timezone.utc)).days > 21
        if (not _have_merged) and len(ice_src.get(_id, [])) > 1:
            s3.put_object(Bucket=BUCKET, Key=merged_key,
                           Body=json.dumps(
                               {"id": _id, "source": "multi-source merge",
                                 "banked_at": now,
                                 "observations":
                                     [{"date": d0, "value": v0}
                                      for d0, v0 in _pairs]},
                               separators=(",", ":")).encode(),
                           ContentType="application/json")
            _have_merged = True
        if _stale and fred_key:
            try:
                import urllib.request as _ur
                _u = ("https://api.stlouisfed.org/fred/series/observations"
                      f"?series_id={_id}&api_key={fred_key}"
                      f"&file_type=json&observation_start={_last}")
                _rq = _ur.Request(_u, headers={
                    "User-Agent": "JustHodl raafouis@gmail.com"})
                with _ur.urlopen(_rq, timeout=45) as _r2:
                    _obs = json.loads(_r2.read()).get("observations") or []
                _newp = dict(_pairs)
                for _o in _obs:
                    try:
                        _newp[_o["date"]] = float(_o["value"])
                    except Exception:
                        pass
                _pairs = sorted(_newp.items())
                s3.put_object(Bucket=BUCKET, Key=merged_key,
                               Body=json.dumps(
                                   {"id": _id, "source":
                                     "archived-fred + FRED API self-heal",
                                     "banked_at": now,
                                     "observations":
                                         [{"date": d0, "value": v0}
                                          for d0, v0 in _pairs]},
                                   separators=(",", ":")).encode(),
                               ContentType="application/json")
            except Exception:
                pass
        _nm = ""
        if isinstance(_doc, dict):
            _nm = str(_doc.get("title") or _doc.get("name") or "")[:150]
        scope.append({"mnemonic": _id,
                       "name": _nm or f"ICE BofA {_id} (OAS/yield, FRED)",
                       "family": "ICEBOFA",
                       "s3_key": (merged_key if (_have_merged or _stale)
                                   else ice_src[_id][0]),
                       "api_url": f"https://fred.stlouisfed.org/series/{_id}",
                       "tier": 2})
        ice_added += 1

    # v1.8: FRED repo & SOFR complex -- ids DISCOVERED, not hardcoded:
    # the 'repurchase agreements' tag inventory + the SOFR family, each
    # banked permanently on first touch (FRED_Repo_Complex/{ID}.json)
    # and tail-refreshed when stale >3d. Server-side buckets for the
    # page: SOFR / Fed RRP ops / Fed repo ops / balances / other.
    FREPO_DIR = "data/warm/fred-scoped/FRED_Repo_Complex"
    have_ids = {e["mnemonic"] for e in scope}
    frepo_meta = {}
    if fred_key:
        try:
            import urllib.request as _ur
            import urllib.parse as _up
            for _off in range(0, 1000, 500):
                _u = ("https://api.stlouisfed.org/fred/tags/series?"
                      + _up.urlencode({"tag_names":
                                        "repurchase agreements",
                                        "api_key": fred_key,
                                        "file_type": "json",
                                        "limit": 500, "offset": _off}))
                with _ur.urlopen(_ur.Request(_u, headers={
                        "User-Agent": "JustHodl raafouis@gmail.com"}),
                        timeout=45) as _r:
                    _js = json.loads(_r.read())
                for _sr in _js.get("seriess") or []:
                    frepo_meta[_sr["id"]] = str(
                        _sr.get("title") or "")[:150]
                if len(_js.get("seriess") or []) < 500:
                    break
            _u2 = ("https://api.stlouisfed.org/fred/series/search?"
                   + _up.urlencode({"search_text": "SOFR",
                                     "api_key": fred_key,
                                     "file_type": "json", "limit": 60}))
            with _ur.urlopen(_ur.Request(_u2, headers={
                    "User-Agent": "JustHodl raafouis@gmail.com"}),
                    timeout=45) as _r:
                for _sr in json.loads(_r.read()).get("seriess") or []:
                    if _sr["id"].startswith("SOFR"):
                        frepo_meta.setdefault(_sr["id"], str(
                            _sr.get("title") or "")[:150])
        except Exception as _e:
            print("[repo] frepo discovery failed:", type(_e).__name__)

    def _bucket(fid, title):
        t = (title or "").lower()
        if fid.startswith("SOFR"):
            return "SOFR"
        if fid.startswith("RRP") or "reverse repurchase" in t:
            return "Fed reverse repo (ON RRP)"
        if fid.startswith("RP") or ("repurchase" in t and "reverse"
                                      not in t and "operation" in t):
            return "Fed repo ops (incl SRF)"
        if any(w in t for w in ("liabilit", "assets", "h.4.1", "held")):
            return "Balance-sheet lines"
        return "Other repo series"

    CORE = ["SOFR", "SOFR30DAYAVG", "SOFR90DAYAVG", "SOFR180DAYAVG",
             "SOFRINDEX", "RRPONTSYD", "RRPONTSYAWARD", "RPONTSYD",
             "RPONAGYD", "RPONMBSD", "WLRRAL", "WORAL", "IORB",
             "WREPOFOR", "WLRRAFOIAL"]
    if fred_key:
        try:
            import urllib.request as _ur
            for _cid in CORE:
                if _cid in frepo_meta:
                    continue
                try:
                    with _ur.urlopen(_ur.Request(
                            "https://api.stlouisfed.org/fred/series?"
                            f"series_id={_cid}&api_key={fred_key}"
                            "&file_type=json",
                            headers={"User-Agent":
                                      "JustHodl raafouis@gmail.com"}),
                            timeout=30) as _r:
                        _ss = json.loads(_r.read()).get("seriess") or []
                    if _ss:
                        frepo_meta[_cid] = str(
                            _ss[0].get("title") or "")[:150]
                except Exception:
                    pass
        except Exception:
            pass
    _order = [k for k in CORE if k in frepo_meta] +         sorted(k for k in frepo_meta if k not in CORE)
    frepo_added = 0
    for fid in _order:
        title = frepo_meta[fid]
        if fid in have_ids or time.time() - t0 > 600:
            continue
        wkey = f"{FREPO_DIR}/{fid}.json"
        pairs = []
        stale = True
        try:
            _d = sread(wkey)
            pairs = extract_pairs(_d)
            if pairs:
                stale = (datetime.now(timezone.utc) -
                          datetime.strptime(pairs[-1][0], "%Y-%m-%d")
                          .replace(tzinfo=timezone.utc)).days > 3
        except Exception:
            pass
        if stale and fred_key:
            try:
                import urllib.request as _ur
                _st = pairs[-1][0] if pairs else "1970-01-01"
                _u = ("https://api.stlouisfed.org/fred/series/"
                      f"observations?series_id={fid}&api_key={fred_key}"
                      f"&file_type=json&observation_start={_st}")
                with _ur.urlopen(_ur.Request(_u, headers={
                        "User-Agent": "JustHodl raafouis@gmail.com"}),
                        timeout=45) as _r:
                    _obs = json.loads(_r.read()).get(
                        "observations") or []
                _pp = dict(pairs)
                for _o in _obs:
                    try:
                        _pp[_o["date"]] = float(_o["value"])
                    except Exception:
                        pass
                if _pp:
                    pairs = sorted(_pp.items())
                    s3.put_object(Bucket=BUCKET, Key=wkey,
                                   Body=json.dumps(
                                       {"id": fid, "title": title,
                                         "source": "FRED tag discovery",
                                         "banked_at": now,
                                         "observations":
                                             [{"date": d0, "value": v0}
                                              for d0, v0 in pairs]},
                                       separators=(",", ":")).encode(),
                                   ContentType="application/json")
            except Exception:
                pass
        if not pairs:
            continue
        scope.append({"mnemonic": fid, "name": title or f"FRED {fid}",
                       "family": "FREDREPO", "bucket": _bucket(fid, title),
                       "s3_key": wkey,
                       "api_url": f"https://fred.stlouisfed.org/series/{fid}",
                       "tier": 1 if fid in ("SOFR", "RRPONTSYD") else 2})
        frepo_added += 1

    # v2.0-A: surface already-banked OFR complexes -- MMF repo
    # counterparties (dataset mmf) + FICC sponsored repo (HFM)
    for _mm in ("MMF-MMF_RP_wFICC-M", "MMF-MMF_RP_wFR-M",
                 "MMF-MMF_RP_wDFI-M", "MMF-MMF_RP_wFFI-M",
                 "MMF-MMF_RP_wOCP-M"):
        _k = f"data/warm/ofr/series/{_mm}.json.gz"
        try:
            s3.head_object(Bucket=BUCKET, Key=_k)
            scope.append({"mnemonic": _mm, "name": "",
                           "family": "MMFRP", "s3_key": _k, "tier": 2,
                           "api_url": "https://data.financialresearch.gov"
                                       f"/v1/series/full?mnemonic={_mm}"})
        except Exception:
            pass
    for _fs, _fn in (("FICC-SPONSORED_REPO_VOL",
                       "FICC Sponsored Service repo volume ($B)"),
                      ("FICC-SPONSORED_REVREPO_VOL",
                       "FICC Sponsored Service reverse repo volume ($B)")):
        _k = f"data/warm/ofr-hfm/series/{_fs}.json.gz"
        try:
            s3.head_object(Bucket=BUCKET, Key=_k)
            scope.append({"mnemonic": _fs, "name": _fn,
                           "family": "SPONSORED", "s3_key": _k,
                           "tier": 1,
                           "api_url": "https://data.financialresearch.gov"
                                       f"/hf/v1/series/full?mnemonic={_fs}"})
        except Exception:
            pass

    # v2.0-B: NY Fed secured-rate PERCENTILES (SOFR/TGCR/BGCR) --
    # banked + self-extending; P25/75/99 become first-class rows via
    # pairs_inline (no doc round-trip needed)
    import urllib.request as _ur2
    RATES_DIR = "data/warm/nyfed-markets/rates"
    PCT_FIELDS = {"25": ["percentPercentile25", "percentilePercent25"],
                   "75": ["percentPercentile75", "percentilePercent75"],
                   "99": ["percentPercentile99", "percentilePercent99"],
                   "1": ["percentPercentile1", "percentilePercent1"]}
    pct_pairs = {}
    for _rt in ("sofr", "tgcr", "bgcr"):
        _wk = f"{RATES_DIR}/{_rt}.json.gz"
        _rows = {}
        try:
            _doc = sread(_wk)
            for _r0 in _doc.get("rows") or []:
                _rows[_r0.get("effectiveDate", "")[:10]] = _r0
        except Exception:
            pass
        _last = max(_rows) if _rows else "2014-01-01"
        _stale = (datetime.now(timezone.utc) -
                   datetime.strptime(_last, "%Y-%m-%d")
                   .replace(tzinfo=timezone.utc)).days > 1
        if _stale:
            try:
                _u = ("https://markets.newyorkfed.org/api/rates/secured/"
                      f"{_rt}/search.json?startDate={_last}"
                      f"&endDate={now[:10]}")
                with _ur2.urlopen(_ur2.Request(_u, headers={
                        "User-Agent": "JustHodl raafouis@gmail.com"}),
                        timeout=60) as _r:
                    _js = json.loads(_r.read())
                for _r0 in (_js.get("refRates") or []):
                    _d = str(_r0.get("effectiveDate", ""))[:10]
                    if _d:
                        _rows[_d] = _r0
                s3.put_object(Bucket=BUCKET, Key=_wk,
                               Body=gzip.compress(json.dumps(
                                   {"rate": _rt, "banked_at": now,
                                     "rows": [
                                         _rows[k] for k in sorted(_rows)]},
                                   separators=(",", ":")).encode()),
                               ContentType="application/json",
                               ContentEncoding="gzip")
            except Exception as _e:
                print(f"[repo] rates {_rt}: {type(_e).__name__}")
        for _pl, _cands in PCT_FIELDS.items():
            _pp = {}
            for _d, _r0 in _rows.items():
                for _c in _cands:
                    _v = _r0.get(_c)
                    if _v is not None:
                        try:
                            _pp[_d] = float(_v)
                        except Exception:
                            pass
                        break
            if len(_pp) > 50:
                _mn = f"{_rt.upper()}_P{_pl}"
                pct_pairs[_mn] = sorted(_pp.items())
                scope.append({"mnemonic": _mn,
                               "name": f"NY Fed {_rt.upper()} "
                                        f"{_pl}th percentile rate",
                               "family": "RATESPCT", "tier": 2,
                               "s3_key": _wk,
                               "pairs_inline": pct_pairs[_mn],
                               "api_url":
                                   "https://markets.newyorkfed.org/api/"
                                   f"rates/secured/{_rt}/search.json"})

    # v2.0-C: Standing Repo Facility take-up + min bid rate from rp ops
    srf_take = {}
    srf_rate = {}
    _srf_wk = "data/warm/nyfed-markets/rp-repo-history.json.gz"
    _ops = {}
    try:
        _doc = sread(_srf_wk)
        for _r0 in _doc.get("rows") or []:
            _ops[str(_r0.get("operationDate", ""))[:10] +
                  "|" + str(_r0.get("operationId", ""))] = _r0
    except Exception:
        pass
    _lastd = max((k.split("|")[0] for k in _ops), default="2021-01-01")
    try:
        _u = ("https://markets.newyorkfed.org/api/rp/results/"
              f"search.json?startDate={_lastd}&endDate={now[:10]}")
        with _ur2.urlopen(_ur2.Request(_u, headers={
                "User-Agent": "JustHodl raafouis@gmail.com"}),
                timeout=90) as _r:
            _js = json.loads(_r.read())
        for _r0 in (_js.get("repo", {}).get("operations")
                     or _js.get("operations") or []):
            _ot = str(_r0.get("operationType", ""))
            if "Repo" in _ot and "Reverse" not in _ot:
                _ops[str(_r0.get("operationDate", ""))[:10] + "|" +
                      str(_r0.get("operationId", ""))] = _r0
        s3.put_object(Bucket=BUCKET, Key=_srf_wk,
                       Body=gzip.compress(json.dumps(
                           {"banked_at": now,
                             "rows": list(_ops.values())},
                           separators=(",", ":")).encode()),
                       ContentType="application/json",
                       ContentEncoding="gzip")
    except Exception as _e:
        print(f"[repo] srf: {type(_e).__name__}")
    for _r0 in _ops.values():
        _d = str(_r0.get("operationDate", ""))[:10]
        try:
            _amt = float(_r0.get("totalAmtAccepted") or 0)
        except Exception:
            _amt = 0.0
        srf_take[_d] = srf_take.get(_d, 0.0) + _amt / 1e9
        for _rf in ("minimumBidRate", "percentMinimumBidRate",
                     "minBidRate"):
            if _r0.get(_rf) is not None:
                try:
                    srf_rate[_d] = float(_r0[_rf])
                except Exception:
                    pass
                break
        if _d not in srf_rate:
            for _dd in (_r0.get("details") or []):
                for _rf in ("minimumBidRate", "percentMinimumBidRate"):
                    if isinstance(_dd, dict) and _dd.get(_rf) is not None:
                        try:
                            srf_rate[_d] = float(_dd[_rf])
                        except Exception:
                            pass
    if len(srf_take) > 20:
        scope.append({"mnemonic": "SRF_TAKEUP",
                       "name": "Standing Repo Facility total accepted "
                                "($B/day, all repo ops)",
                       "family": "SRF", "tier": 1,
                       "s3_key": _srf_wk,
                       "pairs_inline": sorted(srf_take.items()),
                       "api_url": "https://markets.newyorkfed.org/api/rp/"
                                   "results/search.json"})
    if len(srf_rate) > 20:
        scope.append({"mnemonic": "SRF_MINRATE",
                       "name": "SRF minimum bid rate (%)",
                       "family": "SRF", "tier": 2, "s3_key": _srf_wk,
                       "pairs_inline": sorted(srf_rate.items()),
                       "api_url": "https://markets.newyorkfed.org/api/rp/"
                                   "results/search.json"})

    # v2.0-D: DTCC daily Treasury fails -- attempt, honest skip
    dtcc_status = "skipped"
    for _du in ("https://www.dtcc.com/charts/daily-total-us-treasury-"
                 "trade-fails.json",
                 "https://www.dtcc.com/-/media/Files/Downloads/Charts/"
                 "daily-treasury-fails.csv"):
        try:
            with _ur2.urlopen(_ur2.Request(_du, headers={
                    "User-Agent": "JustHodl raafouis@gmail.com"}),
                    timeout=30) as _r:
                _raw = _r.read()
            if len(_raw) > 500:
                s3.put_object(Bucket=BUCKET,
                               Key="data/warm/dtcc/daily-treasury-fails"
                                    + (".json" if _du.endswith("json")
                                       else ".csv"),
                               Body=_raw)
                dtcc_status = f"banked {len(_raw)}B from {_du[-40:]}"
                break
        except Exception:
            continue
    print(f"[repo] dtcc: {dtcc_status}")

    # v2.1: tri-party haircut monthly series (parsed by ops 4789)
    try:
        _hix = sread("data/warm/nyfed-research/haircuts-series/"
                      "_index.json").get("series") or []
    except Exception:
        _hix = []
    for _e in _hix:
        scope.append({"mnemonic": "HAIRCUT-" + _e["id"],
                       "name": _e["title"], "family": "HAIRCUT",
                       "bucket": _e["title"].split("—")[-1].strip()
                       if "—" in _e["title"] else "haircuts",
                       "s3_key": "data/warm/nyfed-research/"
                                  f"haircuts-series/{_e['id']}.json",
                       "api_url": "https://www.newyorkfed.org/data-and-statistics/data-visualization/tri-party-repo",
                       "tier": 2})

    # v2.2-EU: (a) ECB MMSR secured-market collateral usage by issuer
    # country (free API; maintenance-period aggregates) -- how much
    # O/N cash is raised on DE/IT/FR/ES collateral; (b) EU sovereign
    # 10Y yields (monthly OECD via FRED) feeding BTP/OAT/Bono-Bund
    # derived spreads. Daily RFR (CME entitlement) + SFTR weekly =
    # source-recon flagged, not faked.
    MMSR = {"DE": "German", "IT": "Italian", "FR": "French",
             "ES": "Spanish"}
    for _cc, _nm in MMSR.items():
        _wkey = f"data/warm/ecb-mmsr/{_cc.lower()}-on-borrow-turnover.json"
        _pairs2 = []
        _stale = True
        try:
            _d = sread(_wkey)
            _pairs2 = extract_pairs(_d)
            if _pairs2:
                _stale = (datetime.now(timezone.utc) -
                           datetime.strptime(_pairs2[-1][0], "%Y-%m-%d")
                           .replace(tzinfo=timezone.utc)).days > 20
        except Exception:
            pass
        if _stale:
            try:
                import urllib.request as _ur
                _u = ("https://data-api.ecb.europa.eu/service/data/MMSR/"
                      f"B.U2._X.{_cc}.S1ZV._Z.T.BO.AT._X.MA._Z._Z.EUR._Z"
                      "?format=csvdata")
                with _ur.urlopen(_ur.Request(_u, headers={
                        "User-Agent": "JustHodl raafouis@gmail.com"}),
                        timeout=45) as _r:
                    _txt = _r.read().decode("utf-8", "replace")
                import csv as _csv
                import io as _io
                _rd = _csv.DictReader(_io.StringIO(_txt))
                _pp = dict(_pairs2)
                for _row in _rd:
                    _dt = (_row.get("TIME_PERIOD") or "")[:10]
                    try:
                        _pp[_dt] = float(_row.get("OBS_VALUE"))
                    except Exception:
                        pass
                if _pp:
                    _pairs2 = sorted(_pp.items())
                    s3.put_object(Bucket=BUCKET, Key=_wkey,
                        Body=json.dumps({"id": f"MMSR-{_cc}-ON-BO-AT",
                            "source": "ECB data-api MMSR",
                            "banked_at": now,
                            "observations": [{"date": d0, "value": v0}
                                              for d0, v0 in _pairs2]},
                            separators=(",", ":")).encode(),
                        ContentType="application/json")
            except Exception:
                pass
        if _pairs2:
            scope.append({"mnemonic": f"MMSR-{_cc}-ON-BORROW-TURNOVER",
                "name": (f"ECB MMSR: O/N cash raised on {_nm} sovereign "
                          "collateral (avg daily turnover, EUR, "
                          "maintenance-period)"),
                "family": "EUROREPO", "s3_key": _wkey,
                "api_url": ("https://data.ecb.europa.eu/data/datasets/"
                             "MMSR"),
                "tier": 2})
    EUY = {"IRLTLT01DEM156N": "Germany 10Y (monthly, OECD)",
            "IRLTLT01ITM156N": "Italy 10Y (monthly, OECD)",
            "IRLTLT01FRM156N": "France 10Y (monthly, OECD)",
            "IRLTLT01ESM156N": "Spain 10Y (monthly, OECD)"}
    for _fid, _t in EUY.items():
        _wk = f"data/warm/fred-scoped/EU_Sovereign_Yields/{_fid}.json"
        _ok = False
        try:
            s3.head_object(Bucket=BUCKET, Key=_wk)
            _ok = True
        except Exception:
            if fred_key:
                try:
                    import urllib.request as _ur
                    _u = ("https://api.stlouisfed.org/fred/series/"
                          f"observations?series_id={_fid}"
                          f"&api_key={fred_key}&file_type=json"
                          "&observation_start=1970-01-01")
                    with _ur.urlopen(_ur.Request(_u, headers={
                            "User-Agent":
                                "JustHodl raafouis@gmail.com"}),
                            timeout=45) as _r:
                        _obs = json.loads(_r.read()).get(
                            "observations") or []
                    if _obs:
                        s3.put_object(Bucket=BUCKET, Key=_wk,
                            Body=json.dumps({"id": _fid,
                                "banked_at": now,
                                "observations": _obs},
                                separators=(",", ":")).encode(),
                            ContentType="application/json")
                        _ok = True
                except Exception:
                    pass
        if _ok:
            scope.append({"mnemonic": _fid, "name": _t,
                "family": "EUYIELD", "s3_key": _wk,
                "api_url": f"https://fred.stlouisfed.org/series/{_fid}",
                "tier": 2})

    # v2.5: DAILY EU sovereign 10Y via Trading Economics, with
    # self-reporting diagnostics into repo.json's diag dict.
    diag = {}
    te_key = os.environ.get('TE_API_KEY', '')
    diag['te_key_present'] = bool(te_key)
    TECN = {'germany': 'DE10Y_TE', 'italy': 'IT10Y_TE',
             'france': 'FR10Y_TE', 'spain': 'ES10Y_TE'}
    for _cty, _tid in TECN.items():
        _wk = 'data/warm/te-yields/' + _cty + '-10y.json'
        _pp = []
        _stale = True
        try:
            _pp = extract_pairs(sread(_wk))
            if _pp:
                _stale = (datetime.now(timezone.utc) -
                           datetime.strptime(_pp[-1][0], '%Y-%m-%d')
                           .replace(tzinfo=timezone.utc)).days > 3
        except Exception:
            pass
        if _stale and te_key:
            try:
                import urllib.request as _ur
                import urllib.parse as _up
                _u = ('https://api.tradingeconomics.com/historical/'
                      'country/' + _up.quote(_cty) + '/indicator/'
                      + _up.quote('government bond 10y')
                      + '?c=' + te_key + '&f=json')
                with _ur.urlopen(_ur.Request(_u, headers={
                        'User-Agent': 'Mozilla/5.0'}),
                        timeout=50) as _r:
                    _body = _r.read()
                _js = json.loads(_body)
                _d2 = dict(_pp)
                _added = 0
                for _row in (_js if isinstance(_js, list) else []):
                    _dt = str(_row.get('DateTime') or '')[:10]
                    _v = _row.get('Value')
                    if len(_dt) == 10 and _v is not None:
                        try:
                            _d2[_dt] = float(_v)
                            _added += 1
                        except Exception:
                            pass
                if _d2:
                    _pp = sorted(_d2.items())
                    s3.put_object(Bucket=BUCKET, Key=_wk,
                        Body=json.dumps({'id': _tid,
                            'source': 'TradingEconomics historical',
                            'banked_at': now,
                            'observations': [{'date': a, 'value': b}
                                              for a, b in _pp]},
                            separators=(',', ':')).encode(),
                        ContentType='application/json')
                diag.setdefault('te', {})[_cty] = (
                    'ok:' + str(len(_pp)) + '/+' + str(_added)
                    if _pp else 'empty:' + _body[:60].decode(
                        'utf-8', 'replace'))
            except Exception as _te_e:
                diag.setdefault('te', {})[_cty] = (
                    type(_te_e).__name__ + ':' + str(_te_e)[:60])
        elif _pp:
            diag.setdefault('te', {})[_cty] = 'warm:' + str(len(_pp))
        if _pp:
            scope.append({'mnemonic': _tid,
                'name': _cty.title() + ' 10Y yield (daily, TE)',
                'family': 'EUYIELD', 'bucket': 'Daily (TE)',
                's3_key': _wk,
                'api_url': 'https://tradingeconomics.com/bonds',
                'tier': 2})

    def _xlsx_sheets(raw):
        import zipfile as _zf
        import io as _io
        import re as _re
        z = _zf.ZipFile(_io.BytesIO(raw))
        shared = []
        if 'xl/sharedStrings.xml' in z.namelist():
            sx = z.read('xl/sharedStrings.xml').decode('utf-8',
                                                         'replace')
            shared = [_re.sub(r'<[^>]+>', '', m)
                       for m in _re.findall(r'<si>(.*?)</si>', sx,
                                             _re.S)]
        wbxml = z.read('xl/workbook.xml').decode('utf-8', 'replace')
        names = _re.findall(r'<sheet[^>]*name="([^"]+)"', wbxml)
        out_sheets = {}
        snames = sorted(n for n in z.namelist()
                         if _re.match(r'xl/worksheets/sheet\d+\.xml',
                                       n))

        def _colidx(letters):
            n = 0
            for ch in letters:
                n = n * 26 + (ord(ch) - 64)
            return n - 1

        for _si, sheet in enumerate(snames):
            xml = z.read(sheet).decode('utf-8', 'replace')
            rows = []
            for rm in _re.findall(r'<row[^>]*>(.*?)</row>', xml,
                                    _re.S):
                cellmap = {}
                pat = (r'<c\s+[^>]*?r="([A-Z]+)\d+"[^>]*?'
                        r'(?:t="(\w+)")?[^>]*>'
                        r'(?:<is><t[^>]*>(.*?)</t></is>|'
                        r'<v>(.*?)</v>)?</c>')
                for cm in _re.finditer(pat, rm, _re.S):
                    ci = _colidx(cm.group(1))
                    t, inl, v = (cm.group(2), cm.group(3),
                                  cm.group(4))
                    if t == 's' and v is not None:
                        try:
                            cellmap[ci] = shared[int(v)]
                        except Exception:
                            cellmap[ci] = ''
                    else:
                        cellmap[ci] = (inl if inl is not None
                                        else (v or ''))
                width = max(cellmap) + 1 if cellmap else 0
                rows.append([cellmap.get(i) for i in range(width)])
            out_sheets[names[_si] if _si < len(names)
                        else sheet] = rows
        return out_sheets

    try:
        import urllib.request as _ur
        import re as _re
        _pg = _ur.urlopen(_ur.Request(
            'https://www.icmagroup.org/market-practice-and-regulatory-'
            'policy/repo-and-collateral-markets/market-data/'
            'sftr-public-data/',
            headers={'User-Agent': 'Mozilla/5.0'}), timeout=45
            ).read().decode('utf-8', 'replace')
        diag['sftr'] = {'stage': 'page'}
        _links = sorted(set(_re.findall(
            r'href="([^"]*SFTR-Public-Data-(?:EU|UK)[^"]*\.xlsx)"',
            _pg)))
        diag['sftr']['links'] = len(_links)
        for _lk in _links[-2:]:
            _full = _lk if _lk.startswith('http') else \
                'https://www.icmagroup.org' + _lk
            _fn = _full.rsplit('/', 1)[-1]
            _rk = 'data/warm/icma-sftr/files/' + _fn
            try:
                _raw = s3.get_object(Bucket=BUCKET,
                                       Key=_rk)['Body'].read()
            except Exception:
                _raw = _ur.urlopen(_ur.Request(_full, headers={
                    'User-Agent': 'Mozilla/5.0'}), timeout=60).read()
                s3.put_object(Bucket=BUCKET, Key=_rk, Body=_raw,
                    ContentType='application/vnd.openxmlformats-'
                                 'officedocument.spreadsheetml.sheet')
            diag['sftr']['files_new'] = diag['sftr'].get('files_new', 0) + 1
            _region = 'EU' if '-EU-' in _fn else 'UK'
            _m = _re.search(r'we-(\d{1,2})-(\w+)-(\d{4})', _fn)
            _months = {m0: i for i, m0 in enumerate(
                ['January', 'February', 'March', 'April', 'May',
                  'June', 'July', 'August', 'September', 'October',
                  'November', 'December'], 1)}
            _wdate = ('%s-%02d-%02d' % (_m.group(3),
                       _months.get(_m.group(2), 1),
                       int(_m.group(1)))) if _m else now[:10]
            for _shname, _rows in _xlsx_sheets(_raw).items():
                _kind = ('newt' if 'NEWT' in _shname.upper()
                          else 'outstanding' if 'OUTSTANDING'
                          in _shname.upper() else None)
                if not _kind:
                    continue
                _section = ''
                for _row in _rows:
                    _c = list(_row) + [None] * 12
                    if isinstance(_c[1], str) and _c[1].strip():
                        _section = _c[1].strip()
                    _item = (_c[3].strip() if isinstance(_c[3], str)
                              and _c[3].strip() else None)
                    if not _item:
                        continue
                    for _ci, _stat in ((6, 'cash-value-eur-mn'),
                                         (8, 'n-transactions'),
                                         (10, 'collateral-mv-eur-mn')):
                        try:
                            _v = float(str(_c[_ci]).replace(',', ''))
                        except Exception:
                            continue
                        _slug = _re.sub(r'[^a-z0-9]+', '-',
                                         (_kind + '-' + _section + '-'
                                           + _item + '-' + _stat)
                                         .lower()).strip('-')[:80]
                        _sk = ('data/warm/icma-sftr/'
                                + _region.lower() + '/' + _slug
                                + '.json')
                        try:
                            _doc = sread(_sk)
                        except Exception:
                            _doc = {'id': 'SFTR-' + _region + '-'
                                     + _slug,
                                     'title': ('SFTR ' + _region + ' '
                                       + _kind.title() + ': '
                                       + _section + ' / ' + _item
                                       + ' -- '
                                       + _stat.replace('-', ' '))[:150],
                                     'observations': []}
                        if not any(o.get('date') == _wdate for o
                                    in _doc['observations']):
                            _doc['observations'].append(
                                {'date': _wdate, 'value': _v})
                            _doc['observations'].sort(
                                key=lambda o: o['date'])
                            diag['sftr']['series_put'] = diag['sftr'].get('series_put', 0) + 1
                            s3.put_object(Bucket=BUCKET, Key=_sk,
                                Body=json.dumps(
                                    _doc, separators=(',', ':')
                                    ).encode(),
                                ContentType='application/json')
        # scope all banked sftr series
        for _region in ("eu", "uk"):
            _tok = None
            while True:
                _kw = dict(Bucket=BUCKET,
                            Prefix=f"data/warm/icma-sftr/{_region}/",
                            MaxKeys=1000)
                if _tok:
                    _kw["ContinuationToken"] = _tok
                _r3 = s3.list_objects_v2(**_kw)
                for _o in _r3.get("Contents") or []:
                    _sid = _o["Key"].rsplit("/", 1)[-1
                            ].replace(".json", "")
                    scope.append({"mnemonic":
                                    f"SFTR-{_region.upper()}-{_sid}",
                        "name": f"SFTR {_region.upper()} weekly: "
                                 + _sid.replace("-", " ")[:110],
                        "family": "SFTR", "s3_key": _o["Key"],
                        "api_url": "https://www.icmagroup.org/market-"
                                    "practice-and-regulatory-policy/"
                                    "repo-and-collateral-markets/"
                                    "market-data/sftr-public-data/",
                        "tier": 2})
                _tok = _r3.get("NextContinuationToken")
                if not _tok:
                    break
    except Exception as _e:
        diag['sftr_error'] = type(_e).__name__ + ':' + str(_e)[:80]
        print("[repo] sftr block:", type(_e).__name__, str(_e)[:80])

    # v2.6: DTCC/FICC daily GROSS Treasury settlement fails --
    # public CSV, ~1yr rolling window; merge-on-write into warm docs
    # so OUR bank becomes the permanent history (ops 4802 backfills
    # via Wayback snapshots).
    diag['dtcc'] = {}
    try:
        import urllib.request as _ur
        _txt = _ur.urlopen(_ur.Request(
            'https://www.dtcc.com/data/failsdata.csv',
            headers={'User-Agent': 'Mozilla/5.0'}), timeout=45
            ).read().decode('utf-8', 'replace')
        _tsy, _agy = {}, {}
        for _ln in _txt.splitlines():
            if _ln.startswith('TRAILER') or _ln.startswith('Date'):
                continue
            _pc = _ln.split(',')
            if len(_pc) < 3:
                continue
            import re as _re3
            _dm = _re3.match(r'(\d{2})/(\d{2})/(\d{4})', _pc[0])
            if not _dm:
                continue
            _ds = (_dm.group(3) + '-' + _dm.group(1) + '-'
                    + _dm.group(2))
            try:
                _tsy[_ds] = float(_pc[1])
                _agy[_ds] = float(_pc[2])
            except Exception:
                continue
        diag['dtcc']['live_rows'] = len(_tsy)
        for _sid, _newd, _ttl in (
                ('treasury', _tsy,
                  'FICC daily GROSS Treasury settlement fails (USD; '
                  'FtD+FtR aggregated, can double-count -- NY Fed '
                  'caveat)'),
                ('agency', _agy,
                  'FICC daily GROSS Agency settlement fails (USD)')):
            _wk = 'data/warm/dtcc-fails/' + _sid + '.json'
            try:
                _doc = sread(_wk)
                _m2 = {o['date']: o['value']
                        for o in _doc.get('observations', [])}
            except Exception:
                _m2 = {}
            _before = len(_m2)
            _m2.update(_newd)
            if len(_m2) > _before or _before == 0:
                s3.put_object(Bucket=BUCKET, Key=_wk,
                    Body=json.dumps({'id': 'DTCC-' + _sid.upper()
                                       + '-FAILS',
                        'title': _ttl, 'banked_at': now,
                        'source': 'dtcc.com/data/failsdata.csv',
                        'observations': [{'date': a, 'value': b}
                            for a, b in sorted(_m2.items())]},
                        separators=(',', ':')).encode(),
                    ContentType='application/json')
            diag['dtcc'][_sid] = len(_m2)
            scope.append({'mnemonic': 'DTCC-' + _sid.upper()
                            + '-FAILS',
                'name': _ttl, 'family': 'DTCCFAILS',
                's3_key': _wk,
                'api_url': 'https://www.dtcc.com/charts/daily-total-'
                            'us-treasury-trade-fails',
                'tier': 1 if _sid == 'treasury' else 2})
    except Exception as _de:
        diag['dtcc']['error'] = (type(_de).__name__ + ':'
                                   + str(_de)[:80])

    values = {}
    rows = []
    skipped = []
    for e in scope:
        if (time.time() - t0) > 780:
            skipped.append({"id": e["mnemonic"], "reason": "time_cap"})
            continue
        sid = safe_id(e["mnemonic"])
        try:
            if e.get("pairs_inline"):
                pairs = list(e["pairs_inline"])
            else:
                doc = sread(e["s3_key"])
                pairs = extract_pairs(doc)
            sm = _spl.get(e["mnemonic"])
            if sm and pairs:
                try:
                    sp = sread("data/warm/nyfed-markets/pd-spliced/"
                                f"{sm['keyid']}.json.gz")
                    f = 1.0 / (sm.get("factor") or 1.0)
                    first_have = pairs[0][0]
                    pre = [(d0, v0 * f) for d0, v0 in
                            zip(sp.get("dates") or [],
                                sp.get("values") or [])
                            if d0 < first_have]
                    if pre:
                        pairs = sorted(pre) + pairs
                except Exception:
                    pass
            if not pairs:
                skipped.append({"id": e["mnemonic"],
                                 "reason": "no_pairs_extracted"})
                continue
            last_d, last_v, chg = changes(pairs)
            values[e["mnemonic"]] = (last_d, last_v, pairs)
            s3.put_object(
                Bucket=BUCKET, Key=f"data/repo-history/{sid}.json",
                Body=json.dumps({"id": e["mnemonic"],
                                  "label": e.get("name") or e["mnemonic"],
                                  "dates": [p[0] for p in pairs],
                                  "values": [p[1] for p in pairs]},
                                 separators=(",", ":")).encode(),
                ContentType="application/json",
                CacheControl="public, max-age=1800")
            rows.append({"id": e["mnemonic"], "sid": sid,
                          "label": names.get(e["mnemonic"]) or e.get("name") or "",
                          "tenor": e.get("tenor"),
                          "bucket": e.get("bucket"),
                          "collateral": e.get("collateral"),
                          "group": group_of(e),
                          "tier": e.get("tier", 2),
                          "n_obs": len(pairs),
                          "first": pairs[0][0], "last": last_d,
                          "last_value": last_v, "chg": chg})
        except Exception as ex:
            skipped.append({"id": e["mnemonic"],
                             "reason": f"{type(ex).__name__}: {str(ex)[:60]}"})

    # v2.0-E: DERIVED stress indicators -- first-class rows with full
    # computed histories (formula in the name), from component pairs
    def _pairs(mn):
        return dict(values.get(mn, (None, None, []))[2] or [])

    def _emit(mn, name, pairs, tier=1, unit=""):
        if len(pairs) < 30:
            return
        pl = sorted(pairs.items())
        last_d, last_v, chg = changes(pl)
        sid = safe_id(mn)
        s3.put_object(Bucket=BUCKET,
                       Key=f"data/repo-history/{sid}.json",
                       Body=json.dumps({"id": mn, "label": name,
                                          "dates": [x[0] for x in pl],
                                          "values": [x[1] for x in pl]},
                                         separators=(",", ":")).encode(),
                       ContentType="application/json",
                       CacheControl="public, max-age=1800")
        values[mn] = (last_d, last_v, pl)
        rows.append({"id": mn, "sid": sid, "label": name,
                      "group": "Derived stress indicators",
                      "tier": tier, "n_obs": len(pl),
                      "first": pl[0][0], "last": last_d,
                      "last_value": last_v, "chg": chg})

    def _spread(a, b, scale=100.0):
        pa, pb = _pairs(a), _pairs(b)
        return {d: (pa[d] - pb[d]) * scale for d in pa if d in pb}

    _emit("D_SOFR_IORB", "SOFR − IORB (bps): GC repo vs reserves floor",
           _spread("SOFR", "IORB"))
    _emit("D_TGCR_IORB", "TGCR − IORB (bps)", _spread("FNYR-TGCR-A",
                                                         "IORB"))
    _emit("D_DVP_TRI", "DVP − Tri-party rate (bps): cleared-bilateral "
           "premium", _spread("REPO-DVP_AR_TOT-P", "REPO-TRIV1_AR_TOT-P"))
    _emit("D_GCF_TRI", "GCF − Tri-party rate (bps)",
           _spread("REPO-GCF_AR_TOT-P", "REPO-TRIV1_AR_TOT-P"))
    _emit("D_DVP_SOFR", "DVP − SOFR (bps): specials/scarcity tilt",
           _spread("REPO-DVP_AR_TOT-P", "SOFR"))
    _emit("D_TRI_TGCR", "Tri-party(TRIV1) − TGCR (bps)",
           _spread("REPO-TRIV1_AR_TOT-P", "FNYR-TGCR-A"))
    _p25, _p75 = _pairs("SOFR_P25"), _pairs("SOFR_P75")
    _emit("D_SOFR_P75_P25", "SOFR P75 − P25 (bps): repo-rate "
           "dispersion, tails widen before the median moves",
           {d: (_p75[d] - _p25[d]) * 100 for d in _p75 if d in _p25})
    _emit("D_BTP_BUND_D", "BTP − Bund 10Y DAILY (bps, TE)",
           _spread("IT10Y_TE", "DE10Y_TE"))
    _emit("D_OAT_BUND_D", "OAT − Bund 10Y DAILY (bps, TE)",
           _spread("FR10Y_TE", "DE10Y_TE"))
    _emit("D_BONO_BUND_D", "Bono − Bund 10Y DAILY (bps, TE)",
           _spread("ES10Y_TE", "DE10Y_TE"))
    _emit("D_BTP_BUND", "BTP − Bund 10Y (bps, monthly OECD; daily "
           "feed = MTS/RFR, source-recon flagged)",
           _spread("IRLTLT01ITM156N", "IRLTLT01DEM156N"))
    _emit("D_OAT_BUND", "OAT − Bund 10Y (bps, monthly OECD)",
           _spread("IRLTLT01FRM156N", "IRLTLT01DEM156N"))
    _emit("D_BONO_BUND", "Bono − Bund 10Y (bps, monthly OECD)",
           _spread("IRLTLT01ESM156N", "IRLTLT01DEM156N"))
    _tvoo, _tvtot = _pairs("REPO-TRIV1_TV_OO-P"),         _pairs("REPO-TRIV1_TV_TOT-P")
    _emit("D_ON_SHARE_TRI", "Overnight/Open share of tri-party volume "
           "(%): rollover-risk gauge",
           {d: _tvoo[d] / _tvtot[d] * 100 for d in _tvoo
             if d in _tvtot and _tvtot[d]})
    _ov = [_pairs(f"REPO-DVP_OV_{b}-P") for b in
            ("OO", "B27", "B830", "G30", "LE30")]
    _oo = _ov[0]
    _emit("D_ON_SHARE_DVP", "Overnight/Open share of DVP outstanding "
           "(%): rollover-risk gauge",
           {d: _oo[d] / sum(x.get(d, 0) for x in _ov) * 100
             for d in _oo if sum(x.get(d, 0) for x in _ov) > 0})
    _ft, _tvt = _pairs("NYPD-PD_AFtD_T-A"), _pairs("REPO-TRIV1_TV_T-P")
    _emit("D_FAILS_T_RATIO", "Treasury fails-to-deliver ÷ tri-party "
           "Treasury volume (%, weekly, venue-proxy ratio)",
           {d: _ft[d] / (_tvt[d] * 1e3) * 100 for d in _ft
             if d in _tvt and _tvt[d]}, tier=2)
    _rp = _pairs("NYPD-PD_RP_TOT-A") or _pairs("NYPD-PD_RP_T_TOT-A")
    _rr = _pairs("NYPD-PD_RRP_TOT-A") or _pairs("NYPD-PD_RRP_T_TOT-A")
    _emit("D_DEALER_NET", "Dealer repo − reverse repo ($M): net "
           "financing imbalance", {d: _rp[d] - _rr[d]
                                     for d in _rp if d in _rr})
    _emit("D_SOFR_SRF", "SOFR − SRF min bid rate (bps): market vs "
           "backstop; positive = knocking on the ceiling",
           _spread("SOFR", "SRF_MINRATE"))

    # ── barometer: labeled, transparent heuristic ────────────────────
    def latest(m):
        return values.get(m, (None, None, None))[1]

    def chg_of(m, tag):
        for r in rows:
            if r["id"] == m:
                return (r["chg"] or {}).get(tag)
        return None

    comps = []

    def add(name, raw, unit, z, note):
        if raw is None or z is None:
            comps.append({"name": name, "value": None, "unit": unit,
                           "z": None, "note": note + " (unavailable)"})
            return
        comps.append({"name": name, "value": round(raw, 3), "unit": unit,
                       "z": round(max(-3.0, min(3.0, z)), 2), "note": note})

    sofr, tgcr = latest("FNYR-SOFR-A"), latest("FNYR-TGCR-A")
    sp = (sofr - tgcr) * 100 if (sofr is not None and tgcr is not None) \
        else None
    add("SOFR−TGCR spread", sp, "bps",
         (sp / 3.0) if sp is not None else None,
         "bilateral-vs-triparty GC premium; wide = collateral pressure")
    dvp, tri = latest("REPO-DVP_AR_TOT-P"), latest("REPO-TRIV1_AR_TOT-P")
    sp2 = (dvp - tri) * 100 if (dvp is not None and tri is not None) else None
    add("DVP−Triparty rate", sp2, "bps",
         (sp2 / 4.0) if sp2 is not None else None,
         "cleared-bilateral premium over triparty")
    cord, tt = latest("REPO-TRIV1_AR_CORD-P"), latest("REPO-TRIV1_AR_T-P")
    sp3 = (cord - tt) * 100 if (cord is not None and tt is not None) else None
    add("Corp−Tsy collateral spread", sp3, "bps",
         ((sp3 - 8.0) / 6.0) if sp3 is not None else None,
         "collateral risk premium inside triparty")
    fm = chg_of("NYPD-PD_AFtD_TOT-A", "m")
    add("Total fails Δ30d", fm, "%",
         (fm / 40.0) if fm is not None else None,
         "settlement stress momentum")
    ftm = chg_of("NYPD-PD_AFtD_T-A", "m")
    add("Treasury fails Δ30d", ftm, "%",
         (ftm / 40.0) if ftm is not None else None,
         "UST-specific settlement stress")
    vm = chg_of("REPO-TRIV1_TV_OO-P", "m")
    add("Overnight triparty volume Δ30d", vm, "%",
         (-vm / 8.0) if vm is not None else None,
         "falling ON volume = funding withdrawal (sign inverted)")
    dy = chg_of("DTWEXBGS", "m")
    add("Broad dollar Δ30d", dy, "%",
         (dy / 2.0) if dy is not None else None,
         "dollar impulse tightens global funding")
    zs = [c["z"] for c in comps if c["z"] is not None]
    score = round(50 + 10 * (sum(zs) / len(zs)), 1) if zs else None
    label = (None if score is None else
              "CALM" if score < 45 else
              "NORMAL" if score < 55 else
              "TIGHTENING" if score < 65 else "STRESS")

    rows.sort(key=lambda r: (r["tier"], r["group"], r["id"]))
    groups = {}
    for r in rows:
        groups.setdefault(r["group"], []).append(r)
    out = {"as_of": now, "engine_v": "2.6", "diag": diag,
            "note": ("barometer is a labeled heuristic: score = 50 + "
                     "10*mean(clipped z), components listed in full"),
            "counts": {"series": len(rows), "skipped": len(skipped),
                        "history_files": len(rows)},
            "barometer": {"score": score, "label": label,
                           "components": comps},
            "groups": [{"name": g, "series": groups[g]}
                        for g in sorted(groups)],
            "skipped": skipped[:40], "dollar_missing": dollar_miss}
    s3.put_object(Bucket=BUCKET, Key="data/repo.json",
                   Body=json.dumps(out, separators=(",", ":")).encode(),
                   ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "v": "2.6", "ice": ice_added, "frepo": frepo_added, "series": len(rows), "skipped": len(skipped),
            "barometer": score, "label": label,
            "secs": round(time.time() - t0, 1)}
    print("[repo] " + json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
