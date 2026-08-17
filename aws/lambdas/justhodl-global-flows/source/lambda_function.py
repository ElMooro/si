"""justhodl-global-flows v1.1.0 -- worldwide foreign capital
inflows by country.  Marker: global-flows v1.2.0

Khalid directive 2026-08-17: country inflows worldwide, the more the
better, specialists first (industries + raw materials).  Doctrine
from his research doc: track PORTFOLIO INVESTMENT -> LIABILITIES
(nonresidents acquiring the country's securities), never the mixed
headline financial account; separate slow macro capital (BOP) from
daily hot money (exchange feeds).

v1.0 binds ONLY what probe ops 4832 proved live:
  PERU (BCRP, keyless, quarterly since 2012, USD mn):
    PN39285BQ private portfolio liabilities -- total
    PN39286BQ private portfolio liabilities -- equity
    PN39287BQ private portfolio liabilities -- fixed income
    PN39414FQ general-government bonds acquired by nonresidents
Everything else ships as a NAMED deferral with its unlock condition
(never guessed, never silent):
  taiwan_cbc   BPP2Q01en alive ({data,meta} shape) -> micro-probe
  taiwan_twse  daily foreign flow -- OpenAPI paths were HTML; rwd
               endpoints to probe
  korea        BOK ECOS + KRX -- API keys required (Khalid)
  chile        BCCh bcchapi -- token required (Khalid)
  imf_layer    api.imf.org sdmx/2.1 ALIVE -- BOP dataflow id probe
v1.1 (probes 4837-4838): TAIWAN LIVE --
  macro: CBC BPP2Q01en dataSets rows ["YYYYQn", v0..v401] mapping
  +1-offset onto structure.Table1 labels; bound by EXACT label
  ("Portfolio investment-Liabilities", "...Equity and investment
  fund shares-Liabilities", first "Debt securities-Liabilities"
  AFTER the equity row -- never by hardcoded index), USD mn, "-"
  = null, quarterly since 1984Q1.
  hot_money: MOVED to justhodl-hot-money (Khalid directive
  2026-08-17: separate engines for hot money vs capital flow).
Series bank permanently under data/providers/bcrp/{sid}.json
(Deny-Delete zone, union-merge).  Weekly Mon 12:00 UTC (quarterly
data; cheap check).  CFI and hot-money composites stay DEFERRED
until their components exist -- partial composites are lies.
"""
import gzip
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.2.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/global-flows.json"
BANK_FMT = "data/providers/bcrp/%s.json"

PE_SERIES = {"portfolio_total": "PN39285BQ",
             "portfolio_equity": "PN39286BQ",
             "portfolio_fixed_income": "PN39287BQ",
             "gov_bonds_nonresident": "PN39414FQ"}
PE_URL = ("https://estadisticas.bcrp.gob.pe/estadisticas/series/"
          "api/%s/json/2012-1/%s")
MIN_Q = 24

DEFERRED = {
    "korea": {"status": "DEFERRED",
              "why": "BOK ECOS + KRX require API keys -- pending "
                     "Khalid",
              "specialty": "memory/electronics"},
    "chile": {"status": "DEFERRED",
              "why": "BCCh bcchapi requires token -- pending "
                     "Khalid",
              "specialty": "copper"},
    "imf_layer": {"status": "DEFERRED",
                  "why": "dataflow BOP (IMF.STA v21) + dims "
                         "COUNTRY.BOP_ACCOUNTING_ENTRY.INDICATOR."
                         "UNIT.FREQUENCY resolved (4838); INDICATOR"
                         " codelist probe queued",
                  "specialty": "all countries, one methodology"},
}

s3 = boto3.client("s3")


def _g(key):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, separators=(",", ":")).encode(),
                  ContentType="application/json")


def fnum(v):
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else None
    except (TypeError, ValueError):
        return None


def endq_prev(now):
    """BCRP end period = PREVIOUS completed quarter: asking through
    the current (incomplete) quarter makes the API append junk
    after the JSON (Extra data -- burned ops 4833)."""
    q = (now.month - 1) // 3 + 1
    return "%d-4" % (now.year - 1) if q == 1 \
        else "%d-%d" % (now.year, q - 1)


def bcrp_fetch():
    """One combined call -> {name: [(period, val_musd)]} or
    (None, reason).  Seam for the harness."""
    now = datetime.now(timezone.utc)
    endq = endq_prev(now)
    sids = "-".join(PE_SERIES.values())
    try:
        req = urllib.request.Request(
            PE_URL % (sids, endq),
            headers={"User-Agent": "justhodl-global-flows"})
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read().decode("utf-8", "replace")
        try:
            j = json.loads(raw)
        except ValueError:
            j = json.JSONDecoder().raw_decode(raw)[0]
    except Exception as e:  # noqa: BLE001
        return None, "fetch_error:%s" % str(e)[:70]
    periods = j.get("periods") or []
    if len(periods) < MIN_Q:
        return None, "too_short(n=%d)" % len(periods)
    order = list(PE_SERIES)          # values arrive in call order
    out = {k: [] for k in order}
    for p in periods:
        name = p.get("name")
        vals = p.get("values") or []
        for i, k in enumerate(order):
            v = fnum(vals[i]) if i < len(vals) else None
            if v is not None:
                out[k].append((name, v))
    return out, None


def bank_merge(sid, rows):
    key = BANK_FMT % sid
    bank = _g(key) or {"id": sid, "source": "BCRP", "rows": {}}
    n_new = 0
    for per, v in rows:
        if per not in bank["rows"]:
            n_new += 1
        bank["rows"][per] = v
    if n_new:
        _put(key, bank)
    return len(bank["rows"]), n_new


def zlast(vals):
    if len(vals) < MIN_Q:
        return None
    hist, last = vals[:-1], vals[-1]
    mu = sum(hist) / len(hist)
    sd = (sum((v - mu) ** 2 for v in hist)
          / max(1, len(hist) - 1)) ** 0.5
    if sd <= 1e-12:
        return None
    return round(max(-4.0, min(4.0, (last - mu) / sd)), 2)


CBC_URL = ("https://cpx.cbc.gov.tw/API/DataAPI/Get"
           "?FileName=BPP2Q01en")
CBC_LAB_TOTAL = "Portfolio investment-Liabilities"
CBC_LAB_EQ = ("Portfolio investment-Equity and investment fund "
              "shares-Liabilities")
CBC_LAB_DEBT = "Debt securities-Liabilities"
CBC_BANK_FMT = "data/providers/cbc/%s.json"


def cbc_fetch():
    """(labels, rows) with rows=[(period,[vals])] or (None,
    reason).  Seam for the harness."""
    try:
        req = urllib.request.Request(
            CBC_URL, headers={"User-Agent": "justhodl-gf"})
        with urllib.request.urlopen(req, timeout=70) as r:
            j = json.loads(r.read())
        labels = [(x or {}).get("data") or ""
                  for x in (((j.get("data") or {})
                             .get("structure") or {})
                            .get("Table1") or [])]
        rows = []
        for row in ((j.get("data") or {}).get("dataSets") or []):
            if not row or not isinstance(row, list):
                continue
            per = str(row[0])
            vals = []
            for v in row[1:]:
                vals.append(fnum(v))
            rows.append((per, vals))
        if not labels or not rows:
            return None, "empty labels/rows"
        return labels, rows
    except Exception as e:  # noqa: BLE001
        return None, "fetch_error:%s" % str(e)[:70]


def cbc_locate(labels):
    """Exact-label binding; debt = first bare Debt-securities-
    Liabilities AFTER the equity row.  Returns dict or None."""
    try:
        i_tot = labels.index(CBC_LAB_TOTAL)
        i_eq = labels.index(CBC_LAB_EQ)
    except ValueError:
        return None
    i_debt = None
    for i in range(i_eq + 1, len(labels)):
        if labels[i] == CBC_LAB_DEBT:
            i_debt = i
            break
    if i_debt is None:
        return None
    return {"portfolio_liab_total": i_tot,
            "portfolio_liab_equity": i_eq,
            "portfolio_liab_debt": i_debt}


def taiwan_block(doc, event):
    tw = {"specialty": "semiconductors", "macro": {},
          "hot_money": {}}
    labels, rows = cbc_fetch()
    if labels is None:
        tw["macro"] = {"status": "MISSING", "why": rows}
    else:
        loc = cbc_locate(labels)
        width_ok = all(len(v) == len(labels)
                       for _, v in rows[-4:])
        if not loc or not width_ok:
            tw["macro"] = {"status": "MISSING",
                           "why": "labels moved or width mismatch "
                                  "-- refusing positional bind"}
        else:
            tw["macro"] = {"status": "LIVE", "freq": "quarterly",
                           "unit": "USD mn",
                           "source": "CBC BPP2Q01en (keyless)",
                           "series": {}}
            for name, idx in loc.items():
                ser = [(per, v[idx]) for per, v in rows
                       if idx < len(v) and v[idx] is not None]
                if len(ser) < MIN_Q:
                    tw["macro"]["series"][name] = {
                        "status": "SHORT", "n": len(ser)}
                    continue
                bank_key = CBC_BANK_FMT % name
                bank = _g(bank_key) or {"id": name,
                                        "source": "CBC",
                                        "rows": {}}
                n_new = 0
                for per, v in ser:
                    if per not in bank["rows"]:
                        n_new += 1
                    bank["rows"][per] = v
                if n_new:
                    _put(bank_key, bank)
                vals = [v for _, v in ser]
                tw["macro"]["series"][name] = {
                    "label_index": idx,
                    "latest": round(vals[-1], 1),
                    "period": ser[-1][0],
                    "sum_4q": round(sum(vals[-4:]), 1),
                    "z_all": zlast(vals), "n_obs": len(vals),
                    "first": ser[0][0]}
            tw["macro"]["latest_period"] = rows[-1][0]

    tw["hot_money"] = {"status": "MOVED",
                       "see": "data/hot-money.json",
                       "why": "hot money split into justhodl-"
                              "hot-money per doctrine"}
    tw["status"] = ("LIVE" if tw["macro"].get("status")
                    == "LIVE" else "MISSING")
    doc["countries"]["taiwan"] = tw


def build(event=None):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    doc = {"v": VERSION, "engine": "justhodl-global-flows",
           "as_of": now.date().isoformat(),
           "generated_at": now.isoformat(),
           "doctrine": "portfolio LIABILITIES = nonresidents "
                       "buying the country; macro (BOP) and hot "
                       "money (daily exchange) kept separate",
           "countries": {}, "deferred": dict(DEFERRED),
           "composites": {
               "cfi": {"value": None,
                       "why": "needs FDI + other-investment "
                              "liabilities per country -- partial "
                              "composites are lies"},
               "hot_money": {"value": None,
                             "why": "needs daily exchange feeds "
                                    "(TWSE/KRX) -- deferred"}},
           "diag": {}}
    data, err = bcrp_fetch()
    if data is None:
        pe = {"status": "MISSING", "why": err}
        doc["countries"]["peru"] = pe
        taiwan_block(doc, event)
        tw_ok = (doc["countries"].get("taiwan")
                 or {}).get("status") == "LIVE"
        doc["status"] = "LIVE" if tw_ok else "INSUFFICIENT_DATA"
        if not tw_ok:
            doc["why"] = "peru core failed (%s) and taiwan not " \
                "live" % err
        doc["diag"]["runtime_ms"] = int((time.time() - t0) * 1000)
        return doc
    pe = {"status": "LIVE", "freq": "quarterly", "unit": "USD mn",
          "source": "BCRP (keyless)", "specialty": "copper/gold "
          "mining", "series": {}}
    latest_period = None
    for name, sid in PE_SERIES.items():
        rows = data.get(name) or []
        if len(rows) < MIN_Q:
            pe["series"][name] = {"status": "SHORT",
                                  "n": len(rows), "id": sid}
            continue
        n_bank, n_new = bank_merge(sid, rows)
        vals = [v for _, v in rows]
        latest_period = rows[-1][0]
        pe["series"][name] = {
            "id": sid, "latest": round(vals[-1], 1),
            "period": rows[-1][0],
            "sum_4q": round(sum(vals[-4:]), 1),
            "z_all": zlast(vals), "n_obs": len(vals),
            "first": rows[0][0], "bank_n": n_bank}
    live_n = sum(1 for s in pe["series"].values()
                 if "latest" in s)
    pe["latest_period"] = latest_period
    if live_n < 3:
        pe["status"] = "THIN"
        pe["why"] = "only %d/4 series usable" % live_n
    doc["countries"]["peru"] = pe
    taiwan_block(doc, event)
    tw_ok = (doc["countries"].get("taiwan") or {}).get("status") \
        == "LIVE"
    doc["status"] = "LIVE" if (pe["status"] == "LIVE" or tw_ok) \
        else "INSUFFICIENT_DATA"
    doc["diag"]["runtime_ms"] = int((time.time() - t0) * 1000)
    return doc


def lambda_handler(event, context):
    doc = build(event or {})
    _put(OUT_KEY, doc)
    pe = (doc.get("countries") or {}).get("peru") or {}
    tw = (doc.get("countries") or {}).get("taiwan") or {}
    return {"ok": doc.get("status") == "LIVE", "v": VERSION,
            "status": doc.get("status"),
            "peru_period": pe.get("latest_period"),
            "tw_macro": (tw.get("macro") or {}).get("status"),
            "tw_ledger_days": (tw.get("hot_money")
                               or {}).get("ledger_days"),
            "n_deferred": len(doc.get("deferred") or {})}
