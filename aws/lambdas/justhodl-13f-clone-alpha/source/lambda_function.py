"""justhodl-13f-clone-alpha — ops 3294 (13F desk flagship).

CLONE-ALPHA: measures each tracked manager's *followable skill* from
their own filing history, so every 13F board can weight SKILLED money
over FAMOUS money.

Method (honest, replication-grade):
  * For each WATCHLIST fund, pull the last ~13 quarterly 13F-HR
    filings from SEC EDGAR submissions (data.sec.gov).
  * Each filing -> top-15 equity longs by value (options putCall
    EXCLUDED; SEC value-units auto-detect as in 13f-positions),
    value-weighted, 12% single-name cap, renormalized.
  * Clone window = you could only act AFTER disclosure: entry at the
    first close ON/AFTER filed date; exit at the NEXT filing's entry.
    Only CLOSED windows count toward skill.
  * Window return = sum(w_i * (Pexit/Pentry - 1)) over priced names;
    coverage = sum of priced weight, window kept only if >= 0.60.
  * alpha_w = clone - SPY over the identical window.
  * Skill per fund (needs >= 6 windows): annualized clone vs SPY
    (geometric), hit rate, IR = mean(alpha)/std(alpha)*sqrt(4),
    SKILL 0-100 and label: WORTH CLONING / SELECTIVE EDGE /
    MARKET-LIKE / FAMOUS != SKILLED.

Heavy backfill converges across runs (FILING_BUDGET / PRICE_BUDGET),
self-chains async up to MAX_HOPS, then weekly refresh keeps it warm.
Caches: 13f/clone-holdings-cache.json, 13f/clone-price-cache.json.
Output: data/13f-clone-alpha.json (status CONVERGING|COMPLETE).
"""
import json
import math
import os
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/13f-clone-alpha.json"
HOLD_KEY = "13f/clone-holdings-cache.json"
PX_KEY = "13f/clone-price-cache.json"
CUSIP_MAP_KEY = "data/13f-cusip-map.json"
USER_AGENT = os.environ.get("USER_AGENT",
                            "JustHodl Research raafouis@gmail.com")
FMP_KEY = os.environ.get("FMP_API_KEY") or os.environ.get("FMP_KEY")

N_FILINGS = 13          # ~3y of quarters -> up to 12 closed windows
TOP_N = 15
W_CAP = 0.12
MIN_COV = 0.60
MIN_WINDOWS = 6
FILING_BUDGET = int(os.environ.get("FILING_BUDGET", "30"))
PRICE_BUDGET = int(os.environ.get("PRICE_BUDGET", "130"))
MAX_HOPS = 10

s3 = boto3.client("s3")
lam = boto3.client("lambda")

WATCHLIST = {
    "BERKSHIRE": "0001067983", "BRIDGEWATER": "0001350694",
    "RENAISSANCE": "0001037389", "AQR": "0001167557",
    "TWO_SIGMA": "0001179392", "CITADEL": "0001423053",
    "MILLENNIUM": "0001273087", "PERSHING": "0001336528",
    "GREENLIGHT": "0001079114", "SOROS": "0001029160",
    "TIGER_GLOBAL": "0001167483", "COATUE": "0001135730",
    "BAUPOST": "0001061165", "ELLIOTT": "0001286922",
    "SCION": "0001649339", "DURATION": "0001582202",
    "POINT72": "0001603466", "LONE_PINE": "0001061768",
}
NAMES = {
    "BERKSHIRE": "Berkshire Hathaway", "BRIDGEWATER": "Bridgewater",
    "RENAISSANCE": "Renaissance Tech", "AQR": "AQR Capital",
    "TWO_SIGMA": "Two Sigma", "CITADEL": "Citadel",
    "MILLENNIUM": "Millennium", "PERSHING": "Pershing Square",
    "GREENLIGHT": "Greenlight", "SOROS": "Soros Fund",
    "TIGER_GLOBAL": "Tiger Global", "COATUE": "Coatue",
    "BAUPOST": "Baupost", "ELLIOTT": "Elliott",
    "SCION": "Scion (Burry)", "DURATION": "Duration Capital",
    "POINT72": "Point72", "LONE_PINE": "Lone Pine",
}
HEDGE_BOOK = {"CITADEL", "MILLENNIUM"}   # doctrine: options/hedge churn


def _fetch(url, timeout=25):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def get_j(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default if default is not None else {}


def put_j(key, body):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(body, separators=(",", ":")),
                  ContentType="application/json")


# ── SEC: filing history per CIK ─────────────────────────────────────
def filing_history(cik):
    """Last N_FILINGS 13F-HR (period, filed, accession); /A replaces
    same period."""
    url = ("https://data.sec.gov/submissions/CIK%s.json"
           % cik.zfill(10))
    d = json.loads(_fetch(url).decode("utf-8", "ignore"))
    r = (d.get("filings") or {}).get("recent") or {}
    out = {}
    for form, acc, filed, period in zip(
            r.get("form", []), r.get("accessionNumber", []),
            r.get("filingDate", []), r.get("reportDate", [])):
        if form not in ("13F-HR", "13F-HR/A") or not period:
            continue
        cur = out.get(period)
        if cur is None or filed > cur["filed"]:
            out[period] = {"period": period, "filed": filed,
                           "accession": acc}
    time.sleep(0.25)
    return sorted(out.values(), key=lambda x: x["period"])[-N_FILINGS:]


def filing_dir(cik, accession):
    return ("https://www.sec.gov/Archives/edgar/data/%s/%s/"
            % (str(int(cik)), accession.replace("-", "")))


def find_infotable(fdir):
    try:
        t = _fetch(fdir + "infotable.xml", timeout=20).decode(
            "utf-8", "ignore")
        if "<infoTable" in t or "infoTable>" in t:
            return t
    except Exception:
        pass
    try:
        idx = json.loads(_fetch(fdir + "index.json").decode("utf-8"))
        for it in idx.get("directory", {}).get("item", []):
            nm = it.get("name", "")
            if nm.endswith(".xml") and nm != "primary_doc.xml":
                try:
                    t = _fetch(fdir + nm, timeout=25).decode(
                        "utf-8", "ignore")
                    if "infoTable" in t:
                        return t
                except Exception:
                    continue
    except Exception:
        pass
    return None


def parse_top(xml_text, cmap):
    """Top-N equity longs -> [(ticker, weight)]; options excluded;
    value-units auto-detected (same doctrine as 13f-positions)."""
    cleaned = re.sub(r"<(/?)\w+:", r"<\1", xml_text)
    cleaned = re.sub(r'\s+xmlns(:\w+)?="[^"]*"', "", cleaned)
    cleaned = re.sub(r'\s+\w+:\w+="[^"]*"', "", cleaned)
    try:
        root = ET.fromstring(cleaned)
    except ET.ParseError:
        return [], 0
    rows = []
    for it in root.iter("infoTable"):
        try:
            if (it.findtext("putCall") or "").strip():
                continue                      # options out of the book
            cusip = (it.findtext("cusip") or "").strip().upper()
            v = int(float(it.findtext("value") or "0"))
            sn = it.find("shrsOrPrnAmt")
            sh = int(float((sn.findtext("sshPrnamt") if sn is not None
                            else "0") or "0"))
            if v <= 0 or not cusip:
                continue
            if sh > 0:
                p_th, p_d = v * 1000 / sh, v / sh
                if 0.5 <= p_th <= 5000 and not 0.5 <= p_d <= 5000:
                    v *= 1000
                elif 0.5 <= p_th <= 5000 and p_d < 0.5:
                    v *= 1000
            rows.append((cusip, v))
        except Exception:
            continue
    if not rows:
        return [], 0
    agg = {}
    for cu, v in rows:
        agg[cu] = agg.get(cu, 0) + v
    top = sorted(agg.items(), key=lambda x: -x[1])[:TOP_N]
    res, tot_named = [], 0
    for cu, v in top:
        tk = ((cmap.get(cu) or {}).get("ticker") or "").upper()
        if tk and tk.isalpha() and len(tk) <= 5:
            res.append([tk, v])
            tot_named += v
    if not tot_named:
        return [], len(top)
    w = [[tk, min(W_CAP, v / tot_named)] for tk, v in res]
    z = sum(x[1] for x in w) or 1.0
    return [[tk, round(x / z, 5)] for tk, x in w], len(top)


# ── prices ──────────────────────────────────────────────────────────
def hist_closes(sym, frm):
    for u in (
        "https://financialmodelingprep.com/stable/historical-price-"
        "eod/light?symbol=%s&from=%s&apikey=%s" % (sym, frm, FMP_KEY),
        "https://financialmodelingprep.com/stable/historical-price-"
        "eod/full?symbol=%s&from=%s&apikey=%s" % (sym, frm, FMP_KEY),
    ):
        try:
            d = json.loads(_fetch(u, timeout=25).decode())
            rows = d if isinstance(d, list) else (
                (d or {}).get("historical") or [])
            out = {r["date"]: float(r.get("close") or r.get("price"))
                   for r in rows
                   if r.get("date") and (r.get("close")
                                         or r.get("price"))}
            if out:
                return out
        except Exception:
            continue
    return {}


def close_ge(closes, d):
    ks = sorted(k for k in closes if k >= d)
    return (closes[ks[0]], ks[0]) if ks else (None, None)


def lambda_handler(event=None, context=None):
    event = event or {}
    hop = int(event.get("hop") or 0)
    t0 = time.time()
    cmap = get_j(CUSIP_MAP_KEY, {})
    hold = get_j(HOLD_KEY, {})
    pxc = get_j(PX_KEY, {})
    warns = []

    # ── phase 1: holdings backfill (budgeted) ──
    fetched = 0
    filings_pending = 0
    earliest = "2099-01-01"
    for fk, cik in WATCHLIST.items():
        fund = hold.setdefault(fk, {})
        try:
            hist = fund.get("_hist")
            if not hist or fund.get("_hist_at", "") < time.strftime(
                    "%Y-%m-%d"):
                hist = filing_history(cik)
                fund["_hist"] = hist
                fund["_hist_at"] = time.strftime("%Y-%m-%d")
        except Exception as e:
            warns.append("%s hist: %s" % (fk, str(e)[:60]))
            hist = fund.get("_hist") or []
        for f in hist:
            per = f["period"]
            earliest = min(earliest, f["filed"])
            if per in fund and fund[per].get("w") is not None:
                continue
            if fetched >= FILING_BUDGET:
                filings_pending += 1
                continue
            fetched += 1
            try:
                xml_t = find_infotable(filing_dir(cik, f["accession"]))
                time.sleep(0.3)
                if not xml_t:
                    fund[per] = {"filed": f["filed"], "w": [],
                                 "err": "no-infotable"}
                    continue
                w, _n = parse_top(xml_t, cmap)
                fund[per] = {"filed": f["filed"], "w": w}
            except Exception as e:
                warns.append("%s %s: %s" % (fk, per, str(e)[:50]))
                filings_pending += 1
        if time.time() - t0 > 560:
            filings_pending += 1
            break
    put_j(HOLD_KEY, hold)

    # ── phase 2: price backfill (budgeted) ──
    need = {"SPY"}
    for fk in WATCHLIST:
        for per, rec in (hold.get(fk) or {}).items():
            if per.startswith("_"):
                continue
            for tk, _w in rec.get("w") or []:
                need.add(tk)
    frm = max("2021-01-01", earliest[:8] + "01")
    px_fetched = 0
    for tk in sorted(need, key=lambda t: (t != "SPY", t)):
        if tk in pxc and pxc[tk]:
            continue
        if px_fetched >= PRICE_BUDGET:
            break
        if time.time() - t0 > 780:
            break
        px_fetched += 1
        cl = hist_closes(tk, frm)
        pxc[tk] = cl or {"_miss": 1}
        time.sleep(0.12)
    prices_pending = sum(1 for t in need if t not in pxc)
    put_j(PX_KEY, pxc)

    # ── phase 3: windows + skill ──
    spy = {k: v for k, v in (pxc.get("SPY") or {}).items()
           if not k.startswith("_")}
    managers = {}
    for fk in WATCHLIST:
        fund = hold.get(fk) or {}
        pers = sorted(p for p in fund if not p.startswith("_")
                      and fund[p].get("w"))
        wins = []
        for i in range(len(pers) - 1):
            a, b = fund[pers[i]], fund[pers[i + 1]]
            e_spy, e_d = close_ge(spy, a["filed"])
            x_spy, x_d = close_ge(spy, b["filed"])
            if not e_spy or not x_spy or e_d >= x_d:
                continue
            num = cov = 0.0
            for tk, w in a["w"]:
                cl = pxc.get(tk) or {}
                pe, _ = close_ge(cl, a["filed"])
                pxx, _ = close_ge(cl, b["filed"])
                if pe and pxx:
                    num += w * (pxx / pe - 1)
                    cov += w
            if cov < MIN_COV:
                continue
            r = num / cov * 100
            sret = (x_spy / e_spy - 1) * 100
            wins.append({"q": pers[i], "entry": e_d, "exit": x_d,
                         "ret": round(r, 2), "spy": round(sret, 2),
                         "alpha": round(r - sret, 2),
                         "cov": round(cov, 2)})
        m = {"name": NAMES.get(fk, fk), "n_windows": len(wins),
             "hedge_book": fk in HEDGE_BOOK, "windows": wins[-12:]}
        if len(wins) >= MIN_WINDOWS:
            als = [w["alpha"] for w in wins]
            g_c = 1.0
            g_s = 1.0
            for w in wins:
                g_c *= 1 + w["ret"] / 100
                g_s *= 1 + w["spy"] / 100
            ann_c = (g_c ** (4.0 / len(wins)) - 1) * 100
            ann_s = (g_s ** (4.0 / len(wins)) - 1) * 100
            mu = sum(als) / len(als)
            sd = (sum((a - mu) ** 2 for a in als)
                  / max(1, len(als) - 1)) ** 0.5
            ir = (mu / sd) * 2 if sd > 0 else 0.0
            hit = sum(1 for a in als if a > 0) / len(als)
            score = max(0, min(100, round(
                50 + 24 * math.tanh(ir / 1.1)
                + (hit - 0.5) * 56
                + max(-14, min(14, (ann_c - ann_s) * 1.4)))))
            label = ("WORTH CLONING" if score >= 70 else
                     "SELECTIVE EDGE" if score >= 55 else
                     "MARKET-LIKE" if score >= 45 else
                     "FAMOUS \u2260 SKILLED")
            m.update({"ann_clone_pct": round(ann_c, 2),
                      "ann_spy_pct": round(ann_s, 2),
                      "ann_alpha_pct": round(ann_c - ann_s, 2),
                      "hit_rate": round(hit, 2), "ir": round(ir, 2),
                      "worst_alpha": round(min(als), 2),
                      "best_alpha": round(max(als), 2),
                      "skill_score": score, "label": label})
        else:
            m["label"] = "INSUFFICIENT_HISTORY"
        managers[fk] = m

    done_f = sum(1 for fk in WATCHLIST
                 for p, r in (hold.get(fk) or {}).items()
                 if not p.startswith("_") and r.get("w") is not None)
    total_f = sum(len(hold.get(fk, {}).get("_hist") or [])
                  for fk in WATCHLIST) or 1
    pct = round(100 * min(1.0, done_f / total_f)
                * (0.5 + 0.5 * (1 - prices_pending
                                / max(1, len(need)))), 1)
    complete = filings_pending == 0 and prices_pending == 0
    out = {"as_of": datetime.now(timezone.utc).isoformat(),
           "status": "COMPLETE" if complete else "CONVERGING",
           "pct_complete": 100.0 if complete else pct,
           "filings_done": done_f, "filings_total": total_f,
           "prices_pending": prices_pending, "hop": hop,
           "method": ("Clone-Alpha: top-%d disclosed equity longs, "
                      "value-weighted (%.0f%% cap), entry at first "
                      "close after each filing date, exit at next "
                      "filing's entry — the return YOU could have "
                      "earned by following the paperwork. Options "
                      "excluded; windows need \u2265%.0f%% price "
                      "coverage; skill needs \u2265%d closed windows."
                      % (TOP_N, W_CAP * 100, MIN_COV * 100,
                         MIN_WINDOWS)),
           "managers": managers, "warns": warns[:12]}
    put_j(OUT_KEY, out)
    print("[clone] hop=%d filings+%d prices+%d pending f=%d p=%d "
          "pct=%s" % (hop, fetched, px_fetched, filings_pending,
                      prices_pending, out["pct_complete"]))

    if not complete and hop < MAX_HOPS:
        try:
            lam.invoke(FunctionName=context.function_name
                       if context else "justhodl-13f-clone-alpha",
                       InvocationType="Event",
                       Payload=json.dumps({"hop": hop + 1}).encode())
        except Exception as e:
            warns.append("self-chain: %s" % str(e)[:60])
    return {"statusCode": 200,
            "body": json.dumps({"status": out["status"],
                                "pct": out["pct_complete"]})}
