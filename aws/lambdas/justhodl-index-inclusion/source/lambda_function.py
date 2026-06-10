"""
justhodl-index-inclusion v1.0 — Item 14: S&P 500 Inclusion Probability
=======================================================================
Generalizes the russell-recon edge to the S&P 500: names get bought by ~$8T of
indexed money on inclusion day, and the committee's rules are public:
  • US domicile, primary US exchange listing
  • Float-adjusted mcap above the smallest current members
  • GAAP-positive: sum of 4 most recent quarters AND most recent quarter > 0
This engine builds the candidate list weekly (Mon): non-members passing the
mcap floor and the profitability screen, ranked by mcap. Probe-gated — if the
constituents feed is unavailable the brief says so instead of guessing.
"""
import json, os, time, urllib.request, urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/index-inclusion.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
VERSION = "1.0.3"
MCAP_FLOOR = 18e9


def fmp(path, params=None):
    params = dict(params or {}); params["apikey"] = FMP_KEY
    u = f"https://financialmodelingprep.com/stable/{path}?" + urllib.parse.urlencode(params)
    try:
        return json.loads(urllib.request.urlopen(u, timeout=30).read())
    except Exception as e:
        print(f"[fmp] {path}: {str(e)[:60]}")
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    avail = {}

    members = None
    for ep in ("sp500-constituent", "sp500_constituent", "historical/sp500-constituent"):
        j = fmp(ep)
        if isinstance(j, list) and len(j) > 400:
            members = {str(r.get("symbol", "")).upper() for r in j if r.get("symbol")}
            avail["constituents"] = ep
            break
    avail["constituents"] = avail.get("constituents", False)

    cands = None
    scr = fmp("company-screener", {"marketCapMoreThan": int(MCAP_FLOOR), "isEtf": "false",
                                   "isFund": "false", "isActivelyTrading": "true",
                                   "country": "US", "limit": 1000})
    if isinstance(scr, list) and len(scr) > 50:
        avail["screener"] = True
        cands = [r for r in scr
                 if str(r.get("exchangeShortName", r.get("exchange", ""))).upper()
                 in ("NYSE", "NASDAQ", "AMEX", "NASDAQ GLOBAL SELECT", "NEW YORK STOCK EXCHANGE")]
    else:
        avail["screener"] = False

    watch, checked = [], 0
    if members and cands:
        member_bases = {m.split("-")[0].split(".")[0] for m in members}
        def keep(r):
            sym = str(r.get("symbol", "")).upper()
            if sym in members or sym.split("-")[0].split(".")[0] in member_bases:
                return False                      # share-class twin of a member (BRK-A)
            if str(r.get("country", "US")).upper() not in ("US", "USA"):
                return False                      # foreign domicile (ASML)
            if len(sym) == 5 and sym.endswith("X"):
                return False                      # mutual-fund ticker convention (VTSAX)
            nm = str(r.get("companyName", "")).lower()
            if any(w in nm for w in (" fund", " trust", "index ", "etf", "portfolio")):
                return False
            if nm.rstrip().endswith((" lp", " l.p.", " l.p")) or any(
                    w in nm for w in ("%", " notes", " nts", " due 20", "preferred",
                                      "depositary", " l.p", " lp ", "partners l", "lp,")):
                return False                      # exchange-listed debt / prefs / LPs
            return True
        nonmem = sorted((r for r in cands if keep(r)),
                        key=lambda r: -(r.get("marketCap") or 0))[:40]

        def profit_check(r):
            sym = str(r["symbol"]).upper()
            prof = fmp("profile", {"symbol": sym})
            p0 = prof[0] if isinstance(prof, list) and prof else {}
            if p0.get("isAdr") or str(p0.get("country", "US")).upper() not in ("US", "USA"):
                return None                       # ADR / foreign domicile (S&P rule)
            if p0.get("isFund") or p0.get("isEtf"):
                return None
            inc = fmp("income-statement", {"symbol": sym, "period": "quarter", "limit": 4})
            if not isinstance(inc, list) or len(inc) < 4:
                return None
            nis = [q.get("netIncome") for q in inc if isinstance(q.get("netIncome"), (int, float))]
            if len(nis) < 4:
                return None
            ttm, latest = sum(nis[:4]), nis[0]
            return {"ticker": sym, "name": r.get("companyName"), "sector": r.get("sector"),
                    "mcap_bn": round((r.get("marketCap") or 0) / 1e9, 1),
                    "ni_ttm_bn": round(ttm / 1e9, 2), "ni_latest_q_bn": round(latest / 1e9, 2),
                    "passes_profit_rule": bool(ttm > 0 and latest > 0),
                    "exchange": r.get("exchangeShortName") or r.get("exchange")}

        with ThreadPoolExecutor(max_workers=5) as ex:
            for f in as_completed({ex.submit(profit_check, r): r for r in nonmem}):
                try:
                    row = f.result()
                    checked += 1
                    if row:
                        watch.append(row)
                except Exception as e:
                    print(f"[cand] {str(e)[:50]}")
        watch.sort(key=lambda r: (-int(r["passes_profit_rule"]), -r["mcap_bn"]))

    eligible = [w for w in watch if w["passes_profit_rule"]]
    out = {"engine": "index-inclusion", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "availability": avail,
           "n_members": len(members) if members else None,
           "mcap_floor_bn": MCAP_FLOOR / 1e9, "n_candidates_checked": checked,
           "n_eligible": len(eligible),
           "watch_list": watch[:25],
           "caveats": ["Float adjustment, sector balance, and committee discretion are NOT modeled — "
                       "this is the rule-based eligible set, not a prediction of the next add.",
                       "Mcap floor is a fixed $18bn proxy for the smallest-member threshold."],
           "methodology": ("Weekly S&P 500 inclusion watch: US-listed non-members above the mcap "
                           "floor, screened on the committee's public GAAP rule (trailing 4Q sum "
                           "positive AND most recent quarter positive). Ranked by size; pure free "
                           "data (FMP constituents + screener + quarterly income statements).")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[inclusion] members={out['n_members']} checked={checked} eligible={len(eligible)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"eligible": len(eligible)})}
