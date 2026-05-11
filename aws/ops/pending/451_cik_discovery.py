#!/usr/bin/env python3
"""Step 451 — Discover famous hedge fund CIKs by name-matching.

Strategy:
  1. Pull institutional-ownership/latest across 10+ pages (~1000 unique 13F filers)
  2. Match filer names against a long regex of famous fund identifiers
  3. For each match, call holder-performance-summary to validate + get AUM
  4. Also test pre-known CIKs from v2 list that may have been failing
  5. Output a sorted, verified, ready-to-paste Python list for the Lambda

Includes specific famous funds the user wants surfaced:
  Activists: Trian (Peltz), Starboard (Smith), Icahn, Engine No. 1
  Macro:     Caxton (Kovner), Druckenmiller (Duquesne), Discovery (Citrone),
              Brevan Howard, Element Capital
  Quants:    AQR, Schonfeld, Balyasny, Verition, WorldQuant
  Growth:    ARK (Cathie Wood), T. Rowe Price, Capital Group
  Value:     Yacktman, Davis, Akre, Polen, Ariel, GAMCO (Gabelli)
  Activists: Carl Icahn, Mario Gabelli, Bill Miller, Wally Weitz
  Endowments: Norges Bank, Princeton, Yale, Harvard Mgmt
  Other:     Marshall Wace, Davidson Kempner, Farallon, TCI, Egerton,
              Lansdowne, BlueCrest, Cooperman/Omega
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/451_cik_discovery.json"
NAME = "justhodl-tmp-451"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = r'''
import json, re, urllib.request
from concurrent.futures import ThreadPoolExecutor
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"

# Patterns: name fragment (case-insensitive) → display label
# Matches in this order; longer/more-specific patterns first
FAMOUS_PATTERNS = [
    # === existing v2 — re-validate ===
    (r"\bberkshire hathaway\b",              "Berkshire Hathaway"),
    (r"\bpershing square\b",                  "Pershing Square Capital"),
    (r"\bgreenlight capital\b",               "Greenlight Capital"),
    (r"\bthird point\b",                      "Third Point"),
    (r"\belliott (?:investment|management|capital)\b",  "Elliott Investment Mgmt"),
    (r"\bvalueact\b",                         "ValueAct Capital"),
    (r"\blone pine\b",                        "Lone Pine Capital"),
    (r"\bcoatue\b",                           "Coatue Management"),
    (r"\btiger global\b",                     "Tiger Global"),
    (r"\bsoros fund\b",                       "Soros Fund Management"),
    (r"\bbaupost\b",                          "Baupost Group"),
    (r"\bpoint72\b",                          "Point72 Asset Management"),
    (r"\bviking global\b",                    "Viking Global Investors"),
    (r"\bglenview capital\b",                 "Glenview Capital"),
    (r"\bsands capital\b",                    "Sands Capital Management"),
    (r"\bwhale rock\b",                       "Whale Rock Capital"),
    (r"\bmaverick capital\b",                 "Maverick Capital"),
    (r"\bbridgewater\b",                      "Bridgewater Associates"),
    (r"\brenaissance tech\b",                 "Renaissance Technologies"),
    (r"\bcitadel\b",                          "Citadel Advisors"),
    (r"\btwo sigma\b",                        "Two Sigma Investments"),
    (r"\bd[.\s]?e[.\s]? shaw\b",             "D.E. Shaw"),
    (r"\bmillennium man\b",                   "Millennium Management"),
    (r"\btudor investment\b",                 "Tudor Investment"),
    # === NEW additions ===
    # Activist / deep value
    (r"\btrian fund\b",                       "Trian Fund Management (Peltz)"),
    (r"\bstarboard value\b",                  "Starboard Value (Smith)"),
    (r"\bicahn\b",                            "Icahn Capital (Carl Icahn)"),
    (r"\bengine no\.? ?1\b",                  "Engine No. 1"),
    (r"\bcarl c\.? icahn\b",                  "Carl C. Icahn"),
    # Macro
    (r"\bcaxton associates\b",                "Caxton Associates (Kovner)"),
    (r"\bduquesne\b",                         "Duquesne Family Office (Druckenmiller)"),
    (r"\bdiscovery capital\b",                "Discovery Capital (Citrone)"),
    (r"\bbrevan howard\b",                    "Brevan Howard"),
    (r"\belement capital\b",                  "Element Capital"),
    (r"\bbluecrest\b",                        "BlueCrest Capital"),
    (r"\bmoore capital\b",                    "Moore Capital Management"),
    # Quants
    (r"\baqr capital\b",                      "AQR Capital Management"),
    (r"\bworldquant\b",                       "WorldQuant"),
    (r"\bschonfeld\b",                        "Schonfeld Strategic Advisors"),
    (r"\bbalyasny\b",                         "Balyasny Asset Management"),
    (r"\bverition\b",                         "Verition Fund Management"),
    (r"\bjane street\b",                      "Jane Street Group"),
    (r"\bjump trading\b",                     "Jump Trading"),
    # Growth / VC-adjacent
    (r"\bark invest\b",                       "ARK Investment Management"),
    (r"\bt\.? rowe price\b",                  "T. Rowe Price"),
    (r"\bcapital research\b",                 "Capital Research Global"),
    (r"\bcapital world\b",                    "Capital World Investors"),
    (r"\bwellington man\b",                   "Wellington Management"),
    (r"\bjanus henderson\b",                  "Janus Henderson"),
    (r"\bjanus capital\b",                    "Janus Capital"),
    (r"\bpolen capital\b",                    "Polen Capital Management"),
    (r"\bandurand\b",                         "Andurand Capital (Oil)"),
    # Value / activists
    (r"\bgabelli\b",                          "GAMCO / Gabelli"),
    (r"\bmiller value\b",                     "Miller Value Partners (Bill Miller)"),
    (r"\bweitz\b",                            "Weitz Investment Management"),
    (r"\bdavis selected\b",                   "Davis Selected Advisers"),
    (r"\bsequoia fund\b",                     "Sequoia Fund"),
    (r"\byacktman\b",                         "Yacktman Asset Management"),
    (r"\bakre capital\b",                     "Akre Capital Management"),
    (r"\bariel inv\b",                        "Ariel Investments"),
    (r"\bcooperman\b",                        "Cooperman (Omega Advisors)"),
    (r"\bomega advisors\b",                   "Omega Advisors"),
    (r"\btweedy(?:,? browne)?\b",             "Tweedy, Browne"),
    (r"\bfirst eagle\b",                      "First Eagle Investment Mgmt"),
    # Other big names
    (r"\bmarshall wace\b",                    "Marshall Wace"),
    (r"\bdavidson kempner\b",                 "Davidson Kempner"),
    (r"\bfarallon\b",                         "Farallon Capital"),
    (r"\btci fund\b",                         "TCI Fund Management (Hohn)"),
    (r"\begerton capital\b",                  "Egerton Capital"),
    (r"\blansdowne\b",                        "Lansdowne Partners"),
    (r"\bking street capital\b",              "King Street Capital"),
    (r"\banchorage capital\b",                "Anchorage Capital"),
    (r"\bcanyon (?:capital|partners)\b",      "Canyon Capital"),
    (r"\boaktree\b",                          "Oaktree Capital (Marks)"),
    (r"\bappaloosa\b",                        "Appaloosa Management (Tepper)"),
    (r"\bglenmede\b",                         "Glenmede"),
    # Sovereigns / endowments
    (r"\bnorges bank\b",                      "Norges Bank Investment Mgmt (Norway)"),
    # Other Tiger cubs
    (r"\bblue ridge capital\b",               "Blue Ridge Capital"),
    (r"\bhound partners\b",                   "Hound Partners"),
    (r"\blibert(y|us)\b.*?\bcapital\b",      "Libertus / Liberty Capital"),
    (r"\bmaple capital\b",                    "Maple Capital"),
]

ALWAYS_INCLUDE_CIKS = [
    "0001067983",  # Berkshire
    "0001336528",  # Pershing Square
    "0001067187",  # Carl Icahn (Icahn Enterprises)
    "0000813762",  # Icahn Capital LP
    "0001346824",  # ARK
]

def fetch_json(path):
    url = BASE + path + ("&" if "?" in path else "?") + "apikey=" + FMP
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
            timeout=15)
        return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return None


def lambda_handler(event, context):
    out = {}

    # Step 1: discover CIKs from latest 13F filings (up to 12 pages = ~1200 records)
    seen_ciks = {}  # cik -> name
    for page in range(12):
        recs = fetch_json(f"/institutional-ownership/latest?page={page}&limit=100")
        if not isinstance(recs, list) or not recs: break
        for r in recs:
            cik = r.get("cik"); name = r.get("name") or r.get("investorName")
            if cik and name and cik not in seen_ciks:
                seen_ciks[cik] = name
    out["discovered_filers"] = len(seen_ciks)

    # Step 2: match against famous patterns
    compiled = [(re.compile(pat, re.IGNORECASE), label) for pat, label in FAMOUS_PATTERNS]
    matches = []  # (cik, found_name, label)
    for cik, name in seen_ciks.items():
        for rx, label in compiled:
            if rx.search(name):
                matches.append({"cik": cik, "found_name": name, "label": label})
                break
    out["pattern_matches"] = len(matches)

    # Plus always-include CIKs (try them regardless)
    ai_seen = set()
    for cik in ALWAYS_INCLUDE_CIKS:
        if cik not in [m["cik"] for m in matches] and cik not in ai_seen:
            matches.append({"cik": cik, "found_name": "?", "label": "(force-include)"})
            ai_seen.add(cik)

    # Step 3: validate each by calling holder-performance-summary?cik=X
    def validate(m):
        cik = m["cik"]
        d = fetch_json(f"/institutional-ownership/holder-performance-summary?cik={cik}")
        if not isinstance(d, list) or not d:
            return None
        # Sort by date desc and take latest
        d.sort(key=lambda r: r.get("date",""), reverse=True)
        latest = d[0]
        mv = latest.get("marketValue") or 0
        size = latest.get("portfolioSize") or 0
        return {
            "cik": cik,
            "found_name": m["found_name"],
            "label": m["label"],
            "official_name": latest.get("investorName"),
            "date": latest.get("date","")[:10],
            "market_value": mv,
            "market_value_b": round(mv/1e9, 2),
            "portfolio_size": size,
            "added": latest.get("securitiesAdded"),
            "removed": latest.get("securitiesRemoved"),
            "qoq_pct": latest.get("changeInMarketValuePercentage"),
        }

    validated = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for r in ex.map(validate, matches):
            if r:
                validated.append(r)
    out["validated_count"] = len(validated)

    # Sort by market value desc
    validated.sort(key=lambda v: -(v.get("market_value") or 0))
    out["validated"] = validated

    # Categorize: concentrated (active, <500 holdings) vs diversified (index/quant)
    concentrated = [v for v in validated if (v.get("portfolio_size") or 999) <= 500]
    diversified = [v for v in validated if (v.get("portfolio_size") or 0) > 500]
    out["concentrated_count"] = len(concentrated)
    out["diversified_count"] = len(diversified)

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=1024, Timeout=600, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:20000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
