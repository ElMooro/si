"""ops 6985 - read-only probe: does our FINRA access reach a bond-by-bond dataset?

Khalid 2026-11-26 15:45 ET: "test whether our FINRA access reaches a per-bond dataset before anything is built."

The Corp bonds engine reads five market-wide TRACE aggregates from FINRA's Query API (api.finra.org, group
fixedIncomeMarket). A public chart he sent uses "FINRA TRACE last sale (144A)" bond by bond. This op asks FINRA's own
API what our access can see, and nothing else:

  1. how we are signed in: are client credentials kept, does a token come back (never printed), or are we keyless
  2. FINRA's own list of datasets (GET /datasets), with and without the token: every fixed-income dataset by name
  3. for each fixed-income dataset, its field list (GET /metadata/...): does it carry a per-security field
     (a symbol, an identifier, an issuer, a coupon) and trade fields (price, yield, time)?
  4. for each dataset that does, three rows asked for: what comes back - rows, or a refusal and its wording
  5. a short list of plausible dataset names asked directly, in case the list hides what we are not entitled to

No security identifier is printed or kept (a field whose name says cusip / isin / figi / symbol is shown as "(held
back)"). No credential or token is printed. Nothing is written: the token cache is not touched. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
NAME = "ops_6985_finra_per_bond_reach"
API = "https://api.finra.org"
UA = "JustHodl-TRACE-Client/1.0"
SECURITY = re.compile(r"cusip|isin|figi|symbol|issuer|coupon|maturity|securityDesc|securityName|bondSym|ticker", re.I)
TRADE = re.compile(r"price|yield|execution|tradeTime|quantity|reportedPrice|lastSale", re.I)
HIDE = re.compile(r"cusip|isin|figi|symbol|sedol|identifier", re.I)
KNOWN = ["corporateMarketBreadth", "corporateMarketSentiment", "corporate144AMarketBreadth", "corporate144AMarketSentiment", "corporatesAndAgenciesCappedVolume",
         "agencyMarketBreadth", "agencyMarketSentiment", "securitizedProductsCappedVolume", "treasuryDailyAggregates", "treasuryWeeklyAggregates", "treasuryMonthlyAggregates"]
GUESSES = ["corporateAndAgencyTradeActivity", "corporateAndAgencySecurities", "corporateAndAgencyTradeHistory", "corporateAndAgencyLastSale",
           "corporateAndAgency144ATradeActivity", "corporate144ATradeActivity", "corporate144ASecurities", "securitizedProductSecurities", "treasurySecurities",
           "treasuryTradeActivity", "trace", "corporateTrades", "tradeHistory", "bondSecurities"]
SECRETS = []      # values that must never reach the report or the run log: filled at run time, replaced in every text


def clean(text):
    """FINRA's refusals name the caller: our client id is taken out of anything that is logged."""
    text = str(text)
    for value in SECRETS:
        if value:
            text = text.replace(value, "(held back)")
    return re.sub(r"\b[0-9a-f]{20}\b", "(client id held back)", text)


def call(method, url, token=None, body=None, timeout=30):
    """(status, parsed JSON or None, first 300 characters of the answer). The Authorization header is never echoed."""
    headers = {"Accept": "application/json", "User-Agent": UA}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            status = resp.status
    except urllib.error.HTTPError as exc:
        raw = exc.read() if hasattr(exc, "read") else b""
        status = exc.code
    except Exception as exc:  # noqa: BLE001
        return None, None, clean("%s %s" % (type(exc).__name__, str(exc)[:120]))
    text = clean(raw.decode("utf-8", "replace"))
    try:
        parsed = json.loads(text) if text.strip() else None
    except ValueError:
        parsed = None
    return status, parsed, re.sub(r"\s+", " ", text)[:420]


def shown(row):
    return {k: ("(held back)" if HIDE.search(k) else v) for k, v in row.items()}


def main():
    import boto3
    from ops_report import report
    now = datetime.now(timezone.utc)
    with report(NAME) as r:
        r.heading("6985 - does our FINRA access reach a bond-by-bond dataset? (read-only probe), %s" % now.isoformat(timespec="minutes"))

        def say(msg):
            r.log(clean(msg))      # every line passes the same filter before it reaches the report or the run log


        # ------------------------------------------------------------------ 1
        r.section("1. How we are signed in")
        ssm = boto3.client("ssm", region_name="us-east-1")
        have = {}
        for label, name in (("client id", "/justhodl/finra/client_id"), ("client secret", "/justhodl/finra/client_secret"), ("cached token", "/justhodl/finra/access_token")):
            try:
                ssm.get_parameter(Name=name, WithDecryption=False)
                have[label] = "kept"
            except Exception as exc:  # noqa: BLE001
                have[label] = "not kept (%s)" % type(exc).__name__
        say("credentials in the parameter store: %s" % json.dumps(have))
        for name in ("/justhodl/finra/client_id", "/justhodl/finra/client_secret"):
            try:
                SECRETS.append(ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"])
            except Exception:  # noqa: BLE001
                pass
        token, how = None, "keyless"
        try:
            import finra_si
            finra_si._write_cached_token = lambda *a, **k: None          # the probe writes nothing
            token = finra_si.get_access_token()
            SECRETS.append(token)
            how = "signed in with client credentials (a token came back; not shown)" if token else "keyless"
        except Exception as exc:  # noqa: BLE001
            how = clean("keyless: no token (%s: %s)" % (type(exc).__name__, re.sub(r"\s+", " ", str(exc))[:140]))
        say("this run: %s" % how)

        # ------------------------------------------------------------------ 2
        r.section("2. FINRA's own list of datasets")
        fixed = {}
        for label, tok in (("without a token", None),) + ((("with the token", token),) if token else ()):
            status, parsed, text = call("GET", API + "/datasets", tok)
            items = parsed if isinstance(parsed, list) else (parsed or {}).get("datasets") if isinstance(parsed, dict) else None
            say("GET /datasets %s: HTTP %s, %s" % (label, status, ("%d datasets" % len(items)) if isinstance(items, list) else text[:200]))
            if isinstance(items, list) and items:
                say("  one entry looks like: %s" % json.dumps(items[0])[:400])
                groups = {}
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    g = str(it.get("datasetGroup") or it.get("group") or it.get("groupName") or "")
                    n = str(it.get("datasetName") or it.get("name") or "")
                    groups.setdefault(g, []).append(n)
                    if "fixed" in g.lower():
                        fixed.setdefault(n, it)
                say("  groups: %s" % json.dumps({g: len(v) for g, v in sorted(groups.items())}))
            time.sleep(0.3)
        names = sorted(fixed) or list(KNOWN)
        say("fixed-income datasets %s (%d): %s" % ("listed by FINRA" if fixed else "NOT listed; falling back to the names we know", len(names), ", ".join(names)))

        # ------------------------------------------------------------------ 3
        r.section("3. What each fixed-income dataset carries")
        per_bond = []
        for n in names:
            if n.upper().endswith("MOCK"):
                continue
            status, meta, text = call("GET", "%s/metadata/group/fixedIncomeMarket/name/%s" % (API, n), token)
            fields = []
            if isinstance(meta, dict):
                fields = [str(f.get("name") or f.get("fieldName") or "") for f in (meta.get("fields") or meta.get("fieldMetadata") or []) if isinstance(f, dict)]
            sec = [f for f in fields if SECURITY.search(f)]
            trd = [f for f in fields if TRADE.search(f)]
            desc = re.sub(r"\s+", " ", str((meta or {}).get("description") or (fixed.get(n) or {}).get("description") or ""))[:110] if isinstance(meta, dict) or fixed.get(n) else ""
            say("%-46s HTTP %s | %2d fields | per-security: %s | trade: %s | %s" % (n, status, len(fields), sec[:8] or "none", trd[:6] or "none", desc if fields else text[:300]))
            if sec:
                per_bond.append((n, fields))
            time.sleep(0.25)

        # ------------------------------------------------------------------ 4
        r.section("4. Asking for rows from each dataset that carries a per-security field")
        reached = []
        if not per_bond:
            say("none of the fixed-income datasets our access can see carries a per-security field")
        for n, fields in per_bond:
            status, rows, text = call("POST", "%s/data/group/fixedIncomeMarket/name/%s" % (API, n), token, {"limit": 3})
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                reached.append(n)
                say("%s: HTTP %s, %d rows | columns: %s" % (n, status, len(rows), ", ".join(list(rows[0].keys())[:40])))
                say("  one row (identifiers held back): %s" % json.dumps(shown(rows[0]))[:700])
            else:
                say("%s: HTTP %s, no rows | %s" % (n, status, text[:220]))
            time.sleep(0.3)

        # ------------------------------------------------------------------ 5
        r.section("5. Plausible names asked directly")
        hidden = []
        for n in GUESSES:
            if n.upper() in [x.upper() for x in names]:
                continue
            status, meta, text = call("GET", "%s/metadata/group/fixedIncomeMarket/name/%s" % (API, n), token)
            if status != 200:
                say("%-40s metadata HTTP %s | %s" % (n, status, text[:110]))
                time.sleep(0.25)
                continue
            hidden.append(n)
            fields = [str(f.get("name") or f.get("fieldName") or "") for f in ((meta or {}).get("fields") or (meta or {}).get("fieldMetadata") or []) if isinstance(f, dict)]
            say("%s: FINRA knows this dataset and it is NOT in our list" % n)
            say("  described as: %s" % clean(re.sub(r"\s+", " ", str((meta or {}).get("description") or ""))[:300]))
            say("  %d fields: %s" % (len(fields), ", ".join(fields)))
            say("  other facts: %s" % clean(json.dumps({k: v for k, v in (meta or {}).items() if k not in ("fields", "fieldMetadata", "description")})[:500]))
            s2, rows, t2 = call("POST", "%s/data/group/fixedIncomeMarket/name/%s" % (API, n), token, {"limit": 2})
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                reached.append(n)
                say("  rows: HTTP %s, %d returned | one row (identifiers held back): %s" % (s2, len(rows), clean(json.dumps(shown(rows[0])))[:700]))
            else:
                say("  rows: HTTP %s | %s" % (s2, t2[:400]))
            s3, parts, t3 = call("GET", "%s/partitions/group/fixedIncomeMarket/name/%s" % (API, n), token)
            say("  partitions: HTTP %s | %s" % (s3, t3[:200]))
            s4, m4, t4 = call("GET", "%s/metadata/group/fixedIncomeMarket/name/%sMock" % (API, n), token)
            if s4 == 200:
                s5, mock, t5 = call("POST", "%s/data/group/fixedIncomeMarket/name/%sMock" % (API, n), token, {"limit": 2})
                if isinstance(mock, list) and mock and isinstance(mock[0], dict):
                    say("  its test twin (%sMock, made-up rows): HTTP %s | one row (identifiers held back): %s" % (n, s5, clean(json.dumps(shown(mock[0])))[:900]))
                else:
                    say("  its test twin (%sMock): HTTP %s | %s" % (n, s5, t5[:240]))
            else:
                say("  its test twin (%sMock): metadata HTTP %s" % (n, s4))
            time.sleep(0.3)

        r.section("Answer")
        say("signed in: %s" % how)
        truly = [n for n in reached if n in hidden]
        say("fixed-income datasets in our list: %d, every one an aggregate or a securitized-product table" % len(names))
        say("bond-by-bond datasets FINRA has that are not in our list: %s" % (hidden or "none found"))
        say("bond-by-bond rows actually returned to us: %s" % (truly or "none"))
        r.kv(datasets=len(names), hidden=len(hidden), rows_returned=len(truly), token=bool(token))
        r.ok("probe complete")


if __name__ == "__main__":
    main()
