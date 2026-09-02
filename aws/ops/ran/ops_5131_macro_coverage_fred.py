"""ops_5131 -- macro coverage: FRED bank re-count without the 5,000-keys-per-root cap (5129 undercounted).

Khalid pasted a third-party audit of https://justhodl.ai/data.html. It read the
page WITHOUT JavaScript, so it saw only the ECB/FRED retrieval-playbook text and
concluded the data plane is two providers ("data.html only names ECB and FRED").
The hub actually holds 57 providers / 779k datasets. Two of its findings are
still right and are fixed in this push: plumbing.html published stale layer
weights (25/35/25/20/20 = 125% vs the engine's 25/30/15/10/20), and data.html is
not readable without JS (bake_data_inventory.py now injects a static provider
table + JSON blob at Pages build).

This op reconciles the audit's nine requested domains against what the
warehouse REALLY holds -- evidence, not claims:

  * for each domain, a list of concrete series/datasets/providers to look for
    (FRED banked docs with first/last obs, OFR datasets, NY Fed rates, BIS/ECB/
    IMF/OECD flows, FRB DDP packages, FiscalData, Census EITS, BLS surveys,
    PortWatch, TIC, CFTC, Cboe, coinmetrics, national lanes)
  * verdict per domain: HAVE / PARTIAL / MISSING with the gap named and the
    source that would fill it (free official vs licensed)
Writes data/audit/macro-coverage-5131-fred.json and the report. Never RED unless
the hub is unreadable.
"""
import gzip
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1", config=Config(max_pool_connections=32, retries={"max_attempts": 4}))


def get_json(key):
    try:
        b = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if key.endswith(".gz") or b[:2] == b"\x1f\x8b":
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:  # noqa: BLE001
        return None


def head(key):
    try:
        h = s3.head_object(Bucket=B, Key=key)
        return {"bytes": h["ContentLength"], "modified": h["LastModified"].isoformat()[:19]}
    except Exception:  # noqa: BLE001
        return None


def list_keys(prefix, cap=None):
    out, tok = [], None
    while True:
        kw = {"Bucket": B, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        d = s3.list_objects_v2(**kw)
        out.extend(o["Key"] for o in d.get("Contents") or [])
        tok = d.get("NextContinuationToken")
        if not tok or (cap and len(out) >= cap):
            break
    return out


def main():
    with report("5131-macro-coverage-fred") as r:
        r.heading("ops 5131 -- FRED bank coverage of the audit-named series (uncapped listing)")
        hub = get_json("data/provider-catalog.json")
        if not hub:
            r.fail("hub unreadable")
            sys.exit(1)
        provs = {p["slug"]: p for p in hub.get("providers") or []}
        r.section("A. what the auditor could not see")
        r.log(f"hub: {len(provs)} providers, {hub.get('datasets_total'):,} datasets, {hub.get('totals', {}).get('gb')} GB (as_of {hub.get('as_of')})")
        r.log("  audit claim 'data.html only names ECB and FRED' = the page's provider grid is client-rendered; fixed by scripts/bake_data_inventory.py (static table + JSON at Pages build)")
        warm = [c["Prefix"].split("/")[2] for c in s3.list_objects_v2(Bucket=B, Prefix="data/warm/", Delimiter="/").get("CommonPrefixes", [])]
        r.log(f"  warm prefixes ({len(warm)}): {', '.join(warm)}")

        # ---- FRED banked docs: first/last obs for the audit's named series
        r.section("B. FRED banked series named by the audit (first obs / last obs / n)")
        roots = [c["Prefix"] for c in s3.list_objects_v2(Bucket=B, Prefix="data/warm/fred-scoped/", Delimiter="/").get("CommonPrefixes", [])]
        banked = {}
        with ThreadPoolExecutor(16) as ex:
            for objs in ex.map(list_keys, roots):
                for k in objs:
                    base = k.rsplit("/", 1)[-1]
                    if base.endswith(".json") and not base.startswith("_"):
                        banked[base[:-5]] = k
        wanted = {
            "us_liquidity": ["WALCL", "WTREGEN", "RRPONTSYD", "WRESBAL", "RESPPLLOPNWW", "WORAL", "SWPT", "H41RESPPALDKNWW", "WLRRAL", "SOFR", "EFFR", "IORB", "OBFR", "TGCR", "BGCR", "DPCREDIT", "TOTRESNS", "BOGMBASE", "WSHOSHO", "WLCFLPCL"],
            "us_credit": ["BAMLC0A0CM", "BAMLH0A0HYM2", "BAMLEMCBPIOAS", "BAMLC0A4CBBB", "DRTSCILM", "DRTSCIS", "TOTCI", "BUSLOANS", "NFCI", "ANFCI", "STLFSI4", "T10Y2Y", "T10Y3M", "DGS10", "DGS2", "DGS30", "DFII10", "T10YIE", "MORTGAGE30US"],
            "fx_usd": ["DTWEXBGS", "DTWEXAFEGS", "DTWEXEMEGS", "DEXUSEU", "DEXJPUS", "DEXCHUS", "DEXUSUK", "DEXKOUS", "DEXBZUS", "DEXMXUS", "DEXINUS", "DEXSZUS", "VIXCLS", "EVZCLS", "JPYUSD", "USDONTD156N"],
            "global_cb": ["JPNASSETS", "ECBASSETSW", "UKASSETS", "CHNASSETS", "SNBASSETS", "TOTALASSETS", "IRLTLT01DEM156N", "IRLTLT01JPM156N", "INTGSTJPM193N", "IR3TIB01EZM156N", "ECBDFR", "ECBMLFR"],
            "us_manufacturing": ["INDPRO", "IPMAN", "TCU", "MCUMFN", "AMTMNO", "DGORDER", "NEWORDER", "AMTMUO", "MANEMP", "AWHMAN", "UMTMTI", "ISRATIO", "BUSINV", "GACDISA066MSFRBNY", "GACDFSA066MSFRBPHI", "TXPCTDEMSA", "RMFSA", "MNFCTRIRSA"],
            "global_manufacturing": ["JPNPROINDMISMEI", "DEUPROINDMISMEI", "KORPROINDMISMEI", "CHNPRINTO01IXPYM", "EA19PRINTO01GYSAM", "MEXPROINDMISMEI", "BRAPROINDMISMEI", "INDPROINDMISMEI", "OECDPRINTO01GYSAM", "XTEXVA01KRM667S", "XTEXVA01TWM667S", "XTEXVA01CNM667S", "XTEXVA01DEM667S", "XTEXVA01JPM667S", "XTEXVA01MXM667S", "XTEXVA01VNM667S"],
            "em_frontier": ["TEDRATE", "EMVOVERALLEMV", "VXEEMCLS", "DEXBZUS", "DEXINUS", "DEXSFUS", "DEXTHUS", "DEXMAUS", "DEXKOUS", "RBBRBIS", "RBINBIS", "RBMXBIS", "RBZABIS", "RBTRBIS", "RBIDBIS", "TRESEGCHM052N", "TRESEGBRM052N", "TRESEGINM052N", "TRESEGMXM052N"],
        }
        cov = {}

        def obs_span(sid):
            k = banked.get(sid)
            if not k:
                return sid, None
            try:
                j = json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
                obs = [o for o in (j.get("observations") or []) if o.get("value") not in (None, ".", "")]
                return sid, {"first": obs[0]["date"] if obs else None, "last": obs[-1]["date"] if obs else None, "n": len(obs)}
            except Exception as e:  # noqa: BLE001
                return sid, {"error": str(e)[:60]}
        with ThreadPoolExecutor(24) as ex:
            spans = dict(ex.map(obs_span, sorted({s for v in wanted.values() for s in v})))
        for dom, ids in wanted.items():
            have = [s for s in ids if spans.get(s)]
            miss = [s for s in ids if not spans.get(s)]
            cov[dom] = {"have": {s: spans[s] for s in have}, "missing_from_fred_bank": miss}
            r.log(f"  {dom:<20} banked {len(have)}/{len(ids)}: " + ", ".join(f"{s}({(spans[s] or {}).get('first', '?')[:4]}→{(spans[s] or {}).get('last', '?')[:7]})" for s in have[:9]) + (" …" if len(have) > 9 else ""))
            if miss:
                r.log(f"      not banked: {', '.join(miss)}")

        for dom, c in cov.items():
            r.kv(domain=dom, banked=len(c["have"]), missing=len(c["missing_from_fred_bank"]))
        ev, verdict, claims = {"banked_total": len(banked)}, {}, []
        r.log(f"  fred-scoped banked total (uncapped): {len(banked):,}")
        s3.put_object(Bucket=B, Key="data/audit/macro-coverage-5131-fred.json", Body=json.dumps({"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "fred": cov, "evidence": ev, "verdict": verdict, "claims": claims}, default=str).encode(),
                      ContentType="application/json", CacheControl="max-age=300")
        r.ok("coverage matrix written: data/audit/macro-coverage-5131-fred.json")


if __name__ == "__main__":
    main()
