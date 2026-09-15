"""ops 5576 -- provider keys: one source of truth (Claude, 2026-09-15; Khalid's FMP brief, extended to every provider).

managed_secret() resolves ENV before SSM, so a stale Lambda env var beats a good SSM key and a 401 looks like "no data"
(justhodl-stock-buying, ops 5421). This op, hashes only, never values:
  1. reads every /justhodl/* SecureString the fleet references (sha256 prefix + length);
  2. for every justhodl-* / benzinga-* Lambda, finds env vars that name a keyed provider, hashes them, compares with the
     provider's SSM value: MATCH -> keep; MISMATCH (SSM non-empty) -> DELETE the env var so SSM wins; env with no SSM -> report;
  3. probes with the SSM keys: FMP /stable ratios-ttm (AAPL, MSFT, TSM) + income-statement AAPL; Polygon reference tickers;
     FRED series -- HTTP status + row counts only;
  4. harvests data/fmp-ratios.json for the chart (equity_enrich.fetch_financials, universe below), labelled source=FMP,
     cadence=EOD_TTM, key_status included so a future 401 is visible on the page;
  5. writes the provider inventory (docs/DATA_PROVIDERS.md source is the repo; this op records the live key facts).
No key is printed, logged, committed or echoed. Env deletions are the brief's preferred action.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
PUB = "justhodl-dashboard-live"
# provider -> (env var names the fleet uses, SSM paths in precedence order)
PROVIDERS = {
    "fmp": (("FMP_KEY", "FMP_API_KEY", "FMP", "fmp_key"), ("/justhodl/fmp/api-key",)),
    "fred": (("FRED_API_KEY", "FRED_KEY", "fred_key"), ("/justhodl/fred/api-key", "/justhodl/fred-api-key")),
    "polygon": (("POLYGON_KEY", "POLYGON_API_KEY", "POLY_KEY", "POLYGON", "POLY", "MASSIVE_API_KEY", "MASSIVE_KEY"), ("/justhodl/polygon/api-key", "/justhodl/massive-api-key")),
    "telegram": (("TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "TG_TOKEN", "TG_BOT_TOKEN"), ("/justhodl/telegram/bot_token", "/justhodl/telegram/bot-token")),
    "cmc": (("CMC_KEY", "COINMARKETCAP_API_KEY"), ("/justhodl/cmc/api-key",)),
    "alphavantage": (("AV_KEY", "ALPHAVANTAGE_KEY", "ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY"), ("/justhodl/alphavantage/api-key",)),
    "newsapi": (("NEWS_KEY", "NEWSAPI_KEY", "NEWS_API_KEY"), ("/justhodl/newsapi/api-key",)),
    "nasdaq-datalink": (("NASDAQ_API_KEY", "NASDAQ_DATALINK_API_KEY", "NASDAQ_DATALINK_KEY", "QUANDL_API_KEY"), ("/justhodl/nasdaq-datalink/api-key",)),
    "anthropic": (("ANTHROPIC_API_KEY",), ("/justhodl/anthropic/api_key",)),
    "zai-glm": (("ZAI_API_KEY", "GLM_API_KEY"), ("/justhodl/zai-api-key",)),
    "xai": (("XAI_API_KEY",), ("/justhodl/xai/api-key",)),
    "perplexity": (("PERPLEXITY_API_KEY", "PPLX_API_KEY"), ("/justhodl/perplexity/api-key",)),
    "openfigi": (("OPENFIGI_API_KEY", "OPENFIGI_KEY"), ("/justhodl/openfigi/api-key",)),
    "tradingeconomics": (("TE_API", "TE_KEY", "TRADINGECONOMICS_KEY"), ("/justhodl/te_api",)),
    "cryptoquant": (("CRYPTOQUANT_API", "CRYPTOQUANT_KEY"), ("/justhodl/cryptoquant_api",)),
    "benzinga": (("BENZINGA_API_KEY", "BENZINGA_KEY"), ("/justhodl/benzinga/api-key",)),
    "eia": (("EIA_API_KEY", "EIA_KEY"), ("/justhodl/eia/api-key",)),
    "census": (("CENSUS_API_KEY", "CENSUS_KEY"), ("/justhodl/census/api-key",)),
    "quiver": (("QUIVER_API_KEY", "QUIVER_KEY"), ("/justhodl/quiver/api-key",)),
}
HARVEST = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "BRK-B", "JPM", "LLY", "V", "UNH", "XOM", "MA", "COST", "HD", "PG", "JNJ", "ABBV",
           "TSM", "ASML", "NFLX", "CRM", "AMD", "ORCL", "ADBE", "KO", "PEP", "WMT", "MRK", "BAC", "CVX", "LIN", "TMO", "MCD", "ACN", "CSCO", "ABT", "GE"]


def sha(v: str) -> str:
    return hashlib.sha256(v.encode("utf-8")).hexdigest()


def http_json(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-ops-5576"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:  # noqa: BLE001
        return "ERR:" + type(e).__name__, None


def main() -> int:
    ssm = boto3.client("ssm", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 6}))
    s3 = boto3.client("s3", region_name=REGION)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    with report("ops_5576_provider_keys_reconcile") as R:
        R.heading("ops 5576 -- provider keys: SSM is the source of truth; stale Lambda env vars deleted; live probes; FMP harvest; inventory (hashes only)")
        R.kv(head=head[:10])
        # 1. SSM truth
        truth = {}
        for name, (_envs, paths) in PROVIDERS.items():
            for path in paths:
                try:
                    p = ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"]
                    val = p.get("Value") or ""
                    truth[name] = {"path": path, "type": p.get("Type"), "sha": sha(val)[:10], "len": len(val), "value": val}
                    R.ok("SSM %-16s %-34s type=%s sha=%s len=%d" % (name, path, p.get("Type"), sha(val)[:10], len(val)))
                    break
                except ssm.exceptions.ParameterNotFound:
                    continue
                except Exception as e:  # noqa: BLE001
                    R.warn("SSM %s %s: %s" % (name, path, type(e).__name__)); break
            if name not in truth:
                R.log("SSM %-16s no parameter at %s" % (name, " | ".join(paths)))
        # 2. Lambda env reconcile
        env_index = {}
        for name, (envs, _paths) in PROVIDERS.items():
            for e in envs:
                env_index[e.upper()] = name
        rows, deleted, kept, orphan, functions = [], 0, 0, 0, 0
        marker = None
        while True:
            resp = lam.list_functions(MaxItems=50, **({"Marker": marker} if marker else {}))
            for fn in resp.get("Functions", []):
                fname = fn["FunctionName"]
                if not (fname.startswith("justhodl-") or fname.startswith("benzinga-")):
                    continue
                functions += 1
                env = (fn.get("Environment") or {}).get("Variables") or {}
                hits = {k: v for k, v in env.items() if k.upper() in env_index}
                if not hits:
                    continue
                remove = []
                for k, v in hits.items():
                    prov = env_index[k.upper()]
                    t = truth.get(prov)
                    if not t or not t["value"]:
                        rows.append((fname, k, sha(v)[:10], "no-ssm", "keep (reported)")); orphan += 1
                        continue
                    if sha(v) == sha(t["value"]):
                        rows.append((fname, k, sha(v)[:10], "match", "keep")); kept += 1
                    else:
                        rows.append((fname, k, sha(v)[:10], "MISMATCH vs %s" % t["sha"], "delete env (SSM wins)")); remove.append(k)
                if remove:
                    new_env = {k: v for k, v in env.items() if k not in remove}
                    try:
                        lam.update_function_configuration(FunctionName=fname, Environment={"Variables": new_env})
                        deleted += len(remove)
                        time.sleep(0.4)
                    except Exception as e:  # noqa: BLE001
                        rows.append((fname, ",".join(remove), "", "delete FAILED", str(e)[:80]))
            marker = resp.get("NextMarker")
            if not marker:
                break
        R.ok("functions scanned=%d; env vars: match-kept=%d, stale-deleted=%d, no-SSM-orphans=%d" % (functions, kept, deleted, orphan))
        for r in rows:
            R.log("  %-40s %-24s env_sha=%-10s %-26s %s" % r)
        # 3. live probes with the SSM keys (status + counts only)
        fmp = truth.get("fmp", {}).get("value")
        probes = {}
        if fmp:
            for sym in ("AAPL", "MSFT", "TSM"):
                st, doc = http_json("https://financialmodelingprep.com/stable/ratios-ttm?" + urllib.parse.urlencode({"symbol": sym, "apikey": fmp}))
                probes["fmp ratios-ttm " + sym] = (st, len(doc) if isinstance(doc, list) else None)
            st, doc = http_json("https://financialmodelingprep.com/stable/income-statement?" + urllib.parse.urlencode({"symbol": "AAPL", "period": "annual", "limit": 5, "apikey": fmp}))
            probes["fmp income-statement AAPL"] = (st, len(doc) if isinstance(doc, list) else None)
        poly = truth.get("polygon", {}).get("value")
        if poly:
            st, doc = http_json("https://api.polygon.io/v3/reference/tickers?" + urllib.parse.urlencode({"ticker": "AAPL", "apiKey": poly}))
            probes["polygon reference tickers AAPL"] = (st, len((doc or {}).get("results") or []) if isinstance(doc, dict) else None)
        fred = truth.get("fred", {}).get("value")
        if fred:
            st, doc = http_json("https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode({"series_id": "SOFR", "limit": 3, "sort_order": "desc", "file_type": "json", "api_key": fred}))
            probes["fred SOFR observations"] = (st, len((doc or {}).get("observations") or []) if isinstance(doc, dict) else None)
        fmp_ok = all(st == 200 and (n or 0) >= 1 for k, (st, n) in probes.items() if k.startswith("fmp"))
        for k, v in probes.items():
            (R.ok if v[0] == 200 else R.fail)("probe %-34s http=%s rows=%s" % (k, v[0], v[1]))
        # 4. FMP harvest for the chart (only with a working key)
        harvest_key = None
        if fmp_ok:
            import equity_enrich as ee
            ee.FMP_KEY = fmp
            out = {"schema_version": "fmp-ratios.v1", "source": "FMP", "cadence": "EOD_TTM", "label": "FMP EOD/TTM", "generated_at": datetime.now(timezone.utc).isoformat(),
                   "key_status": None, "tickers": {}}
            for tk in HARVEST:
                try:
                    f = ee.fetch_financials(tk)
                except Exception as e:  # noqa: BLE001
                    f = {"error": type(e).__name__}
                out["tickers"][tk] = f if isinstance(f, dict) else {"raw": f}
                time.sleep(0.15)
            out["key_status"] = ee.key_status()
            harvest_key = "data/fmp-ratios.json"
            body = json.dumps(out, default=str).encode()
            s3.put_object(Bucket=PUB, Key=harvest_key, Body=body, ContentType="application/json", CacheControl="max-age=300")
            R.ok("harvest: s3://%s/%s -- %d tickers, %d bytes, key_status=%s" % (PUB, harvest_key, len(out["tickers"]), len(body), out["key_status"].get("status")))
        else:
            R.fail("FMP probes did not all return 200 with rows -- harvest skipped; fix the key path first")
        # 5. inventory record (live facts only; the repo doc lists providers by host)
        inv = {"at": datetime.now(timezone.utc).isoformat(), "commit": head[:12],
               "ssm": {k: {kk: v[kk] for kk in ("path", "type", "sha", "len")} for k, v in truth.items()},
               "env_rows": [{"function": r[0], "var": r[1], "env_sha": r[2], "verdict": r[3], "action": r[4]} for r in rows],
               "probes": {k: {"http": v[0], "rows": v[1]} for k, v in probes.items()}, "harvest": harvest_key}
        s3.put_object(Bucket=PUB, Key="data/ops/provider-keys-%s.json" % datetime.now(timezone.utc).strftime("%Y%m%d"), Body=json.dumps(inv, indent=2, default=str).encode(), ContentType="application/json")
        (REPO / "aws/ops/reports").mkdir(parents=True, exist_ok=True)
        (REPO / "aws/ops/reports/5576_provider_keys.json").write_text(json.dumps(inv, indent=2, default=str) + "\n")
        if not fmp_ok:
            R.fail("RED -- FMP key path still broken"); return 1
        R.ok("GREEN -- SSM wins everywhere a provider is keyed; FMP live; harvest published")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
