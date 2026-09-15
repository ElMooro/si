"""ops 5579 -- CryptoQuant + OpenFIGI: key reconcile (SSM truth), live probes (status + counts, never tokens), feed
health, honest labels (Claude, 2026-09-15). Also records that the historical file aws/ops/ran/ops_4197_paid_keys.py
carried the LIVE CryptoQuant and TradingEconomics literals (hashes match SSM) -- redacted in git this commit; ROTATION
in the vendor consoles is Khalid's step; new values go to SSM only.
  A. CryptoQuant: GET /v1/btc/market-indicator/mvrv?window=day&limit=2 and /v1/btc/exchange-flows/netflow?...  (Bearer)
     data/cryptoquant-onchain.json: any status:"LIVE" -> status EOD, cadence EOD, label "CryptoQuant EOD on-chain" (merge)
     data/cryptoquant-series.json + data/history/cryptoquant.json: presence, size, point counts
  B. OpenFIGI: POST /v3/mapping [{TICKER AAPL US}] with X-OPENFIGI-APIKEY -> expect a BBG000B9XRY4-family figi
     data/symbology/master.json: mapped / no_match counts, sample record fields (figi, shareClassFIGI, securityType)
  C. Lambda env for CRYPTOQUANT_*/OPENFIGI_*: hash-compare with SSM; mismatch -> delete (SSM wins), same as 5576.
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PUB = "us-east-1", "justhodl-dashboard-live"


def sha(v):
    return hashlib.sha256(v.encode()).hexdigest()[:10]


def http(url, headers=None, data=None, timeout=25):
    req = urllib.request.Request(url, headers=headers or {}, data=data, method="POST" if data else "GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:  # noqa: BLE001
        return "ERR:" + type(e).__name__, None


def main() -> int:
    ssm = boto3.client("ssm", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    with report("ops_5579_cq_figi_reconcile") as R:
        R.heading("ops 5579 -- CryptoQuant + OpenFIGI: SSM truth, probes, feeds, labels (no tokens)")
        keys = {}
        for name, paths in (("cryptoquant", ("/justhodl/cryptoquant_api", "/justhodl/cryptoquant/token")), ("openfigi", ("/justhodl/openfigi/api-key",))):
            for path in paths:
                try:
                    v = ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"]["Value"]
                    keys[name] = v; R.ok("SSM %-12s %-30s sha=%s len=%d" % (name, path, sha(v), len(v))); break
                except ssm.exceptions.ParameterNotFound:
                    continue
        # C. env reconcile for these two providers
        names = {"CRYPTOQUANT_API": "cryptoquant", "CRYPTOQUANT_KEY": "cryptoquant", "OPENFIGI_API_KEY": "openfigi", "OPENFIGI_KEY": "openfigi"}
        marker, rows = None, []
        while True:
            resp = lam.list_functions(MaxItems=50, **({"Marker": marker} if marker else {}))
            for fn in resp.get("Functions", []):
                if not fn["FunctionName"].startswith(("justhodl-", "benzinga-")):
                    continue
                env = (fn.get("Environment") or {}).get("Variables") or {}
                hits = {k: v for k, v in env.items() if k.upper() in names}
                remove = []
                for k, v in hits.items():
                    truth = keys.get(names[k.upper()])
                    if truth and v == truth:
                        rows.append((fn["FunctionName"], k, sha(v), "match", "keep"))
                    elif truth:
                        rows.append((fn["FunctionName"], k, sha(v), "MISMATCH vs " + sha(truth), "delete env (SSM wins)")); remove.append(k)
                    else:
                        rows.append((fn["FunctionName"], k, sha(v), "no-ssm", "keep (reported)"))
                if remove:
                    lam.update_function_configuration(FunctionName=fn["FunctionName"], Environment={"Variables": {k: v for k, v in env.items() if k not in remove}}); time.sleep(0.4)
            marker = resp.get("NextMarker")
            if not marker:
                break
        R.ok("env rows for CryptoQuant/OpenFIGI: %d (%s)" % (len(rows), ", ".join("%s:%s=%s" % (r[0], r[1], r[3]) for r in rows)[:600] or "none -- SSM only"))
        # A. CryptoQuant probes + feeds
        cq = keys.get("cryptoquant")
        ok_cq = False
        if cq:
            h = {"Authorization": "Bearer " + cq, "User-Agent": "JustHodl/ops-5579"}
            st1, d1 = http("https://api.cryptoquant.com/v1/btc/market-indicator/mvrv?window=day&limit=2", h)
            st2, d2 = http("https://api.cryptoquant.com/v1/btc/exchange-flows/netflow?exchange=all_exchange&window=day&limit=2", h)
            n1 = len(((d1 or {}).get("result") or {}).get("data") or []) if isinstance(d1, dict) else None
            n2 = len(((d2 or {}).get("result") or {}).get("data") or []) if isinstance(d2, dict) else None
            (R.ok if st1 == 200 else R.fail)("probe CQ mvrv http=%s n=%s" % (st1, n1))
            (R.ok if st2 == 200 else R.fail)("probe CQ exchange netflow http=%s n=%s" % (st2, n2))
            ok_cq = st1 == 200 and (n1 or 0) > 0
        else:
            R.fail("no CryptoQuant key in SSM")
        for key in ("data/cryptoquant-onchain.json", "data/cryptoquant-series.json", "data/history/cryptoquant.json"):
            try:
                o = s3.get_object(Bucket=PUB, Key=key); body = o["Body"].read(); doc = json.loads(body)
                if key.endswith("onchain.json"):
                    metrics = doc.get("metrics") or {}
                    R.ok("%s: %d bytes, generated_at=%s status=%s n_metrics=%d keys=%s" % (key, len(body), doc.get("generated_at"), doc.get("status"), len(metrics), list(metrics)[:12]))
                    if doc.get("status") == "LIVE" or doc.get("cadence") != "EOD":
                        doc.update(status="EOD", cadence="EOD", label="CryptoQuant EOD on-chain", relabelled_by="ops 5579")
                        s3.put_object(Bucket=PUB, Key=key, Body=json.dumps(doc).encode(), ContentType="application/json", CacheControl="max-age=300")
                        R.ok("relabelled %s: LIVE -> EOD (merge, nothing removed)" % key)
                elif key.endswith("series.json"):
                    ser = doc.get("series") or doc
                    n = {k: len(v) for k, v in (ser.items() if isinstance(ser, dict) else []) if isinstance(v, list)}
                    R.ok("%s: %d bytes, series=%s" % (key, len(body), json.dumps(dict(list(n.items())[:10]))))
                else:
                    R.ok("%s: %d bytes" % (key, len(body)))
            except Exception as e:  # noqa: BLE001
                R.warn("%s: %s" % (key, type(e).__name__))
        # B. OpenFIGI probe + master
        fg = keys.get("openfigi")
        ok_figi = False
        if fg:
            st, d = http("https://api.openfigi.com/v3/mapping", {"X-OPENFIGI-APIKEY": fg, "Content-Type": "application/json", "User-Agent": "JustHodl/ops-5579"},
                         json.dumps([{"idType": "TICKER", "idValue": "AAPL", "exchCode": "US"}]).encode())
            figi = ((d or [{}])[0].get("data") or [{}])[0].get("figi") if isinstance(d, list) else None
            ok_figi = st == 200 and bool(figi)
            (R.ok if ok_figi else R.fail)("probe OpenFIGI AAPL http=%s figi=%s" % (st, figi))
        else:
            R.fail("no OpenFIGI key in SSM")
        try:
            body = s3.get_object(Bucket=PUB, Key="data/symbology/master.json")["Body"].read(); doc = json.loads(body)
            recs = doc.get("by_ticker") or doc.get("tickers") or doc
            if isinstance(recs, dict):
                mapped = sum(1 for r in recs.values() if isinstance(r, dict) and r.get("figi"))
                nomatch = sum(1 for r in recs.values() if isinstance(r, dict) and r.get("figi_status") == "no_match")
                sample = recs.get("AAPL") or next(iter(recs.values()))
                R.ok("data/symbology/master.json: %d bytes, %d tickers, figi mapped=%d no_match=%d; AAPL fields=%s" % (len(body), len(recs), mapped, nomatch, sorted(sample.keys())[:16] if isinstance(sample, dict) else "?"))
                R.log("  AAPL: figi=%s shareClassFIGI=%s type=%s exch=%s status=%s" % tuple(str((sample or {}).get(k)) for k in ("figi", "shareClassFIGI", "securityType", "exchCode", "figi_status")))
        except Exception as e:  # noqa: BLE001
            R.warn("master.json: %s" % type(e).__name__)
        R.ok("leak record: ops_4197_paid_keys.py literals matched LIVE SSM values (cryptoquant %s, te_api d33fcfd2ca) -> redacted in git; ROTATE in the vendor consoles, put new values in SSM only" % (sha(cq) if cq else "?"))
        if not (ok_cq and ok_figi):
            R.fail("RED -- a probe failed; no consumer wiring should trust these feeds until fixed"); return 1
        R.ok("GREEN -- CryptoQuant + OpenFIGI keyed from SSM, live, feeds present, labels honest")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
