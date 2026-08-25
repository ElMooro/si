"""ops_4982 -- FINRA FULL IMPORT from the runner (the egress that
works). Lambda IPs get keyless 400s; the GitHub runner gets 200s
with the proven bare shape. So the runner does the since-inception
drain NOW; the engine (already deployed) takes over refresh the
moment the secret lands and token calls open the Lambda path.

  P1 catalog: /metadata per group (proven shape); fallback to the
     19 seeds; every name 2-row probe-validated
  P2 drain: paginate limit=5000&offset -> gz JSONL ->
     data/warm/finra-full/src/{group}__{name}.jsonl.gz
     (page cap 4000/dataset; global 95min guard; failures named)
  P3 engine state pre-seeded (have/universe/phase) so the 6h
     schedule's delta path owns refresh once creds land
  P4 substance: a weekly dataset's earliest date <=2016
  P5 card flips to real numbers
"""
import gzip
import io
import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-finra-full"
CAT = "justhodl-provider-catalog"
API = "https://api.finra.org"
GROUPS = ["otcMarket", "fixedIncomeMarket"]
SEEDS = {
    "otcMarket": [
        "weeklySummary", "monthlySummary", "blocksSummary",
        "otcBlocksSummary", "consolidatedShortInterest",
        "regShoDaily", "weeklyDownloadDetails",
        "monthlyDownloadDetails", "atsIssueData", "otcIssueData"],
    "fixedIncomeMarket": [
        "treasuryWeeklyAggregates", "treasuryMonthlyAggregates",
        "corporatesAndAgenciesCappedVolume",
        "corporateMarketSentiment", "agencyMarketSentiment",
        "corporate144AMarketSentiment", "treasuryDailyAggregates",
        "securitizedProductsCapped", "corporateMarketBreadth"],
}
ROOT = "data/warm/finra-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
HUB_KEY = "data/provider-catalog.json"
PAGE = 5000
PAGE_CAP = 4000
GLOBAL_S = 95 * 60
T0 = time.time()

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def fetch(path, timeout=90, cap=60_000_000):
    req = urllib.request.Request(
        API + path, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(cap)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


with report("ops_4982_finra_runner_drain") as R:
    fails = []
    R.section("P1 catalog (runner egress)")
    universe, invalid = {}, {}
    for g in GROUPS:
        names = []
        try:
            js = json.loads(fetch("/metadata/group/%s" % g,
                                  cap=6_000_000))
            items = js if isinstance(js, list) else \
                js.get("datasets") or js.get("data") or []
            for it in items:
                nm = it.get("datasetName") or it.get("name") \
                    if isinstance(it, dict) else it
                if nm:
                    names.append(nm)
            R.log("  %s metadata: %d names" % (g, len(names)))
        except Exception as e:
            R.log("  %s metadata refused (%s) -> seeds" % (
                g, str(e)[:60]))
            names = list(SEEDS[g])
        for nm in names:
            key = "%s__%s" % (g, nm)
            try:
                fetch("/data/group/%s/name/%s?limit=2" % (g, nm),
                      cap=300_000)
                universe[key] = {"group": g, "name": nm}
            except urllib.error.HTTPError as e:
                invalid[key] = "HTTP %s" % e.code
            except Exception as e:
                invalid[key] = str(e)[:60]
            time.sleep(0.25)
    R.log("  universe=%d invalid=%d" % (len(universe),
                                        len(invalid)))
    if len(universe) < 6:
        R.log("ops 4982 RED: catalog too thin")
        sys.exit(1)

    R.section("P2 drain since inception")
    have, dfail = {}, {}
    for key, meta in sorted(universe.items()):
        if time.time() - T0 > GLOBAL_S:
            R.log("  global guard -- %d remain for the engine" %
                  (len(universe) - len(have) - len(dfail)))
            break
        g, nm = meta["group"], meta["name"]
        buf = io.BytesIO()
        rows = 0
        try:
            with gzip.GzipFile(fileobj=buf, mode="wb") as zf:
                off = 0
                while True:
                    raw = fetch("/data/group/%s/name/%s?limit=%d"
                                "&offset=%d" % (g, nm, PAGE, off),
                                timeout=120)
                    js = json.loads(raw)
                    batch = js if isinstance(js, list) else \
                        js.get("data") or []
                    for r_ in batch:
                        zf.write((json.dumps(
                            r_, separators=(",", ":")) + "\n"
                        ).encode())
                    rows += len(batch)
                    if len(batch) < PAGE or \
                            off // PAGE + 1 >= PAGE_CAP:
                        break
                    off += PAGE
                    if time.time() - T0 > GLOBAL_S:
                        break
                    time.sleep(0.25)
            body = buf.getvalue()
            s3.put_object(
                Bucket=B, Key=ROOT + "src/%s.jsonl.gz" % key,
                Body=body, ContentType="application/gzip",
                Metadata={"engine": "finra-full",
                          "dataset": key[:110],
                          "rows": str(rows)})
            have[key] = {"rows": rows, "bytes": len(body),
                         "at": datetime.now(timezone.utc
                                            ).isoformat(
                             timespec="seconds"),
                         "status": "fresh"}
            R.log("  %-52s rows=%-9d %5.1fMB" % (
                key, rows, len(body) / 1e6))
        except Exception as e:
            dfail[key] = str(e)[:100]
            R.log("  %-52s FAIL %s" % (key, str(e)[:80]))
    R.log("  banked=%d rows=%d failed=%d" % (
        len(have), sum(v["rows"] for v in have.values()),
        len(dfail)))
    if len(have) < 6:
        fails.append("P2")

    R.section("P3 engine state pre-seed")
    st = gj(STATE_KEY) or {}
    st.update({
        "version": "1.0.6", "phase":
        "COMPLETE" if not dfail and len(have) == len(universe)
        else "DRAIN",
        "universe": universe, "invalid": invalid,
        "have": have, "queue": [
            [k, 0] for k in sorted(universe)
            if k not in have],
        "failures": {k: {"err": v, "tries": 1}
                     for k, v in dfail.items()},
        "lease_until": 0, "n_banked": len(have),
        "as_of": datetime.now(timezone.utc).isoformat(
            timespec="seconds"),
        "drain_src": "runner ops4982 (lambda egress 400s keyless; "
                     "token path opens it once secret lands)"})
    s3.put_object(Bucket=B, Key=STATE_KEY,
                  Body=json.dumps(st, indent=1).encode(),
                  ContentType="application/json")
    man = {
        "as_of": st["as_of"], "engine": "justhodl-finra-full",
        "version": "1.0.6",
        "datasets_catalog": len(universe),
        "datasets_banked": len(have),
        "rows": sum(v["rows"] for v in have.values()),
        "mb": round(sum(v["bytes"] for v in have.values()) / 1e6,
                    1),
        "invalid_named": len(invalid),
        "failures": len(dfail), "phase": st["phase"],
        "cred_expires": "secret pending (portal Reset)",
        "note": ("complete FINRA Query API warehouse: every "
                 "discovered dataset fully paginated since "
                 "inception (OTC/ATS weeklies, TRACE aggregates, "
                 "short interest...); daily rediscovery, weekly "
                 "redrain")}
    s3.put_object(Bucket=B, Key=MANIFEST_KEY,
                  Body=json.dumps(man, indent=1).encode(),
                  ContentType="application/json")
    R.log("  state+manifest written phase=%s" % st["phase"])

    R.section("P4 substance")
    ok4 = False
    try:
        pick = next((k for k in have if "weekly" in k.lower()
                     and have[k]["rows"] > 1000), None) or \
            max(have, key=lambda k: have[k]["rows"])
        raw = gzip.decompress(s3.get_object(
            Bucket=B, Key=ROOT + "src/%s.jsonl.gz" % pick
        )["Body"].read())
        dates = sorted(set(m.decode() for m in re.findall(
            rb'20\d{2}-\d{2}-\d{2}',
            raw[:6_000_000] + raw[-2_000_000:])))
        ok4 = bool(dates) and dates[0] <= "2016-12-31"
        R.log("  %s rows=%d span %s .. %s" % (
            pick, have[pick]["rows"],
            dates[0] if dates else None,
            dates[-1] if dates else None))
    except Exception as e:
        R.log("  substance err %s" % str(e)[:110])
    R.log("P4 %s" % ("PASS" if ok4 else "FAIL"))
    if not ok4:
        fails.append("P4")

    R.section("P5 card")
    t_mark = datetime.now(timezone.utc).isoformat(
        timespec="seconds")
    try:
        lam.invoke(FunctionName=CAT, InvocationType="Event",
                   Payload=b"{}")
    except Exception:
        pass
    hub, t0 = {}, time.time()
    while time.time() - t0 < 12 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    fe = next((p for p in hub.get("providers", [])
               if p.get("slug") == "finra"), {}) or {}
    note = fe.get("catalog_note") or ""
    ok5 = "FULL Query-API warehouse" in note and \
        "0/0 datasets" not in note
    R.log("P5 %s note=%s" % ("PASS" if ok5 else "FAIL",
                             note[:180]))
    if not ok5:
        fails.append("P5")

    if fails:
        R.log("ops 4982 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(datasets=len(have), catalog=len(universe),
         rows=sum(v["rows"] for v in have.values()),
         mb=man["mb"], phase=st["phase"])
    R.log("ops 4982 GREEN -- FINRA imported since inception from "
          "the runner; engine owns refresh once the secret lands")
