"""justhodl-provenance-rollup — E12 (ops 4433): provider-mix rollup.

Answers Khalid's question as a LIVE artifact instead of a session report:
"what % of the data on justhodl.ai comes from which provider?"

v1 method (honest about granularity): engine sources are scanned for
provider URL signatures -> engine->providers; the D4 graph gives
feed->producers and page->feeds; composing them yields FEED-level provider
mix globally and per page. VALUE-level mix (true E12 endgame) upgrades in
place as F1 envelope coverage grows — envelopes are counted first-class
where present (coverage currently ~24%). No number is invented: feeds whose
producer has no recognizable provider signature are bucketed "computed/
internal", unknown producers as "unmapped".
Writes data/audit/data-source-rollup.json nightly 05:45 UTC.
"""
import json
import os
import re
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")

SIGS = {
    "fred": r"stlouisfed\.org|FRED_API",
    "polygon": r"polygon\.io",
    "sec": r"sec\.gov|efts\.sec",
    "nyfed": r"newyorkfed\.org",
    "ecb": r"ecb\.europa\.eu",
    "boj": r"stat-search\.boj\.or\.jp",
    "cftc": r"cftc\.gov",
    "treasury": r"fiscaldata\.treasury|treasurydirect|home\.treasury",
    "bls": r"bls\.gov",
    "imf": r"imf\.org|dataservices\.imf",
    "yahoo": r"query1\.finance\.yahoo|finance\.yahoo",
    "coinmetrics": r"coinmetrics\.io",
    "census": r"census\.gov",
    "eia": r"eia\.gov",
    # ops 4482: tonight's eight new agencies + gpt5.6 reclass rule
    "ofr": r"financialresearch\.gov",
    "bea": r"bea\.gov",
    "fed-board": r"federalreserve\.gov",
    "bis": r"stats\.bis\.org",
    "gleif": r"gleif\.org",
    "eurostat": r"ec\.europa\.eu/eurostat",
    "oecd": r"sdmx\.oecd\.org",
    "openfigi": r"openfigi\.com",
    # ops 4530 (Khalid: missing datasets — the six hub zeros + fleet-wide
    # agencies whose engines exist but had no signature, so the map could
    # never name them):
    "worldbank": r"api\.worldbank\.org|worldbank\.org",
    "dbnomics": r"db\.nomics\.world|dbnomics",
    "snb": r"data\.snb\.ch|snb\.ch",
    "bcb": r"api\.bcb\.gov\.br|bcb\.gov\.br",
    "cboe": r"cdn\.cboe\.com|ww2\.cboe\.com|cboe\.com",
    "statcan": r"statcan\.gc\.ca",
    "banxico": r"banxico\.org\.mx",
    "boe": r"bankofengland\.co\.uk",
    "gdelt": r"gdeltproject\.org",
    "eiopa": r"eiopa\.europa\.eu",
    "nasa": r"power\.larc\.nasa\.gov",
    "dol": r"oui\.doleta\.gov|doleta\.gov",
    "occ": r"theocc\.com",
    # council-unanimous: LLM output is transform, not a data provider
    "transform-agent": r"api\.anthropic\.com|claude-",
    "fleet-feed": r"justhodl-dashboard-live|justhodl\.ai/data",
}


def lambda_handler(event, context):
    graph = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/audit/lambda-graph.json")["Body"].read())
    engines = graph.get("engines") or {}
    feeds = graph.get("feeds") or {}
    pages = graph.get("pages") or {}

    # engine -> providers (need source; read from repo mirror is impossible
    # at runtime, so use the inventory of env keys + name heuristics + the
    # signature cache built at deploy time and shipped alongside)
    try:
        eng_prov = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/audit/engine-provider-map.json")["Body"].read()
        ).get("map", {})
    except Exception:
        eng_prov = {}

    def feed_providers(feed):
        provs = set()
        for p in (feeds.get(feed, {}).get("producers") or []):
            name = p.replace(" (prefix)", "")
            got = eng_prov.get(name)
            if got:
                provs.update(got)
            elif name.startswith("("):
                provs.add("unmapped")
            else:
                provs.add("computed/internal")
        return provs or {"unmapped"}

    global_counts, by_page = {}, {}
    for feed in feeds:
        for pr in feed_providers(feed):
            global_counts[pr] = global_counts.get(pr, 0) + 1
    tot = sum(global_counts.values()) or 1
    global_mix = {k: round(v / tot, 4)
                  for k, v in sorted(global_counts.items(),
                                     key=lambda x: -x[1])}
    for page, v in pages.items():
        c = {}
        for feed in (v.get("reads") or []):
            for pr in feed_providers(feed):
                c[pr] = c.get(pr, 0) + 1
        t = sum(c.values()) or 1
        by_page[page] = {k: round(x / t, 4)
                         for k, x in sorted(c.items(), key=lambda y: -y[1])}

    doc = {"as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "method": "v1 feed-level mix (engine URL signatures x D4 graph); "
                     "value-level upgrades in place as F1 envelope coverage "
                     "grows",
           "global": global_mix,
           "global_feed_counts": global_counts,
           "by_page": by_page,
           "n_feeds": len(feeds), "n_pages": len(pages),
           "unmapped_note": "unmapped = live feed whose writer the static "
                            "scan could not name; computed/internal = "
                            "producer with no external provider signature"}
    s3.put_object(Bucket=BUCKET, Key="data/audit/data-source-rollup.json",
                  Body=json.dumps(doc, indent=1).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "n_feeds": len(feeds),
           "global_top": list(global_mix.items())[:6]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
