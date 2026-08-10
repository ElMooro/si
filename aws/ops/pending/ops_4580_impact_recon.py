"""ops 4580 — impact-layer recon (read-only).

The wo4580+ arc builds a shared Impact Mapper + deep upgrades to all ten
ops-4559 engines. Rule: never build against imagined schemas (the
changes_summary{} drift class). This op samples ground truth:

  1. top-level shape + one trimmed row for every feed the arc consumes
  2. S3 prefix inventory (history/snapshot conventions, feature bus)
  3. SSM parameter presence under /justhodl/ (names only, never values)
     + calibration accuracy signal_type census (what's already graded)
  4. DynamoDB signals table discovery + key schema
[skip-deploy]
"""
import json
import boto3

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception as e:
        return {"__err__": "%s: %s" % (type(e).__name__, str(e)[:80])}


def shape(x, depth=0):
    if isinstance(x, dict):
        return "{%s}" % ",".join(sorted(x.keys())[:24])
    if isinstance(x, list):
        return "[n=%d]" % len(x)
    return type(x).__name__


def row_fields(x):
    if isinstance(x, dict) and x:
        return sorted(x.keys())[:30]
    return []


def main():
    with report("4580_impact_recon") as r:
        r.section("1. Feed schemas (top-level + first-row fields)")
        feeds = [
            ("census", "data/fundamental-census.json"),
            ("readthrough", "data/readthrough.json"),
            ("congress", "data/congress-direct.json"),
            ("activist", "data/activist-13d.json"),
            ("deal-scanner", "data/deal-scanner.json"),
            ("etf-true-flows", "data/etf-true-flows.json"),
            ("etf-shares-hist", "data/etf-shares-history.json"),
            ("flow-lookthrough", "data/flow-lookthrough.json"),
            ("dark-pool", "data/dark-pool.json"),
            ("finra-short", "data/finra-short.json"),
            ("finra-short-hist", "data/finra-short-history.json"),
            ("share-flows", "data/share-flows.json"),
            ("stealth", "data/stealth-accumulation.json"),
            ("accum-composite", "data/accum-composite.json"),
            ("radar", "data/accumulation-radar.json"),
            ("grid-queue", "data/grid-queue.json"),
            ("port-cargo", "data/port-cargo.json"),
            ("portwatch", "data/portwatch.json"),
            ("freight-pulse", "data/freight-pulse.json"),
            ("insider-enr", "data/insider-buys-enriched.json"),
            ("13f-clusters", "data/smart-money-clusters.json"),
        ]
        for name, key in feeds:
            j = gj(key)
            if "__err__" in (j if isinstance(j, dict) else {}):
                r.warn("  %s %s → %s" % (name, key, j["__err__"]))
                continue
            r.log("  %s %s" % (name, shape(j)))
            # dig one representative row from the obvious containers
            for cand in ("rows", "items", "setups", "ranked", "names", "boards",
                         "ports", "isos", "etfs", "funds", "series", "events",
                         "positions", "clusters", "trades", "filings",
                         "companies", "results", "data", "top"):
                v = j.get(cand) if isinstance(j, dict) else None
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    r.log("    .%s[0] fields: %s" % (cand, row_fields(v[0])))
                    break
                if isinstance(v, dict) and v:
                    k0 = sorted(v.keys())[0]
                    if isinstance(v[k0], dict):
                        r.log("    .%s['%s'] fields: %s"
                              % (cand, k0, row_fields(v[k0])))
                        break
        # census row detail (the exposure-graph backbone)
        c = gj("data/fundamental-census.json")
        if isinstance(c, dict):
            for cand in ("rows", "companies", "items"):
                v = c.get(cand)
                if isinstance(v, list) and v:
                    r.log("  census row FULL fields: %s" % sorted(v[0].keys()))
                    break

        r.section("2. S3 prefix inventory")
        for pfx in ("data/history/", "data/warm/", "data/impact/",
                    "data/etf-shares-snapshots/", "data/features/",
                    "data/providers/", "data/archive/"):
            try:
                resp = s3.list_objects_v2(Bucket=B, Prefix=pfx, Delimiter="/",
                                          MaxKeys=12)
                subs = [p["Prefix"] for p in resp.get("CommonPrefixes", [])][:10]
                keys = [k["Key"] for k in resp.get("Contents", [])][:6]
                n = resp.get("KeyCount", 0)
                r.log("  %s n>=%s dirs=%s keys=%s" % (pfx, n, subs, keys))
            except Exception as e:
                r.warn("  %s → %s" % (pfx, str(e)[:70]))

        r.section("3. SSM inventory (names only) + graded signal census")
        try:
            names = []
            tok = None
            while True:
                kw = dict(ParameterFilters=[{"Key": "Name",
                                             "Option": "BeginsWith",
                                             "Values": ["/justhodl/"]}],
                          MaxResults=50)
                if tok:
                    kw["NextToken"] = tok
                resp = ssm.describe_parameters(**kw)
                names += [p["Name"] for p in resp.get("Parameters", [])]
                tok = resp.get("NextToken")
                if not tok or len(names) > 300:
                    break
            keyish = sorted(n for n in names
                            if any(t in n.lower() for t in
                                   ("key", "token", "secret", "api")))
            r.log("  %d params total; key-ish names: %s" % (len(names), keyish))
        except Exception as e:
            r.warn("  describe_parameters → %s" % str(e)[:90])
        try:
            acc = json.loads(ssm.get_parameter(
                Name="/justhodl/calibration/accuracy")["Parameter"]["Value"])
            graded = sorted((k, v.get("n_correct", 0) + v.get("n_wrong", 0))
                            for k, v in acc.items() if isinstance(v, dict))
            r.log("  calibration graded signal_types (n_scored): %s"
                  % [g for g in graded if g[1] > 0][:40])
        except Exception as e:
            r.warn("  calibration/accuracy → %s" % str(e)[:80])

        r.section("4. DynamoDB signals table")
        try:
            tabs = ddb.list_tables(Limit=100).get("TableNames", [])
            jt = [t for t in tabs if "signal" in t.lower() or "justhodl" in t.lower()]
            r.log("  tables: %s" % jt[:20])
            for t in jt[:3]:
                d = ddb.describe_table(TableName=t)["Table"]
                r.log("  %s keys=%s n_items~%s"
                      % (t, [(k["AttributeName"], k["KeyType"])
                             for k in d["KeySchema"]], d.get("ItemCount")))
        except Exception as e:
            r.warn("  dynamo → %s" % str(e)[:90])

        r.ok("recon complete — build proceeds against these shapes")


if __name__ == "__main__":
    main()
