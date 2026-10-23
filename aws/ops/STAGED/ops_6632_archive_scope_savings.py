"""ops 6632 - READ-ONLY: exact saving of each folder in config/archive-scope.json before anything changes (Khalid
2026-10-23, archive option B: "protect only data that can't be re-downloaded ... skip files your engines rebuild every
day"; promised: measure the saving per folder before changing anything).

ops 6631 measured the archive cost of every folder; reading the writers' code then split some folders (data/fundgraph/
keeps a self-built estimates ledger in vintage/ next to a 20-hour cache; data/providers/<slug>/ holds data.html catalog
pages next to provider data). This op measures exactly the prefixes the new replication rules would carry:

  - the config is checked first: no excluded prefix covers or sits inside a must_keep prefix, no two excluded prefixes
    overlap, every excluded prefix matches real objects (a typo would silently exclude nothing - or the wrong thing);
  - per excluded prefix, from the live bucket's newest S3 Inventory (all versions): writes/day, new keys/day, rewrites/
    day, bytes; what it costs the archive today (Standard-IA: $0.01 per 1,000 copies, 128 KB minimum size, 30-day
    minimum on replaced copies, storage growth) - that is the saving; plus what the archive already holds of it (those
    copies stay - only the root user can delete in the archive);
  - what is inside each excluded prefix (next folder level), flagging names that suggest originals or history;
  - every data/providers/<slug>/page-NNN.json catalog-page family (all slugs), to size the remaining catalog pages;
  - the archive after the change, and the same scope priced as S3 Intelligent-Tiering instead of Standard-IA ($0.005 per
    1,000 copies, actual size under 128 KB, no 30-day minimum, objects >= 128 KB tier down to $0.0125 after 30 days and
    $0.004 after 90, monitoring $0.0025 per 1,000 such objects a month) - an estimate for a separate decision.
Window: the full day before the newest inventory (every version written in it is still listed - the live bucket keeps
replaced copies at least a day); a key is NEW when the previous day's inventory does not list it (ops 6631 judged that
from the oldest listed version, which over-counts keys rewritten dozens of times a day). Nothing is written but this
report and its data file.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import csv
import gzip
import io
import json
import re
import sys
import urllib.parse

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
INV_PREFIX = "inventory/%s/" % LIVE
SCOPE = ROOT / "config" / "archive-scope.json"
THAW = datetime(2026, 10, 21, 1, 0, tzinfo=timezone.utc)
SMALL = 128 * 1024
IA_PUT, IA_GB = 0.01 / 1000, 0.0125
IT_PUT, IT_FA, IT_IA, IT_AIA, IT_MON = 0.005 / 1000, 0.023, 0.0125, 0.004, 0.0025 / 1000
REWRITE_DAYS = 4.0          # a replaced archive copy lives ~1 day past 3 newer copies (archive rule) - conservative
PAGE_RE = re.compile(r"^data/providers/([^/]+)/page-\d+\.json$")
HINT_RE = re.compile(r"(original|source|raw|evidence|capture|vault|histor|ledger|vintage|archive|journal)", re.I)
KEPT = "(kept)"
BROAD = {"", "data/", "audit-private/", "audit-private/20260909-originals/", "data/warm/", "data/providers/", "_health/"}


def it_large_12():
    """Average monthly Intelligent-Tiering cost over 12 months per GB/month of large (>=128 KB) new data that is never
    read: each month's cohort pays Frequent ($0.023) in its first month, Infrequent ($0.0125) in months 2-3, Archive
    Instant ($0.004) after."""
    def rate(age):
        return IT_FA if age == 1 else (IT_IA if age <= 3 else IT_AIA)
    total = sum(rate(age) for added in range(1, 13) for age in range(1, 14 - added))
    return total / 12.0


IT_LARGE_12 = it_large_12()


def overlaps(a, b):
    return a.startswith(b) or b.startswith(a)


def validate(scope):
    problems = []
    ex = [e["prefix"] for e in scope["exclude"]]
    keep = [k["prefix"] for k in scope["must_keep"]]
    for p in ex:
        if "/" not in p or p in BROAD:
            problems.append("excluded prefix too broad: %r" % p)
    for i, a in enumerate(ex):
        for b in ex[i + 1:]:
            if overlaps(a, b):
                problems.append("excluded prefixes overlap: %r / %r" % (a, b))
        for k in keep:
            if overlaps(a, k):
                problems.append("excluded %r overlaps must_keep %r" % (a, k))
    st = scope["self_test"]
    for p in ex + keep:
        if overlaps(p, st["excluded_prefix"]) or overlaps(p, st["included_prefix"]):
            problems.append("self-test prefix overlaps %r" % p)
    if len(ex) + 2 > 1000:
        problems.append("more than 1,000 replication rules")
    return problems


def matcher(prefixes):
    alt = "|".join(re.escape(p) for p in sorted(prefixes, key=len, reverse=True))
    rx = re.compile("(?:%s)" % alt) if alt else None

    def match(key):
        if rx is None:
            return KEPT
        m = rx.match(key)
        return m.group(0) if m else KEPT
    return match


def sub_of(bucket, key):
    if bucket == KEPT:
        return None
    rest = key[len(bucket):]
    if not bucket.endswith("/"):
        return "(pages)"
    return rest.split("/", 1)[0] + "/" if "/" in rest else "(files)"


def family(key):
    parts = key.split("/")
    if len(parts) == 1:
        return "(root files)"
    if parts[0] == "data" and len(parts) >= 3 and parts[1] in ("warm", "providers"):
        return "/".join(parts[:3]) + "/" if len(parts) > 3 else "data/%s/(files)" % parts[1]
    if parts[0] == "audit-private" and len(parts) >= 4:
        return "/".join(parts[:3]) + "/"
    if len(parts) == 2:
        return parts[0] + "/(files)"
    if re.fullmatch(r"\d{4}-?\d{2}-?\d{2}", parts[1]):
        return parts[0] + "/"
    return "/".join(parts[:2]) + "/"


def model(a, days):
    """Monthly archive cost of a group's writes. a: Counter with W (versions written), N (new keys), bytes, bill
    (IA-billable bytes), new_bytes, new_bill, new_small_b, new_large_b, new_large_n, rew_large_n."""
    m = 30.0 / days if days else 0.0
    writes_m = a["W"] * m
    rew_bytes = max(0, a["bytes"] - a["new_bytes"])
    rew_bill = max(0, a["bill"] - a["new_bill"])
    ia_put = writes_m * IA_PUT
    ia_min30 = rew_bill * m / 1e9 * IA_GB
    ia_growth_gb = a["new_bill"] * m / 1e9
    it_put = writes_m * IT_PUT
    it_rew = rew_bytes * m / 1e9 * IT_FA * (REWRITE_DAYS / 30.0) + a["rew_large_n"] * m * IT_MON
    small_gb, large_gb = a["new_small_b"] * m / 1e9, a["new_large_b"] * m / 1e9
    large_n = a["new_large_n"] * m
    return {"writes_day": round(a["W"] / days) if days else 0, "new_keys_day": round(a["N"] / days) if days else 0,
            "rewrites_day": round((a["W"] - a["N"]) / days) if days else 0,
            "written_gb_day": round(a["bytes"] / days / 1e9, 4) if days else 0,
            "ia_put": round(ia_put, 3), "ia_min30": round(ia_min30, 3), "ia_growth_gb_month": round(ia_growth_gb, 2),
            "ia_first_month": round(ia_put + ia_min30 + ia_growth_gb * IA_GB, 3),
            "ia_12m_avg": round(ia_put + ia_min30 + ia_growth_gb * IA_GB * 6.5, 3),
            "it_put": round(it_put, 3), "it_growth_gb_month": round(small_gb + large_gb, 3),
            "it_first_month": round(it_put + it_rew + (small_gb + large_gb) * IT_FA + large_n * IT_MON, 3),
            "it_12m_avg": round(it_put + it_rew + small_gb * IT_FA * 6.5 + large_gb * IT_LARGE_12 + large_n * IT_MON * 6.5, 3)}


def ts(v):
    try:
        return datetime.strptime(v[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def selftest():
    scope = json.loads(SCOPE.read_text(encoding="utf-8"))
    assert validate(scope) == [], validate(scope)
    bad = json.loads(json.dumps(scope))
    bad["exclude"].append({"prefix": "data/fundgraph/"})
    assert any("must_keep" in p or "overlap" in p for p in validate(bad))
    bad = json.loads(json.dumps(scope))
    bad["exclude"].append({"prefix": "data/warm/fred-scoped/x/"})
    assert any("must_keep" in p for p in validate(bad))
    mt = matcher([e["prefix"] for e in scope["exclude"]])
    assert mt("data/fundgraph/cache/AAPL_quarter_v21.json") == "data/fundgraph/cache/"
    assert mt("data/fundgraph/vintage/AAPL.json") == KEPT
    assert mt("equity-research/NVDA.json") == "equity-research/"
    assert mt("equity-research-history/2026-10-22/NVDA.json") == KEPT
    assert mt("data/providers/fred/page-003.json") == "data/providers/fred/page-"
    assert mt("data/providers/fred/catalog-manifest.json") == KEPT
    assert mt("data/providers/fred-scoped/manifest.json") == KEPT
    assert mt("data/risk-regime.json") == KEPT and mt("data/risk-regime-research/sources/a/b.json") == KEPT
    assert mt("data/tail-research/runs/a.json") == "data/tail-research/" and mt("data/series-cache/imf-source/x.json") == KEPT
    assert mt("data/providers/worldbank/page-002.json") == "data/providers/worldbank/page-"
    assert mt("audit-private/20260909-originals/fx-research/a.bin") == "audit-private/20260909-originals/fx-research/"
    assert mt("audit-private/20260909-originals/options-research/a.bin") == KEPT
    assert mt("_health/archive-scope-excluded/x.json") == KEPT
    assert sub_of("data/etf-holdings-research/", "data/etf-holdings-research/rows/ab.json") == "rows/"
    assert sub_of("equity-research/", "equity-research/NVDA.json") == "(files)"
    assert sub_of("data/providers/fred/page-", "data/providers/fred/page-001.json") == "(pages)"
    assert PAGE_RE.match("data/providers/gdelt/page-012.json").group(1) == "gdelt"
    a = Counter(W=30, N=10, bytes=30 * 1000, bill=30 * SMALL, new_bytes=10 * 1000, new_bill=10 * SMALL,
                new_small_b=10 * 1000, new_large_b=0, new_large_n=0, rew_large_n=0)
    x = model(a, 1.0)
    assert abs(x["ia_put"] - 900 * IA_PUT) < 1e-3 and abs(x["it_put"] - 900 * IT_PUT) < 1e-3
    assert x["it_first_month"] < x["ia_first_month"]
    assert 0.05 < IT_LARGE_12 < 0.07
    print("selftest ok (IT large 12-month factor %.4f)" % IT_LARGE_12)


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    scope = json.loads(SCOPE.read_text(encoding="utf-8"))
    excluded = [e["prefix"] for e in scope["exclude"]]
    keep = [k["prefix"] for k in scope["must_keep"]]
    match = matcher(excluded)
    keep_match = matcher(keep)
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    with report("ops_6632_archive_scope_savings") as r:
        r.heading("ops 6632 - archive scope B: exact saving per excluded folder, measured before any change (read-only)")
        r.section("A. the proposed scope (config/archive-scope.json)")
        problems = validate(scope)
        r.log("%d excluded prefixes, %d must_keep sentinels, self-test %s" % (len(excluded), len(keep), scope["self_test"]["excluded_prefix"]))
        for p in problems:
            r.warn(p)

        mans, token = [], {}
        while True:
            res = s3.list_objects_v2(Bucket=PRIVATE, Prefix=INV_PREFIX, **token)
            mans += [o["Key"] for o in res.get("Contents", []) if o["Key"].endswith("/manifest.json") and "/hive/" not in o["Key"]]
            if not res.get("IsTruncated"):
                break
            token = {"ContinuationToken": res["NextContinuationToken"]}
        stamps = {}
        for k in mans:
            try:
                stamps[datetime.strptime(k.split("/")[-2], "%Y-%m-%dT%H-%MZ").replace(tzinfo=timezone.utc)] = k
            except ValueError:
                continue
        if not stamps:
            r.fail("no inventory manifest under s3://%s/%s" % (PRIVATE, INV_PREFIX))
            sys.exit(1)
        w_end = max(stamps)
        prev = [t for t in stamps if timedelta(hours=20) <= w_end - t <= timedelta(hours=28)]
        if not prev or min(prev) < THAW - timedelta(hours=1):
            r.fail("no previous-day inventory after the thaw before %s (have %s)" % (w_end.isoformat(), sorted(t.isoformat()[:16] for t in stamps)[-5:]))
            sys.exit(1)
        w_start = max(prev)
        days = (w_end - w_start).total_seconds() / 86400.0
        stamp = stamps[w_end].split("/")[-2]

        def rows_of(mkey):
            man = json.loads(s3.get_object(Bucket=PRIVATE, Key=mkey)["Body"].read())
            schema = [x.strip() for x in man["fileSchema"].split(",")]
            ix = {n: i for i, n in enumerate(schema)}
            for n, f in enumerate(man["files"]):
                body = s3.get_object(Bucket=PRIVATE, Key=f["key"])["Body"]
                with gzip.GzipFile(fileobj=body) as gz:
                    for row in csv.reader(io.TextIOWrapper(gz, encoding="utf-8", newline="")):
                        if row[ix["IsDeleteMarker"]].strip().lower() == "true":
                            continue
                        yield (n, len(man["files"]), urllib.parse.unquote(row[ix["Key"]]), int(row[ix["Size"]] or 0),
                               row[ix["LastModifiedDate"]], row[ix["IsLatest"]].strip().lower() == "true")

        r.section("B. inventory read")
        r.log("window %s -> %s (%.2f days): newest inventory %s, previous %s" % (
            w_start.isoformat()[:16], w_end.isoformat()[:16], days, stamp, stamps[w_start].split("/")[-2]))
        before = set()
        for n, nf, key, size, lm, latest in rows_of(stamps[w_start]):
            before.add(key)
        r.log("previous inventory: %d keys existed" % len(before))

        win = {}                                     # key -> [wn, bytes, bill, small_n, large_n, first_e, first_size]
        cur = defaultdict(Counter)                   # bucket -> objects/bytes/bill of current versions
        cur_sub = defaultdict(Counter)               # (bucket, sub) -> current objects/bytes
        keep_hit = Counter()
        ws, we = w_start.timestamp(), w_end.timestamp()
        rows = 0
        for n, nf, key, size, lm, latest in rows_of(stamps[w_end]):
            rows += 1
            t = ts(lm)
            e = t.timestamp() if t else 0.0
            if ws <= e < we:
                w = win.get(key)
                if w is None:
                    w = win[key] = [0, 0, 0, 0, 0, e, size]
                w[0] += 1
                w[1] += size
                w[2] += max(size, SMALL)
                if size < SMALL:
                    w[3] += 1
                else:
                    w[4] += 1
                if e < w[5]:
                    w[5], w[6] = e, size
            if latest:
                b = match(key)
                c = cur[b]
                c["objects"] += 1
                c["bytes"] += size
                c["bill"] += max(size, SMALL)
                if b != KEPT:
                    sc = cur_sub[(b, sub_of(b, key))]
                    sc["objects"] += 1
                    sc["bytes"] += size
                    k = keep_match(key)
                    if k != KEPT:
                        keep_hit[(b, k)] += 1
        r.log("newest inventory: %d versions, %d keys written in the window, %d of them new" % (
            rows, len(win), sum(1 for k in win if k not in before)))

        agg = defaultdict(Counter)
        agg_sub = defaultdict(Counter)
        agg_fam = defaultdict(Counter)
        pages = defaultdict(Counter)
        for key, (wn, wb, wbill, sn, ln, fe, fsize) in win.items():
            is_new = key not in before
            b = match(key)
            d = Counter(W=wn, N=1 if is_new else 0, bytes=wb, bill=wbill)
            if is_new:
                d["new_bytes"] = fsize
                d["new_bill"] = max(fsize, SMALL)
                if fsize < SMALL:
                    d["new_small_b"] = fsize
                else:
                    d["new_large_b"] = fsize
                    d["new_large_n"] = 1
            d["rew_large_n"] = ln - (1 if is_new and fsize >= SMALL else 0)
            agg[b].update(d)
            agg["(all)"].update(d)
            if b == KEPT:
                agg_fam[family(key)].update(d)
            else:
                agg_sub[(b, sub_of(b, key))].update(d)
            pm = PAGE_RE.match(key)
            if pm:
                pages[pm.group(1)].update(d)

        r.section("C. saving per excluded prefix (Standard-IA archive, today's price)")
        out = []
        for e in scope["exclude"]:
            p = e["prefix"]
            mo = model(agg.get(p, Counter()), days)
            c = cur.get(p, Counter())
            mo.update({"prefix": p, "kind": e.get("kind"), "current_objects": c["objects"], "current_gb": round(c["bytes"] / 1e9, 3),
                       "archived_gb_billable": round(c["bill"] / 1e9, 3),
                       "archived_usd_month_stays": round(c["bill"] / 1e9 * IA_GB, 3)})
            subs = []
            for (bb, sub), sc in cur_sub.items():
                if bb != p:
                    continue
                sm = model(agg_sub.get((bb, sub), Counter()), days)
                subs.append({"sub": sub, "current_objects": sc["objects"], "current_gb": round(sc["bytes"] / 1e9, 3),
                             "writes_day": sm["writes_day"], "new_keys_day": sm["new_keys_day"], "ia_first_month": sm["ia_first_month"],
                             "hint": bool(HINT_RE.search(sub or ""))})
            for (bb, sub), sa in agg_sub.items():
                if bb == p and not any(s["sub"] == sub for s in subs):
                    sm = model(sa, days)
                    subs.append({"sub": sub, "current_objects": 0, "current_gb": 0, "writes_day": sm["writes_day"],
                                 "new_keys_day": sm["new_keys_day"], "ia_first_month": sm["ia_first_month"], "hint": bool(HINT_RE.search(sub or ""))})
            subs.sort(key=lambda s: -(s["ia_first_month"] + s["current_gb"] / 1000))
            mo["inside"] = subs[:25]
            mo["inside_count"] = len(subs)
            out.append(mo)
            hints = [s["sub"] for s in subs if s["hint"]]
            r.log("%-66s %6d writes/d (new %5d) | save IA $%5.2f/mo now (put %.2f + 30d %.2f + %.2f GB/mo growth), 12-mo avg $%5.2f | holds %8d obj %7.2f GB (archived copies stay: $%.2f/mo) | inside %d: %s%s" % (
                p[:66], mo["writes_day"], mo["new_keys_day"], mo["ia_first_month"], mo["ia_put"], mo["ia_min30"], mo["ia_growth_gb_month"],
                mo["ia_12m_avg"], mo["current_objects"], mo["current_gb"], mo["archived_usd_month_stays"], len(subs),
                ", ".join("%s(%d/d)" % (s["sub"], s["writes_day"]) for s in subs[:6]), (" | NAMES TO CHECK: %s" % hints) if hints else ""))
            if c["objects"] == 0:
                problems.append("excluded prefix matches no current object: %r" % p)
        for (b, k), n_ in keep_hit.items():
            problems.append("excluded %r matched %d objects under must_keep %r" % (b, n_, k))

        r.section("D. data.html catalog pages, every provider slug (data/providers/<slug>/page-NNN.json)")
        page_rows = []
        for slug, a in pages.items():
            mo = model(a, days)
            mo["slug"] = slug
            mo["prefix"] = "data/providers/%s/page-" % slug
            mo["in_scope"] = mo["prefix"] in excluded
            page_rows.append(mo)
        page_rows.sort(key=lambda x: -x["ia_first_month"])
        for x in page_rows:
            r.log("%-40s %5d writes/d | IA $%5.3f/mo | %s" % (x["prefix"], x["writes_day"], x["ia_first_month"], "IN SCOPE" if x["in_scope"] else "not listed"))

        r.section("E. the archive before and after (monthly, estimates from the window)")
        tot_all = model(agg.get("(all)", Counter()), days)
        tot_kept = model(agg.get(KEPT, Counter()), days)
        exc = Counter()
        for p in excluded:
            exc.update(agg.get(p, Counter()))
        tot_exc = model(exc, days)
        for label, x in (("whole bucket (today)", tot_all), ("excluded by scope B", tot_exc), ("kept after B", tot_kept)):
            r.log("%-22s %7d writes/d | Standard-IA: $%6.2f first month, $%6.2f 12-month avg, +%6.1f GB/mo | Intelligent-Tiering: $%6.2f first month, $%6.2f 12-month avg" % (
                label, x["writes_day"], x["ia_first_month"], x["ia_12m_avg"], x["ia_growth_gb_month"], x["it_first_month"], x["it_12m_avg"]))
        held_exc = sum(cur.get(p, Counter())["bill"] for p in excluded) / 1e9
        held_all = sum(c["bill"] for c in cur.values()) / 1e9
        r.log("archive holdings (current copies, IA-billable): %.1f GB in all, %.1f GB of it in excluded prefixes ($%.2f/mo, stays unless root deletes it)" % (
            held_all, held_exc, held_exc * IA_GB))

        r.section("F. biggest folders that stay archived (for review)")
        fams = []
        for fam, a in agg_fam.items():
            mo = model(a, days)
            mo["family"] = fam
            fams.append(mo)
        fams.sort(key=lambda x: -x["ia_first_month"])
        for x in fams[:30]:
            r.log("%-62s %6d writes/d (new %5d) | IA $%5.2f/mo, +%5.2f GB/mo | IT $%5.2f/mo" % (
                x["family"][:62], x["writes_day"], x["new_keys_day"], x["ia_first_month"], x["ia_growth_gb_month"], x["it_first_month"]))

        r.kv(inventory=stamp, window_days=round(days, 2), excluded_prefixes=len(excluded),
             save_ia_first_month=tot_exc["ia_first_month"], save_ia_12m_avg=tot_exc["ia_12m_avg"],
             kept_ia_first_month=tot_kept["ia_first_month"], kept_it_first_month=tot_kept["it_first_month"],
             all_ia_first_month=tot_all["ia_first_month"], problems=len(problems))
        latest = ROOT / "aws" / "ops" / "reports" / "latest"
        latest.mkdir(parents=True, exist_ok=True)
        (latest / "ops_6632_archive_scope_savings_data.md").write_text(
            "# ops 6632 raw data (JSON)\n\n```json\n" + json.dumps({
                "inventory": stamp, "window": [w_start.isoformat(), w_end.isoformat()], "days": days,
                "prices": {"ia_put_per_1k": 0.01, "ia_gb_month": IA_GB, "it_put_per_1k": 0.005, "it_fa": IT_FA, "it_ia": IT_IA,
                           "it_aia": IT_AIA, "it_monitoring_per_1k": 0.0025, "it_large_12m_factor": IT_LARGE_12, "rewrite_days_it": REWRITE_DAYS},
                "excluded": out, "catalog_pages": page_rows, "totals": {"all": tot_all, "excluded": tot_exc, "kept": tot_kept},
                "holdings_gb": {"all": round(held_all, 2), "excluded": round(held_exc, 2)},
                "kept_families_top": fams[:80], "problems": problems}, default=str, separators=(",", ":")) + "\n```\n", encoding="utf-8")
        if problems:
            for p in problems:
                r.warn(p)
            r.fail("%d problem(s) in the proposed scope - nothing applied (this op never changes anything)" % len(problems))
            sys.exit(1)
        r.ok("scope measured; read-only, nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
