"""ops 6953 - READ-ONLY audit of the CDS desk (justhodl-cds-desk 1.6.0, aws/shared/cds_desk.py) before it is improved.

Khalid 2026-11-25 11:33 CT: "audit and improve the CDS engine next and improve it also to include more companies and all asset
classes and even sovereign CDS and public companies worldwide. Make sure you use beautiful visuals ... and make sure all the
engine data is visible on the page".

Nothing is invoked and nothing is written. From the warehouse and from the public DTCC files the desk itself reads every day:

  1  the published document: what it holds, how many names it prices, which names it prints twice, which levels are hard to
     believe (AT&T at 7 bp, Expedia at 18 bp)
  2  the desk's bank: the day-by-day record behind those names, and both candidate branches the desk kept
  3  the raw prints of the newest file days: every credit product DTCC disseminates and how much of it the desk reads
     (senior single names only? subordinated, municipal, loans, tranches, options, the index families it does not know)
  4  the raw prints of AT&T and Expedia, and what the desk's own arithmetic makes of each
  5  what else the warehouse banks that a CDS desk would read
"""
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import base64
import csv
import gzip
import io
import json
import sys
import time
import urllib.error
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared")]
LIVE = "justhodl-dashboard-live"
DTCC = "https://kgc0418-tdw-data-0.s3.amazonaws.com"
UA = "justhodl-cds-desk/1.0 (+https://justhodl.ai)"          # the desk's own
KEEP = ("Dissemination Identifier", "Action type", "Event type", "UPI FISN", "UPI Underlier Name", "Unique Product Identifier",
        "Underlying Asset Name", "Underlier ID-Leg 1", "Execution Timestamp", "Effective Date", "Expiration Date",
        "Notional amount-Leg 1", "Notional currency-Leg 1", "Fixed rate-Leg 1", "Spread-Leg 1", "Spread notation-Leg 1",
        "Other payment type", "Other payment amount", "Cleared", "Platform identifier", "Package indicator", "Price",
        "Price notation", "Option Type", "Option Style", "Strike Price", "Seniority", "Index factor", "Embedded Option type",
        "CDS index attachment point", "CDS index detachment point", "Block trade election indicator")


def get_json(s3, key):
    raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
    return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw), len(raw)


def embed(r, name, blob):
    text = base64.b64encode(blob).decode("ascii")
    r.log("fixture %s: %d bytes" % (name, len(blob)))
    r.lines.append("<!-- FIXTURE %s %d bytes" % (name, len(blob)))
    r.lines.extend(text[i:i + 16000] for i in range(0, len(text), 16000))
    r.lines.append("FIXTURE-END -->")


def fetch_zip(source, day_iso):
    url = "%s/%s/eod/%s_CUMULATIVE_CREDITS_%s.zip" % (DTCC, source, source.upper(), day_iso.replace("-", "_"))
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 404):
            return None
        raise


def rows_of(zip_bytes):
    out, header = [], []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for member in z.namelist():
            if not member.lower().endswith(".csv"):
                continue
            with z.open(member) as fh:
                rd = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig", newline=""))
                header = rd.fieldnames or header
                out.extend(rd)
    return out, header


def tail(fisn):
    """The product part of a FISN: what follows the last '/'."""
    f = (fisn or "").strip()
    return f.rsplit("/", 1)[-1].strip() if "/" in f else f


def main():
    import boto3
    from ops_report import report
    import cds_desk as CD
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)

    with report("ops_6953_cds_desk_audit") as r:
        r.heading("ops 6953 - audit of the CDS desk before it is improved (read-only), %s" % now.isoformat(timespec="minutes"))

        # ------------------------------------------------------------------------------------------ 1. the document
        r.section("1. The published document")
        packet, size = get_json(s3, "data/cds-desk.json")
        r.log("data/cds-desk.json: %d bytes | version %s | as_of %s | generated %s" % (size, packet.get("version"), packet.get("as_of"), packet.get("generated_at")))
        r.log("keys: %s" % {k: (type(v).__name__, len(v) if hasattr(v, "__len__") else v) for k, v in packet.items()})
        every = []
        for g, blk in (packet.get("groups") or {}).items():
            r.log("group %s: %d priced rows, %d trade with no level, %d dormant | n_liquid %s, n_tracked %s | other keys %s" % (
                g, len(blk.get("rows") or []), len(blk.get("unpriced") or []), len(blk.get("dormant") or []), blk.get("n_liquid"), blk.get("n_tracked"),
                sorted(k for k in blk if k not in ("rows", "unpriced", "dormant"))))
            for kind in ("rows", "unpriced", "dormant"):
                for x in blk.get(kind) or []:
                    every.append((g, kind, x))
        one = next((x for g, k, x in every if k == "rows"), {})
        r.log("fields of a priced row: %s" % sorted(one))
        one = next((x for g, k, x in every if k == "unpriced"), {})
        r.log("fields of a row with no level: %s" % sorted(one))
        r.log("indices (%d): %s" % (len(packet.get("indices") or []), [(i.get("key"), i.get("spread_bp"), i.get("price"), i.get("last_date")) for i in packet.get("indices") or []]))
        names = Counter((g, x.get("name")) for g, k, x in every if k == "rows")
        twice = {n: c for n, c in names.items() if c > 1}
        r.log("priced names printed more than once: %s" % twice)
        allnames = defaultdict(list)
        for g, k, x in every:
            allnames[x.get("name")].append((g, k, x.get("key"), x.get("spread_bp"), x.get("trades_30d"), x.get("last_date")))
        dup = {n: v for n, v in allnames.items() if len(v) > 1}
        r.log("names on the desk more than once, any list (%d): %s" % (len(dup), json.dumps(dup, default=str)[:3000]))
        # levels that are hard to believe: a corporate under 15 bp, or a level a long way from its own recent middle
        odd = []
        for g, k, x in every:
            if k != "rows" or not isinstance(x.get("spread_bp"), (int, float)):
                continue
            sp = [p[1] for p in (x.get("spark") or []) if isinstance(p[1], (int, float))]
            mid = sorted(sp[:-1])[len(sp[:-1]) // 2] if len(sp) >= 6 else None
            far = mid and abs(CD.math.log(max(x["spread_bp"], 0.5) / max(mid, 0.5))) > 0.9
            low = g != "sovereign" and x["spread_bp"] < 15
            if far or low:
                odd.append((g, x.get("name"), x.get("key"), x["spread_bp"], mid, x.get("chg_1w_bp"), x.get("spread_basis"), x.get("anchor_source"), x.get("sign_evidence"),
                            x.get("trades_30d"), x.get("last_date")))
        r.log("levels hard to believe (corporate under 15 bp, or more than e^0.9 from the middle of its own last 120 days): %d" % len(odd))
        for o in sorted(odd, key=lambda o: o[3])[:60]:
            r.log("  %-12s %-34s key %-28s %8.1f bp | own middle %s | 1w %s | %s / %s / %s | %s trades in 30d | last %s" % (o[0], str(o[1])[:34], str(o[2])[:28], o[3], o[4], o[5], o[6], o[7], o[8], o[9], o[10]))
        embed(r, "cds-packet", gzip.compress(json.dumps(packet, separators=(",", ":")).encode()))

        # ------------------------------------------------------------------------------------------ 2. the bank
        r.section("2. The desk's bank")
        bank, bsize = get_json(s3, "data/warm/cds/bank.json.gz")
        series, meta, emap = bank.get("series") or {}, bank.get("meta") or {}, bank.get("entity_map") or {}
        days = bank.get("days") or []
        r.log("bank: %d bytes gzipped | %d series, %d meta, %d products mapped to a name | %d days %s .. %s" % (bsize, len(series), len(meta), len(emap), len(days), days[:1], days[-1:]))
        kinds = Counter((m or {}).get("kind") for m in meta.values())
        r.log("series by kind: %s" % dict(kinds))
        priced_any = sum(1 for k, s in series.items() if any(v.get("s") is not None for v in s.values()))
        r.log("series with at least one priced day: %d of %d" % (priced_any, len(series)))
        watch = [k for k in series if any(w in k for w in ("AT & T", "AT&T", "SBC", "EXPEDIA", "AT T"))]
        r.log("keys that read like AT&T or Expedia: %s" % watch)
        for k in watch:
            pts = sorted(series[k].items())[-14:]
            r.log("  %s | meta %s | products %s" % (k, json.dumps(meta.get(k))[:200], [u for u, e in emap.items() if e.get("norm") == k][:8]))
            for d, v in pts:
                r.log("    %s %s" % (d, json.dumps(v, separators=(",", ":"))[:330]))
        embed(r, "cds-bank", gzip.compress(json.dumps(bank, separators=(",", ":")).encode()))
        try:
            hist, hsize = get_json(s3, "data/cds-desk-history.json")
            r.log("data/cds-desk-history.json: %d bytes | keys %s | %d names" % (hsize, {k: type(v).__name__ for k, v in hist.items()}, len(hist.get("names") or {})))
            first = next(iter((hist.get("names") or {}).items()), None)
            r.log("one name of the history: %s" % (json.dumps(first)[:500] if first else None))
        except Exception as exc:
            r.log("history: %s %s" % (type(exc).__name__, exc))

        # ------------------------------------------------------------------------------------------ 3. the raw prints
        r.section("3. Every credit product DTCC disseminates, on the newest file days")
        par, _ = get_json(s3, "data/warm/treasury-par/curve.json.gz")
        par_rows = par.get("rows") if isinstance(par, dict) else {}

        def rate_for(day_iso):
            best = None
            for d in sorted(par_rows):
                if d > day_iso:
                    break
                v = par_rows[d].get("5Y") if isinstance(par_rows[d], dict) else None
                if isinstance(v, (int, float)):
                    best = float(v)
            return (best if best is not None else 4.25) - CD.SWAP_PROXY_OFFSET_PCT

        file_days, d = [], now.date()
        while len(file_days) < 4 and (now.date() - d).days < 12:
            if fetch_zip("sec", d.isoformat()) is not None:
                file_days.append(d.isoformat())
            d -= timedelta(days=1)
        for extra in ("2026-11-19", "2026-11-20"):
            if extra not in file_days:
                file_days.append(extra)
        file_days.sort()
        r.log("file days read: %s" % file_days)
        all_rows, header_seen = [], {}
        for day in file_days:
            for source in ("sec", "cftc"):
                t0 = time.time()
                raw = fetch_zip(source, day)
                if raw is None:
                    r.log("  %s %s: not posted" % (source, day))
                    continue
                rows, header = rows_of(raw)
                header_seen[source] = header
                for x in rows:
                    x["_src"], x["_file"] = source, day
                all_rows.extend(rows)
                r.log("  %s %s: %d bytes, %d rows, %.1f s" % (source, day, len(raw), len(rows), time.time() - t0))
        for source, header in header_seen.items():
            r.log("columns of the %s file (%d): %s" % (source, len(header), header))
        acts = Counter((x.get("_src"), x.get("Action type"), x.get("Event type")) for x in all_rows)
        r.log("rows by repository, action, event: %s" % dict(acts.most_common(30)))
        new = [x for x in all_rows if x.get("Action type") == "NEWT" and x.get("Event type") == "TRAD"]
        seen, uniq = set(), []
        for x in new:
            k = x.get("Dissemination Identifier")
            if k in seen:
                continue
            seen.add(k)
            uniq.append(x)
        r.log("new trades: %d (%d distinct identifiers)" % (len(new), len(uniq)))
        kept = {}
        for x in uniq:
            t = CD.normalize_trade(x, x["_src"])
            if t:
                kept[x.get("Dissemination Identifier")] = t
        r.log("the desk reads %d of them (%.1f%%)" % (len(kept), 100.0 * len(kept) / max(len(uniq), 1)))
        by_tail = defaultdict(lambda: {"n": 0, "kept": 0, "names": set(), "ccy": Counter(), "src": Counter(), "under": Counter(), "notional": 0.0})
        for x in uniq:
            b = by_tail[tail(x.get("UPI FISN"))]
            b["n"] += 1
            b["kept"] += 1 if x.get("Dissemination Identifier") in kept else 0
            b["names"].add((x.get("Underlying Asset Name") or "").strip() or (x.get("UPI Underlier Name") or "").strip())
            b["ccy"][(x.get("Notional currency-Leg 1") or "").strip()] += 1
            b["src"][x["_src"]] += 1
            b["under"][(x.get("UPI Underlier Name") or "").strip()] += 1
            try:
                b["notional"] += float((x.get("Notional amount-Leg 1") or "0").replace(",", "").rstrip("+") or 0)
            except ValueError:
                pass
        r.log("products, by what follows the last '/' of the FISN (%d kinds):" % len(by_tail))
        for tl, b in sorted(by_tail.items(), key=lambda kv: -kv[1]["n"])[:70]:
            r.log("  %-34s %6d prints, the desk reads %6d | %5d names | %s | %s | notional %.0f M | e.g. %s" % (
                tl[:34], b["n"], b["kept"], len(b["names"]), dict(b["src"]), dict(b["ccy"].most_common(4)), b["notional"] / 1e6, [u for u, _ in b["under"].most_common(3)]))
        idx = Counter()
        idx_kept = Counter()
        for x in uniq:
            f = x.get("UPI FISN") or ""
            if "Idx" in f or "Index" in f:
                u = (x.get("UPI Underlier Name") or "").strip().upper()
                idx[(u, tail(f))] += 1
                if x.get("Dissemination Identifier") in kept:
                    idx_kept[(u, tail(f))] += 1
        r.log("index underliers (%d): known to the desk = in INDEX_FAMILIES" % len(idx))
        for (u, tl), n in idx.most_common(90):
            r.log("  %-46s %-26s %6d prints, read %6d %s" % (u[:46], tl[:26], n, idx_kept.get((u, tl), 0), "" if u in CD.INDEX_FAMILIES else "<- not a family the desk knows"))
        r.log("families the desk knows: %s" % sorted(CD.INDEX_FAMILIES))
        # single names in the prints against the names on the desk
        on_desk = {x.get("key") for g, k, x in every}
        in_prints = defaultdict(lambda: [0, set()])
        for x in uniq:
            tl = tail(x.get("UPI FISN"))
            if " SN " in " %s " % tl or tl.endswith(" SN"):
                raw_name = (x.get("Underlying Asset Name") or "").strip()
                if CD.entityish(raw_name):
                    norm = CD.canonical_norm(CD.normalize_name(raw_name))
                    in_prints[(tl, norm)][0] += 1
                    in_prints[(tl, norm)][1].add(raw_name)
        tl_names = defaultdict(list)
        for (tl, norm), (n, raws) in in_prints.items():
            tl_names[tl].append((n, norm, norm in on_desk, sorted(raws)[:2]))
        for tl, lst in sorted(tl_names.items(), key=lambda kv: -len(kv[1])):
            missing = [x for x in lst if not x[2]]
            r.log("single names of '%s': %d in these days' prints, %d of them not on the desk at all; most traded missing: %s" % (
                tl, len(lst), len(missing), [(n, norm) for n, norm, _, _ in sorted(missing, reverse=True)[:12]]))
        ten = Counter()
        for t in kept.values():
            ten[(t["kind"], t["bucket"])] += 1
        r.log("tenors of the prints the desk reads: %s" % dict(sorted(ten.items())))

        # ------------------------------------------------------------------------------------------ 4. AT&T and Expedia
        r.section("4. The prints of AT&T and Expedia, and what the desk's own arithmetic makes of each")
        for label, words in (("AT&T", ("AT&T", "AT & T", "AT T ", "SBC COMM")), ("Expedia", ("EXPEDIA",))):
            mine = [x for x in all_rows if any(w in ((x.get("Underlying Asset Name") or "") + " " + (x.get("UPI FISN") or "")).upper() for w in words)]
            r.log("%s: %d rows in these files (any action)" % (label, len(mine)))
            for x in sorted(mine, key=lambda x: (x.get("Execution Timestamp") or ""))[:70]:
                t = CD.normalize_trade(x, x["_src"]) if (x.get("Action type") == "NEWT" and x.get("Event type") == "TRAD") else None
                made = ""
                if t:
                    rate = rate_for(t["date"])
                    made = " -> tenor %.2f %s | quote %s (%s) | candidates %s" % (t["tenor"], t["bucket"], t.get("quoted_bp"), CD.quote_status(t, rate) if t.get("quoted_bp") is not None else "-",
                                                                             [(round(v, 1), b) for v, b in CD.candidate_spreads(t, rate)])
                r.log("  %s %s %s/%s | %s | %s | upi %s | exec %s exp %s | notional %s %s | coupon %s | spread %s notation %s | pay %s %s | cleared %s pkg %s%s" % (
                    x["_file"], x["_src"], x.get("Action type"), x.get("Event type"), (x.get("Underlying Asset Name") or "")[:28], (x.get("UPI FISN") or "")[:34], x.get("Unique Product Identifier"),
                    (x.get("Execution Timestamp") or "")[:16], (x.get("Expiration Date") or "")[:10], x.get("Notional amount-Leg 1"), x.get("Notional currency-Leg 1"), x.get("Fixed rate-Leg 1"),
                    x.get("Spread-Leg 1"), x.get("Spread notation-Leg 1"), x.get("Other payment type"), x.get("Other payment amount"), x.get("Cleared"), x.get("Package indicator"), made))
        # the desk's own day, replayed for the watched keys
        by_day = defaultdict(list)
        for t in kept.values():
            by_day[t["date"]].append(t)
        for day in sorted(by_day):
            if day < file_days[0]:
                continue
            agg = CD.aggregate_day(by_day[day], emap, rate_for(day), CD.anchors_from_bank(bank, day))
            for k in watch:
                if k in agg["entities"]:
                    m = agg["entities"][k]
                    r.log("  replay %s %s: spread %s (%s), anchor %s/%s, n %s, 5y %s, priced %s, basis %s, candidates %s, bank's anchor %s" % (
                        day, k, m["spread_5y"], m["spread_basis"], m["anchor_source"], m["anchor_quality"], m["n"], m["n_5y"], m["n_priced_5y"], m["basis"], m["cand"],
                        CD.anchors_from_bank(bank, day).get(k)))

        # a compact copy of the newest days' new trades, for the bench
        cols = [c for c in KEEP if any(c in h for h in header_seen.values())]
        slim = [[x.get(c) or "" for c in cols] + [x["_src"], x["_file"]] for x in uniq]
        blob = gzip.compress(json.dumps({"cols": cols + ["_src", "_file"], "rows": slim}, separators=(",", ":")).encode())
        r.log("new trades of %d file days, %d columns: %d bytes gzipped" % (len(file_days), len(cols), len(blob)))
        if len(blob) <= 2_500_000:
            embed(r, "cds-prints", blob)
        else:
            last2 = set(file_days[-2:])
            blob = gzip.compress(json.dumps({"cols": cols + ["_src", "_file"], "rows": [s for s in slim if s[-1] in last2]}, separators=(",", ":")).encode())
            embed(r, "cds-prints", blob)

        # ------------------------------------------------------------------------------------------ 5. the warehouse
        r.section("5. What else the warehouse banks that a CDS desk would read")
        for prefix in ("data/warm/cds/", "data/warm/dtcc/", "data/warm/dtcc-fails/", "data/warm/esma-ratings/", "data/warm/ecb-sup/"):
            try:
                n, total, sample = 0, 0, []
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=prefix, PaginationConfig={"MaxItems": 3000}):
                    for o in page.get("Contents") or []:
                        n += 1
                        total += o["Size"]
                        if len(sample) < 8:
                            sample.append((o["Key"][len(prefix):], o["Size"], o["LastModified"].strftime("%Y-%m-%d")))
                r.log("%s: %d objects (first 3,000 counted), %.1f MB | %s" % (prefix, n, total / 1e6, sample))
            except Exception as exc:
                r.log("%s: %s %s" % (prefix, type(exc).__name__, exc))
        for key in ("data/global-sovereign.json", "data/cds-monitor.json", "data/cds-proxy.json", "data/esma-ratings.json", "data/sovereign-stress.json"):
            try:
                doc, sz = get_json(s3, key)
                r.log("%s: %d bytes | generated %s | keys %s" % (key, sz, doc.get("generated_at") or doc.get("updated") or doc.get("as_of"), sorted(doc)[:18]))
            except Exception as exc:
                r.log("%s: %s" % (key, type(exc).__name__))

        r.kv(priced_rows=sum(1 for g, k, x in every if k == "rows"), no_level=sum(1 for g, k, x in every if k == "unpriced"), dormant=sum(1 for g, k, x in every if k == "dormant"),
             indices=len(packet.get("indices") or []), bank_series=len(series), new_trades=len(uniq), read_by_desk=len(kept), product_kinds=len(by_tail))
        r.ok("AUDIT COMPLETE: nothing was invoked or written")


if __name__ == "__main__":
    main()
