"""ops 6955 - CDS desk 2.0, DRY RUN: the whole public tape measured again with the corrected reading; nothing written.

The audit (ops 6953) found that the desk's bank holds levels read with a 1 bp coupon where the print said 1% (AT&T at 7 bp),
one entity under two keys, tranches read as index prints, and jumps nothing repeated.  A wrong day also misled the days after
it, because a name's own recent level is what signs its next prints.  So the history is measured again from the public DTCC
files (2024-09-03 on) by the engine's own 2.0 code, exactly as the engine would, with its writes switched off:

  * every file day, both repositories, through lambda_function.run_range / process_file_date
  * sign evidence from outside the tape as it stood then: a company's own bond spread at the month-end before the day
    (the credit board's month-by-month history), and - only for the newest days - a sovereign's reference quote
  * then lambda_function.publish in dry mode: document, history, tape of prints, listings, all in memory

and the result is set beside what is published today.  ops 6956 is the same run with the writes on.

WRITE is False here.  Nothing is invoked and nothing is written.
"""
from bisect import bisect_left
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import base64
import gzip
import json
import math
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
STAGING = ROOT / "aws" / "ops" / "staging" / "cds2"       # the 2.0 source before it is deployed; gone once it ships
sys.path[:0] = ([str(STAGING)] if STAGING.exists() else []) + [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-cds-desk" / "source")]
LIVE = "justhodl-dashboard-live"
WRITE = False
NAME = "ops_6955_cds_desk_two_dry_run"
WATCH = ("AT & T", "EXPEDIA", "CATERPILLAR", "UNITEDHEALTH", "FORD MOTOR", "ORACLE", "MICROSOFT", "AMAZON COM", "NVIDIA", "UNITED MEXICAN STATES", "VIRGIN MEDIA FINANCE",
         "ARGENTINE REPUBLIC", "COMMUNITY HEALTH SYSTEMS", "TEVA PHARMACEUTICAL INDUSTRIES", "CSX", "COREWEAVE", "HSBC", "KINGDOM OF SPAIN", "FRENCH REPUBLIC", "REPUBLIC OF ITALY")


def get_json(s3, key):
    raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
    return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)


def embed(r, name, blob):
    text = base64.b64encode(blob).decode("ascii")
    r.log("fixture %s: %d bytes" % (name, len(blob)))
    r.lines.append("<!-- FIXTURE %s %d bytes" % (name, len(blob)))
    r.lines.extend(text[i:i + 16000] for i in range(0, len(text), 16000))
    r.lines.append("FIXTURE-END -->")


def jumps(bank):
    """Priced name-days more than e^0.9 from the middle of the name's previous six (the audit's count)."""
    n, tot = 0, 0
    for k, s in (bank.get("series") or {}).items():
        if k.startswith("IDX:"):
            continue
        pr = [r["s"] for d, r in sorted(s.items()) if r.get("s") is not None]
        tot += len(pr)
        for i in range(3, len(pr)):
            if abs(math.log(max(pr[i], .5) / max(statistics.median(pr[max(0, i - 6):i]), .5))) > 0.9:
                n += 1
    return n, tot


def rows_of(packet):
    out = {}
    for g, blk in (packet.get("groups") or {}).items():
        for kind in ("rows", "unpriced", "dormant", "quiet"):
            for x in blk.get(kind) or []:
                out[x["key"]] = (g, kind, x)
    return out


def gate(C, packet, tape, history):
    """What no CDS document may say, whatever the tape held: each line is a defect the audit and the dry runs found."""
    bad = []
    rows = [(g, x) for g, blk in packet["groups"].items() for x in blk["rows"]]
    every = [(g, x) for g, blk in packet["groups"].items() for kind in ("rows", "unpriced", "dormant", "quiet") for x in blk.get(kind) or []]
    by = {x["key"]: x for g, x in rows}
    for key, lo, hi in (("AT & T", 60, 250), ("EXPEDIA", 45, 250)):
        if key not in by or not lo < by[key]["spread_bp"] < hi:
            bad.append("%s reads %s" % (key, by.get(key, {}).get("spread_bp")))
    if "AT&T" in by:
        bad.append("AT&T is on the desk under two keys")
    low = [(x["name"], x["spread_bp"]) for g, x in rows if g != "sovereign" and x["spread_bp"] < 8]
    if low:
        bad.append("a company under 8 bp (the misread coupon): %s" % low[:5])
    if any(x["spread_bp"] > C.SPREAD_CEILING_BP for g, x in rows):
        bad.append("a level past the ceiling")
    wild = [(k, p) for k, v in ((history or {}).get("names") or {}).items() for p in (v.get("points") or []) if p[1] > C.SPREAD_CEILING_BP]
    if wild:
        bad.append("history days past the ceiling (a quote in the wrong unit): %d, e.g. %s" % (len(wild), wild[:4]))
    for g, blk in packet["groups"].items():
        names = [x["name"] for x in blk["rows"]]
        if len(set(names)) != len(names):
            bad.append("%s carries a name twice" % g)
    away = [(x["name"], x["listing"]["symbol"]) for g, x in every if x.get("listing") and not C.listing_allowed(x["listing"], x.get("ccy"))]
    if away:
        bad.append("a listing outside its swaps' part of the world: %s" % away[:5])
    if sum(blk["n_priced"] for blk in packet["groups"].values()) < 380 or len(packet["indices"]) < 12:
        bad.append("coverage fell: %s names with a level, %d indices" % (sum(blk["n_priced"] for blk in packet["groups"].values()), len(packet["indices"])))
    if any(c["usd_mm"] > 5e6 for w in packet["products"]["windows"].values() for c in w["classes"]):
        bad.append("a product class worth trillions (a mis-filed notional)")
    if not tape or not tape.get("names") or "AT & T" not in tape["names"]:
        bad.append("no tape of prints")
    return bad


KEPT = "data/warm/cds/before-2.0/"          # the 1.6 bank, document and history, copied here before the first 2.0 write


def main(write=False, name=NAME, after=None):
    import boto3
    from ops_report import report
    import cds_desk as C
    import lambda_function as L
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)
    L.DRY = not write
    L.TIME_BUDGET_S = 10 ** 9

    with report(name) as r:
        r.heading("%s - CDS desk %s: the public tape measured again, %s, %s" % (name.split("_")[1], C.VERSION, "WRITTEN" if write else "dry run (nothing written)", now.isoformat(timespec="minutes")))
        old_packet = get_json(s3, "data/cds-desk.json")
        old_bank = get_json(s3, "data/warm/cds/bank.json.gz")
        r.log("published today: version %s, as of %s, generated %s | bank %d series, %d days" % (old_packet.get("version"), old_packet.get("as_of"), old_packet.get("generated_at"),
                                                                                               len(old_bank.get("series") or {}), len(old_bank.get("days") or [])))
        if write:
            # the deployed engine must be this checkout's, or tomorrow's scheduled run would measure the new bank with the old reading
            import hashlib
            import io
            import urllib.request
            import zipfile
            lam = boto3.client("lambda", region_name="us-east-1")
            fn = lam.get_function(FunctionName="justhodl-cds-desk")
            z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(fn["Code"]["Location"], timeout=60).read()))
            same = {}
            for member, local in (("lambda_function.py", ROOT / "aws/lambdas/justhodl-cds-desk/source/lambda_function.py"), ("cds_desk.py", ROOT / "aws/shared/cds_desk.py"),
                                  ("cds_long_context.py", ROOT / "aws/shared/cds_long_context.py"), ("cds_risk_layers.py", ROOT / "aws/shared/cds_risk_layers.py")):
                same[member] = member in z.namelist() and hashlib.sha256(z.read(member)).hexdigest() == hashlib.sha256(local.read_bytes()).hexdigest()
            cfg = fn["Configuration"]
            r.log("deployed justhodl-cds-desk: updated %s, memory %s MB, timeout %s s | files as in this checkout: %s" % (cfg.get("LastModified"), cfg.get("MemorySize"), cfg.get("Timeout"), same))
            if not all(same.values()) or cfg.get("MemorySize", 0) < 2048 or cfg.get("Timeout", 0) < 900:
                r.fail("the deployed engine is not this checkout's 2.0: nothing was written")
                raise SystemExit(1)

        # ------------------------------------------------------------------------------ evidence from outside the tape
        r.section("1. What stands beside a name, from the warehouse")
        par_rows = L.par_curve()
        board = get_json(s3, L.CBE_KEY)
        hist = get_json(s3, "data/credit-before-equity-history.json")
        ties_all, st = L.load_ties(old_bank, board)
        r.log("credit board: %s" % st)
        reference, st = L.load_reference()
        r.log("sovereign reference: %s | %s" % (st, {k: v["cds_bp"] for k, v in sorted(reference.items())}))
        dates, comp = hist.get("dates") or [], hist.get("companies") or {}
        tie_ids = {k: t["board_id"] for k, t in ties_all.items() if t.get("board_id") and not k.startswith(C.SUB)}
        r.log("bond history: %d month-ends %s .. %s; %d CDS names tied to a company in it" % (len(dates), dates[:1], dates[-1:], sum(1 for b in tie_ids.values() if b in comp)))
        ref_anchor = {k: v for k, v in L.external_anchors(old_bank, {}, reference).items()}
        ref_from = (now.date() - timedelta(days=10)).isoformat()
        cache = {}

        def external_for(day):
            i = bisect_left(dates, day) - 1          # the last month-end strictly before the day: nothing from the future
            if i not in cache:
                cache.clear()
                cache[i] = {k: (comp[b][i], "bonds") for k, b in tie_ids.items() if i >= 0 and b in comp and i < len(comp[b]) and isinstance(comp[b][i], (int, float)) and comp[b][i] > 0}
            out = cache[i]
            if day >= ref_from:
                out = dict(out)
                out.update(ref_anchor)
            return out

        # ------------------------------------------------------------------------------ the tape, day by day
        r.section("2. Every file day since %s, through the engine's own code" % L.FIRST_PUBLIC_DAY)
        bank = {"version": C.VERSION, "series": {}, "meta": {}, "entity_map": {}, "days": []}
        docs, recent_from = {}, (now.date() - timedelta(days=L.TAPE_DAYS + 6)).isoformat()
        keep_original = L.keep_prints
        kept = {"n": 0, "bytes": 0}

        def keep(file_day, trades):
            if file_day >= recent_from and trades is not None:
                docs[file_day] = L.prints_doc(file_day, trades, L.DAY_RECORDS.get(file_day) or [])
            b = keep_original(file_day, trades)
            kept["n"] += 1
            kept["bytes"] += b or 0
            return b

        L.keep_prints = keep
        t0 = time.time()
        log, last, exhausted = L.run_range(bank, L.FIRST_PUBLIC_DAY, now.date().isoformat(), par_rows, t0, external=external_for)
        L.keep_prints = keep_original
        r.log("%d file days read in %.0f s, the last %s | %d execution days in the bank, %d series, %d products mapped to a name | prints files %s: %d (%.1f MB)" % (
            len(log), time.time() - t0, last, len(bank["days"]), len(bank["series"]), len(bank["entity_map"]), "written" if write else "that would be written", kept["n"], kept["bytes"] / 1e6))
        by_month = Counter()
        how = {}
        for d, c in (bank.get("census") or {}).items():
            by_month[d[:7]] += sum(v[0] for v in c["cls"].values())
            for k, v in (c.get("how") or {}).items():
                how.setdefault(d[:7], Counter())[k] += v
        r.log("new trades by month: %s" % dict(sorted(by_month.items())))
        r.log("single-name coupons written in percent, by month (the platform 1.6 misread): %s" % {m: c.get("percent", 0) for m, c in sorted(how.items()) if c.get("percent")})
        r.log("...bespoke or unreadable: %s" % {m: (c.get("bespoke", 0), c.get("unreadable", 0)) for m, c in sorted(how.items()) if c.get("bespoke") or c.get("unreadable")})
        j_old, t_old = jumps(old_bank)
        j_new, t_new = jumps(bank)
        unc = sum(len(C.unconfirmed_days(sorted(s.items()))) for k, s in bank["series"].items() if not k.startswith("IDX:"))
        r.log("priced name-days far (e^0.9) from the name's own previous six: %d of %d in today's bank, %d of %d measured again; %d of those are held as unconfirmed" % (j_old, t_old, j_new, t_new, unc))

        # ------------------------------------------------------------------------------ the document
        r.section("3. The document it gives, beside today's")
        as_of = bank["days"][-1]
        if write:
            # how to take it back: the 1.6 bank, document and history are copied aside before anything replaces them (never deleted)
            for key in (L.BANK_KEY, L.PACKET_KEY, L.HISTORY_KEY):
                dest = KEPT + key.rsplit("/", 1)[-1]
                try:
                    s3.head_object(Bucket=LIVE, Key=dest)
                    r.log("kept before: %s already holds the 1.6 copy" % dest)
                except Exception:  # noqa: BLE001
                    s3.copy_object(Bucket=LIVE, Key=dest, CopySource={"Bucket": LIVE, "Key": key})
                    r.log("kept before: %s -> %s" % (key, dest))
        # built in memory first and held to the gate below; only a document that passes is written
        L.DRY = True
        out = L.publish(bank, as_of, {"action": "rebuild-ops", "files": len(log), "last_file": last, "by": name}, par_rows=par_rows, today_docs=docs)
        packet, tape, history = L.LAST.get("packet"), L.LAST.get("tape"), L.LAST.get("history")
        refused = gate(C, packet, tape, history)
        r.log("the gate a document must pass before it is written: %s" % ("passed" if not refused else refused))
        if write:
            if refused:
                r.fail("the document did not pass the gate: nothing but the day files of prints was written")
                raise SystemExit(1)
            L.DRY = False
            out = L.publish(bank, as_of, {"action": "rebuild-ops", "files": len(log), "last_file": last, "by": name}, par_rows=par_rows, today_docs=docs)
            packet, tape, history = L.LAST.get("packet"), L.LAST.get("tape"), L.LAST.get("history")
        r.log("publish: %s" % json.dumps({k: out.get(k) for k in ("as_of", "dry_run", "packet_bytes", "bank_bytes_gz", "history_bytes", "tape_bytes", "beside", "n_priced", "n_unpriced", "products")})[:1500])
        for g in C.GROUPS:
            b, o = packet["groups"][g], (old_packet.get("groups") or {}).get(g) or {}
            r.log("%-12s with a level %4d (today %4s) | liquid %4d (%4s) | trade with no level %4d (%4s) | dormant %3d (%3s) | quiet, listed %4d | aged %3d, bracketed %3d, unconfirmed now %2d | tied to a stock %4d" % (
                g, b["n_priced"], o.get("n_priced"), b["n_liquid"], o.get("n_liquid"), b["n_unpriced"], o.get("n_unpriced"), b["n_dormant"], o.get("n_dormant"), b["n_quiet"], b["n_aged"],
                b["n_bracketed"], b["n_unconfirmed"], b["n_tied"]))
        r.log("sovereign coverage: %s (today %s)" % (packet["groups"]["sovereign"].get("coverage"), (old_packet["groups"]["sovereign"] or {}).get("coverage")))
        r.log("indices: %s" % [(i["key"], i["spread_bp"], i.get("price"), i["last_date"], i.get("since")) for i in packet["indices"]])
        r.log("indices today: %s" % [(i["key"], i["spread_bp"], i.get("price")) for i in old_packet.get("indices") or []])
        new_rows, old_rows = rows_of(packet), rows_of(old_packet)
        moved = []
        for k, (g, kind, x) in new_rows.items():
            o = old_rows.get(k)
            if kind == "rows" and o and o[1] == "rows" and isinstance(o[2].get("spread_bp"), (int, float)) and x.get("spread_bp"):
                ratio = x["spread_bp"] / max(o[2]["spread_bp"], 0.01)
                if ratio > 1.25 or ratio < 0.8:
                    moved.append((abs(math.log(ratio)), x["name"], g, o[2]["spread_bp"], o[2].get("last_date"), x["spread_bp"], x["last_date"], x.get("spread_basis"), x.get("anchor_source"), x.get("bond_bp")))
        r.log("names with a level in both whose level differs by more than a fifth: %d" % len(moved))
        for m in sorted(moved, reverse=True)[:45]:
            r.log("  %-34s %-11s today %9.1f (%s) -> %9.1f (%s) %s / %s | its bonds %s" % (m[1][:34], m[2], m[3], m[4], m[5], m[6], m[7], m[8], m[9]))
        gained = [(x["name"], g, x["spread_bp"], x["status"], x.get("spread_basis"), x.get("anchor_source")) for k, (g, kind, x) in new_rows.items() if kind == "rows" and (k not in old_rows or old_rows[k][1] != "rows")]
        lost = [(o[2]["name"], o[0], o[2].get("spread_bp"), new_rows.get(k, (None, "not on the desk"))[1]) for k, o in old_rows.items() if o[1] == "rows" and (k not in new_rows or new_rows[k][1] != "rows")]
        r.log("names that gain a level: %d, by how: %s" % (len(gained), dict(Counter("%s / %s" % (g[3], g[5]) for g in gained).most_common(12))))
        r.log("  e.g. %s" % gained[:30])
        r.log("names that had a level today and have none measured again: %d %s" % (len(lost), lost[:40]))
        r.log("the names the audit watched:")
        for k in WATCH:
            n, o = new_rows.get(k), old_rows.get(k)
            r.log("  %-32s today %-10s %8s bp | measured again %-10s %8s bp (%s, %s, %s) 1w %s 1m %s | bonds %s | ticker %s | listing %s | unconfirmed %s, %s in a year" % (
                k[:32], o[1] if o else "-", (o[2].get("spread_bp") if o else None), n[1] if n else "-", (n[2].get("spread_bp") if n else None), (n[2].get("status") if n else None),
                (n[2].get("spread_basis") if n else None), (n[2].get("anchor_source") if n else None), (n[2].get("chg_1w_bp") if n else None), (n[2].get("chg_1m_bp") if n else None),
                (n[2].get("bond_bp") if n else None), (n[2].get("ticker") if n else None), ((n[2].get("listing") or {}).get("symbol") if n else None),
                (n[2].get("unconfirmed") if n else None), (n[2].get("n_unconfirmed_1y") if n else None)))
        dup = [n for n, c in Counter((g, x.get("name")) for k, (g, kind, x) in new_rows.items()).items() if c > 1]
        r.log("names on the desk more than once in a group: %s" % dup)

        r.section("4. Products, options, mortgage indices, listings")
        pr = packet["products"]
        for w in ("1d", "5d", "30d"):
            r.log("products, %s (%s days to %s, %d prints): %s" % (w, pr["windows"][w]["days"], pr["windows"][w]["last"], pr["windows"][w]["n"],
                                                             [(c["cls"], c["n"], c["usd_mm"], c["eur_mm"]) for c in pr["windows"][w]["classes"]]))
        r.log("families, 30d: %s" % [(f["cls"], f["family"], f["n"], f["measured"]) for f in pr["windows"]["30d"]["families"]][:40])
        for f in packet["options"]["families"]:
            r.log("options %s: %d prints, $%.0fM, strikes in %s, index at %s, struck wider %s%% | ladder %s | expiries %s" % (
                f["family"], f["n"], f["notional_mm"], f["strike_kind"], f["index_level"], f["share_struck_wider_pct"], [(x["strike"], x["notional_mm"]) for x in f["ladder"]][:30],
                [(e["expiry"], e["n"]) for e in f["expiries"]][:8]))
        r.log("mortgage indices: %s" % [(x["label"], x["final"][:4], x["points"], x["n_30d"], x["chg_1m_points"]) for x in packet["structured"]["rows"]])
        r.log("quality: %s" % json.dumps(packet["quality"])[:1500])
        listed = [(x["name"], (x.get("listing") or {}).get("symbol"), (x.get("listing") or {}).get("exchange"), x.get("ticker")) for k, (g, kind, x) in new_rows.items() if x.get("listing")]
        r.log("listings: %d names with a home listing, by exchange %s" % (len(listed), dict(Counter(l[2] for l in listed).most_common(25))))
        r.log("  outside the United States: %s" % [l for l in listed if l[2] not in ("NYSE", "NASDAQ", "AMEX", "OTC")][:60])
        r.log("  a listing that names another company than the credit board's tie: %s" % [(x["name"], x.get("ticker"), x["listing"]["symbol"], x["listing"]["name"]) for k, (g, kind, x) in new_rows.items()
                                                                                         if x.get("listing") and x.get("ticker") and x["listing"]["exchange"] in ("NYSE", "NASDAQ") and x["listing"]["symbol"] != x["ticker"]][:30])
        # second pass (after the first dry run's findings): what the tightened rules give
        away = [(x["name"], x.get("ccy"), x["listing"]["symbol"], x["listing"]["country"]) for k, (g, kind, x) in new_rows.items() if x.get("listing") and not C.listing_allowed(x["listing"], x.get("ccy"))]
        r.log("  a listing outside the part of the world its swaps' currency says: %d %s" % (len(away), away[:12]))
        ceil = [(x["name"], g, x.get("points_upfront"), x["last_date"], x["status"]) for k, (g, kind, x) in new_rows.items() if kind == "rows" and x.get("at_ceiling")]
        r.log("levels carried at the %d bp ceiling and read in points: %s" % (C.SPREAD_CEILING_BP, ceil))
        r.log("widest levels under the ceiling: %s" % sorted([(x["spread_bp"], x["name"][:28], x.get("points_upfront")) for k, (g, kind, x) in new_rows.items() if kind == "rows" and not x.get("at_ceiling")], reverse=True)[:8])
        r.log("indices not printed in ten days (shown aged, with their date): %s" % [(i["key"], i["last_date"], i["stale_days"]) for i in packet["indices"] if i.get("status") == "aged"])
        for f in packet["options"]["families"]:
            r.log("options %s: off the ladder %s | expiry not given %s | expired in the window %s" % (f["family"], [(x["strike"], x["n"]) for x in f.get("off_ladder") or []], f.get("expiry_not_given"),
                                                                                                    [(e["expiry"], e["n"]) for e in f["expiries"] if e.get("expired")]))
        r.log("options on another underlier (counted, not drawn): %s of %s" % (packet["options"].get("n_other_underlier"), packet["options"].get("n_all")))
        big = {c["cls"]: (c["n"], c["usd_mm"], c.get("eur_mm")) for c in pr["windows"]["30d"]["classes"]}
        r.log("products 30d after the notional cap: %s" % big)
        r.log("tape: %s names, %s prints of the last %d days" % ((tape or {}).get("n_names"), (tape or {}).get("n_prints"), L.TAPE_DAYS))
        r.log("quotes read as points or with their unit repaired, in the tape's 30 days: %s" % dict(Counter(row[-1] for v in ((tape or {}).get("names") or {}).values() for row in v if row[-1])))
        hz = ((history or {}).get("names") or {}).get("HERTZ") or {}
        r.log("Hertz, the lowest days of its record (49 bp for a day in 1.6): %s | names cut in the tape document: %s" % (sorted(hz.get("points") or [], key=lambda p: p[1])[:3], len((tape or {}).get("cut") or {})))
        r.log("history: %d names%s" % (len((history or {}).get("names") or {}), ", %d with unconfirmed days set apart" % sum(1 for v in ((history or {}).get("names") or {}).values() if v.get("unconfirmed"))))
        banned = [w for w in ("forecast", "predict") if w in json.dumps({k: v for k, v in packet.items() if k != "groups"}).lower()]
        r.log("the document's own words (outside names) keep the desk's doctrine: %s" % ("yes" if not banned else banned))

        embed(r, "cds-packet-2", gzip.compress(json.dumps(packet, separators=(",", ":")).encode()))
        if tape:
            embed(r, "cds-tape-2", gzip.compress(json.dumps(tape, separators=(",", ":")).encode()))
        if history:
            blob = gzip.compress(json.dumps(history, separators=(",", ":")).encode())
            if len(blob) > 2_600_000:      # keep the report a file a browser can still open: the long records, and the watched names
                slim = dict(history, names={k: v for k, v in history["names"].items() if k in WATCH or k.startswith("IDX:")})
                blob = gzip.compress(json.dumps(slim, separators=(",", ":")).encode())
            embed(r, "cds-history-2", blob)
        r.kv(file_days=len(log), series=len(bank["series"]), with_a_level=sum(packet["groups"][g]["n_priced"] for g in C.GROUPS), with_a_level_today=sum((old_packet["groups"].get(g) or {}).get("n_priced") or 0 for g in C.GROUPS),
             indices=len(packet["indices"]), listed=len(listed), written=write, seconds=round(time.time() - t0))
        if after:
            after(r, packet)
        if write:
            r.ok("CDS DESK %s WRITTEN from the public tape: bank, document, history, tape of prints and %d day files" % (C.VERSION, kept["n"]))
        else:
            r.ok("DRY RUN COMPLETE: nothing was invoked or written")


if __name__ == "__main__":
    main(WRITE, NAME)
