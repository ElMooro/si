"""ops 6823 - 13F book 1.6.0 value units + CUSIP successions, dry run on the live artifacts (read-only, re-runnable).

Khalid 2026-11-17: "Fix these: Baupost shows as $5M because of its units, and Exxon has no row in the ticker table
this quarter." Same method as ops 6817 / 6820: THIS checkout's build() runs on the runner against the live desk, the
native fund artifacts, the security master, the learned ETF classes and the profile bulk - into memory. Every S3
write is intercepted and only counted; OpenFIGI is not called (master_enabled=False). Nothing is deployed or written.

Reports
  1. the value-unit verdict of every manager and period (its reported price per share line against the other books'
     median reported price of the same CUSIP), and what Baupost reads as before / after;
  2. every CUSIP succession merged, per scope: old -> new identity, unit-price ratio, holders before / now, who
     reports more / fewer;
  3. the ticker table the engine would write against the live one: rows gained (Exxon), rows lost, rows changed;
  4. what else moved against the live book, so nothing changes that was not meant to.
Checks the invariants the page relies on and embeds the dry-run book (gzip + base64) for offline page work.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import importlib.util
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)
WANT = "1.6.0"


class ReadOnly:
    """The engine's S3 client with every write intercepted: reads pass through, puts are counted and kept in memory."""

    def __init__(self, s3):
        self._s3, self.puts, self.bodies = s3, [], {}

    def get_object(self, **kw):
        return self._s3.get_object(**kw)

    def head_object(self, **kw):
        return self._s3.head_object(**kw)

    def list_objects_v2(self, **kw):
        return self._s3.list_objects_v2(**kw)

    def put_object(self, **kw):
        self.puts.append((kw.get("Key"), len(kw.get("Body") or b"")))
        self.bodies[kw.get("Key")] = kw.get("Body")
        return {}


def money(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "-"
    a, s = abs(v), "-" if v < 0 else ""
    if a >= 1e9:
        return "%s$%.2fB" % (s, a / 1e9)
    if a >= 1e6:
        return "%s$%.1fM" % (s, a / 1e6)
    if a >= 1e3:
        return "%s$%.0fK" % (s, a / 1e3)
    return "%s$%.0f" % (s, a)


def load_engine():
    for p in (ROOT / "aws" / "shared", ROOT / "aws" / "lambdas" / "justhodl-13f-book" / "source"):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    spec = importlib.util.spec_from_file_location("book13f_dry", ROOT / "aws/lambdas/justhodl-13f-book/source/lambda_function.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["book13f_dry"] = mod
    spec.loader.exec_module(mod)
    return mod


def unzip(body):
    if isinstance(body, (bytes, bytearray)) and body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return json.loads(body)


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6823_13f_units_succession_dry_run") as r:
        r.heading("ops 6823 - 13F value units + CUSIP successions dry run %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        lf = load_engine()
        r.section("1. build() of this checkout against the live artifacts (writes intercepted)")
        live = json.loads(s3.get_object(Bucket=LIVE, Key="data/13f-book.json")["Body"].read())
        live_tk = unzip(s3.get_object(Bucket=LIVE, Key="data/13f-tickers.json")["Body"].read())
        live_bytes = len(json.dumps(live, separators=(",", ":"), ensure_ascii=False))
        ro = ReadOnly(s3)
        logs = []
        t0 = time.time()
        out = lf.build(ro, datetime.now(timezone.utc), log=lambda *a: logs.append(" ".join(str(x) for x in a)), remaining_s=300.0, master_enabled=False)
        body = json.dumps(out, separators=(",", ":"), ensure_ascii=False, default=str, allow_nan=False).encode("utf-8")
        b, m = out["book"], out["book"]["managers"]
        lb, lm = live["book"], live["book"]["managers"]
        r.log("engine %s %s | build %.1fs | book %d bytes (live %s: %d, %+d) | gzip %d | period %s cohort %d/%d errors %d | intercepted writes %s" % (
            out["engine"], out["version"], time.time() - t0, len(body), live.get("version"), live_bytes, len(body) - live_bytes, len(gzip.compress(body, 6)), b["required_period"], len(b["cohort"]), len(m),
            len(b["errors"]), json.dumps(ro.puts)))
        for line in logs:
            if "thousands" in line:
                r.log("engine log: %s" % line)
        if out["version"] != WANT or b["errors"]:
            r.fail("unexpected version or artifact errors: %s %s" % (out["version"], b["errors"]))
            raise SystemExit(1)
        bad = []

        r.section("2. value units - every manager and period against the other books")
        scaled = []
        for fk in sorted(m):
            u = m[fk]["units"]
            cells = []
            for side in ("current", "prior"):
                v = u.get(side)
                cells.append("%s %s: %s x%s (%s lines, %s compared: %s dollars / %s thousands, median ratio %s)" % (
                    side, (v or {}).get("period"), (v or {}).get("status"), (v or {}).get("factor"), (v or {}).get("n_lines"), (v or {}).get("n_compared"), (v or {}).get("n_dollars"),
                    (v or {}).get("n_thousands"), (v or {}).get("median_ratio")) if v else "%s: no table" % side)
                if v and v["status"] == "unverified":
                    bad.append("%s %s: value unit unverified (%s of %s dollars, %s thousands, median %s)" % (fk, v["period"], v["n_dollars"], v["n_compared"], v["n_thousands"], v["median_ratio"]))
            flags = [x for x in m[fk]["flags"] if x.startswith("UNITS_")]
            if any((u.get(s) or {}).get("factor") == 1000 for s in ("current", "prior")):
                scaled.append(fk)
            r.log("%-14s %s%s" % (fk, " | ".join(cells), (" | flags %s" % flags) if flags else ""))
        r.log("**read x1,000: %s**" % (scaled or "none"))
        for fk in scaled:
            x, o = m[fk], lm.get(fk) or {}
            t, src = x["tearsheet"], x["tearsheet"]["source"]
            r.log("%s: 13F value %s (live book: %s) | prior %s (live: %s) | equity value %s (live: %s) | cover page as filed %s x%s = %s, gap %s, reconciles %s | in cohort %s" % (
                fk, money(t["value_usd"]), money((o.get("tearsheet") or {}).get("value_usd")), money(t["value_prior_usd"]), money((o.get("tearsheet") or {}).get("value_prior_usd")),
                money(x["total_value_usd"]), money(o.get("total_value_usd")), (src.get("current") or {}).get("reported_value_sum"), src.get("cover_scale"), money(src.get("cover_value_sum")),
                money(src.get("cover_gap_usd")), src.get("reconciles"), x.get("cohort")))
            r.log("    top lines: %s" % " · ".join("%s %s %s%%" % (z.get("ticker") or z.get("issuer"), money(z["value_usd"]), z.get("pct")) for z in t["lines"][:8]))
            ol = {(z.get("cusip"), z.get("pc")): z for z in ((o.get("tearsheet") or {}).get("lines") or [])}
            for z in t["lines"]:
                p = ol.get((z.get("cusip"), z.get("pc")))
                if p and p.get("value_usd") and abs(z["value_usd"] / p["value_usd"] - 1000.0) > 0.01:
                    bad.append("%s %s: value is not exactly 1,000x the live one (%s vs %s)" % (fk, z.get("ticker"), z["value_usd"], p["value_usd"]))
                if p and (z.get("pct"), z.get("qty")) != (p.get("pct"), p.get("qty")):
                    bad.append("%s %s: share of the book or quantity changed with the unit (%s vs %s)" % (fk, z.get("ticker"), (z.get("pct"), z.get("qty")), (p.get("pct"), p.get("qty"))))
            if src.get("reconciles") is not True:
                bad.append("%s: the scaled lines do not add up to the scaled cover page (gap %s)" % (fk, src.get("cover_gap_usd")))
            if not (1e9 < (t["value_usd"] or 0) < 5e10):
                bad.append("%s: 13F value %s is not a plausible book" % (fk, t["value_usd"]))
            if "UNITS_THOUSANDS" not in x["flags"]:
                bad.append("%s: scaled without the UNITS_THOUSANDS flag" % fk)
        if "BAUPOST" not in scaled and not any("BAUPOST" in fk.upper() for fk in scaled):
            bad.append("Baupost was not read x1,000: %s" % json.dumps({fk: m[fk]["units"] for fk in m if "BAUP" in fk.upper()}))
        # every other manager: nothing but the new keys and its own per-book mix may differ from the live block
        for fk in sorted(m):
            if fk in scaled:
                continue
            a, c = json.loads(json.dumps(m[fk], default=str)), json.loads(json.dumps(lm.get(fk) or {}))
            for side in (a, c):
                side.pop("artifact", None)
                side.pop("units", None)
                ((side.get("tearsheet") or {}).get("source") or {}).pop("cover_scale", None)
            diff = sorted(k for k in set(a) | set(c) if a.get(k) != c.get(k))
            if diff and diff != ["mix"]:
                bad.append("%s: the manager block differs from the live one in %s" % (fk, diff))
            elif diff:
                r.log("%s: only its asset mix moved (a CUSIP succession inside its own book is one unchanged pair now)" % fk)

        r.section("3. CUSIP successions merged")
        for scope in ("cohort", "concentrated"):
            rows = b["successions"][scope]
            r.log("**%s: %d merged**" % (scope, len(rows)))
            for z in rows:
                x = z["succession"]
                r.log("%-6s %s -> %s | %s -> %s | unit price x%s %s | holders %d (prior %d) continuing %d | value %s (prior %s) | new %s absent %s more %s fewer %s flat %d | net est %s | %s" % (
                    z["ticker"] or "-", x["from_cusip"], z["cusip"], x["from_issuer"][:30], z["issuer"][:30], x["unit_price_ratio"], ("basis: %s" % x["basis"]) if x["basis_verified"] else "QTY NOT COMPARED - no votes",
                    z["n_holders"], z["n_prior_holders"], x["n_continuing"], money(z["value_usd"]), money(z["value_prior_usd"]), z["new"], z["absent"], z["inc"], z["dec"], z["n_flat"], money(z["net_usd"]), z["stance"]))
                if not z["n_holders"] or not z["n_prior_holders"]:
                    bad.append("%s/%s: a succession with no holder on one side" % (scope, z["ticker"] or z["cusip"]))
                if z["reclass_candidate"] or (not x["basis_verified"] and (z["n_inc"] or z["n_dec"])):
                    bad.append("%s/%s: a merged identity is still flagged, or votes without a verified share basis" % (scope, z["ticker"] or z["cusip"]))
                if x["n_continuing"] != z["n_inc"] + z["n_dec"] + z["n_flat"] or z["n_holders"] != x["n_continuing"] + z["n_new"] or z["n_prior_holders"] != x["n_continuing"] + z["n_absent"]:
                    bad.append("%s/%s: holders do not add up (%s)" % (scope, z["ticker"] or z["cusip"], [z["n_holders"], z["n_prior_holders"], x["n_continuing"], z["n_new"], z["n_absent"], z["n_inc"], z["n_dec"], z["n_flat"]]))
            for u in ("all", "stocks", "etfs"):
                bd, lbd = b["boards"][scope][u], lb["boards"][scope][u]
                r.log("boards %s/%s: identities %s (live %s) held %s (live %s) | newly present %s (live %s) dropped %s (live %s) increased %s (live %s) decreased %s (live %s) | value %s (live %s) prior %s (live %s)" % (
                    scope, u, bd["n_identities"], lbd["n_identities"], bd["n_held"], lbd["n_held"], bd["n_newly_present"], lbd.get("n_newly_present"), bd["n_dropped"], lbd.get("n_dropped"),
                    bd["n_increased"], lbd.get("n_increased"), bd["n_decreased"], lbd.get("n_decreased"), money(bd["value_usd"]), money(lbd["value_usd"]), money(bd["value_prior_usd"]), money(lbd.get("value_prior_usd"))))
                gone = {z["succession"]["from_cusip"] for z in rows if not z["succession"]["same_cusip"]}
                for k in ("newly_present", "dropped"):
                    for z in bd.get(k) or []:
                        if z.get("succession") or (k == "dropped" and z.get("cusip") in gone and not z.get("n_holders")):
                            bad.append("boards %s/%s/%s: a merged identity is still listed (%s)" % (scope, u, k, z.get("ticker") or z.get("cusip")))
            if b["boards"][scope]["all"]["n_identities"] != lb["boards"][scope]["all"]["n_identities"] - len(rows):
                r.log("note: %s identities %s vs live %s - %s merged" % (scope, b["boards"][scope]["all"]["n_identities"], lb["boards"][scope]["all"]["n_identities"], len(rows)))
            still = [(z.get("ticker") or z.get("issuer"), z.get("cusip")) for k in ("newly_present", "dropped") for z in b["boards"][scope]["all"].get(k) or [] if z.get("reclass_candidate")]
            r.log("still flagged RECLASS? on the %s boards shown (ambiguous or not share lines): %s" % (scope, still or "none"))

        r.section("4. the ticker table the engine would write vs the live one")
        tk = unzip(ro.bodies["data/13f-tickers.json"])
        cols = tk["columns"]
        new_rows, old_rows = tk["by_ticker"], live_tk["by_ticker"]
        gained, lost = sorted(set(new_rows) - set(old_rows)), sorted(set(old_rows) - set(new_rows))
        r.log("tickers %d (live %d) | gained %d: %s | lost %d: %s" % (len(new_rows), len(old_rows), len(gained), gained, len(lost), lost))
        for t in gained + [x for x in ("XOM", "XPRO") if x in new_rows and x not in gained]:
            row = dict(zip(cols, new_rows[t]))
            r.log("%-6s holders %s (prior %s, delta %s) | value %s (prior %s) | in %s out %s net %s | bull %s bear %s %s | %s / %s | concentrated holders %s" % (
                t, row["n_holders"], row["n_prior_holders"], row["holder_delta"], money(row["value_usd"]), money(row["value_prior_usd"]), money(row["inflow_usd"]), money(row["outflow_usd"]), money(row["net_usd"]),
                row["bull"], row["bear"], row["stance"], row.get("sector"), row.get("industry"), row.get("conc_n_holders")))
        if "XOM" not in new_rows:
            bad.append("Exxon (XOM) still has no row in the ticker table")
        else:
            x = dict(zip(cols, new_rows["XOM"]))
            if not (x["n_holders"] >= 5 and x["n_prior_holders"] >= 5 and x["value_prior_usd"] > 0):
                bad.append("XOM's row does not carry both sides: %s" % x)
        if lost:
            bad.append("the ticker table lost rows: %s" % lost)
        changed = []
        lcols = live_tk["columns"]
        for t in sorted(set(new_rows) & set(old_rows)):
            a, c = dict(zip(cols, new_rows[t])), dict(zip(lcols, old_rows[t]))
            d = [k for k in ("n_holders", "n_prior_holders", "bull", "bear", "stance") if a.get(k) != c.get(k)]
            if d:
                changed.append("%s %s" % (t, {k: (c.get(k), a.get(k)) for k in d}))
        r.log("rows whose holders / votes / stance changed (%d): %s" % (len(changed), " · ".join(changed[:60]) or "none"))
        vmoved = sum(1 for t in set(new_rows) & set(old_rows) if dict(zip(cols, new_rows[t]))["value_usd"] != dict(zip(lcols, old_rows[t]))["value_usd"])
        r.log("rows whose reported value changed (a book read x1,000 holds them): %d" % vmoved)

        r.section("5. the rest of the book against the live %s one" % live.get("version"))
        for scope in ("cohort", "concentrated"):
            t, lt = b["tearsheet"][scope], lb["tearsheet"][scope]
            r.log("%s tearsheet: 13F value %s (live %s) prior %s (live %s) | lines %s (live %s) | counts %s (live %s)" % (
                scope, money(t["value_usd"]), money(lt["value_usd"]), money(t["value_prior_usd"]), money(lt["value_prior_usd"]), t["n_lines"], lt["n_lines"], json.dumps(t["counts"]), json.dumps(lt["counts"])))
            mem = [m[fk]["tearsheet"] for fk in t["members"]]
            if abs(t["value_usd"] - sum(x["value_usd"] for x in mem)) > 1 or t["n_lines"] != sum(x["n_lines"] for x in mem):
                bad.append("%s: tearsheet totals are not the members' totals" % scope)
            if t["n_lines"] != lt["n_lines"] or t["counts"] != lt["counts"]:
                bad.append("%s: the scope tearsheet's line counts changed (they are per manager line and must not)" % scope)
            st, lst = b["stance"][scope]["all"], lb["stance"][scope]["all"]
            r.log("%s stance: bullish %s (live %s) | bearish %s (live %s)" % (scope, [z["ticker"] for z in st["bullish"]][:12], [z["ticker"] for z in lst["bullish"]][:12], [z["ticker"] for z in st["bearish"]][:12],
                                                                              [z["ticker"] for z in lst["bearish"]][:12]))
            ct, lct = b["contenders"][scope]["all"], lb["contenders"][scope]["all"]
            r.log("%s contenders: rising %s (live %s) | fading %s (live %s)" % (scope, [z["ticker"] for z in ct["rising"]][:10], [z["ticker"] for z in lct["rising"]][:10], [z["ticker"] for z in ct["fading"]][:10],
                                                                                [z["ticker"] for z in lct["fading"]][:10]))
            sec, lsec = {z["name"]: z for z in b["sectors"][scope]["sectors"]}, {z["name"]: z for z in lb["sectors"][scope]["sectors"]}
            r.log("%s sectors (net est, live in brackets): %s" % (scope, " · ".join("%s %s [%s]" % (n, money(z["net_usd"]), money((lsec.get(n) or {}).get("net_usd"))) for n, z in sec.items())))
            rot, lrot = {z["bucket"]: z for z in b["rotation"][scope]["buckets"]}, {z["bucket"]: z for z in lb["rotation"][scope]["buckets"]}
            r.log("%s rotation: %s" % (scope, " · ".join("%s %s [%s] pairs %s [%s]" % (k, money(z["value_usd"]), money((lrot.get(k) or {}).get("value_usd")), json.dumps(z["pairs"]), json.dumps((lrot.get(k) or {}).get("pairs")))
                                                         for k, z in rot.items())))
        snap = b["history"]["snapshot"]
        r.log("period snapshot: %s" % json.dumps(snap))

        r.section("6. the dry-run book (gzip + base64, for offline page work)")
        blob = base64.b64encode(gzip.compress(body, 9)).decode("ascii")
        r.log("blob %d chars" % len(blob))
        r.lines.append("<!-- BOOK-GZ-B64-BEGIN")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("BOOK-GZ-B64-END -->")
        r.kv(version=out["version"], book_bytes=str(len(body)), live_bytes=str(live_bytes), managers=str(len(m)), read_x1000=",".join(scaled) or "none", successions_cohort=str(len(b["successions"]["cohort"])),
             tickers=str(len(new_rows)), tickers_gained=",".join(gained) or "none", invariant_failures=str(len(bad)))
        if bad:
            for x in bad:
                r.fail(x)
            raise SystemExit(1)
        r.ok("DRY RUN: %s read x1,000 and reconciling to the cover page; %d CUSIP successions merged in the cohort; the ticker table gains %s and loses nothing; every other manager block is unchanged" % (
            ", ".join(scaled), len(b["successions"]["cohort"]), ", ".join(gained) or "nothing"))


if __name__ == "__main__":
    main()
    sys.exit(0)
