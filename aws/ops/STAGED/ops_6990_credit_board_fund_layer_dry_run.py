"""ops 6990 - Credit Before Equity, the fund layer: dry run on the real warehouse (read-only, re-runnable).

Khalid 2026-11-26 20:32 ET: "go" - Credit Before Equity first: who holds each bond, how far apart funds mark it, and how
funds are positioned in each company's credit default swaps.

The engine's own build() runs here against the live bucket with its writes left out (as ops 6942 does), then the new
fund layer (cbe_funds.py) ties the SEC fund-holdings tables of justhodl-sec-datasets to the board. Nothing is invoked and
nothing is written. Printed for review before anything is deployed:

  1. the board as it builds today, and what the fund layer adds: securities read, tied, by which rule; companies reached
  2. every tie made by a naming rule rather than by the issuer's own name (the place a wrong tie would hide), the listed
     companies funds hold bonds of that the board does not cover, and the largest issuers left untied
  3. eight companies' fund figures worked again from the raw table by this script's own arithmetic
  4. the widest gaps between fund marks, and what most positions flag in default or paid in kind
  5. the credit default swaps funds hold: names tied to a company, the largest sold and bought, names left untied
  6. sizes, and that no column is an identifier
  7. the documents embedded (gzip + base64) so the page can be rehearsed on real figures before the engine is deployed
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import re
import resource
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source"
STAGING = ROOT / "aws" / "ops" / "staging" / "cbe_funds"       # the engine's source with the fund layer, before it is deployed (dry run first)
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared")] + ([str(STAGING)] if STAGING.exists() else []) + [str(SRC)]
NAME = "ops_6990_credit_board_fund_layer_dry_run"
LOOK = [("T:CRWV", r"COREWEAVE"), ("T:ORCL", r"^ORACLE"), ("T:F", r"^FORD MOTOR"), ("T:T", r"^AT&T|^AT T "), ("T:BA", r"^BOEING"), ("T:ECHO", r"DISH|ECHOSTAR"),
        ("I:MERIDIAN ARC HOLDCO", r"MERIDIAN ARC"), ("T:CORZ", r"CORE SCIENTIFIC")]


class Body:
    def __init__(self, raw):
        self.raw = raw

    def read(self):
        return self.raw


class ReadOnly:
    """The live bucket: read, never written."""

    def __init__(self, s3):
        self.s3 = s3

    def get_object(self, Bucket, Key):
        return self.s3.get_object(Bucket=Bucket, Key=Key)

    def list_objects_v2(self, **kw):
        return self.s3.list_objects_v2(**kw)

    def put_object(self, **kw):
        raise RuntimeError("the dry run writes nothing")


def money(v):
    return "$%.1fbn" % ((v or 0) / 1e9) if abs(v or 0) >= 1e9 else "$%.0fm" % ((v or 0) / 1e6)


def embed(r, name, obj):
    blob = base64.b64encode(gzip.compress(json.dumps(obj, separators=(",", ":"), default=str).encode(), 9)).decode()
    r.lines.append("<!-- FIXTURE %s %d bytes gzip+base64" % (name, len(blob)))
    r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
    r.lines.append("FIXTURE-END -->")
    r.log("fixture %s embedded: %d characters" % (name, len(blob)))


def main():
    import boto3
    from ops_report import report
    import lambda_function as L
    import cbe_funds as F
    real = ReadOnly(boto3.client("s3", region_name="us-east-1"))
    now = datetime.now(timezone.utc)
    with report(NAME) as r:
        fails = []

        def check(ok, text):
            (r.ok if ok else r.fail)(text)
            if not ok:
                fails.append(text)

        r.heading("ops 6990 - Credit Before Equity fund layer %s, dry run on the real warehouse %s" % (F.VERSION, now.isoformat(timespec="minutes")))
        r.section("1. the board as it builds today, and what the fund layer adds")
        evidence, _ = L.optional(real, L.EVIDENCE_KEY)
        logs, t0 = [], time.time()
        built = L.build(real, now, log=logs.append, evidence=evidence)
        payload = built["payload"]
        r.log("board built in %.0fs: as of %s, %d companies (%d listed), version %s" % (time.time() - t0, payload["as_of"], payload["coverage"]["companies"], payload["coverage"]["listed"], payload["version"]))
        t0 = time.time()
        layer, why = L.fund_layer(real, built)
        if not layer:
            r.fail("no fund layer: %s" % why)
            raise SystemExit(1)
        block, by_co = layer["block"], layer["by_co"]
        took = time.time() - t0
        r.log("fund layer in %.1fs, peak memory %.0f MB | quarter %s, as of %s, the bank's document of %s" % (
            took, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, block["file"], block["as_of"], block["quarter"]["bank_generated_at"]))
        r.log("bonds and loans of companies in the SEC table: %s worth %s | tied to the board: %s worth %s (%s%% of the value) | companies reached: %d (%d listed)" % (
            format(block["read"]["securities"], ","), money(block["read"]["value"]), format(block["tied"]["securities"], ","), money(block["tied"]["value"]), block["tied"]["value_pct"],
            block["tied"]["companies"], block["tied"]["listed"]))
        r.log("tied by: %s" % "; ".join("%s %s securities %s" % (k, format(v[0], ","), money(v[1])) for k, v in block["tied"]["how"].items()))
        cols = payload["issuers"]["cols"]
        rows = [dict(zip(cols, x)) for x in payload["issuers"]["rows"]]
        held_value = sum(x["value"] or 0 for x in rows)
        reached = [x for x in rows if x["id"] in by_co and by_co[x["id"]].get("f_n")]
        r.log("of the board's %d companies, %d have fund holdings tied: %.1f%% of the value the board reads; listed %d of %d; companies with a fund credit swap tied: %d" % (
            len(rows), len(reached), 100.0 * sum(x["value"] or 0 for x in reached) / held_value, sum(1 for x in reached if x["ticker"]), sum(1 for x in rows if x["ticker"]),
            sum(1 for v in by_co.values() if v.get("fc_funds"))))
        big_missing = sorted((x for x in rows if x["id"] not in by_co or not by_co[x["id"]].get("f_n")), key=lambda x: -(x["value"] or 0))[:25]
        r.log("largest board companies with nothing tied: %s" % "; ".join("%s %s" % ((x["ticker"] or x["name"])[:26], money(x["value"])) for x in big_missing))
        check(block["tied"]["value_pct"] is not None and block["tied"]["value_pct"] >= 40, "at least 40%% of the corporate value in the SEC table ties to a board company: %s%%" % block["tied"]["value_pct"])
        check(len(reached) >= 0.5 * len(rows), "at least half of the board's companies are reached: %d of %d" % (len(reached), len(rows)))

        r.section("2. ties made by a naming rule, listed companies the board does not cover, and the largest left untied")
        r.log("ties by rule (issuer as funds write it | rule | company | securities | value): %d listed here" % len(block["ties_by_rule"]))
        for x in block["ties_by_rule"][:70]:
            r.log("  %-44s %-11s %-9s %-34s %4d %9s" % (str(x[0])[:44], x[1], x[2][:9], str(x[3])[:34], x[4], money(x[5])))
        review = layer["lists"]["ties_by_rule"]
        new_rules = [x for x in review if x[1] in ("short name", "co-issuer", "name cut")]
        r.log("ties by a short name, a co-issuer or a cut name: %d issuer names, %s securities worth %s. The %d largest, then every 12th of the rest:" % (
            len(new_rules), format(sum(x[4] for x in new_rules), ","), money(sum(x[5] for x in new_rules)), min(len(new_rules), 260)))
        for x in new_rules[:260] + new_rules[260::12]:
            r.log("  %-44s %-11s %-9s %-34s %4d %9s" % (str(x[0])[:44], x[1], x[2][:9], str(x[3])[:34], x[4], money(x[5])))
        r.log("listed companies funds hold bonds of that the board does not cover: %d; largest (ticker | issuer | securities | value | most funds):" % block["n_not_on_board"])
        for x in block["not_on_board"][:30]:
            r.log("  %-7s %-44s %4d %9s %4d" % (x[0], str(x[1])[:44], x[2], money(x[3]), x[4]))
        r.log("issuers left untied: %d; largest (issuer | securities | value | most funds):" % block["n_untied"])
        for x in block["untied"][:100]:
            r.log("  %-56s %4d %9s %4d" % (x[0][:56], x[1], money(x[2]), x[3]))

        r.section("3. eight companies worked again from the raw table")
        head = L.get_json(real, L.FUNDS_HEAD_KEY)
        r.log("the bank's document: %s, as of %s, file %s, set aside %s, quarters read %d" % (head.get("generated_at"), head.get("as_of"), (head.get("source") or {}).get("file"), head.get("set_aside"), len(head.get("history") or [])))
        table = L.get_json(real, head["tables"]["bonds"])
        tc = table["cols"]
        raw = [dict(zip(tc, x)) for x in table["rows"]]
        r.log("raw table: %s rows, %d columns" % (format(len(raw), ","), len(tc)))
        for co, pat in LOOK:
            rx = re.compile(pat, re.I)
            mine = [b for b in raw if b["cls"] in F.CLASSES and b["itype"] not in F.NOT_COMPANIES and rx.search(b["issuer"] or "")]
            got = by_co.get(co) or {}
            names_seen = sorted({(b["issuer"] or "")[:40] for b in mine})
            r.log("  %-24s the layer: %3s securities, most funds %4s, value %9s, mark %7s, gap %5s, default %s, loans %s | by pattern %r: %3d securities %9s | written as: %s" % (
                co, got.get("f_n"), got.get("f_funds"), money(got.get("f_value")), got.get("f_px"), got.get("f_gap"), got.get("f_def"), got.get("f_loans"), pat, len(mine),
                money(sum(b["value"] or 0 for b in mine)), "; ".join(names_seen[:8])))
        fund_rows = [dict(zip(F.ROW_COLS, x)) for x in layer["rows"]]
        per = {}
        for x in fund_rows:
            per.setdefault(x["co"], []).append(x)
        bad = [co for co, mine in per.items() if by_co[co]["f_n"] != len(mine) or by_co[co]["f_value"] != sum(int(x["value"] or 0) for x in mine) and abs(by_co[co]["f_value"] - sum(x["value"] or 0 for x in mine)) > 1]
        check(not bad and len(per) == len(reached), "every company's count and value are its own rows added up (%d companies)" % len(per))
        check(all(x["co"] in {y["id"] for y in rows} for x in fund_rows), "every published row sits under a company on the board")

        r.section("4. gaps between fund marks, and flags")
        r.log("securities marked by %d funds or more among tied companies: %d | widest gaps (company | title | funds marking | middle | 10th | 90th | gap | value):" % (block["min_marks"], block["n_gaps"]))
        for g in block["gaps"][:25]:
            r.log("  %-30s %-30s %4s %7s %7s %7s %6s %9s" % (str(g[1])[:30], str(g[2])[:30], g[6], g[7], g[8], g[9], g[10], money(g[11])))
        r.log("flagged by most positions in default or paid in kind: %d securities | largest (company | title | funds | positions | default | paid in kind | mark | value):" % block["n_flagged"])
        for f in block["flagged"][:25]:
            r.log("  %-30s %-30s %4s %4s %4s %4s %7s %9s" % (str(f[1])[:30], str(f[2])[:30], f[5], f[6], f[7], f[8], f[9], money(f[10])))
        n_def = sum(1 for v in by_co.values() if v.get("f_def"))
        r.log("companies with a security most positions flag in default: %d | paid in kind: %d | with a gap of 5 points or more: %d" % (
            n_def, sum(1 for v in by_co.values() if v.get("f_pik")), sum(1 for v in by_co.values() if (v.get("f_gap") or 0) >= 5)))

        r.section("5. credit default swaps funds hold")
        cds = block["cds"]
        named = [x for x in cds["names"] if x[1]]
        r.log("names: %d | tied to a board company: %d | totals %s" % (len(cds["names"]), len(named), json.dumps(cds["totals"])))
        r.log("largest tied (name | company | positions | funds | sold | bought | net sold):")
        for x in sorted(named, key=lambda x: -((x[5] or 0) + (x[6] or 0)))[:30]:
            r.log("  %-40s %-10s %5s %4s %9s %9s %9s" % (str(x[0])[:40], x[1][:10], x[3], x[4], money(x[5]), money(x[6]), money(x[8])))
        r.log("largest not tied (name | listed ticker if any | sold | bought): %s" % "; ".join(
            "%s%s %s/%s" % (str(x[0])[:30], (" [%s]" % x[2]) if x[2] else "", money(x[5]), money(x[6])) for x in sorted((x for x in cds["names"] if not x[1]), key=lambda x: -((x[5] or 0) + (x[6] or 0)))[:25]))
        again = F.Ties(built["ties"], built["matcher"], {x["id"]: x["name"] for x in rows})
        ruled = [(x, again.of(x[0])[1]) for x in named]
        r.log("swap names tied by a naming rule rather than the issuer's own name: %d of %d (name | rule | company | sold | bought):" % (sum(1 for _, h in ruled if h != "issuer"), len(named)))
        for x, h in sorted((y for y in ruled if y[1] != "issuer"), key=lambda y: -((y[0][5] or 0) + (y[0][6] or 0)))[:60]:
            r.log("  %-44s %-11s %-10s %9s %9s" % (str(x[0])[:44], h, x[1][:10], money(x[5]), money(x[6])))
        both = [x for x in cds["names"] if x[-1]]
        r.log("reference names that are also a dealer's name: %d, of which left untied for that reason %d (name -> company if a bond issuer on the board is written that way | sold/bought): %s" % (
            len(both), cds["dealer_names_not_tied"],
            "; ".join("%s%s %s/%s" % (str(x[0])[:28], (" -> " + x[1]) if x[1] else "", money(x[5]), money(x[6])) for x in sorted(both, key=lambda x: -((x[5] or 0) + (x[6] or 0)))[:30])))
        net = sorted(((co, v) for co, v in by_co.items() if v.get("fc_funds")), key=lambda kv: kv[1]["fc_net"])
        names_of = {x["id"]: x["ticker"] or x["name"] for x in rows}
        r.log("funds are net BUYERS of protection on: %s" % "; ".join("%s %s" % (str(names_of.get(co))[:18], money(-v["fc_net"])) for co, v in net[:12] if v["fc_net"] < 0))
        r.log("funds are net SELLERS of protection on: %s" % "; ".join("%s %s" % (str(names_of.get(co))[:18], money(v["fc_net"])) for co, v in net[::-1][:12] if v["fc_net"] > 0))
        check(len(named) >= 150, "at least 150 swap names tie to a board company: %d" % len(named))

        r.section("6. sizes, and no identifier")
        before = len(json.dumps(payload, separators=(",", ":"), default=str))
        payload["issuers"] = F.add_columns(payload["issuers"], by_co)
        payload["funds"] = block
        after = json.dumps(payload, separators=(",", ":"), default=str)
        fdoc = json.dumps({"cols": F.ROW_COLS, "rows": layer["rows"], **layer["lists"]}, separators=(",", ":"))
        r.log("main document: %.2f MB before, %.2f MB with the fund columns and block (gzip %.2f MB) | fund document: %s rows, %.2f MB (gzip %.2f MB)" % (
            before / 1048576, len(after) / 1048576, len(gzip.compress(after.encode(), 6)) / 1048576, format(len(layer["rows"]), ","), len(fdoc) / 1048576, len(gzip.compress(fdoc.encode(), 6)) / 1048576))
        check(payload["issuers"]["cols"] == L.ISSUER_COLS + F.COLS and all(len(x) == len(L.ISSUER_COLS) + len(F.COLS) for x in payload["issuers"]["rows"]), "the fund columns come after the board's own %d" % len(L.ISSUER_COLS))
        check(not {"cusip", "isin", "figi", "k"} & {c.lower() for c in F.ROW_COLS + F.COLS + F.CDS_COLS}, "no column is an identifier")
        ident = re.compile(r"(?<![A-Za-z0-9])(?:[A-Z]{2}[A-Z0-9]{9}\d|(?=[A-Z0-9]*\d[A-Z0-9]*\d[A-Z0-9]*\d)[A-Z0-9]{9})(?![A-Za-z0-9])")
        texts = [str(x[c]) for x in fund_rows for c in ("issuer", "title") if x.get(c)]
        shaped = [t for t in texts if ident.search(t)]
        r.log("names and titles in the fund document: %s | shaped like an identifier: %d%s" % (format(len(texts), ","), len(shaped), (" e.g. " + "; ".join(re.sub(r"[A-Z0-9]", "x", s)[:30] for s in shaped[:3])) if shaped else ""))
        check(not shaped, "no name or title is shaped like a CUSIP or an ISIN")

        r.section("7. the documents, for rehearsing the page on real figures")
        embed(r, "cbe-payload-with-funds", payload)
        keep = {co for co, _ in LOOK} | {g[0] for g in block["gaps"][:15]} | {f[0] for f in block["flagged"][:15]} | {x["id"] for x in sorted(reached, key=lambda x: -(x["value"] or 0))[:25]}
        embed(r, "cbe-funds-rows-sample", {"version": F.VERSION, "as_of": block["as_of"], "file": block["file"], "cols": F.ROW_COLS, "rows": [x for x in layer["rows"] if x[0] in keep],
                                            "min_marks": F.MIN_MARKS, "tie_cols": F.TIE_COLS, "absent_cols": F.ABSENT_COLS, "untied_cols": F.UNTIED_COLS,
                                            "ties_by_rule": layer["lists"]["ties_by_rule"], "not_on_board": layer["lists"]["not_on_board"], "untied": layer["lists"]["untied"][:1500],
                                            "note": "a cut for rehearsal: the rows of %d companies, the first 1,500 untied issuers" % len(keep)})
        r.kv(failed_checks=len(fails), companies_reached=len(reached), tied_value_pct=block["tied"]["value_pct"], cds_names_tied=len(named))
        if fails:
            r.fail("%d check(s) failed" % len(fails))
            raise SystemExit(1)
        r.ok("dry run complete: nothing was written")


if __name__ == "__main__":
    main()
