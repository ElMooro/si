"""ops 6817 - 13F book 1.4.0 tearsheet, dry run on the live artifacts (read-only, re-runnable).

Runs THIS checkout's justhodl-13f-book build() on the runner against the live desk, the 18 native fund artifacts, the
security master, the learned ETF classes and the profile bulk - into memory. Every S3 write the engine attempts is
intercepted and only counted; OpenFIGI is not called (master_enabled=False). Nothing is deployed, nothing is written.

Reports, per manager, the new tearsheet block: 13F value now / prior (every line: shares, notes at principal, option
rows at the underlying's reported value), line items, top-10 share, new / increased / reduced / closed / unchanged,
option share, whether the lines add up to the cover page, the most notable addition and cut, the largest stock line,
the top lines and the industries most added to / cut. Checks the invariants the page relies on and the size the book
would have. Embeds the whole dry-run book (gzip + base64) so the page can be developed against real data offline.
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


class ReadOnly:
    """The engine's S3 client with every write intercepted: reads pass through, puts are counted and dropped."""

    def __init__(self, s3):
        self._s3, self.puts = s3, []

    def get_object(self, **kw):
        return self._s3.get_object(**kw)

    def head_object(self, **kw):
        return self._s3.head_object(**kw)

    def list_objects_v2(self, **kw):
        return self._s3.list_objects_v2(**kw)

    def put_object(self, **kw):
        self.puts.append((kw.get("Key"), len(kw.get("Body") or b"")))
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


def name(r):
    if not r:
        return "-"
    return "%s%s" % (r.get("ticker") or (r.get("name") or r.get("issuer") or "")[:22], (" " + r["pc"].lower() + "s") if r.get("pc") else "")


def tag(r):
    st = r.get("status") or ""
    if st.startswith("newly"):
        return "NEW"
    if st.startswith("not_present"):
        return "CLOSED"
    if r.get("multiple") and r["multiple"] >= 2:
        return "%.1fx" % r["multiple"]
    if r.get("qty_change_pct") is not None:
        return "%+.1f%%" % r["qty_change_pct"]
    return "flat" if st.endswith("unchanged") else "?"


def load_engine():
    for p in (ROOT / "aws" / "shared", ROOT / "aws" / "lambdas" / "justhodl-13f-book" / "source"):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    spec = importlib.util.spec_from_file_location("book13f_dry", ROOT / "aws/lambdas/justhodl-13f-book/source/lambda_function.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["book13f_dry"] = mod
    spec.loader.exec_module(mod)
    return mod


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6817_13f_tearsheet_dry_run") as r:
        r.heading("ops 6817 - 13F tearsheet dry run %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        lf = load_engine()
        r.section("1. build() of this checkout against the live artifacts (writes intercepted)")
        live_bytes = s3.head_object(Bucket=LIVE, Key="data/13f-book.json")["ContentLength"]
        ro = ReadOnly(s3)
        t0 = time.time()
        out = lf.build(ro, datetime.now(timezone.utc), log=lambda *a: None, remaining_s=300.0, master_enabled=False)
        body = json.dumps(out, separators=(",", ":"), ensure_ascii=False, default=str, allow_nan=False).encode("utf-8")
        b = out["book"]
        m = b["managers"]
        tear_bytes = sum(len(json.dumps(x.get("tearsheet"), separators=(",", ":"), default=str)) for x in m.values())
        r.log("engine %s %s contract %s | build %.1fs | book %d bytes (live %d, +%d = +%.1f%%; tearsheets %d bytes) | gzip %d | period %s cohort %d/%d errors %d | intercepted writes: %s" % (
            out["engine"], out["version"], out["contract"], time.time() - t0, len(body), live_bytes, len(body) - live_bytes, 100.0 * (len(body) - live_bytes) / live_bytes, tear_bytes,
            len(gzip.compress(body, 6)), b["required_period"], len(b["cohort"]), len(m), len(b["errors"]), json.dumps(ro.puts)))
        if out["version"] != "1.4.0" or b["errors"]:
            r.fail("unexpected version or artifact errors: %s %s" % (out["version"], b["errors"]))
            raise SystemExit(1)

        r.section("2. the tearsheet per manager")
        bad = []
        for fk, x in sorted(m.items(), key=lambda kv: -(kv[1]["tearsheet"]["value_usd"] or 0)):
            t = x["tearsheet"]
            c, o, src, e = t["counts"], t["options"], t.get("source") or {}, t["est"]
            cur, pri = src.get("current") or {}, src.get("prior") or {}
            r.log("**%s** %s vs %s %s %s | 13F value %s (prior %s, %s%%) | lines %d (prior %d) | top-10 %s%% | new %d / increased %d | reduced %d / closed %d | unchanged %d | not compared %d" % (
                fk, x["current_period"], x["prior_period"], x["status"], ",".join(x["flags"]), money(t["value_usd"]), money(t["value_prior_usd"]), t["value_change_pct"], t["n_lines"], t["n_lines_prior"],
                t["top10_pct"], c["new"], c["increased"], c["reduced"], c["closed"], c["unchanged"], c["not_compared"]))
            r.log("    options %d lines (%d calls / %d puts) = %s%% of value (calls %s%% puts %s%%) | equity value %s | est in %s out %s (options in %s out %s) reclass lines %d | cover %s rows %s reconciles %s gap %s | %s filed %s vs %s filed %s" % (
                o["n_lines"], o["n_calls"], o["n_puts"], o["share_pct"], o["call_share_pct"], o["put_share_pct"], money(x["total_value_usd"]), money(e["in_usd"]), money(e["out_usd"]), money(e["option_in_usd"]),
                money(e["option_out_usd"]), e["reclass_lines"], money(src.get("cover_value_sum")), cur.get("reported_rows"), src.get("reconciles"), src.get("cover_gap_usd"), cur.get("form"), cur.get("filed_at"), pri.get("form"), pri.get("filed_at")))
            add, cut, big = t["notable"]["add"], t["notable"]["cut"], t["largest_stock"]
            r.log("    notable add: %s %s %s est %s | notable cut: %s %s %s est %s | largest stock: %s %s %s%% %s" % (
                (add or {}).get("how"), name(add), tag(add) if add else "", money((add or {}).get("est_usd")), (cut or {}).get("how"), name(cut), tag(cut) if cut else "", money((cut or {}).get("est_usd")),
                name(big), money((big or {}).get("value_usd")), (big or {}).get("pct"), tag(big) if big else ""))
            r.log("    lines: %s" % " · ".join("%s %s%% %s" % (name(z), z["pct"], tag(z)) for z in t["lines"]))
            for lab in ("initiations", "increases", "reductions", "terminations"):
                L = t[lab]
                r.log("    %s %d (est %s; shown %d, others %d, more %d): %s" % (lab, L["n"], money(L["est_usd"]), len(L["rows"]), len(L["others"]), L["n_more"],
                                                                               " · ".join("%s %s %s%s" % (name(z), tag(z), money(z.get("est_usd")), " RECLASS" if z.get("reclass") else "") for z in L["rows"][:6])))
            ind = t["industries"]
            r.log("    industries added: %s | cut: %s" % (
                "; ".join("%s %s (%s)" % (a["industry"], money(a["net_usd"]), ", ".join(n["label"] for n in a["names"][:4])) for a in ind["added"]) or "-",
                "; ".join("%s %s (%s)" % (a["industry"], money(a["net_usd"]), ", ".join(n["label"] for n in a["names"][:4])) for a in ind["cut"]) or "-"))
            # invariants the page relies on
            if c["new"] + c["increased"] + c["reduced"] + c["unchanged"] + c["not_compared"] != t["n_lines"]:
                bad.append("%s: change classes do not add up to the line count" % fk)
            if [z["value_usd"] for z in t["lines"]] != sorted((z["value_usd"] for z in t["lines"]), reverse=True):
                bad.append("%s: top lines not ranked by value" % fk)
            if not o["n_lines"] and not x["n_debt_rows"] and abs((t["value_usd"] or 0) - (x["total_value_usd"] or 0)) > 0.5:
                bad.append("%s: no options, no notes, yet the 13F value differs from the equity value" % fk)
            if src.get("reconciles") is False:
                bad.append("%s: lines do not add up to the cover page (gap %s)" % (fk, src.get("cover_gap_usd")))
            if src.get("reconciles") is None:
                r.warn("%s: no cover value sum to reconcile against" % fk)
            for lab in ("initiations", "increases", "reductions", "terminations"):
                L = t[lab]
                if L["n"] != len(L["rows"]) + len(L["others"]) + L["n_more"]:
                    bad.append("%s: %s rows + others + more != n" % (fk, lab))
            if o["n_lines"] != x["n_option_rows"]:
                bad.append("%s: option line count differs from the grid's option rows" % fk)
        no_ticker = sum(1 for x in m.values() for lab in ("initiations", "increases", "reductions", "terminations") for z in x["tearsheet"][lab]["rows"] if not z.get("ticker"))
        shown = sum(len(x["tearsheet"][lab]["rows"]) for x in m.values() for lab in ("initiations", "increases", "reductions", "terminations"))
        r.log("rows shown across the four lists: %d, of which without a ticker: %d" % (shown, no_ticker))
        for z in [z for x in m.values() for lab in ("initiations", "increases", "reductions", "terminations") for z in x["tearsheet"][lab]["rows"] if not z.get("ticker")][:20]:
            r.log("    no ticker: %s | %s | %s | %s | %s" % (z.get("issuer"), z.get("class"), z.get("cusip"), z.get("kind"), z.get("pc")))

        r.section("3. the dry-run book (gzip + base64, for offline page work)")
        blob = base64.b64encode(gzip.compress(body, 9)).decode("ascii")
        r.log("blob %d chars" % len(blob))
        r.lines.append("<!-- BOOK-GZ-B64-BEGIN")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("BOOK-GZ-B64-END -->")
        r.kv(version=out["version"], book_bytes=str(len(body)), live_bytes=str(live_bytes), tearsheet_bytes=str(tear_bytes), managers=str(len(m)), invariant_failures=str(len(bad)))
        if bad:
            for x in bad:
                r.fail(x)
            raise SystemExit(1)
        r.ok("DRY RUN: %d tearsheets built from the live artifacts, every invariant holds; the book would be %d bytes (+%.1f%%)" % (len(m), len(body), 100.0 * (len(body) - live_bytes) / live_bytes))


if __name__ == "__main__":
    main()
    sys.exit(0)
