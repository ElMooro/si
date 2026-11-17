"""ops 6820 - 13F book 1.5.0 scope tearsheets, dry run on the live artifacts (read-only, re-runnable).

Khalid 2026-11-17: "add the same features wherever needed on my 13F". Same method as ops 6817: THIS checkout's build()
runs on the runner against the live artifacts into memory, every S3 write intercepted, OpenFIGI not called. Reports the
new blocks - book.tearsheet[cohort | concentrated] (totals, by manager, the three views with their four lists, top
lines and notable picks), each manager's largest call / put lines, the board totals - checks their invariants against
the per-manager tearsheets, and embeds the dry-run book for offline page work.

(the 1.4.0 text follows, unchanged)
ops 6817 - 13F book 1.4.0 tearsheet, dry run on the live artifacts (read-only, re-runnable).

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
    with report("ops_6820_13f_scope_tearsheet_dry_run") as r:
        r.heading("ops 6820 - 13F scope tearsheet dry run %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        lf = load_engine()
        r.section("1. build() of this checkout against the live artifacts (writes intercepted)")
        live = json.loads(s3.get_object(Bucket=LIVE, Key="data/13f-book.json")["Body"].read())
        live_bytes = len(json.dumps(live, separators=(",", ":"), ensure_ascii=False))
        ro = ReadOnly(s3)
        t0 = time.time()
        out = lf.build(ro, datetime.now(timezone.utc), log=lambda *a: None, remaining_s=300.0, master_enabled=False)
        body = json.dumps(out, separators=(",", ":"), ensure_ascii=False, default=str, allow_nan=False).encode("utf-8")
        b, m = out["book"], out["book"]["managers"]
        scope_bytes = len(json.dumps(b.get("tearsheet"), separators=(",", ":"), default=str))
        r.log("engine %s %s | build %.1fs | book %d bytes (live %s: %d, +%d) scope tearsheets %d bytes | gzip %d | period %s cohort %d/%d errors %d | intercepted writes %s" % (
            out["engine"], out["version"], time.time() - t0, len(body), live.get("version"), live_bytes, len(body) - live_bytes, scope_bytes, len(gzip.compress(body, 6)), b["required_period"], len(b["cohort"]), len(m),
            len(b["errors"]), json.dumps(ro.puts)))
        if out["version"] != "1.5.0" or b["errors"]:
            r.fail("unexpected version or artifact errors: %s %s" % (out["version"], b["errors"]))
            raise SystemExit(1)
        bad = []
        # nothing that 1.4.0 published may change: every manager block (minus the two added option lists) and every other book key
        lm = live["book"]["managers"]
        for fk in m:
            a, c = json.loads(json.dumps(m[fk], default=str)), lm.get(fk) or {}
            for side in (a, c):
                side.pop("artifact", None)
                ((side.get("tearsheet") or {}).get("options") or {}).pop("top_calls", None)
                ((side.get("tearsheet") or {}).get("options") or {}).pop("top_puts", None)
            if live.get("version") in ("1.4.0", "1.5.0") and a != c:
                diff = [k for k in set(a) | set(c) if a.get(k) != c.get(k)]
                bad.append("%s: the manager block differs from the live one in %s" % (fk, diff))
        for k in ("boards", "stance", "contenders", "rotation", "sectors"):
            for scope in ("cohort", "concentrated"):
                na, la = json.loads(json.dumps(b[k][scope], default=str)), live["book"][k][scope]
                if k == "boards":
                    for u in na:
                        for extra in ("n_newly_present", "n_dropped", "n_increased", "n_decreased", "value_prior_usd"):
                            na[u].pop(extra, None)
                            (la.get(u) or {}).pop(extra, None)
                if na != la:
                    bad.append("%s / %s differs from the live book" % (k, scope))
        r.log("unchanged against the live %s book: manager blocks, boards, stance, contenders, rotation, sectors -> %s" % (live.get("version"), "identical" if not bad else bad))

        for scope in ("cohort", "concentrated"):
            t = b["tearsheet"][scope]
            c, o, e = t["counts"], t["options"], t["est"]
            r.section("2. %s tearsheet (%d members)" % (scope, len(t["members"])))
            r.log("13F value %s (prior %s, %s%%) | manager-lines %d (prior %d) | new %d inc %d red %d absent %d flat %d reclass %d n/c %d | options %d lines in %d books = %s%% (calls %s%% puts %s%%) | est in %s out %s, options in %s out %s" % (
                money(t["value_usd"]), money(t["value_prior_usd"]), t["value_change_pct"], t["n_lines"], t["n_lines_prior"], c["new"], c["increased"], c["reduced"], c["closed"], c["unchanged"], c["reclassified"],
                c["not_compared"], o["n_lines"], o["n_managers"], o["share_pct"], o["call_share_pct"], o["put_share_pct"], money(e["in_usd"]), money(e["out_usd"]), money(e["option_in_usd"]), money(e["option_out_usd"])))
            r.log("by manager: %s" % " · ".join("%s %s %s%% (%s%% opt)" % (x["fk"], money(x["value_usd"]), x["pct"], x["option_share_pct"]) for x in t["by_manager"]))
            for kind in ("all", "non_option", "option"):
                v = t["views"][kind]
                add, cut = v["notable"]["add"], v["notable"]["cut"]
                r.log("**%s**: %d lines %s top-10 %s%% | add: %s %s %s %s est %s | cut: %s %s %s %s est %s" % (
                    kind, v["n_lines"], money(v["value_usd"]), v["top10_pct"], (add or {}).get("fk"), (add or {}).get("how"), name(add), tag(add) if add else "", money((add or {}).get("est_usd")),
                    (cut or {}).get("fk"), (cut or {}).get("how"), name(cut), tag(cut) if cut else "", money((cut or {}).get("est_usd"))))
                r.log("    lines: %s" % " · ".join("%s %s %s %s%%" % (z["fk"], name(z), money(z["value_usd"]), z["pct_scope"]) for z in v["lines"]))
                for lab in ("initiations", "increases", "reductions", "terminations"):
                    L = v[lab]
                    r.log("    %s %d (est %s; shown %d, others %d, more %d): %s" % (lab, L["n"], money(L["est_usd"]), len(L["rows"]), len(L["others"]), L["n_more"],
                                                                                   " · ".join("%s %s %s %s" % (z["fk"], name(z), tag(z), money(z.get("est_usd"))) for z in L["rows"][:7])))
                    if L["n"] != len(L["rows"]) + len(L["others"]) + L["n_more"]:
                        bad.append("%s/%s/%s: rows + others + more != n" % (scope, kind, lab))
                    if [z["est_usd"] for z in L["rows"]] != sorted((z["est_usd"] for z in L["rows"]), reverse=True):
                        bad.append("%s/%s/%s: not ranked by estimate" % (scope, kind, lab))
                    if any(z["fk"] not in t["members"] for z in L["rows"]):
                        bad.append("%s/%s/%s: a row names a manager outside the scope" % (scope, kind, lab))
                    if kind != "all" and any(bool(z.get("pc")) != (kind == "option") for z in L["rows"]):
                        bad.append("%s/%s/%s: a row of the wrong kind" % (scope, kind, lab))
            # the scope is exactly its members
            mem = [m[fk]["tearsheet"] for fk in t["members"]]
            if abs(t["value_usd"] - sum(x["value_usd"] for x in mem)) > 1 or t["n_lines"] != sum(x["n_lines"] for x in mem):
                bad.append("%s: totals are not the members' totals" % scope)
            for lab, key in (("initiations", "new"), ("increases", "increased"), ("reductions", "reduced"), ("terminations", "closed")):
                if t["views"]["all"][lab]["n"] != c[key] or t["views"]["non_option"][lab]["n"] + t["views"]["option"][lab]["n"] != c[key]:
                    bad.append("%s: %s count %s vs lists %s / %s + %s" % (scope, key, c[key], t["views"]["all"][lab]["n"], t["views"]["non_option"][lab]["n"], t["views"]["option"][lab]["n"]))
            if abs(t["views"]["non_option"]["value_usd"] + t["views"]["option"]["value_usd"] - t["views"]["all"]["value_usd"]) > 1 or abs(t["views"]["all"]["value_usd"] - t["value_usd"]) > 1:
                bad.append("%s: view values do not add up" % scope)
            best = max((z for x in mem for lab in ("initiations", "increases") for z in x[lab]["rows"]), key=lambda z: z.get("est_usd") or 0, default=None)
            if best and abs((t["views"]["all"]["notable"]["add"] or {}).get("est_usd", 0) - best["est_usd"]) > 0.5:
                bad.append("%s: the notable addition is not the members' largest" % scope)

        r.section("3. largest option lines per manager + board totals")
        for fk, x in m.items():
            o = x["tearsheet"]["options"]
            if o["n_lines"]:
                r.log("%-12s calls: %s | puts: %s" % (fk, " · ".join("%s %s %s" % (name(z), money(z["value_usd"]), tag(z)) for z in o["top_calls"]) or "-", " · ".join("%s %s %s" % (name(z), money(z["value_usd"]), tag(z)) for z in o["top_puts"]) or "-"))
                if len(o["top_calls"]) != min(3, o["n_calls"]) or len(o["top_puts"]) != min(3, o["n_puts"]):
                    bad.append("%s: top option lines do not match the option counts" % fk)
        for scope in ("cohort", "concentrated"):
            for u in ("all", "stocks", "etfs"):
                bd = b["boards"][scope][u]
                r.log("boards %s/%s: identities %s held %s | newly present %s dropped %s increased %s decreased %s | value %s prior %s" % (
                    scope, u, bd["n_identities"], bd["n_held"], bd["n_newly_present"], bd["n_dropped"], bd["n_increased"], bd["n_decreased"], money(bd["value_usd"]), money(bd["value_prior_usd"])))
                if bd["n_newly_present"] < len(bd["newly_present"]) or bd["n_dropped"] < len(bd["dropped"]):
                    bad.append("boards %s/%s: totals below the rows shown" % (scope, u))

        r.section("4. the dry-run book (gzip + base64, for offline page work)")
        blob = base64.b64encode(gzip.compress(body, 9)).decode("ascii")
        r.log("blob %d chars" % len(blob))
        r.lines.append("<!-- BOOK-GZ-B64-BEGIN")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("BOOK-GZ-B64-END -->")
        r.kv(version=out["version"], book_bytes=str(len(body)), live_bytes=str(live_bytes), scope_tearsheet_bytes=str(scope_bytes), managers=str(len(m)), invariant_failures=str(len(bad)))
        if bad:
            for x in bad:
                r.fail(x)
            raise SystemExit(1)
        r.ok("DRY RUN: scope tearsheets for cohort + concentrated built from the live artifacts, every invariant holds, nothing published before changed; the book would be %d bytes (+%d)" % (len(body), len(body) - live_bytes))


if __name__ == "__main__":
    main()
    sys.exit(0)
