"""ops 6859 - READ-ONLY audit before two fixes Khalid asked for on 2026-11-20 (re-runnable). Writes nothing.

Khalid: "fix these: The shared split-detection rule reads 53 real one-day crashes and spikes as stock splits ...
Leaders, the Wyckoff scan and Strength still use the old rule. / The factor book lists 33 notes and preferred shares
as common stock ..."

1. THE FACTOR BOOK.  aws/lambdas/justhodl-bank-factors/source/other_issues.py (v1.0.9, not deployed) is run on the
   live book as it stands: every row it would move out of common stock is printed with the book's name, Polygon's
   own name and type, its dollar volume and the ticker it extends - so a false hit can be seen before anything
   changes.  Share classes that must stay (GOOGL, FOXA, NWSA ...) are printed as controls.
2. THE SPLIT RULE.  Each engine's own build() runs twice on the runner against the real warehouse - once with the
   at-the-open test switched off (the rule that is live today), once with it on - and the two artifacts are compared
   leaf by leaf: which tickers change, which fields, and what the refused "splits" were.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import copy
import gzip
import importlib
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))
LIVE = "justhodl-dashboard-live"
RUN_SPLIT_AB = False   # run 44764318291 (2026-11-20 15:32 UTC) did part 2 on this code: leaders 0 tickers, strength 0, campaign scan 15 (53 events refused). True repeats it (10 minutes)
LAMBDAS = ROOT / "aws" / "lambdas"
CONTROLS = ["BBD", "CIB", "BAK", "ARM", "AGNC", "HBAN", "GOOGL", "GOOG", "FOXA", "FOX", "NWSA", "NWS", "LBTYA", "LBTYK", "LBRDA", "LBRDK", "FWONA", "FWONK", "Z", "ZG", "UA", "UAA", "PFBC", "CMCSA", "BATRA", "BATRK", "LILAK", "LILA", "SIRI", "QRTEA"]
SKIP_KEYS = {"generated_at", "run_seconds", "trigger", "elapsed_s", "built_at", "seconds", "run_id"}
TICKER_KEYS = ("ticker", "t", "symbol", "sym")
MISSING = "<absent>"


class ReadOnly:
    """The real S3 client with every write refused."""

    def __init__(self, s3):
        self._s3 = s3

    def __getattr__(self, name):
        if name.startswith(("get_", "head_", "list_")) or name in ("get_paginator", "meta", "exceptions"):
            return getattr(self._s3, name)
        raise RuntimeError("audit: %s refused" % name)


def load_engine(name):
    """A fresh import of one engine's lambda_function (and its own helper modules) from its source folder."""
    src = str(LAMBDAS / name / "source")
    for mod in [m for m, v in list(sys.modules.items()) if getattr(v, "__file__", None) and "/aws/lambdas/" in str(v.__file__)]:
        del sys.modules[mod]
    sys.path[:] = [p for p in sys.path if "/aws/lambdas/" not in p]
    sys.path.insert(0, src)
    importlib.invalidate_caches()
    return importlib.import_module("lambda_function")


def tidy(x):
    """cols + rows tables become lists of dicts so they can be matched by ticker."""
    if isinstance(x, dict):
        if isinstance(x.get("cols"), list) and isinstance(x.get("rows"), list) and x["rows"] and isinstance(x["rows"][0], list):
            x = dict(x, rows=[dict(zip(x["cols"], r)) for r in x["rows"]])
        return {k: tidy(v) for k, v in x.items()}
    if isinstance(x, list):
        return [tidy(v) for v in x]
    return x


def keyed(lst):
    if not lst or not all(isinstance(v, dict) for v in lst):
        return None
    for k in TICKER_KEYS:
        if all(k in v for v in lst):
            keys = [str(v[k]) for v in lst]
            if len(set(keys)) == len(keys):
                return k
    return None


def walk(a, b, path, out):
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in SKIP_KEYS:
                walk(a.get(k, MISSING), b.get(k, MISSING), path + [str(k)], out)
    elif isinstance(a, list) and isinstance(b, list):
        ka, kb = keyed(a), keyed(b)
        if ka and ka == kb:
            ia, ib = {str(v[ka]): v for v in a}, {str(v[kb]): v for v in b}
            for t in sorted(set(ia) | set(ib)):
                walk(ia.get(t, MISSING), ib.get(t, MISSING), path + ["[%s]" % t], out)
        elif all(isinstance(v, str) for v in a + b):
            if a != b:
                gone, new = [v for v in a if v not in b], [v for v in b if v not in a]
                out.append((path, "-%d %s" % (len(gone), gone[:8]), "+%d %s%s" % (len(new), new[:8], "" if gone or new else " (order)")))
        elif len(a) == len(b):
            for i, (x, y) in enumerate(zip(a, b)):
                walk(x, y, path + [str(i)], out)
        else:
            out.append((path, "list of %d" % len(a), "list of %d" % len(b)))
    else:
        if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool):
            if abs(a - b) <= 1e-9:
                return
        elif a == b:
            return
        out.append((path, a, b))


def short(v, n=60):
    s = json.dumps(v, default=str) if not isinstance(v, str) else v
    return s if len(s) <= n else s[:n] + "..."


def compare(r, name, before, after):
    diffs = []
    walk(tidy(before), tidy(after), [], diffs)
    by_t, plain, fields = {}, [], {}
    for path, a, b in diffs:
        t = next((p[1:-1] for p in path if p.startswith("[") and p.endswith("]")), None)
        if t is None:
            plain.append((path, a, b))
        else:
            by_t.setdefault(t, []).append((path, a, b))
            fields[path[-1]] = fields.get(path[-1], 0) + 1
    heavy = sorted(by_t.items(), key=lambda kv: -len(kv[1]))
    r.log("%s: %d leaves differ; %d tickers touched (%d with 6 or more fields, %d with 1 to 5); %d differences outside ticker rows" % (
        name, len(diffs), len(by_t), sum(1 for _, v in heavy if len(v) >= 6), sum(1 for _, v in heavy if len(v) < 6), len(plain)))
    r.log("  fields that moved most: %s" % ", ".join("%s %d" % kv for kv in sorted(fields.items(), key=lambda kv: -kv[1])[:18]))
    for t, v in heavy[:45]:
        r.log("  %-7s %3d fields | %s" % (t, len(v), " ; ".join("%s: %s -> %s" % (".".join(p[-2:]) if not p[-1].startswith("[") else p[-1], short(a, 34), short(b, 34)) for p, a, b in v[:7])))
    for path, a, b in plain[:45]:
        r.log("  %s: %s -> %s" % (".".join(path), short(a, 90), short(b, 90)))
    return {"leaves": len(diffs), "tickers": len(by_t), "plain": len(plain)}


def main():
    now = datetime.now(timezone.utc)
    raw = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=5, read_timeout=60, max_pool_connections=48))
    s3 = ReadOnly(raw)
    with report("ops_6859_split_rule_and_book_typing_audit") as r:
        r.heading("ops 6859 - the split rule and the factor book's typing, before the fix %s (read-only)" % now.isoformat(timespec="minutes"))

        # ------------------------------------------------------------------ 1. the factor book
        r.section("1. the factor book: what other_issues.py would move out of common stock")
        sys.path.insert(0, str(LAMBDAS / "justhodl-bank-factors" / "source"))
        import other_issues as OI
        import bank_enrich
        rows = bank_enrich.rows(fields=["dollar_vol_20d", "mcap"])
        uni = {str(x["symbol"]).upper(): dict(x) for x in rows if x.get("symbol")}
        dv = {s: x.get("dollar_vol_20d") for s, x in uni.items()}
        n_common = sum(1 for x in uni.values() if x.get("is_common"))
        try:
            ref = json.loads(gzip.decompress(raw.get_object(Bucket=LIVE, Key="data/warm/polygon-bank/tickers-stocks-active/latest.json.gz")["Body"].read()))
            ref = ref.get("results") if isinstance(ref, dict) else ref
            pname = {str(x.get("ticker")).upper(): x for x in ref or [] if isinstance(x, dict) and x.get("ticker")}
        except Exception as exc:  # noqa: BLE001
            pname = {}
            r.log("Polygon reference list not read: %s" % str(exc)[:120])
        what_if = copy.deepcopy(uni)
        names = {s: str(x.get("name")) for s, x in pname.items() if x.get("name")}
        hits = OI.apply(what_if, dv, names)
        r.log("book rows %d, common stock by its flags %d; would be moved out: %d (%s; by evidence %s); Polygon reference rows %d" % (
            len(uni), n_common, len(hits), json.dumps({w: sum(1 for h in hits.values() if h["flag"] == w) for w in sorted({h["flag"] for h in hits.values()})}),
            json.dumps({w: sum(1 for h in hits.values() if h["evidence"] == w) for w in sorted({h["evidence"] for h in hits.values()})}), len(pname)))
        for s in sorted(hits, key=lambda s: (hits[s]["flag"], s)):
            x, p = uni[s], pname.get(s) or pname.get(s.replace("-", ".")) or {}
            r.log("  %-7s -> %-22s %-62s | book: %s | %s | type %s | $vol %s | mcap %s | Polygon: %s" % (
                s, hits[s]["flag"], hits[s]["why"][:62], str(x.get("name"))[:64], x.get("exchange"), x.get("security_type"),
                "%.0f" % x["dollar_vol_20d"] if x.get("dollar_vol_20d") else None, "%.0fM" % (x["mcap"] / 1e6) if x.get("mcap") else None, str(p.get("name"))[:70]))
        r.log("controls (must stay common stock): %s" % " | ".join("%s %s%s" % (s, "common" if (what_if.get(s) or {}).get("is_common") else "NOT common", " <-- HIT" if s in hits else "") for s in CONTROLS if s in uni))
        # Polygon's own name as evidence the book's name does not carry
        extra = []
        for s, x in uni.items():
            if x.get("is_common") and s not in hits:
                nm = str((pname.get(s) or pname.get(s.replace("-", ".")) or {}).get("name") or "")
                if nm and OI.by_name(nm, depositary=True):
                    extra.append((s, nm, x.get("name"), x.get("dollar_vol_20d")))
        r.log("common-stock rows the rule leaves alone whose POLYGON name names a note or a preferred (ADRs: the company's listed equity): %d" % len(extra))
        for s, nm, bn, v in sorted(extra, key=lambda e: -(e[3] or 0))[:60]:
            r.log("  %-7s Polygon: %-80s | book: %s | $vol %s" % (s, nm[:80], str(bn)[:50], "%.0f" % v if v else None))
        liquid = [s for s in hits if (uni[s].get("dollar_vol_20d") or 0) >= 250000]
        r.log("of the %d, %d trade $250k a day or more (the floor of the Wyckoff scan and the Volatility desk): %s" % (len(hits), len(liquid), ", ".join(sorted(liquid))))

        # ------------------------------------------------------------------ 2. the split rule, engine by engine
        summary = {}
        if not RUN_SPLIT_AB:
            r.section("2. the split rule: not repeated in this run")
            r.log("run 44764318291 built each engine with the old and the new rule on this code: leaders 0 tickers differ, strength 0, campaign scan 15 tickers (53 inferred splits refused)")
        for name, where in ((("justhodl-leaders", "T"), ("justhodl-campaign-scan", "T"), ("justhodl-strength", None)) if RUN_SPLIT_AB else ()):
            r.section("2. %s: build() with the at-the-open test off, then on" % name)
            try:
                LF = load_engine(name)
                holder = getattr(LF, where) if where else LF
                outs = {}
                for label, share in (("off", 0), ("on", 0.5)):
                    holder.SPLIT_GAP_SHARE = share
                    t0 = time.time()
                    outs[label] = LF.build(s3, now, log=lambda *_: None, remaining_s=1500.0)
                    r.log("%s, test %s: built in %.0fs, version %s, as of %s" % (name, label, time.time() - t0, outs[label].get("version"), outs[label].get("as_of")))
                holder.SPLIT_GAP_SHARE = 0.5
                after = outs["on"]
                refused = (after.get("splits") or {}).get("refused") if name != "justhodl-campaign-scan" else None
                if name == "justhodl-campaign-scan":
                    sr = (after.get("universe") or {}).get("splits_refused") or {}
                    r.log("refused as splits: %s - %s" % (sr.get("n"), "; ".join("%s %s x%s (opened x%s)" % (x["t"], x["date"], x["ratio"], x.get("open_gap")) for x in sr.get("sample") or [])))
                else:
                    r.log("refused as splits: %d tickers - %s" % (len(refused or {}), "; ".join("%s %s" % (t, ", ".join("%s x%s (opened x%s)" % (e["date"], e["ratio"], e.get("open_gap")) for e in ev)) for t, ev in sorted((refused or {}).items()))))
                summary[name] = compare(r, name, outs["off"], after)
            except Exception as exc:  # noqa: BLE001
                import traceback
                r.log("%s FAILED: %s" % (name, traceback.format_exc()[-900:]))
                summary[name] = {"error": str(exc)[:200]}
        r.kv(**{k.replace("justhodl-", "").replace("-", "_") + "_tickers": v.get("tickers") for k, v in summary.items()}, book_hits=len(hits), controls_hit=len([s for s in CONTROLS if s in hits]))
        if any(s in hits for s in CONTROLS):
            r.fail("a control would be moved out of common stock: %s" % [s for s in CONTROLS if s in hits])
            sys.exit(1)
        if any("error" in v for v in summary.values()):
            r.fail("an engine did not build: %s" % json.dumps(summary)[:600])
            sys.exit(1)
        r.ok("AUDIT: the book's would-be reclassifications are listed; each engine was built with the old and the new split rule and compared. Nothing written")


if __name__ == "__main__":
    main()
