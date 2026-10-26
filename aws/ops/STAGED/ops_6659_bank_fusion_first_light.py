"""ops 6659 - bank fusion wave 1, first light (read-mostly): kick the six fused engines once, then read their outputs for the
fusion labels and their logs for errors.

Engines (docs/providers/BANK_FUSION.md): magic-formula v1.1.0, divcut-warning v1.1.0, deep-value-screener v1.1,
equity-ftd v1.1.0, stock-valuations v1.6.0, analyst-consensus v1.1.  For each: deployed version/LastModified, one
async kick (idempotent engines; their own schedules keep the daily cadence), wait, then the output's universe_basis /
price_source / bank_context / squeeze / source labels and counts, and the last 20 minutes of logs grepped for
Traceback/ERROR/"factor book".  Nothing is written except the engines' own outputs.  No key involved.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300)
ENGINES = [
    ("justhodl-magic-formula", "data/magic-formula.json", 300),
    ("justhodl-divcut-warning", "data/divcut-warning.json", 300),
    ("justhodl-deep-value-screener", "data/deep-value.json", 300),
    ("justhodl-equity-ftd", "data/equity-ftd.json", 240),
    ("justhodl-stock-valuations", "data/stock-valuations.json", 420),
    ("justhodl-analyst-consensus", "data/analyst-consensus.json", 420),
    ("justhodl-quality-on-sale", "data/quality-on-sale.json", 300),
    ("justhodl-beneish", "data/beneish.json", 540),
    ("justhodl-insider-radar", "data/insider-radar.json", 240),
]


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def jget(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def logs_tail(logs, fn, since_ms, pattern):
    try:
        r = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=since_ms, filterPattern=pattern, limit=40)
        return [e.get("message", "").strip()[:220] for e in r.get("events", [])]
    except Exception as e:  # noqa: BLE001
        return ["(logs: %s)" % type(e).__name__]


def summarize(fn, doc):
    if not isinstance(doc, dict):
        return "no / non-dict output"
    out = []
    if fn == "justhodl-magic-formula":
        out.append("version %s | universe %s | n_universe %s n_book %s | eligible %s | regime %s" % (doc.get("version"), str(doc.get("universe"))[:120], doc.get("n_universe"), doc.get("n_book_candidates"), doc.get("n_universe_eligible"), doc.get("regime")))
        srcs = {}
        for r in doc.get("all_eligible") or []:
            srcs[str(r.get("price_source"))[:24]] = srcs.get(str(r.get("price_source"))[:24], 0) + 1
        out.append("price_source counts %s | top5 %s" % (srcs, [(r.get("ticker"), r.get("earnings_yield_pct"), r.get("roic_pct")) for r in (doc.get("top_30") or [])[:5]]))
    elif fn == "justhodl-divcut-warning":
        s = doc.get("summary") or {}
        out.append("version %s | state %s | universe %s | basis %s | flagged %s high %s" % (doc.get("version"), doc.get("state"), s.get("universe_size"), str(s.get("universe_basis"))[:140], s.get("flagged_n"), s.get("high_risk_n")))
        ws = doc.get("warnings") or []
        out.append("price_source counts %s | with bank_context %d/%d | top %s" % ({str(w.get("price_source"))[:24]: sum(1 for x in ws if x.get("price_source") == w.get("price_source")) for w in ws}, sum(1 for w in ws if w.get("bank_context")), len(ws), [(w.get("ticker"), w.get("n_signals"), w.get("yield_pct")) for w in ws[:5]]))
    elif fn == "justhodl-deep-value-screener":
        st = doc.get("stats") or {}
        out.append("method %s | universe %s book %s | basis %s | qualifying %s tier_a %s | statuses %s" % (doc.get("method"), st.get("n_universe"), st.get("n_book_candidates"), str(st.get("universe_basis"))[:120], st.get("n_qualifying"), st.get("n_tier_a"), json.dumps(st.get("statuses"))[:200]))
        rows = doc.get("all_qualifying") or []
        out.append("price_source counts %s | with bank_context %d/%d | top %s" % ({str(r.get("price_source"))[:24]: sum(1 for x in rows if x.get("price_source") == r.get("price_source")) for r in rows}, sum(1 for r in rows if r.get("bank_context")), len(rows), [(r.get("symbol"), r.get("score"), r.get("flag")) for r in rows[:5]]))
    elif fn == "justhodl-equity-ftd":
        rows = doc.get("top_spikes") or []
        out.append("version %s | ok %s | candidates %s with squeeze %s | book_as_of %s | vol_source %s" % (doc.get("version"), doc.get("ok"), doc.get("n_candidates"), doc.get("n_with_squeeze_context"), json.dumps(doc.get("book_as_of"))[:80], {str(r.get("vol_source")): sum(1 for x in rows if x.get("vol_source") == r.get("vol_source")) for r in rows}))
        out.append("top spikes %s" % [(r.get("t"), r.get("spike"), (r.get("squeeze") or {}).get("si_pct_float"), (r.get("squeeze") or {}).get("dtc_si")) for r in rows[:6]])
    elif fn == "justhodl-stock-valuations":
        out.append("version %s | hp_src %s | hp_logged %s | n_serious %s | keys %s" % (doc.get("version"), doc.get("hp_src"), doc.get("hp_logged"), doc.get("n_serious"), sorted(doc.keys())[:16]))
        diag = doc.get("diag") or doc.get("DIAG") or []
        out.append("diag (hp): %s" % [d for d in diag if "hp universe" in str(d) or "hp cache" in str(d)][:4])
    elif fn == "justhodl-analyst-consensus":
        out.append("method %s | universe %s | from_book %s | basis %s | with data %s" % (doc.get("method"), doc.get("universe_size"), doc.get("n_from_book"), str(doc.get("universe_basis"))[:140], doc.get("n_with_data")))
        out.append("top5 %s" % [(t.get("ticker"), t.get("composite_score"), t.get("upside_pct")) for t in (doc.get("top_consensus_25") or [])[:5]])
    elif fn == "justhodl-quality-on-sale":
        bs = doc.get("book_screen") or {}
        out.append("version %s | regime %s | 5-of-5 %s | book_screen available %s n %s as_of %s" % (doc.get("version"), doc.get("regime"), doc.get("n_qualified_5_of_5"), bs.get("available"), bs.get("n"), json.dumps(bs.get("as_of"))[:60]))
        out.append("book candidates %s" % [(c.get("ticker"), c.get("quality_pct"), c.get("off_52w_high_pct")) for c in (bs.get("candidates") or [])[:8]])
    elif fn == "justhodl-eps-revision-velocity":
        st = doc.get("stats") or {}
        rows = doc.get("all_qualifying") or doc.get("results") or []
        out.append("method %s | universe %s book %s | basis %s | qualifying %s | price_source %s" % (doc.get("method"), st.get("n_universe"), st.get("n_book_candidates"), str(st.get("universe_basis"))[:100], st.get("n_qualifying"), {str(r.get("price_source"))[:24]: sum(1 for x in rows if x.get("price_source") == r.get("price_source")) for r in rows}))
    elif fn == "justhodl-insider-radar":
        out.append("version %s | source_used %s | n_raw %s | buys %s clusters %s | diag %s" % (doc.get("version"), doc.get("source_used"), doc.get("n_raw"), len(doc.get("latest_buys") or []), len(doc.get("clusters") or []), [d for d in (doc.get("diag") or doc.get("DIAG") or []) if "bank" in str(d)][:2]))
    elif fn == "justhodl-beneish":
        m = doc.get("market") or {}
        out.append("version %s | top-50 state %s (%s) | analyzed %s | market candidates %s scored %s red %s (%s%%)" % (doc.get("version"), doc.get("universe_state"), doc.get("universe_state_scope"), doc.get("n_universe_analyzed"), m.get("n_candidates"), m.get("n_scored_total"), m.get("n_red_total"), m.get("pct_red_total")))
        out.append("market top red flags %s" % [(x.get("ticker"), x.get("m_score")) for x in (m.get("top_red_flags") or [])[:8]])
    return "\n".join(out)


def main():
    s3, lam, logs = c("s3"), c("lambda"), c("logs")
    with report("ops_6659_bank_fusion_first_light") as r:
        r.heading("ops 6659 - bank fusion wave 1: first light")
        r.section("0. the factor book")
        man = jget(s3, "data/warm/factor-book/manifest.json") or {}
        r.log("factor book: symbols %s updated %s as_of %s coverage %s" % (man.get("symbols"), man.get("updated_at"), json.dumps(man.get("as_of")), json.dumps(man.get("coverage"))[:400]))
        if not man.get("symbols"):
            r.warn("the factor book is empty - engines will run on their fallbacks")
        r.section("1. deployed versions + kicks")
        t_kick = int(time.time() * 1000)
        for fn, key, _ in ENGINES:
            try:
                cf = lam.get_function_configuration(FunctionName=fn)
                r.log("%s: modified %s, %s MB, %s s, desc %s" % (fn, (cf.get("LastModified") or "")[:19], cf.get("MemorySize"), cf.get("Timeout"), (cf.get("Description") or "")[:80]))
            except Exception as e:  # noqa: BLE001
                r.fail("%s: %s" % (fn, type(e).__name__))
                continue
            try:
                resp = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
                r.log("   kick -> %s" % resp.get("StatusCode"))
            except Exception as e:  # noqa: BLE001
                r.warn("   kick failed: %s" % type(e).__name__)
        r.section("2. outputs after the kicks")
        deadline = time.time() + 14 * 60
        pending = {fn: (key, budget) for fn, key, budget in ENGINES}
        seen = {}
        while pending and time.time() < deadline:
            time.sleep(30)
            for fn in list(pending):
                key, _ = pending[fn]
                doc = jget(s3, key)
                stamp = None
                if isinstance(doc, dict):
                    stamp = doc.get("generated_at") or doc.get("as_of") or doc.get("updated_at") or doc.get("generated")
                if stamp and str(stamp)[:16] >= datetime.fromtimestamp(t_kick / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M")[:16]:
                    seen[fn] = doc
                    pending.pop(fn, None)
        for fn, key, _ in ENGINES:
            doc = seen.get(fn) or jget(s3, key)
            fresh = fn in seen
            r.log("== %s (%s) %s" % (fn, key, "fresh output" if fresh else "output NOT refreshed within 14 min - showing the previous one"))
            for line in summarize(fn, doc).split("\n"):
                r.log("   " + line)
            errs = logs_tail(logs, fn, t_kick - 60000, '?Traceback ?ERROR ?"Task timed out" ?"factor book" ?"factor-book" ?"universe:"')
            r.log("   logs: %s" % (errs[:8] if errs else "nothing matched (no error, no fusion line)"))
            if any("Traceback" in e or "Task timed out" in e for e in errs):
                r.warn("   %s raised or timed out - read the log group" % fn)
        r.ok("ops 6659 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
