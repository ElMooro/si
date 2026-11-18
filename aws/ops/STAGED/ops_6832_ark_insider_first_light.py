"""ops 6832 - first light: ARK tracker 2.1.0 and insider scanner 2.2.0 (re-runnable).

Khalid 2026-11-17: "Yes fix them."  After the deploy of 2bd60598 this op runs each engine once and proves the fix on
the live bucket, with expectations computed here independently of the engine.

ARK (justhodl-ark-holdings)
  1. Seed the retained baseline once: the engine used to overwrite its "previous" snapshot with the current one, so
     tonight nothing says what the published diff was measured against.  The stored S3 version of the snapshot whose
     generated_at equals the feed's prev_snapshot_at is written to data/ark-holdings-base.json (only when that key
     does not exist).  Without it a run before the source publishes a new day would publish an empty diff.
  2. Invoke once.  Whatever the source serves - a new holdings date, or the same holdings again - the diff must be
     measured against the last snapshot with an EARLIER holdings date, the complete lists must equal a diff computed
     here from the two snapshots (1% share-count rule), the 15-row highlights must be the head of those lists, and
     every position must be published.
  3. Invoke again: the same holdings must carry the same moves (diff_is_carried), not an empty diff.
INSIDER (justhodl-insider-cluster-scanner)
  4. Invoke once.  The published list must equal what the banked feed gives when it is read here and passed through
     the same hygiene rules: one row per filing line, one filing once across an issuer's tickers, impossible prices
     set aside.  No ticker in the list may be larger than the largest real purchase; LONA / AASP may appear only under
     hygiene.set_aside; MNKKQ / MNKPF must be gone and MNK must name them.
Reads the previous outputs first and reports what changed.  Fails loudly; nothing else is written.
"""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import os
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-insider-cluster-scanner" / "source"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-ark-holdings" / "source"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=600)
ARK_FN, INS_FN = "justhodl-ark-holdings", "justhodl-insider-cluster-scanner"
ARK_OUT, ARK_PREV, ARK_BASE, INS_OUT = "data/ark-holdings.json", "data/ark-holdings-prev.json", "data/ark-holdings-base.json", "data/insider-clusters.json"
ACC = re.compile(r"(\d{10}-\d{2}-\d{6})")


def get(s3, key, version=None):
    try:
        kw = {"Bucket": LIVE, "Key": key}
        if version:
            kw["VersionId"] = version
        return json.loads(s3.get_object(**kw)["Body"].read())
    except Exception as exc:  # noqa: BLE001
        if "NoSuchKey" in type(exc).__name__ or "NoSuchKey" in str(exc):
            return None
        raise


def invoke(lam, fn):
    resp = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
    raw = resp["Payload"].read().decode("utf-8", "replace")
    if resp.get("FunctionError"):
        raise RuntimeError("%s raised: %s" % (fn, raw[:600]))
    doc = json.loads(raw)
    body = doc.get("body") if isinstance(doc, dict) else None
    return json.loads(body) if isinstance(body, str) else (body or doc)


def ark_expected(current, baseline):
    """The four lists as sets of (fund, ticker), from the two snapshots, by the engine's stated rule (1% of shares)."""
    cur = {(f, p["ticker"]): p for f, rows in current.items() for p in rows}
    old = {(f, p["ticker"]): p for f, rows in (baseline or {}).items() for p in rows}
    out = {"new_positions": set(), "position_adds": set(), "position_trims": set(), "closed_positions": set()}
    if not old:
        return out
    for k, p in cur.items():
        o = old.get(k)
        if not o:
            out["new_positions"].add(k)
            continue
        delta = p["shares"] - o["shares"]
        pct = delta / max(o["shares"], 1) * 100
        if delta > 0 and pct >= 1.0:
            out["position_adds"].add(k)
        elif delta < 0 and pct <= -1.0:
            out["position_trims"].add(k)
    out["closed_positions"] = set(old) - set(cur)
    return out


def main():
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    fails = []

    def ck(cond, msg):
        if not cond:
            fails.append(msg)
        return cond

    with report("ops_6832_ark_insider_first_light") as r:
        r.heading("ops 6832 - ARK tracker 2.1.0 + insider scanner 2.2.0, first light %s" % now.isoformat(timespec="minutes"))
        for fn in (ARK_FN, INS_FN):
            c = lam.get_function_configuration(FunctionName=fn)
            r.log("%s: last modified %s, handler %s, %d MB, timeout %ds, code %s" % (fn, c.get("LastModified"), c.get("Handler"), c.get("MemorySize"), c.get("Timeout"), (c.get("CodeSha256") or "")[:12]))

        import ark_moves
        r.section("ARK 1. the baseline the published diff was measured against")
        before = get(s3, ARK_OUT)
        latest0, base0 = get(s3, ARK_PREV), get(s3, ARK_BASE)
        pub = before["diff_vs_prev"]
        r.log("before: engine %s, generated %s, measured against the snapshot of %s; published %d added to, %d cut, %d new, %d closed; highlights list %d additions" % (
            before.get("version") or "2.0 (old)", before.get("generated_at"), before.get("prev_snapshot_at"), pub["n_position_adds"], pub["n_position_trims"], pub["n_new_positions"], pub["n_closed_positions"], len(pub["highlights"]["biggest_adds"])))
        r.log("stored newest snapshot: generated %s, holdings dated %s; retained baseline: %s" % ((latest0 or {}).get("generated_at"), ark_moves.snapshot_date(latest0), "holdings dated %s (snapshot %s)" % (ark_moves.snapshot_date(base0), base0.get("generated_at")) if base0 else "none"))
        if base0 is None:
            want, seeded = before.get("prev_snapshot_at"), None
            vers = [v for v in (s3.list_object_versions(Bucket=LIVE, Prefix=ARK_PREV, MaxKeys=40).get("Versions") or []) if v["Key"] == ARK_PREV]
            for v in vers:
                doc = get(s3, ARK_PREV, v["VersionId"])
                if doc and doc.get("generated_at") == want and ark_moves.snapshot_date(doc) and ark_moves.snapshot_date(doc) < ark_moves.snapshot_date(latest0):
                    seeded = doc
                    break
            if seeded:
                s3.put_object(Bucket=LIVE, Key=ARK_BASE, Body=json.dumps(seeded, separators=(",", ":")).encode("utf-8"), ContentType="application/json", CacheControl="public, max-age=3600")
                base0 = seeded
                r.log("seeded %s with the engine's own stored snapshot of %s (holdings dated %s, %d lines)" % (ARK_BASE, want, ark_moves.snapshot_date(seeded), sum(len(v) for v in seeded["holdings_by_fund"].values())))
            else:
                r.log("no stored version matches %s - nothing seeded (a run on unchanged holdings will publish an empty diff until the source moves)" % want)

        r.section("ARK 2. one run")
        body1 = invoke(lam, ARK_FN)
        r.log("engine answered: %s" % json.dumps(body1))
        out1, latest1, base1 = get(s3, ARK_OUT), get(s3, ARK_PREV), get(s3, ARK_BASE)
        d1 = out1["diff_vs_prev"]
        ck(out1.get("version") == "2.1.0" and body1.get("version") == "2.1.0", "ARK: the function is not running 2.1.0 (%s)" % out1.get("version"))
        baseline_used = latest0 if body1.get("new_day") else (base0 or latest0)
        r.log("holdings dated %s, measured against %s (%s); the op expects the baseline dated %s" % (out1.get("holdings_date"), out1.get("prev_holdings_date"), "a new day at the source" if body1.get("new_day") else "the same holdings as the stored snapshot - carried", ark_moves.snapshot_date(baseline_used)))
        ck(out1.get("holdings_date") == ark_moves.holdings_date(latest1["holdings_by_fund"]), "ARK: holdings_date is not the newest position date")
        ck(out1.get("prev_holdings_date") == ark_moves.snapshot_date(baseline_used) and (out1.get("prev_holdings_date") or "") < (out1.get("holdings_date") or ""), "ARK: measured against %s, expected %s (and earlier than %s)" % (out1.get("prev_holdings_date"), ark_moves.snapshot_date(baseline_used), out1.get("holdings_date")))
        ck(out1.get("prev_snapshot_at") == (baseline_used or {}).get("generated_at"), "ARK: prev_snapshot_at is not the baseline's")
        exp = ark_expected(latest1["holdings_by_fund"], (baseline_used or {}).get("holdings_by_fund"))
        for kind, n_key in (("new_positions", "n_new_positions"), ("position_adds", "n_position_adds"), ("position_trims", "n_position_trims"), ("closed_positions", "n_closed_positions")):
            got = {(x["fund"], x["ticker"]) for x in d1["all"][kind]}
            r.log("  %-17s engine %3d, computed here %3d, published count %3d%s" % (kind, len(got), len(exp[kind]), d1[n_key], "" if got == exp[kind] else "  DIFFERENT: only engine %s, only here %s" % (sorted(got - exp[kind])[:5], sorted(exp[kind] - got)[:5])))
            ck(got == exp[kind] and len(d1["all"][kind]) == d1[n_key], "ARK: %s does not match the two snapshots" % kind)
        ck(d1["highlights"]["biggest_adds"] == d1["all"]["position_adds"][:15] and d1["highlights"]["biggest_trims"] == d1["all"]["position_trims"][:15] and d1["highlights"]["new_positions"] == d1["all"]["new_positions"][:15]
           and d1["highlights"]["closed_positions"] == d1["all"]["closed_positions"][:10], "ARK: the highlights are not the head of the complete lists")
        n_pub = sum(len(v) for v in out1["holdings_by_fund"].values())
        r.log("positions published %d of %d; complete lists hold %d moves where the highlights hold %d" % (n_pub, out1["n_positions_total"], sum(len(d1["all"][k]) for k in d1["all"]), sum(len(v) for v in d1["highlights"].values())))
        ck(n_pub == out1["n_positions_total"] == sum(len(v) for v in latest1["holdings_by_fund"].values()), "ARK: not every position is published")
        if not body1.get("new_day"):
            same = all(d1[k] == pub[k] for k in ("n_new_positions", "n_position_adds", "n_position_trims", "n_closed_positions"))
            r.log("the source has not published a new day: the carried diff %s the diff published this morning" % ("equals" if same else "DIFFERS FROM"))
            ck(same and out1.get("diff_is_carried") is True, "ARK: a run on unchanged holdings did not carry this morning's diff")
        else:
            ck(base1 and base1.get("generated_at") == (latest0 or {}).get("generated_at"), "ARK: the previous newest snapshot was not retained as the baseline")
        fund_dates = {f: max((p.get("date") or "") for p in rows) for f, rows in latest1["holdings_by_fund"].items()}
        stale = {f: dt for f, dt in fund_dates.items() if dt < out1["holdings_date"]}
        r.log("holdings date by fund: %s%s" % (json.dumps(fund_dates), "  STALE AT THE SOURCE: %s" % json.dumps(stale) if stale else ""))
        top = sorted(d1["all"]["position_adds"], key=lambda x: -(x["shares_added"] * (x["current_value"] / x["current_shares"] if x["current_shares"] else 0)))[:5]
        for x in top:
            r.log("  largest by dollars: %s %s +%s sh (%+.1f%%), about $%.1fM" % (x["fund"], x["ticker"], format(x["shares_added"], ","), x["pct_change"], x["shares_added"] * x["current_value"] / x["current_shares"] / 1e6))

        r.section("ARK 3. the same holdings again")
        body2 = invoke(lam, ARK_FN)
        out2, base2 = get(s3, ARK_OUT), get(s3, ARK_BASE)
        d2 = out2["diff_vs_prev"]
        r.log("engine answered: %s" % json.dumps(body2))
        ck(body2.get("new_day") is False and out2.get("diff_is_carried") is True, "ARK: the second run did not carry the diff")
        ck(d2["all"] == d1["all"] and out2.get("prev_holdings_date") == out1.get("prev_holdings_date") and (base2 or {}).get("generated_at") == (base1 or {}).get("generated_at"), "ARK: the second run changed the moves or the baseline")
        r.log("second run: %d added to, %d cut, %d new, %d closed - identical lists: %s; baseline unchanged: %s; carried: %s" % (d2["n_position_adds"], d2["n_position_trims"], d2["n_new_positions"], d2["n_closed_positions"], d2["all"] == d1["all"], (base2 or {}).get("generated_at") == (base1 or {}).get("generated_at"), out2.get("diff_is_carried")))

        r.section("INSIDER 4. one run, against the banked feed read here")
        import bank_enrich
        import fmp_bank
        import insider_hygiene as hygiene
        prior = get(s3, INS_OUT)
        r.log("before: engine %s, generated %s: %d scored, %d in the purchase list, largest %s" % (prior.get("version"), prior.get("generated_at"), len(prior["clusters"]), len(prior["all_ticker_buys"]),
              ", ".join("%s $%.0fM" % (b["ticker"], b["total_value"] / 1e6) for b in sorted(prior["all_ticker_buys"], key=lambda b: -b["total_value"])[:4])))
        body = invoke(lam, INS_FN)
        r.log("engine answered: %s" % json.dumps(body))
        out = get(s3, INS_OUT)
        h, st = out.get("hygiene") or {}, out["stats"]
        ck(out.get("version") == "2.2.0" and h.get("applied") is True, "INSIDER: the function is not running 2.2.0 with hygiene applied (%s, %s)" % (out.get("version"), h.get("applied")))
        r.log("hygiene: %s feed rows -> %s filing lines; in range %s, off the tape (counted) %s, no tape %s, set aside %s ($%.0f as filed); multi-ticker filings merged: %s" % (
            h.get("n_feed_purchase_rows"), h.get("n_filing_lines"), h.get("n_in_range"), h.get("n_off_tape"), h.get("n_no_tape"), h.get("n_set_aside"), h.get("usd_set_aside") or 0, json.dumps(h.get("merged_filings"))))
        for a in h.get("set_aside") or []:
            r.log("  SET ASIDE %s %s | %s | %s sh at %s = $%.0f | tape %s | %s" % (a["ticker"], a.get("company"), a.get("insider_name"), format(a.get("shares") or 0, ","), a.get("price"), a.get("value") or 0, a.get("tape_price"), a.get("reason")))
        floor = float(out["thresholds"]["min_buy_value_usd"])
        days = fmp_bank.walker_days("insider-trading-latest", days=int(out["lookback_days"]) + 3,
                                    where=lambda x: isinstance(x, dict) and ((x.get("transactionType") or "").upper().strip().startswith("P-") or (x.get("transactionType") or "").upper().strip() == "P"))
        txns = []
        for _day, rows in days:
            for x in rows:
                ad = (x.get("acquisitionOrDisposition") or x.get("acquisitionDisposition") or "A").upper().strip()
                sym = (x.get("symbol") or "").strip().upper()
                try:
                    shares, price = float(x.get("securitiesTransacted") or 0), float(x.get("price") or 0)
                except (TypeError, ValueError):
                    continue
                if (ad and ad != "A") or not sym or sym == "N/A" or len(sym) > 6 or shares <= 0 or price <= 0 or shares * price < floor:
                    continue
                filed = (x.get("filingDate") or x.get("transactionDate") or "")[:10]
                txn_date = (x.get("transactionDate") or filed)[:10]
                m = ACC.search(str(x.get("url") or x.get("link") or ""))
                txns.append({"ticker": sym, "cik": str(x.get("companyCik") or ""), "insider_name": (x.get("reportingName") or "Unknown").strip(), "shares": int(shares), "price": round(price, 4), "value": round(shares * price, 2),
                             "txn_date": txn_date, "accession": m.group(1) if m else "bank-%s-%s-%s-%d" % (sym, x.get("reportingCik") or x.get("reportingName") or "", txn_date, int(shares))})
        lines = hygiene.unique(txns)
        refs = {}
        for t in sorted({x["ticker"] for x in lines}):
            row = bank_enrich.lookup(t, list(hygiene.REF_FIELDS))
            if row:
                refs[t] = row
        lines, merged = hygiene.merge_same_filing(lines, lambda s: s in refs)
        counted, aside, summary = hygiene.screen(lines, refs)
        r.log("read here: %d feed days, %d purchase rows -> %d lines after the merge, %d counted, %d set aside (%s); tickers on the tape %d" % (len(days), len(txns), len(lines), len(counted), len(aside), ", ".join(sorted({a["ticker"] for a in aside})) or "none", len(refs)))
        ck(h.get("n_feed_purchase_rows") == len(txns) and h.get("n_filing_lines") == len(lines) and h.get("n_set_aside") == len(aside), "INSIDER: rows / lines / set-aside differ from the feed read here (%s/%s/%s vs %d/%d/%d)" % (h.get("n_feed_purchase_rows"), h.get("n_filing_lines"), h.get("n_set_aside"), len(txns), len(lines), len(aside)))
        ck(sorted((a["ticker"], a["accession"]) for a in h.get("set_aside") or []) == sorted((a["ticker"], a["accession"]) for a in aside), "INSIDER: the set-aside records differ from the ones computed here")
        ck(st["n_buy_transactions"] == len(counted) and st["n_feed_purchase_rows"] == len(txns), "INSIDER: stats count %s lines / %s rows, computed %d / %d" % (st["n_buy_transactions"], st.get("n_feed_purchase_rows"), len(counted), len(txns)))
        cutoff = (datetime.now(timezone.utc).date() - timedelta(days=int(out["lookback_days"]))).isoformat()
        want, seen = defaultdict(float), set()
        for t in counted:                                    # the engine's own window and its own per-ticker line key
            k = (t["ticker"], t["accession"], t["insider_name"], t["txn_date"], t["shares"])
            if k in seen or t["txn_date"] < cutoff:
                continue
            seen.add(k)
            want[t["ticker"]] += t["value"]
        got = {b["ticker"]: b["total_value"] for b in out["all_ticker_buys"]}
        off = [(t, got.get(t), round(v, 2)) for t, v in want.items() if abs((got.get(t) or 0) - v) > 0.06]
        r.log("purchase list: %d tickers, $%.2fM; computed here %d tickers, $%.2fM; tickers that differ: %d %s" % (len(got), sum(got.values()) / 1e6, len(want), sum(want.values()) / 1e6, len(off) + len(set(got) - set(want)), (off[:5] + sorted(set(got) - set(want))[:5]) or ""))
        ck(not off and set(got) == set(want), "INSIDER: the purchase list differs from the feed passed through the same rules")
        biggest = max(out["all_ticker_buys"], key=lambda b: b["total_value"])
        r.log("largest company in the list: %s $%.1fM (%s); before the fix: %s $%.1fM" % (biggest["ticker"], biggest["total_value"] / 1e6, biggest.get("company"), max(prior["all_ticker_buys"], key=lambda b: b["total_value"])["ticker"], max(b["total_value"] for b in prior["all_ticker_buys"]) / 1e6))
        ck(biggest["total_value"] < 1e9, "INSIDER: a ticker in the list is still above $1B (%s)" % biggest["ticker"])
        gone = sorted({a["ticker"] for a in aside} & set(got))
        ck(not gone or all(got[t] < 1e9 for t in gone), "INSIDER: a set-aside record is still counted")
        dup = defaultdict(list)
        for b in out["all_ticker_buys"]:
            if b.get("cik"):
                dup[b["cik"]].append(b["ticker"])
        multi = {c: t for c, t in dup.items() if len(t) > 1}
        r.log("issuers listed under more than one ticker: %s; merged filings: %s" % (json.dumps(multi) if multi else "none", ", ".join("%s (also filed as %s)" % (m_["kept"], ", ".join(m_["also_filed_as"])) for m_ in merged) or "none"))
        for m_ in merged:
            row = next((b for b in out["all_ticker_buys"] if b["ticker"] == m_["kept"]), None)
            ck(not (set(m_["also_filed_as"]) & set(got)) and (row is None or row.get("also_filed_as") == m_["also_filed_as"]), "INSIDER: %s still listed under %s" % (m_["kept"], m_["also_filed_as"]))
        before_c, after_c = {c["ticker"]: c for c in prior["clusters"]}, {c["ticker"]: c for c in out["clusters"]}
        r.log("scored companies %d -> %d; no longer scored: %s; newly scored: %s" % (len(before_c), len(after_c), sorted(set(before_c) - set(after_c))[:12], sorted(set(after_c) - set(before_c))[:12]))
        moved = [(t, before_c[t]["score"], after_c[t]["score"]) for t in before_c if t in after_c and before_c[t]["score"] != after_c[t]["score"]]
        r.log("scores that changed among the %d names scored both times: %d %s" % (len(set(before_c) & set(after_c)), len(moved), moved[:8]))
        r.log("top five now: %s" % ", ".join("%s %.1f %s" % (c["ticker"], c["score"], c["signal_type"]) for c in out["clusters"][:5]))
        r.log("names for unscored companies: %d of %d rows carry a company name, %d carry a CIK" % (sum(1 for b in out["all_ticker_buys"] if b.get("company")), len(out["all_ticker_buys"]), sum(1 for b in out["all_ticker_buys"] if b.get("cik"))))
        r.kv(ark_version=str(out1.get("version")), ark_moves_listed=str(sum(len(d1["all"][k]) for k in d1["all"])), ark_new_day=str(body1.get("new_day")), insider_version=str(out.get("version")),
             insider_lines=str(h.get("n_filing_lines")), insider_set_aside=str(h.get("n_set_aside")), insider_usd_m="%.1f" % (sum(got.values()) / 1e6), checks_failed=str(len(fails)))
        if fails:
            for f in fails:
                r.fail(f)
            raise SystemExit(1)
        r.ok("FIRST LIGHT: ARK 2.1.0 lists all %d moves against the %s holdings and carries them on a second run; insider 2.2.0 publishes %d companies ($%.1fM), %d impossible record(s) set aside, one row per filing" % (
            sum(len(d1["all"][k]) for k in d1["all"]), out1.get("prev_holdings_date"), len(got), sum(got.values()) / 1e6, len(aside)))


if __name__ == "__main__":
    main()
    sys.exit(0)
