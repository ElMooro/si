"""ops 6816 - 13F manager tearsheet, source probe (read-only, re-runnable).

Khalid 2026-11-17: the managers section should carry what a one-manager 13F tearsheet shows (13F value incl. options,
line items, new / increased / reduced / closed counts, a top-10 donut with options ranked beside stock, initiations /
increases / reductions / terminations with before -> after values, most notable buy and sale). Before the engine is
written this reads what the live artifacts actually hold:

  1. the live book: version, period, cohort, season keys, each manager's period / status / rows / values;
  2. per manager, from its native artifact: option rows (identity keys, put/call spellings, quantity type, class text),
     whether option position ids repeat across the two periods, comparison statuses split equity / option / debt,
     the filing summaries per period (form, filed_at) and the cover keys;
  3. the security master's reach over option-only underlyings (CUSIPs held only through option rows).
Invokes nothing. Writes nothing but this report.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)


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


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def get(s3, key):
    return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6816_13f_tearsheet_source_probe") as r:
        r.heading("ops 6816 - 13F tearsheet source probe %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. the live book")
        book = get(s3, "data/13f-book.json")
        b = book.get("book") or {}
        r.log("version %s contract %s generated %s trigger %s run %ss | required period %s | cohort %d / %d | concentrated %s" % (
            book.get("version"), book.get("contract"), book.get("generated_at"), json.dumps(book.get("trigger")), book.get("run_seconds"),
            b.get("required_period"), len(b.get("cohort") or []), len(b.get("managers") or {}), json.dumps(b.get("concentrated"))))
        se = book.get("season") or {}
        r.log("season keys: %s" % json.dumps({k: (v if not isinstance(v, (dict, list)) else "<%s %d>" % (type(v).__name__, len(v))) for k, v in se.items()}))
        r.log("book bytes %d; manager keys: %s" % (len(json.dumps(book)), json.dumps(sorted((next(iter((b.get("managers") or {}).values()), {}) or {}).keys()))))
        for fk, m in sorted((b.get("managers") or {}).items(), key=lambda kv: -(kv[1].get("total_value_usd") or 0)):
            r.log("%-12s %s vs %s %-8s %-28s eq %5s opt %5s debt %4s | equity %9s prior %9s | puts %9s calls %9s | top10 %s%% | filed %s %s" % (
                fk, m.get("current_period"), m.get("prior_period"), m.get("status"), ",".join(m.get("flags") or []), m.get("n_equity_positions"), m.get("n_option_rows"),
                m.get("n_debt_rows"), money(m.get("total_value_usd")), money(m.get("total_value_prior_usd")), money(m.get("put_value_usd")), money(m.get("call_value_usd")),
                m.get("top10_pct_of_book"), (m.get("latest_submission") or {}).get("form"), (m.get("latest_submission") or {}).get("filed_at")))

        r.section("2. native artifacts: option rows, comparison statuses, filing summaries")
        desk = get(s3, "data/13f-desk.json")
        master = get(s3, "data/13f-security-master.json")
        rows = master.get("by_cusip") or {}
        r.log("desk generated %s required %s | master rows %d generated %s" % (desk.get("generated_at"), desk.get("required_period_for_current_cohort"), len(rows), master.get("generated_at")))
        opt_only, opt_cusips, all_sh = {}, set(), set()
        sample_done = 0
        for fk, meta in sorted((desk.get("by_fund") or {}).items()):
            key = ((meta or {}).get("detail") or {}).get("key")
            if not key:
                r.warn("%s: no artifact key" % fk)
                continue
            d = get(s3, key)
            cur_p, pri_p = d.get("current_holdings_period"), d.get("prior_holdings_period")
            periods = d.get("periods") or {}
            cur = (periods.get(cur_p) or {}).get("positions") or {}
            pri = (periods.get(pri_p) or {}).get("positions") or {}
            comp = {x.get("position_id"): x for x in ((d.get("comparison") or {}).get("rows") or []) if isinstance(x, dict)}

            def kind(p):
                i = p.get("identity") or {}
                return "option" if i.get("put_call") else ("debt" if i.get("quantity_type") == "PRN" else "equity" if i.get("quantity_type") == "SH" else "other:%s" % i.get("quantity_type"))

            kc, kp = Counter(kind(p) for p in cur.values()), Counter(kind(p) for p in pri.values())
            st = Counter()
            for pid, x in comp.items():
                p = cur.get(pid) or pri.get(pid) or {}
                st[(kind(p) if p else "missing", x.get("status"))] += 1
            opt = {pid: p for pid, p in cur.items() if kind(p) == "option"}
            opt_pri = {pid: p for pid, p in pri.items() if kind(p) == "option"}
            pc = Counter((p.get("identity") or {}).get("put_call") for p in opt.values())
            qt = Counter((p.get("identity") or {}).get("quantity_type") for p in opt.values())
            both = len(set(opt) & set(opt_pri))
            tot_all = sum(num(p.get("reported_value_usd")) for p in cur.values())
            tot_all_pri = sum(num(p.get("reported_value_usd")) for p in pri.values())
            tot_opt = sum(num(p.get("reported_value_usd")) for p in opt.values())
            r.log("%-12s %s vs %s | cur %s prior %s | all-lines value %s (prior %s) options %s = %.1f%% | option ids in both periods %d of %d (prior %d) | put/call %s qtype %s" % (
                fk, cur_p, pri_p, dict(kc), dict(kp), money(tot_all), money(tot_all_pri), money(tot_opt), 100 * tot_opt / tot_all if tot_all else 0.0,
                both, len(opt), len(opt_pri), dict(pc), dict(qt)))
            r.log("    comparison statuses: %s | counts block %s" % (json.dumps(sorted(("%s/%s" % k, v) for k, v in st.items())), json.dumps((d.get("comparison") or {}).get("counts"))))
            fs = d.get("filing_summaries") or {}
            r.log("    filings: %s" % "; ".join("%s %s period %s filed %s cover %s" % (a, (x or {}).get("form"), (x or {}).get("period_of_report"), (x or {}).get("filed_at"),
                                                                                    json.dumps({k: v for k, v in ((x or {}).get("cover") or {}).items() if k in ("reported_rows", "reported_value_sum", "amendment_type", "amendment_number")})) for a, x in fs.items()))
            if opt and sample_done < 4 and len(cur) <= 400:
                sample_done += 1
                r.log("    artifact top-level keys %s | a position's keys %s | identity keys %s | a comparison row's keys %s" % (
                    json.dumps(sorted(d.keys())), json.dumps(sorted(next(iter(cur.values())).keys())), json.dumps(sorted((next(iter(cur.values())).get("identity") or {}).keys())),
                    json.dumps(sorted(next(iter(comp.values()), {}).keys()))))
                for pid, p in sorted(opt.items(), key=lambda kv: -num(kv[1].get("reported_value_usd")))[:6]:
                    c = comp.get(pid) or {}
                    i = p.get("identity") or {}
                    mr = rows.get(str(i.get("cusip") or "").upper()) or {}
                    r.log("    option %s | %s | identity %s | qty %s value %s | status %s qty-change %s value-change %s | prior qty %s value %s | master %s %s" % (
                        pid[:12], (p.get("issuer_names") or [""])[0][:30], json.dumps(i), p.get("reported_quantity"), money(p.get("reported_value_usd")), c.get("status"),
                        c.get("reported_quantity_change"), c.get("reported_value_change_usd"), (pri.get(pid) or {}).get("reported_quantity"), money((pri.get(pid) or {}).get("reported_value_usd")),
                        mr.get("status"), mr.get("ticker")))
                # the same underlying held as stock and as an option: do the ids differ?
                eq_by_cusip = {(p.get("identity") or {}).get("cusip"): pid for pid, p in cur.items() if kind(p) == "equity"}
                twins = [(pid, eq_by_cusip[(p.get("identity") or {}).get("cusip")]) for pid, p in opt.items() if (p.get("identity") or {}).get("cusip") in eq_by_cusip][:3]
                r.log("    stock + option twins on one CUSIP (option id, stock id): %s | position id length %d" % (json.dumps([(a[:16], b2[:16]) for a, b2 in twins]), len(next(iter(cur)))))
            sh = {str((p.get("identity") or {}).get("cusip") or "").upper() for p in list(cur.values()) + list(pri.values()) if kind(p) in ("equity", "debt")}
            all_sh |= sh
            for p in list(opt.values()) + list(opt_pri.values()):
                c = str((p.get("identity") or {}).get("cusip") or "").upper()
                opt_cusips.add(c)
                opt_only.setdefault(c, {"issuer": (p.get("issuer_names") or [""])[0], "value": 0.0, "funds": set()})
                opt_only[c]["value"] += num(p.get("reported_value_usd"))
                opt_only[c]["funds"].add(fk)

        r.section("3. the security master over option underlyings")
        only = {c: v for c, v in opt_only.items() if c not in all_sh}
        in_master = sum(1 for c in opt_cusips if c in rows)
        resolved = sum(1 for c in opt_cusips if (rows.get(c) or {}).get("status") == "resolved")
        only_missing = {c: v for c, v in only.items() if c not in rows}
        r.log("option underlyings (both periods, all managers): %d CUSIPs | in the master %d (resolved %d) | held ONLY through options (no SH/PRN row anywhere) %d, of which absent from the master %d" % (
            len(opt_cusips), in_master, resolved, len(only), len(only_missing)))
        for c, v in sorted(only_missing.items(), key=lambda kv: -kv[1]["value"])[:25]:
            r.log("  not in master: %s %-36s option value (both periods) %s funds %s" % (c, v["issuer"][:36], money(v["value"]), ",".join(sorted(v["funds"]))))
        conc = set(b.get("concentrated") or [])
        conc_missing = {c: v for c, v in only_missing.items() if v["funds"] & conc}
        r.log("of those, held by a concentrated book: %d" % len(conc_missing))
        for c, v in sorted(conc_missing.items(), key=lambda kv: -kv[1]["value"])[:25]:
            r.log("  concentrated, not in master: %s %-36s %s funds %s" % (c, v["issuer"][:36], money(v["value"]), ",".join(sorted(v["funds"] & conc))))
        r.kv(version=book.get("version"), period=str(b.get("required_period")), cohort=str(len(b.get("cohort") or [])), option_cusips=str(len(opt_cusips)), option_only_missing=str(len(only_missing)))
        r.ok("PROBE: live book %s at %s, cohort %d; %d option underlyings, %d option-only and absent from the master" % (
            book.get("version"), b.get("required_period"), len(b.get("cohort") or []), len(opt_cusips), len(only_missing)))


if __name__ == "__main__":
    main()
    sys.exit(0)
