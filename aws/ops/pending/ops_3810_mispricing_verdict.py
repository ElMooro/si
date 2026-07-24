#!/usr/bin/env python3
"""ops 3810 — v5.0 MISPRICING VERDICT: why cheap, who's positioned, what forces it.

THE PROBLEM WITH v4.4. capture-gap finds companies that are STRUCTURALLY
IMPORTANT and CHEAP VS PEERS. That is a value screen with a quality filter, and
it is exactly how value investors get killed: a cheap critical name with falling
estimates, no institutional flow and a decaying industry is a VALUE TRAP, not a
mispricing. The engine currently cannot tell those apart.

A fundamental long/short desk underwrites three further questions before sizing:
  WHY is it cheap?      -> is the E stale, or is the P wrong?
  WHO is on the other side? -> is smart money accumulating, or is it a crowded short?
  WHAT closes the gap?  -> is there a catalyst with a clock, or does it stay cheap?

v5.0 answers each from feeds that ALREADY EXIST and were never wired. Coverage
was measured first (ops 3808) and keys resolved (ops 3809) — nothing guessed:

  estimate-revisions  direction_map (393)      -> IS THE 'E' STALE     [DISQUALIFIER]
  dark-pool           dark_map (939, 38% cov)  -> SMART MONEY FLOW     [confirm]
  finra-short         tickers (501, 21% cov)   -> CROWDED BEAR/SQUEEZE [context]
  earnings-pead       all_qualifying (244)     -> CATALYST WITH CLOCK  [confirm]
  industry-boom       league (119, industry)   -> IS THE INDUSTRY INFLECTING [regime]

DESIGN PRINCIPLE — these are QUALIFIERS, NOT SCORE BOOSTERS. Folding them into
the blended rank would just shuffle order and hide the reasoning. Instead the
engine emits a VERDICT that a human can argue with:

  MISPRICED     gap + structural importance + >=2 confirming legs + no disqualifier
  VALUE_TRAP    gap but estimates falling, or industry in the boom-league bottom
  CROWDED_SHORT gap + heavy short interest -> either the market knows, or squeeze
  UNPROVEN      gap but no evidence either way (the honest majority)

Plus TIME PERSISTENCE from the ledger itself: gap_first_seen / gap_days_open.
A gap open for months that never closed is the market disagreeing for a reason —
the single most honest trap detector available, and it costs no new feed.
"""
import sys, json, time, zipfile, io, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "shared"))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

FN = "justhodl-chokepoint"
SRC = ROOT / "lambdas" / FN / "source"
LF = SRC / "lambda_function.py"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)


BLOCK = '''
            # ══ ops 3810 · v5.0 MISPRICING VERDICT ═══════════════════════════
            # A wide capture gap says the market pays little for a company that
            # matters. It does NOT say the market is wrong. These legs separate
            # a mispricing from a value trap using feeds that already exist.
            # Every key below was resolved against the live artifact (ops 3809),
            # never guessed — six bugs in this arc came from guessed keys.
            try:
                _rev = _read("data/estimate-revisions.json") or {}
                _dirmap = _rev.get("direction_map") or {}
                _dp = _read("data/dark-pool.json") or {}
                _dmap = _dp.get("dark_map") or {}
                _fs = _read("data/finra-short.json") or {}
                _smap = _fs.get("tickers") or {}
                _pe = _read("data/earnings-pead.json") or {}
                _pmap = {r.get("symbol"): r for r in (_pe.get("all_qualifying") or [])
                         if r.get("symbol")}
                _ib = _read("data/industry-boom.json") or {}
                _league = _ib.get("league") or []
                _boom = {x.get("industry"): x for x in _league if x.get("industry")}
                _bs = sorted([x.get("boom_score") for x in _league
                              if isinstance(x.get("boom_score"), (int, float))])
                _b_hi = _bs[int(len(_bs) * 0.75)] if _bs else None
                _b_lo = _bs[int(len(_bs) * 0.25)] if _bs else None

                # ── time persistence from the ledger (no new feed) ──
                _today = datetime.now(timezone.utc).date().isoformat()
                _prev = _led_rows if isinstance(_led_rows, dict) else {}

                _n_rev = _n_flow = _n_short = _n_cat = _n_boom = 0
                for _c in cap_rows:
                    _t = _c["ticker"]

                    # 1. IS THE 'E' STALE?  DISQUALIFIER.
                    _dir = _dirmap.get(_t)
                    if _dir:
                        _n_rev += 1
                        _c["estimate_direction"] = _dir
                        _c["estimates_falling"] = (str(_dir).upper() in
                                                   ("DOWN", "FALLING", "NEGATIVE", "CUT"))
                    else:
                        _c["estimate_direction"] = None
                        _c["estimates_falling"] = None

                    # 2. SMART MONEY FLOW (best coverage: 38% of ledger)
                    _d = _dmap.get(_t) or {}
                    if _d:
                        _n_flow += 1
                        _c["dark_pool_pct"] = _d.get("dark_pool_pct")
                        _c["dark_accel"] = _d.get("dark_accel")
                        _c["dark_state"] = _d.get("state")
                        _c["smart_accumulation"] = bool(
                            str(_d.get("state") or "").upper().find("ACCUM") >= 0
                            or (isinstance(_d.get("dark_accel"), (int, float))
                                and _d["dark_accel"] > 0))
                    else:
                        _c["dark_state"] = None
                        _c["smart_accumulation"] = None

                    # 3. CROWDED BEAR / SQUEEZE FUEL
                    _sh = _smap.get(_t) or {}
                    if _sh:
                        _n_short += 1
                        _c["short_volume_ratio_pct"] = _sh.get("svr_pct")
                        _c["short_z"] = _sh.get("z_score")
                        _c["days_to_cover"] = _sh.get("days_to_cover")
                        _c["crowded_short"] = bool(
                            isinstance(_sh.get("svr_pct"), (int, float))
                            and _sh["svr_pct"] >= 55)
                    else:
                        _c["crowded_short"] = None

                    # 4. CATALYST WITH A CLOCK
                    _p = _pmap.get(_t) or {}
                    if _p:
                        _n_cat += 1
                        _m = _p.get("metrics") or {}
                        _c["pead_tier"] = _p.get("tier")
                        _c["beat_streak"] = _p.get("beat_streak")
                        _c["days_since_earnings"] = _m.get("days_since_earnings")
                        _c["drift_active"] = bool(_m.get("drift_active"))
                        _c["has_catalyst"] = bool(_m.get("drift_active")
                                                  or (_p.get("beat_streak") or 0) >= 3)
                    else:
                        _c["has_catalyst"] = None

                    # 5. IS THE INDUSTRY INFLECTING OR DECAYING?
                    _bi = _boom.get(_c.get("industry")) or {}
                    if _bi:
                        _n_boom += 1
                        _c["industry_boom_score"] = _bi.get("boom_score")
                        _c["industry_boom_delta_20d"] = _bi.get("score_delta_20d")
                        if _b_hi is not None and isinstance(_bi.get("boom_score"), (int, float)):
                            _c["industry_regime"] = ("BOOMING" if _bi["boom_score"] >= _b_hi
                                                     else "DECAYING" if _bi["boom_score"] <= _b_lo
                                                     else "NEUTRAL")
                        else:
                            _c["industry_regime"] = None
                    else:
                        _c["industry_regime"] = None

                    # 6. HOW LONG HAS THIS GAP BEEN OPEN? (trap detector)
                    _pr = _prev.get(_t) or {}
                    _open_now = (_c.get("capture_gap") or -999) >= 20
                    _first = _pr.get("gap_first_seen")
                    if _open_now:
                        _c["gap_first_seen"] = _first or _today
                        try:
                            _d0 = datetime.fromisoformat(_c["gap_first_seen"]).date()
                            _c["gap_days_open"] = (datetime.now(timezone.utc).date() - _d0).days
                        except Exception:
                            _c["gap_days_open"] = 0
                    else:
                        _c["gap_first_seen"] = None
                        _c["gap_days_open"] = None

                    # ── VERDICT ──────────────────────────────────────────────
                    _conf, _why = 0, []
                    if _c.get("smart_accumulation"):
                        _conf += 1; _why.append("institutional accumulation off-exchange")
                    if _c.get("has_catalyst"):
                        _conf += 1; _why.append("post-earnings drift active")
                    if _c.get("industry_regime") == "BOOMING":
                        _conf += 1; _why.append("industry inflecting up")
                    if _c.get("estimate_direction") and not _c.get("estimates_falling"):
                        _conf += 1; _why.append("estimates stable or rising")

                    _dis, _dwhy = 0, []
                    if _c.get("estimates_falling"):
                        _dis += 1; _dwhy.append("earnings estimates falling — the E is stale, not the P wrong")
                    if _c.get("industry_regime") == "DECAYING":
                        _dis += 1; _dwhy.append("industry in structural decline")
                    if (_c.get("gap_days_open") or 0) > 120:
                        _dis += 1; _dwhy.append("gap open %d days without closing" % _c["gap_days_open"])

                    _wide = (_c.get("capture_gap") or -999) >= 20
                    _important = (_c.get("structural_importance") or 0) >= 40
                    if not _wide:
                        _v = "NO_GAP"
                    elif _dis > 0 and _conf < 2:
                        _v = "VALUE_TRAP"
                    elif _c.get("crowded_short") and _conf < 2:
                        _v = "CROWDED_SHORT"
                    elif _wide and _important and _conf >= 2 and _dis == 0:
                        _v = "MISPRICED"
                    else:
                        _v = "UNPROVEN"
                    _c["mispricing_verdict"] = _v
                    _c["verdict_confirms"] = _why
                    _c["verdict_disqualifiers"] = _dwhy
                    _c["verdict_evidence_n"] = _conf

                capture["stats"]["verdict_counts"] = {}
                for _c in cap_rows:
                    _k = _c.get("mispricing_verdict")
                    capture["stats"]["verdict_counts"][_k] = \\
                        capture["stats"]["verdict_counts"].get(_k, 0) + 1
                capture["stats"]["joins"] = {
                    "revisions": _n_rev, "dark_pool": _n_flow, "short": _n_short,
                    "pead": _n_cat, "industry_boom": _n_boom}
                capture["mispriced_book"] = sorted(
                    [c for c in cap_rows if c.get("mispricing_verdict") == "MISPRICED"],
                    key=lambda x: -(x.get("undervaluation_score") or 0))[:40]
                capture["value_trap_book"] = sorted(
                    [c for c in cap_rows if c.get("mispricing_verdict") == "VALUE_TRAP"],
                    key=lambda x: -(x.get("capture_gap") or 0))[:25]
                capture["verdict_note"] = (
                    "A wide capture gap says the market pays little for a company that "
                    "matters to its industry. It does NOT say the market is wrong. "
                    "MISPRICED requires the gap AND structural importance AND at least "
                    "two independent confirmations (institutional accumulation, active "
                    "post-earnings drift, an inflecting industry, stable-or-rising "
                    "estimates) AND no disqualifier. VALUE_TRAP means the gap exists but "
                    "earnings estimates are falling, the industry is decaying, or the gap "
                    "has stayed open over 120 days without closing — the market "
                    "disagreeing for a reason. UNPROVEN is the honest majority: a gap "
                    "with no evidence either way. These are qualifiers, never score "
                    "boosters — folding them into the rank would hide the reasoning.")
                diag.append("v5 verdicts: %s" % json.dumps(capture["stats"]["verdict_counts"]))
            except Exception as _ve:
                capture["verdict_error"] = str(_ve)[:250]
                diag.append("v5 verdict FAILED: %s" % str(_ve)[:150])

'''


def main():
    with report("3810_mispricing_verdict") as rep:
        rep.heading("ops 3810 — v5.0 mispricing verdict (why/who/what)")

        src = LF.read_text()
        rep.section("G0 — keys verified live in ops 3809")
        gate(rep, "G0.v441", 'VERSION = "4.4.1"' in src, "engine at v4.4.1")
        gate(rep, "G0.struct", '_c["structural_importance"] = _si' in src, "structural score present")
        gate(rep, "G0.ledger_var", "_led_rows" in src, "ledger rows in scope for persistence")
        gate(rep, "G0.datetime", "from datetime import datetime" in src, "datetime available")
        anchor = '            capture["stats"]["with_structural_importance"] = sum('
        gate(rep, "G0.anchor", src.count(anchor) == 1, "splice anchor unique")
        if FAILED:
            sys.exit(1)

        # verify producer containers one last time against the live artifacts
        for key, cont in (("data/estimate-revisions.json", "direction_map"),
                          ("data/dark-pool.json", "dark_map"),
                          ("data/finra-short.json", "tickers"),
                          ("data/earnings-pead.json", "all_qualifying"),
                          ("data/industry-boom.json", "league")):
            try:
                j = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
                gate(rep, "G0." + cont, cont in j and bool(j[cont]),
                     "%s -> %s n=%s" % (key, cont, len(j.get(cont) or [])))
            except Exception as e:
                gate(rep, "G0." + cont, False, str(e)[:100])
        if FAILED:
            sys.exit(1)

        rep.section("Splice v5.0")
        src = src.replace(anchor, BLOCK + anchor, 1)
        src = src.replace('VERSION = "4.4.1"', 'VERSION = "5.0"', 1)
        LF.write_text(src)
        import py_compile
        py_compile.compile(str(LF), doraise=True)
        rep.ok("v5.0 spliced + compile clean")

        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Capture gap v5.0 — mispricing verdict: revisions, dark-pool flow, short interest, PEAD catalyst, industry regime, gap persistence.",
                      create_function_url=False, smoke=False)
        settled = False
        for i in range(14):
            time.sleep(15)
            c0 = lam.get_function_configuration(FunctionName=FN)
            if c0.get("State") != "Active" or c0.get("LastUpdateStatus") != "Successful":
                continue
            u = lam.get_function(FunctionName=FN)["Code"]["Location"]
            with urllib.request.urlopen(u, timeout=90) as r:
                blob = r.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                if "MISPRICING VERDICT" in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True; rep.ok("settled attempt %d" % (i + 1)); break
        gate(rep, "DEPLOY.settled", settled, "v5.0 live")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke")
        from botocore.config import Config
        ll = boto3.client("lambda", region_name="us-east-1",
                          config=Config(read_timeout=890, retries={"max_attempts": 0}))
        t0 = time.time()
        r = ll.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      Payload=json.dumps({"mode": "full"}).encode())
        rep.kv(invoke_status=r.get("StatusCode"), invoke_seconds=round(time.time() - t0, 1))

        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        st = cap.get("stats") or {}
        gate(rep, "LIVE.v50", d.get("version") == "5.0", "version=%s" % d.get("version"))
        gate(rep, "LIVE.no_err", "verdict_error" not in cap, "err=%s" % cap.get("verdict_error"))

        rep.section("Join coverage — every leg must be alive")
        joins = st.get("joins") or {}
        for k, v in joins.items():
            rep.log("  %-16s %d rows" % (k, v))
            gate(rep, f"JOIN.{k}", (v or 0) > 0, "%d joined" % (v or 0))

        rep.section("Verdict distribution")
        vc = st.get("verdict_counts") or {}
        for k, v in sorted(vc.items(), key=lambda z: -(z[1] or 0)):
            rep.log("  %-16s %d" % (k, v))
        gate(rep, "VERDICT.discriminates", len([k for k in vc if k and k != "NO_GAP"]) >= 2,
             "more than one verdict class populated")
        gate(rep, "VERDICT.not_all_mispriced", (vc.get("MISPRICED") or 0) < len(rows) * 0.15,
             "MISPRICED=%s is a minority, not a rubber stamp" % vc.get("MISPRICED"))

        rep.section("MISPRICED book — gap + importance + 2 confirmations, no disqualifier")
        for x in (cap.get("mispriced_book") or [])[:12]:
            rep.log("  %-6s %-26s gap=%+5.1f SI=%4.1f  %s" % (
                x.get("ticker"), (x.get("industry") or "")[:26], x.get("capture_gap") or 0,
                x.get("structural_importance") or 0, "; ".join(x.get("verdict_confirms") or [])[:60]))

        rep.section("VALUE_TRAP book — the names this engine used to rank highly")
        for x in (cap.get("value_trap_book") or [])[:10]:
            rep.log("  %-6s gap=%+5.1f  %s" % (
                x.get("ticker"), x.get("capture_gap") or 0,
                "; ".join(x.get("verdict_disqualifiers") or [])[:70]))

        rep.section("Additive")
        for k in ("capture_gap", "structural_importance", "catchup_pct", "revenue_share_pct"):
            gate(rep, f"ADDITIVE.{k}", any(x.get(k) is not None for x in rows), "preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — mispricing separated from value traps with evidence")


if __name__ == "__main__":
    main()
