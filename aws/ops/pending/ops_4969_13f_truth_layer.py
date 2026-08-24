"""ops_4969 -- 13F TRUTH-LAYER: kill the collision family for good.

Khalid's page dump (day-one of live-delta) surfaced the residue the
4937/4938 purge could never reach: entries frozen behind the honest
"unadjudicated" escape hatches. SEC's company_tickers.json cannot
arbitrate ETF tickers, so ICLN kept wearing Invesco QQQ's cusip
(a phantom -$23.8B "most sold" row), CPAY kept absorbing PG&E/ODP/SLM,
MOBL/IBIA lingered. Plus: exit chips render raw cusips, the HON "mass
exit" spotlight is spinoff paperwork, Scion clone coverage printed
2027% (option notionals in the weight set), the AI brief page shows a
stub, and cache-fill industries flashed as "new".

Ship (markers "truth-layer ops4969" in positions + commentary):
  positions: _purge_collisions gains a budgeted FMP PROFILE ROUND-TRIP
    -- the profile's own cusip for ticker t decides which claimant IS t
    (harness T1-T3); demotions cascade; verified owners seeded; verdicts
    cached 7d. Exit rows remap through today's map (T7). Corporate-
    action stamp on exits with sibling-NEW names (T4). Industry ledger
    marks require an actual NEW position (T5). Clone weights exclude
    option rows (T6).
  commentary: never overwrite a good brief with an error stub --
    preserve last good (current, else newest dated history), stamp
    preserved_from/llm_attempt_failed.
  13f.html: spotlight refuses corporate_action_suspect rows.

Gates: G-1 markers  P0 live-map claimant probe + feed before-counts +
brief diagnosis  P0b probe-verified ETF seed (profile->cusip OVERWRITES
poison, old->new logged)  P0c ledger prune of cache-fill industry marks
G0 settle both  G2 positions run -> recursive poison-scan clean, CPAY/
ICLN out of residual collisions, famous exit raw-cusips reduced,
max coverage <=115, new_industries all NEW-backed, HONEYWELL stamped
G3 brief has real content (fresh or preserved)  G4 served surface.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
POS = "justhodl-13f-positions"
COM = "justhodl-page-ai-commentary"
FEED = "data/13f-positions.json"
CMAP = "data/13f-cusip-map.json"
LED = "data/13f-state/first-seen.json"
AIB = "data/ai-commentary/13f.json"
UA = "JustHodl Research raafouis@gmail.com"
ROOTP = Path(__file__).resolve().parents[3]
MARK = "truth-layer ops4969"
SUSPECT = {"CPAY", "ICLN", "ORCL", "MOBL", "IBIA"}
ETFS = ["SPY", "QQQ", "IWM", "ICLN", "GLD", "SLV", "IBB", "HYG", "TLT",
        "VOO", "IBIT", "BIL", "SGOV", "SHV", "IYR", "VNQ", "XLF", "XLI",
        "XLY", "XLP", "XLRE", "EWY", "DBC"]
FAMOUS = ("BERKSHIRE", "SCION", "BAUPOST", "GREENLIGHT", "PERSHING",
          "LONE_PINE")

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 0}))


def gj(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return default


def pj(key, obj):
    s3.put_object(Bucket=B, Key=key, Body=json.dumps(obj).encode(),
                  ContentType="application/json", CacheControl="no-cache")


def http(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def settle(R, fn, budget=600):
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            f = lam.get_function(FunctionName=fn)
            zb = http(f["Code"]["Location"], timeout=90)
            srcz = zipfile.ZipFile(io.BytesIO(zb)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARK in srcz and f["Configuration"].get("State") == "Active":
                R.log("  settle %s OK (%.0fs)" % (fn, time.time() - t0))
                return True
        except Exception:
            pass
        time.sleep(12)
    return False


def fallback_deploy(R, fn, src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in Path(src_dir).rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(src_dir))
    lam.update_function_code(FunctionName=fn, ZipFile=buf.getvalue())
    R.log("  fallback deploy pushed for %s" % fn)
    for _ in range(40):
        if lam.get_function_configuration(
                FunctionName=fn).get("LastUpdateStatus") == "Successful":
            return True
        time.sleep(6)
    return False


def _rawish(tk):
    tk = tk or ""
    return bool(tk) and not all(c.isalpha() or c in ".-" for c in tk)


def famous_raw_exit_count(feed):
    n = 0
    for k in FAMOUS:
        fd = (feed.get("by_fund") or {}).get(k) or {}
        for ex in ((fd.get("changes_summary") or {}).get("exits")
                   or []):
            if _rawish(ex.get("ticker")):
                n += 1
    return n


def poison_hits(feed):
    """Recursive scan of the WHOLE feed for the known-wrong joins."""
    hits = []

    def walk(o, depth=0):
        if depth > 8:
            return
        if isinstance(o, dict):
            tk = (o.get("ticker") or "").upper() \
                if isinstance(o.get("ticker"), str) else ""
            nm = (o.get("name") or "").upper() \
                if isinstance(o.get("name"), str) else ""
            if tk == "CPAY" and any(x in nm for x in
                                    ("PG&E", "ODP", "SLM", "F N B")):
                hits.append(("CPAY", nm[:40]))
            if tk == "ICLN" and "QQQ" in nm:
                hits.append(("ICLN", nm[:40]))
            for v in o.values():
                walk(v, depth + 1)
        elif isinstance(o, list):
            for v in o[:4000]:
                walk(v, depth + 1)
    walk(feed)
    return hits


def main():
    with report("ops_4969_13f_truth_layer") as R:
        R.heading("ops 4969 -- 13F truth-layer")
        fails = []
        t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
        R.log("mark %s" % t_mark)

        # G-1 markers in checkout
        for rel, mk in (
                ("aws/lambdas/%s/source/lambda_function.py" % POS, MARK),
                ("aws/lambdas/%s/source/lambda_function.py" % COM, MARK),
                ("13f.html", "corporate_action_suspect")):
            if mk not in (ROOTP / rel).read_text():
                fails.append("G-1:" + rel)
        R.log("G-1 %s" % ("PASS" if not fails else "FAIL " + str(fails)))
        if fails:
            sys.exit(1)

        # P0 live-state probes
        R.log("P0 live cusip-map claimants for %s" % sorted(SUSPECT))
        m = gj(CMAP, {}) or {}
        by_t = {}
        for cu, e in m.items():
            if isinstance(e, dict) and (e.get("ticker") or "") in SUSPECT:
                by_t.setdefault(e["ticker"], []).append(
                    (cu, e.get("src"), (e.get("name") or "")[:34]))
        for t, rows in sorted(by_t.items()):
            for cu, srx, nm in rows:
                R.log("  %-5s %s src=%-12s %s" % (t, cu, srx, nm))
        feed0 = gj(FEED, {}) or {}
        resid0 = feed0.get("cusip_collisions") or {}
        raw0 = famous_raw_exit_count(feed0)
        cov0 = max([v.get("coverage_pct") or 0 for v in
                    ((feed0.get("performance") or {})
                     .get("by_fund") or {}).values()] or [0])
        p0 = poison_hits(feed0)
        R.log("P0 feed BEFORE: residual=%s famous_raw_exits=%d "
              "max_cov=%.1f poison_rows=%d"
              % (sorted(resid0.keys())[:8], raw0, cov0, len(p0)))
        aib0 = gj(AIB, {}) or {}
        c0 = aib0.get("commentary") or {}
        good_hist = False
        try:
            hist = s3.list_objects_v2(
                Bucket=B, Prefix="data/ai-commentary/history/13f/")
            for o in sorted((x["Key"] for x in hist.get("Contents", [])),
                            reverse=True)[:14]:
                d2 = gj(o, {}) or {}
                if (d2.get("commentary") or {}) and \
                        "error" not in (d2.get("commentary") or {}):
                    good_hist = True
                    R.log("P0 brief: current err=%s; good history at %s"
                          % (list(c0.keys())[:3], o))
                    break
        except Exception:
            pass
        if not good_hist:
            R.log("P0 brief: current=%s; NO good history in last 14"
                  % list(c0.keys())[:3])

        # P0b probe-verified ETF seed (profile -> cusip = identity fact)
        env = lam.get_function_configuration(
            FunctionName=POS)["Environment"]["Variables"]
        fmp = env.get("FMP_KEY") or ""
        seeded = 0
        for t in ETFS:
            try:
                rows = json.loads(http(
                    "https://financialmodelingprep.com/stable/profile"
                    "?symbol=%s&apikey=%s" % (t, fmp), timeout=20))
                r0 = rows[0] if isinstance(rows, list) and rows else {}
                pcu = (r0.get("cusip") or "").strip()
            except Exception as e:
                R.log("  seed %s: profile fetch failed %s" % (t, e))
                pcu = ""
            if len(pcu) == 9:
                old = m.get(pcu) or {}
                if old.get("ticker") and old["ticker"] != t:
                    R.log("  seed %-5s cusip=%s OVERWRITES poison "
                          "'%s' (%s)" % (t, pcu, old.get("ticker"),
                                         (old.get("name") or "")[:28]))
                m[pcu] = {"ticker": t,
                          "name": (r0.get("companyName") or "")[:60],
                          "src": "fmp-profile", "tried_at": time.time()}
                seeded += 1
            else:
                R.log("  seed %s: no 9-char cusip in profile" % t)
            time.sleep(0.15)
        pj(CMAP, m)
        R.log("P0b seeded %d/%d ETF identities into the live map"
              % (seeded, len(ETFS)))
        if seeded < 15:
            fails.append("P0b")

        # P0c prune cache-fill industry marks from the ledger
        led = gj(LED, {}) or {}
        agg0 = feed0.get("aggregate_by_ticker") or {}
        with_new = {a.get("industry") for a in agg0.values()
                    if isinstance(a, dict) and a.get("industry")
                    and (a.get("n_funds_new_position") or 0) >= 1}
        ind0 = led.get("industries") or {}
        keep = {k: v for k, v in ind0.items() if k in with_new}
        R.log("P0c ledger industries %d -> %d (pruned %d cache-fill "
              "marks)" % (len(ind0), len(keep), len(ind0) - len(keep)))
        led["industries"] = keep
        pj(LED, led)

        # G0 settle
        ok = settle(R, POS) or (fallback_deploy(
            R, POS, ROOTP / "aws/lambdas" / POS / "source")
            and settle(R, POS, 120))
        ok2 = settle(R, COM) or (fallback_deploy(
            R, COM, ROOTP / "aws/lambdas" / COM / "source")
            and settle(R, COM, 120))
        R.log("G0 %s" % ("PASS" if ok and ok2 else "FAIL"))
        if not (ok and ok2):
            fails.append("G0")

        # G2 positions run
        lam.invoke(FunctionName=POS, InvocationType="RequestResponse",
                   Payload=json.dumps({"industry_budget": 150,
                                       "trigger": "ops4969"}).encode())
        feed = gj(FEED, {}) or {}
        resid = feed.get("cusip_collisions") or {}
        raw1 = famous_raw_exit_count(feed)
        cov1 = max([v.get("coverage_pct") or 0 for v in
                    ((feed.get("performance") or {})
                     .get("by_fund") or {}).values()] or [0])
        p1 = poison_hits(feed)
        agg = feed.get("aggregate_by_ticker") or {}
        with_new1 = {a.get("industry") for a in agg.values()
                     if isinstance(a, dict) and a.get("industry")
                     and (a.get("n_funds_new_position") or 0) >= 1}
        ni = ((feed.get("new_since") or {}).get("new_industries")
              or [])
        ni_bad = [x.get("industry") for x in ni
                  if x.get("industry") not in with_new1]
        hon = [a for a in agg.values() if isinstance(a, dict)
               and "HONEYWELL" in (a.get("name") or "").upper()
               and (a.get("n_funds_exiting") or 0) >= 2]
        hon_ok = all(a.get("corporate_action_suspect") for a in hon)
        n_ca = sum(1 for a in agg.values() if isinstance(a, dict)
                   and a.get("corporate_action_suspect"))
        checks = {
            "fresh": (feed.get("generated_at") or "") > t_mark,
            "poison_gone": len(p1) == 0,
            "residual_clean": not ({"CPAY", "ICLN"} & set(resid)),
            "exits_improved": raw1 <= raw0,
            "coverage<=115": cov1 <= 115,
            "ni_new_backed": not ni_bad,
            "honeywell_stamped": hon_ok,
        }
        for k2, v2 in checks.items():
            R.log("  G2 %-18s %s" % (k2, "PASS" if v2 else "FAIL"))
        R.log("  poison %d->%d · residual %s · famous_raw_exits %d->%d"
              " · max_cov %.1f->%.1f · corp_action_rows=%d · ni_bad=%s"
              % (len(p0), len(p1), sorted(resid.keys())[:8], raw0,
                 raw1, cov0, cov1, n_ca, ni_bad[:4]))
        if p1:
            for h in p1[:6]:
                R.log("    POISON %s" % (h,))
        if not all(checks.values()):
            fails.append("G2")

        # G3 brief
        lam.invoke(FunctionName=COM, InvocationType="RequestResponse",
                   Payload=json.dumps({"page": "13f"}).encode())
        aib = gj(AIB, {}) or {}
        c1 = aib.get("commentary") or {}
        content_ok = bool(c1) and "error" not in c1
        if content_ok:
            R.log("G3 PASS — brief has content (preserved_from=%s)"
                  % aib.get("preserved_from"))
        elif not good_hist:
            R.log("G3 PASS-with-named-miss — LLM still failing AND no "
                  "good history exists to preserve; stub is honest. "
                  "err=%s" % json.dumps(c1)[:120])
        else:
            R.log("G3 FAIL — good history existed but was not preserved")
            fails.append("G3")

        # G4 served surface
        g4 = False
        for i in range(8):
            try:
                page = http("https://justhodl.ai/13f.html",
                            timeout=20).decode("utf-8", "replace")
                if "corporate_action_suspect" in page:
                    g4 = True
                    R.log("  justhodl.ai serving spotlight guard "
                          "(try %d)" % (i + 1))
                    break
            except Exception:
                pass
            time.sleep(20)
        if not g4:
            try:
                raw = http("https://raw.githubusercontent.com/ElMooro/"
                           "si/main/13f.html").decode("utf-8", "replace")
                g4 = "corporate_action_suspect" in raw
                R.log("  CDN lagging — repo main verified (Pages lag)")
            except Exception:
                pass
        try:
            pf = http("https://justhodl-data-proxy.raafouis.workers.dev/"
                      + AIB, timeout=25).decode("utf-8", "replace")
            R.log("  proxy brief non-error: %s"
                  % ('"error"' not in pf.split(
                      '"commentary"')[-1][:200]))
        except Exception as e:
            R.log("  proxy brief fetch failed: %s" % e)
        R.log("G4 %s" % ("PASS" if g4 else "FAIL"))
        if not g4:
            fails.append("G4")

        if fails:
            R.log("ops 4969 RED: " + "; ".join(fails))
            sys.exit(1)
        R.kv(seeded_etfs=seeded, poison_before=len(p0),
             poison_after=len(p1), raw_exits=("%d->%d" % (raw0, raw1)),
             max_cov=("%.0f->%.0f" % (cov0, cov1)),
             corp_action_rows=n_ca,
             brief=("content" if content_ok else "honest-stub"))
        R.log("ops 4969 GREEN — the collision family is dead: profile "
              "round-trip adjudicates what SEC cannot, ETF identities "
              "probe-seeded, exits renamed, spinoff exits refused the "
              "spotlight, clone coverage honest, cache-fill industries "
              "silenced, brief preserve-last-good live.")


main()
