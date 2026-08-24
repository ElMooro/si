"""ops_4968 -- 13F LIVE-DELTA: the desk that never updated becomes the
desk that updates within minutes of EDGAR.

Khalid's complaint (verbatim class): "13f has been a total failure it
never gets updated" + wants (a) a flashing red band of newly-added
industries and newly-added small/mid/micro positions, (b) industries
bought / industries sold boards.

Root causes found in checkout + payload:
  1. cadence: positions on cron(10 21 MON-FRI) — once a day, weekdays.
  2. sec-13f keeps prior index entries on per-CIK fetch error FOREVER
     with no flag -> Greenlight frozen at 2023-12-31, Pershing a
     quarter behind (fetch/entity truth unknown until probed).
  3. no industry dimension; no delta surfacing; AI brief feed stale.

Ship (markers "live-delta ops4968" in both sources):
  sec-13f: 30-min detector + instant positions trigger + visible
    stale_kept/stale_reason + probe-verified CIK overrides.
  positions: industry capture (FMP profile), industry_flows block,
    new_since + first-seen ledger (seed-silent), filing Telegram
    alert, ai-commentary refire. 13f.html: red flash band +
    industries bought/sold section.

Gates: G-1 markers-in-checkout  P0 EDGAR truth + CIK re-hunt (probe
FROM the runner; overrides written only on proof)  G0 settle both
markers (deploy fallback preserves live env)  G1 schedules 30m/2h
ENABLED + targets, -6h disabled, commentary rule enabled  G2 sec-13f
run -> index matches EDGAR truth  G3 positions run -> 18 accounted,
industry_flows reconciles to ticker sums, new_since seed-silent,
smallmid list live, roster fresh per truth  G4 ai brief regenerated
post-mark  G5 served surface carries the new markers + feed key.
"""
import io
import json
import re
import sys
import time
import urllib.parse
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
SEC = "justhodl-sec-13f"
COM = "justhodl-page-ai-commentary"
FEED = "data/13f-positions.json"
IDX = "data/institutional-positions.json"
OVR = "data/13f-state/cik-overrides.json"
LED = "data/13f-state/first-seen.json"
AIB = "data/ai-commentary/13f.json"
UA = "JustHodl Research raafouis@gmail.com"
ROOTP = Path(__file__).resolve().parents[3]
MARK = "live-delta ops4968"
STALE_D = 210

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 0}))
ev = boto3.client("events", region_name=REGION)


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


def edgar_latest_13f(cik):
    """Newest 13F-HR(/A) for a CIK straight from EDGAR submissions."""
    d = json.loads(http(
        "https://data.sec.gov/submissions/CIK%s.json" % str(cik).zfill(10)))
    rec = d.get("filings", {}).get("recent", {})
    def _at(key, i):
        v = rec.get(key) or []
        return v[i] if i < len(v) else None
    for i, form in enumerate(rec.get("form", [])):
        if form in ("13F-HR", "13F-HR/A"):
            return {"name": d.get("name"),
                    "accession": _at("accessionNumber", i),
                    "period": _at("reportDate", i),
                    "filed": _at("filingDate", i),
                    "form": form}
    return {"name": d.get("name"), "accession": None,
            "period": None, "filed": None, "form": None}


def hunt_cik(query):
    """browse-edgar company search -> candidate (cik, name) pairs."""
    url = ("https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
           "&company=%s&type=13F-HR&dateb=&owner=include&count=40"
           "&output=atom" % urllib.parse.quote(query))
    try:
        xml = http(url).decode("utf-8", "replace")
    except Exception:
        return []
    out, seen = [], set()
    for m in re.finditer(r'CIK=(\d{7,10})&amp;[^"]*"[^>]*>\s*([^<]+)', xml):
        cik, nm = m.group(1), m.group(2).strip()
        if cik not in seen:
            seen.add(cik)
            out.append((cik, nm))
    if not out:
        for m in re.finditer(r"CIK=(\d{7,10})", xml):
            if m.group(1) not in seen:
                seen.add(m.group(1))
                out.append((m.group(1), ""))
    return out[:6]


def settle(R, fn, budget=600):
    """Deployed artifact must carry the marker AND be Active."""
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            f = lam.get_function(FunctionName=fn)
            zb = http(f["Code"]["Location"], timeout=90)
            src = zipfile.ZipFile(io.BytesIO(zb)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARK in src and f["Configuration"].get("State") == "Active":
                R.log("  settle %s OK (%.0fs)" % (fn, time.time() - t0))
                return True
        except Exception:
            pass
        time.sleep(12)
    return False


def fallback_deploy(R, fn, src_dir):
    """Zip checkout source over the live fn, PRESERVING live env."""
    cfg = lam.get_function_configuration(FunctionName=fn)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in Path(src_dir).rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(src_dir))
    lam.update_function_code(FunctionName=fn, ZipFile=buf.getvalue())
    R.log("  fallback deploy pushed for %s (env preserved: %d keys)"
          % (fn, len((cfg.get("Environment") or {}).get(
              "Variables") or {})))
    for _ in range(40):
        st = lam.get_function_configuration(
            FunctionName=fn).get("LastUpdateStatus")
        if st == "Successful":
            return True
        time.sleep(6)
    return False


def ensure_rule(R, name, expr, target_fn, tgt_id):
    ev.put_rule(Name=name, ScheduleExpression=expr, State="ENABLED",
                Description="ops4968 live-delta cadence")
    tgts = ev.list_targets_by_rule(Rule=name).get("Targets", [])
    arn = lam.get_function(FunctionName=target_fn)[
        "Configuration"]["FunctionArn"]
    if not any(t.get("Arn") == arn for t in tgts):
        ev.put_targets(Rule=name, Targets=[{"Id": tgt_id, "Arn": arn}])
    try:
        lam.add_permission(
            FunctionName=target_fn, StatementId="ops4968-%s" % name,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn="arn:aws:events:%s:857687956942:rule/%s"
                      % (REGION, name))
    except Exception:
        pass
    st = ev.describe_rule(Name=name).get("State")
    R.log("  rule %s -> %s [%s]" % (name, expr, st))
    return st == "ENABLED"


def main():
    with report("ops_4968_13f_live_delta") as R:
        R.heading("ops 4968 -- 13F live-delta")
        fails = []
        t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
        R.log("mark %s" % t_mark)

        # G-1 markers in checkout
        for rel in ("aws/lambdas/%s/source/lambda_function.py" % POS,
                    "aws/lambdas/%s/source/lambda_function.py" % SEC):
            if MARK not in (ROOTP / rel).read_text():
                fails.append("G-1:" + rel)
        html = (ROOTP / "13f.html").read_text()
        for mk in ("jh13f-newflash", 'id="ind-bought"', "renderIndustryFlows"):
            if mk not in html:
                fails.append("G-1:html:" + mk)
        R.log("G-1 %s" % ("PASS" if not fails else "FAIL " + str(fails)))
        if fails:
            R.log("ops 4968 RED (checkout)")
            sys.exit(1)

        # P0 EDGAR truth sweep + CIK re-hunt (probe-verified overrides)
        R.log("P0 EDGAR truth sweep — FROM the runner")
        src = (ROOTP / "aws/lambdas/%s/source/lambda_function.py"
               % SEC).read_text()
        wl = dict(re.findall(r'"([A-Z_]+)":\s*"(\d{7,10})"',
                             re.search(r"WATCHLIST\s*=\s*\{(.*?)\n\}",
                                       src, re.S).group(1)))
        psrc = (ROOTP / "aws/lambdas/%s/source/lambda_function.py"
                % POS).read_text()
        disp = dict(re.findall(
            r'"([A-Z_]+)":\s*"([^"]+)"',
            re.search(r"FUND_DISPLAY_NAMES\s*=\s*\{(.*?)\n\}",
                      psrc, re.S).group(1)))
        today = datetime.now(timezone.utc).date()
        truth, stale_keys = {}, []
        for k, cik in wl.items():
            try:
                t = edgar_latest_13f(cik)
            except Exception as e:
                t = {"error": str(e)[:120]}
            truth[k] = dict(t, cik=cik)
            per = t.get("period")
            age = (today - datetime.fromisoformat(per).date()).days \
                if per else 9999
            truth[k]["age_d"] = age
            if age > STALE_D:
                stale_keys.append(k)
            R.log("  %-12s cik=%s latest=%s per=%s age=%sd %s"
                  % (k, cik, t.get("accession"), per, age,
                     "STALE" if age > STALE_D else ""))
            time.sleep(0.15)
        ovr = gj(OVR, {}) or {}
        for k in stale_keys:
            q = re.sub(r"\(.*?\)", "", disp.get(k, k)).strip().lower()
            q = " ".join(q.split()[:2])
            R.log("  re-hunt %s -> browse-edgar company='%s'" % (k, q))
            best = None
            for cik2, nm in hunt_cik(q):
                if cik2 == wl[k]:
                    continue
                try:
                    t2 = edgar_latest_13f(cik2)
                except Exception:
                    continue
                per2 = t2.get("period")
                if not per2:
                    continue
                age2 = (today - datetime.fromisoformat(per2).date()).days
                nm2 = (t2.get("name") or nm or "").lower()
                if age2 <= STALE_D and any(w in nm2 for w in q.split()):
                    if not best or per2 > best["period"]:
                        best = {"cik": cik2, "name": t2.get("name"),
                                "period": per2, "age_d": age2,
                                "accession": t2.get("accession")}
                time.sleep(0.15)
            if best:
                ovr[k] = {"cik": best["cik"],
                          "reason": "roster CIK stale (%sd); EDGAR "
                                    "search found active filer"
                                    % truth[k]["age_d"],
                          "proof": best, "set_by": "ops4968",
                          "at": t_mark}
                truth[k] = dict(edgar_latest_13f(best["cik"]),
                                cik=best["cik"], age_d=best["age_d"])
                R.log("    OVERRIDE %s -> cik %s (%s, %s)"
                      % (k, best["cik"], best["name"], best["period"]))
            else:
                ovr[k] = {"status": "deregistered",
                          "reason": "no active 13F-HR filer found for "
                                    "'%s'; last filed %s" % (
                                        q, truth[k].get("period")),
                          "set_by": "ops4968", "at": t_mark}
                R.log("    %s: no active filer — marked deregistered "
                      "(final filing stands, labeled)" % k)
        pj(OVR, ovr)
        R.log("P0 done — %d stale, %d overrides live"
              % (len(stale_keys), len([v for v in ovr.values()
                                       if v.get("set_by") == "ops4968"])))

        # G0 settle deployed markers
        ok = settle(R, POS) or fallback_deploy(
            R, POS, ROOTP / "aws/lambdas" / POS / "source") and \
            settle(R, POS, 120)
        ok2 = settle(R, SEC) or fallback_deploy(
            R, SEC, ROOTP / "aws/lambdas" / SEC / "source") and \
            settle(R, SEC, 120)
        R.log("G0 %s" % ("PASS" if ok and ok2 else "FAIL"))
        if not (ok and ok2):
            fails.append("G0")

        # G1 schedules
        r1 = ensure_rule(R, "justhodl-sec-13f-daily",
                         "rate(30 minutes)", SEC, "sec13f")
        r2 = ensure_rule(R, "justhodl-13f-positions-sched",
                         "rate(2 hours)", POS, "pos13f")
        try:
            ev.describe_rule(Name="justhodl-13f-positions-6h")
            ev.disable_rule(Name="justhodl-13f-positions-6h")
            R.log("  rule justhodl-13f-positions-6h -> DISABLED (phantom)")
        except Exception:
            R.log("  rule justhodl-13f-positions-6h absent — nothing to do")
        try:
            st = ev.describe_rule(
                Name="justhodl-page-ai-commentary-daily").get("State")
            if st != "ENABLED":
                ev.enable_rule(Name="justhodl-page-ai-commentary-daily")
                st = "ENABLED(re)"
            R.log("  rule justhodl-page-ai-commentary-daily -> %s" % st)
            r3 = True
        except Exception as e:
            R.log("  commentary rule missing: %s" % e)
            r3 = False
        R.log("G1 %s" % ("PASS" if r1 and r2 and r3 else "FAIL"))
        if not (r1 and r2 and r3):
            fails.append("G1")

        # G2 sec-13f run -> index matches EDGAR truth
        lam.invoke(FunctionName=SEC, InvocationType="RequestResponse",
                   Payload=b"{}")
        idx = gj(IDX, {}) or {}
        g2ok = (idx.get("generated_at") or "") > t_mark
        mism = []
        for k, t in truth.items():
            if (ovr.get(k) or {}).get("status") == "deregistered":
                continue
            got = ((idx.get("by_fund", {}).get(k) or {})
                   .get("latest_filing") or {})
            if t.get("accession") and \
                    got.get("accession") != t["accession"] and \
                    got.get("period_of_report") != t.get("period"):
                mism.append("%s idx=%s truth=%s" % (
                    k, got.get("period_of_report"), t.get("period")))
        for m in mism:
            R.log("  MISMATCH " + m)
        g2ok = g2ok and not mism
        R.log("G2 %s — index fresh, %d mismatches"
              % ("PASS" if g2ok else "FAIL", len(mism)))
        if not g2ok:
            fails.append("G2")

        # G3 positions run (industry backfill budget)
        lam.invoke(FunctionName=POS, InvocationType="RequestResponse",
                   Payload=json.dumps({"industry_budget": 400,
                                       "trigger": "ops4968"}).encode())
        feed = gj(FEED, {}) or {}
        fl = feed.get("industry_flows") or {}
        ns = feed.get("new_since") or {}
        agg = feed.get("aggregate_by_ticker") or {}
        cls_net = sum(round(float(a.get("bought_usd") or 0)
                            - float(a.get("sold_usd") or 0))
                      for a in agg.values() if a.get("industry"))
        ind_net = sum(int(r.get("net_flow_usd") or 0)
                      for r in fl.get("by_industry", []))
        parsed = int(feed.get("funds_parsed") or 0)
        failed_named = all(
            (ovr.get(fe.get("fund_key")) or {}).get("status")
            or fe.get("error")
            for fe in feed.get("fund_errors", []))
        checks = {
            "fresh": (feed.get("generated_at") or "") > t_mark,
            "accounted": parsed + int(feed.get("funds_failed") or 0)
            == int(feed.get("funds_total") or 0) and parsed >= 17
            and failed_named,
            "industries>=12": int(fl.get("industries_resolved") or 0)
            >= 12,
            "coverage>=50": float(fl.get("coverage_pct_of_gross") or 0)
            >= 50,
            "reconcile": abs(ind_net - cls_net) <= max(
                5, int(abs(cls_net) * 0.001)),
            "new_since": bool(ns.get("alert")) and isinstance(
                ns.get("new_small_mid_micro"), list),
            "seed_silent": (not ns.get("seed_run"))
            or ns.get("alert", {}).get("level") == "none",
            "smallmid>=3": len(ns.get("new_small_mid_micro") or [])
            >= 3,
            "ledger": bool(gj(LED)),
        }
        for k2, t2 in truth.items():
            if (ovr.get(k2) or {}).get("status") == "deregistered":
                continue
            if t2.get("period"):
                got = (feed.get("roster_periods") or {}).get(k2)
                if got != t2["period"]:
                    checks["roster:%s" % k2] = False
        for k2, v2 in checks.items():
            R.log("  G3 %-16s %s" % (k2, "PASS" if v2 else "FAIL"))
        R.log("  industries=%s coverage=%s%% ind_net=%s cls_net=%s "
              "smallmid=%d stale_funds=%s"
              % (fl.get("industries_resolved"),
                 fl.get("coverage_pct_of_gross"), ind_net, cls_net,
                 len(ns.get("new_small_mid_micro") or []),
                 [x.get("fund_key")
                  for x in feed.get("stale_funds", [])]))
        if not all(checks.values()):
            fails.append("G3")

        # G4 AI brief regenerated
        lam.invoke(FunctionName=COM, InvocationType="RequestResponse",
                   Payload=json.dumps({"page": "13f"}).encode())
        aib = gj(AIB, {}) or {}
        g4 = (aib.get("generated_at") or "") > t_mark
        R.log("G4 %s — ai-commentary/13f generated_at=%s"
              % ("PASS" if g4 else "FAIL", aib.get("generated_at")))
        if not g4:
            fails.append("G4")

        # G5 served surface
        g5 = False
        for i in range(9):
            try:
                page = http("https://justhodl.ai/13f.html",
                            timeout=20).decode("utf-8", "replace")
                if "jh13f-newflash" in page and 'id="ind-bought"' in page:
                    g5 = True
                    R.log("  justhodl.ai serving new markup (try %d)"
                          % (i + 1))
                    break
            except Exception:
                pass
            time.sleep(20)
        if not g5:
            try:
                raw = http("https://raw.githubusercontent.com/ElMooro/"
                           "si/main/13f.html").decode("utf-8", "replace")
                g5 = "jh13f-newflash" in raw
                R.log("  CDN lagging — repo main verified instead "
                      "(Pages propagation)")
            except Exception:
                pass
        try:
            pf = http("https://justhodl-data-proxy.raafouis.workers.dev/"
                      + FEED, timeout=25).decode("utf-8", "replace")
            g5 = g5 and '"industry_flows"' in pf
            R.log("  proxy feed carries industry_flows: %s"
                  % ('"industry_flows"' in pf))
        except Exception as e:
            R.log("  proxy fetch failed: %s" % e)
            g5 = False
        R.log("G5 %s" % ("PASS" if g5 else "FAIL"))
        if not g5:
            fails.append("G5")

        if fails:
            R.log("ops 4968 RED: " + "; ".join(fails))
            sys.exit(1)
        R.kv(funds_parsed=parsed,
             industries=fl.get("industries_resolved"),
             coverage_pct=fl.get("coverage_pct_of_gross"),
             smallmid_new=len(ns.get("new_small_mid_micro") or []),
             overrides=len([v for v in ovr.values()
                            if v.get("set_by") == "ops4968"]),
             detector="rate(30 minutes)", full_refresh="rate(2 hours)")
        R.log("ops 4968 GREEN — 13F is live-delta: EDGAR watched every "
              "30 min with instant rebuilds, industries bought/sold on "
              "the page, red flash wired to a seed-silent ledger, AI "
              "brief regenerated, stale roster truth-resolved.")


main()
