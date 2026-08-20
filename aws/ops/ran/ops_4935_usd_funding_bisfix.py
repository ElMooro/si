"""ops/4932 -- justhodl-usd-funding: deploy from inside ops, then accept.

deploy-lambdas.yml reported SUCCESS three times and created nothing.
Every step showed green; the function never existed. Rather than keep
guessing at the workflow's selection logic, this op deploys via the
house helper -- the same `deploy_lambda()` other ops use -- so the
create either happens or throws where I can read it.

TRAP BANKED (new): a green deploy-lambdas run is NOT proof a NEW
function exists. The workflow's per-Lambda loop swallows failures and
the job still reports success, so for a first-time deploy the only
proof is `get-function` or an invoke. Verify creation, never infer it.

Also carries the smoke=False guard from the ops-4922 trap: this engine
does ~30 paced FRED calls plus a BIS pull, which outlives a synchronous
smoke test and would time the op out while the Lambda ran on fine.
Deploy, then invoke explicitly with a long read timeout.

Then the full 8-gate acceptance from 4929.
"""
import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda, function_exists  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-usd-funding"
OUT_KEY = "data/usd-funding.json"
MARKER = "usd-funding-panel-v2"
UA = "JustHodl ops4935 edge-acceptance"
SRC = ROOT / "aws" / "lambdas" / FN / "source"
CFG = json.loads((ROOT / "aws" / "lambdas" / FN / "config.json").read_text())

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=440,
                                 retries={"max_attempts": 0}))


def num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def edge(url, cap=400000):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read(cap).decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        return "%s:%s" % (type(e).__name__, str(e)[:70]), ""


def main():
    t0 = datetime.now(timezone.utc)
    fails, notes = [], []
    with report("4935-usd-funding-bisfix") as r:
        r.heading("ops 4935 — justhodl-usd-funding deploy (in-ops) + accept")

        # ── env: inherit the FRED key from the donor, explicitly ──────
        r.section("0. Env inheritance")
        env = dict(CFG.get("env") or {})
        ie = CFG.get("inherit_env") or {}
        donor = ie.get("from_function")
        for k in (ie.get("keys") or []):
            try:
                denv = (lam.get_function_configuration(FunctionName=donor)
                        .get("Environment") or {}).get("Variables") or {}
                if denv.get(k):
                    env[k] = denv[k]
                    r.ok("inherited %s from %s (len %d)"
                         % (k, donor, len(denv[k])))
                else:
                    r.fail("%s absent on donor %s" % (k, donor))
                    fails.append("env:%s" % k)
            except Exception as e:  # noqa: BLE001
                r.fail("donor %s unreadable: %s" % (donor, str(e)[:90]))
                fails.append("env:donor")
        if not env.get("FRED_API_KEY"):
            r.fail("FRED_API_KEY missing — engine cannot run")
            fails.append("env:fred")

        r.section("0b. Pre-state")
        existed = function_exists(FN)
        r.kv(function_existed_before=existed)

        # ── deploy ───────────────────────────────────────────────────
        try:
            deploy_lambda(
                report=r, function_name=FN, source_dir=SRC, env_vars=env,
                eb_rule_name=(CFG.get("schedule") or {}).get("name"),
                eb_schedule=(CFG.get("schedule") or {}).get("expression"),
                timeout=int(CFG.get("timeout", 420)),
                memory=int(CFG.get("memory", 1024)),
                description=CFG.get("description", "")[:250],
                create_function_url=False,
                smoke=False,
            )
        except Exception as e:  # noqa: BLE001
            r.fail("deploy_lambda raised: %s: %s"
                   % (type(e).__name__, str(e)[:200]))
            fails.append("deploy")

        r.section("0c. Post-state — creation VERIFIED, not inferred")
        now_exists = function_exists(FN)
        r.kv(function_exists_after=now_exists)
        if not now_exists:
            r.fail("function STILL absent after deploy_lambda")
            fails.append("create")
            r.kv(verdict="RED")
            return

        # ── in-Lambda probe: diagnose the 400 where it actually lives ──
        r.section("0d. NY Fed request-shape ladder, FROM INSIDE LAMBDA")
        try:
            pr = lam.invoke(FunctionName=FN,
                            InvocationType="RequestResponse",
                            Payload=json.dumps({"probe": "nyfed"}).encode())
            pj = json.loads(json.loads(pr["Payload"].read())["body"])
            for rid, dg in (pj.get("diag") or {}).items():
                r.log("  %-5s shape_used=%-10s rows=%s"
                      % (rid, dg.get("shape_used"), dg.get("rows")))
                for a in (dg.get("attempts") or []):
                    r.log("       %-12s %s"
                          % (a.get("shape"),
                             ("ok rows=%s" % a.get("rows")) if a.get("ok")
                             else ("FAIL %s" % a.get("error"))))
        except Exception as e:  # noqa: BLE001
            r.warn("probe invoke failed: %s" % str(e)[:160])

        # ── invoke ───────────────────────────────────────────────────
        r.section("1. Invoke")
        try:
            resp = lam.invoke(FunctionName=FN,
                              InvocationType="RequestResponse", Payload=b"{}")
            body = json.loads(resp["Payload"].read())
            r.kv(status_code=resp.get("StatusCode"),
                 fn_error=resp.get("FunctionError", "none"),
                 body=str(body)[:220])
            if resp.get("FunctionError"):
                fails.append("invoke:%s" % resp["FunctionError"])
        except Exception as e:  # noqa: BLE001
            r.fail("invoke failed: %s" % str(e)[:200])
            fails.append("invoke")

        r.section("2. Live payload")
        try:
            d = json.loads(s3.get_object(Bucket=B,
                                         Key=OUT_KEY)["Body"].read())
        except Exception as e:  # noqa: BLE001
            r.fail("cannot read %s: %s" % (OUT_KEY, str(e)[:140]))
            r.kv(verdict="RED")
            return
        r.kv(version=d.get("version"), status=d.get("status"),
             generated=d.get("generated", "")[:19],
             errors=len(d.get("errors") or []))
        if d.get("errors"):
            r.warn("collector errors: %s" % " · ".join(d["errors"][:10]))

        rr = d.get("reference_rates") or {}
        sp = d.get("spreads_vs_iorb") or {}
        r.section("G1 — TGCR/BGCR direct (absent from FRED entirely)")
        for rid in ("sofr", "tgcr", "bgcr", "effr", "obfr"):
            m = rr.get(rid) or {}
            if not m.get("ok"):
                r.fail("%s NOT collected: %s" % (rid, m.get("error", "?")))
                fails.append("rate:%s" % rid)
                continue
            s = sp.get(rid) or {}
            r.log("  %-5s %7.4f%%  vol %-9s IQR %-6s fan %-6s −IORB %-8s "
                  "z %-7s %s"
                  % (m["label"], m["rate"],
                     ("$%.0fbn" % m["volume_bn"]) if num(m.get("volume_bn"))
                     else "n/a", m.get("iqr_bp"), m.get("fan_bp"),
                     ("%.1fbp" % s["value_bp"]) if num(s.get("value_bp"))
                     else "n/a", s.get("z"), m.get("date")))
        for rid in ("tgcr", "bgcr"):
            if not num((rr.get(rid) or {}).get("volume_bn")):
                r.fail("G0 field: %s.volume_bn not numeric" % rid)
                fails.append("g0:%s" % rid)

        r.section("G2 — H.8 bank wholesale funding")
        for k, m in (d.get("bank_wholesale_funding") or {}).items():
            if not num(m.get("value")):
                r.fail("H.8 %s missing (%s)" % (k, m.get("error", "?")))
                fails.append("h8:%s" % k)
            else:
                r.log("  %-34s %14s %-4s 13wΔ %-10s z %-7s %s [%s]"
                      % (m["label"], "{:,.0f}".format(m["value"]),
                         m.get("unit", ""), m.get("chg_13w"),
                         m.get("z_13w_chg"), m.get("date"),
                         m.get("series_id")))

        cp = d.get("commercial_paper") or {}
        r.section("G3 — CP quantities + tenor grid")
        for k, m in (cp.get("outstandings") or {}).items():
            if not num(m.get("value")):
                r.fail("CP outstanding %s missing" % k)
                fails.append("cp:%s" % k)
            else:
                r.log("  %-34s $%9.1fbn  4wΔ %-9s 13wΔ %-9s (%s%%) %s"
                      % (m["label"], m["value"], m.get("chg_4w"),
                         m.get("chg_13w"), m.get("pct_13w"), m.get("date")))
        r.kv(abcp_share_pct=cp.get("abcp_share_pct"),
             quality_spread_30d_bp=cp.get("quality_spread_30d_bp"))
        rates = cp.get("rates") or {}
        live = sum(1 for v in rates.values() if num(v.get("rate")))
        r.kv(cp_cells=len(rates), cp_cells_live=live)
        if live < 10:
            r.fail("only %d/13 CP rate cells live" % live)
            fails.append("cp:grid")
        for k, v in sorted(rates.items()):
            if num(v.get("rate")):
                r.log("  %-22s %-4s %7.3f%%  −SOFR %-9s %s"
                      % (v.get("tier"), v.get("tenor"), v["rate"],
                         v.get("spread_to_sofr_bp"), v.get("date")))

        t = d.get("sofr_term_structure") or {}
        r.section("G4 — SOFR term structure")
        for k in ("sofr_30d_avg", "sofr_90d_avg", "sofr_180d_avg",
                  "sofr_index"):
            m = t.get(k) or {}
            if not num(m.get("value")):
                r.fail("term %s missing (%s)" % (k, m.get("error", "?")))
                fails.append("term:%s" % k)
            else:
                r.log("  %-26s %14.6f  %s"
                      % (m["label"], m["value"], m.get("date")))
        r.kv(term_slope_bp=t.get("term_slope_bp"))

        b = d.get("bis_lbs_usd") or {}
        r.section("G5 — BIS LBS USD cross-border positions")
        r.kv(bis_ambiguous=b.get("ambiguous"),
             claims_liab_ratio=b.get("claims_liab_ratio"),
             key_pinned=b.get("key_used"))
        if b.get("ambiguous"):
            r.fail("BIS AMBIGUOUS — nothing published: %s"
                   % b.get("ambiguous_note"))
            fails.append("bis:ambiguous")
        if b.get("ok"):
            pos = b.get("positions") or {}
            c = (pos.get("claims") or {}).get("latest_usd_bn")
            li = (pos.get("liabilities") or {}).get("latest_usd_bn")
            ratio = b.get("claims_liab_ratio")
            if not (ratio and 0.2 <= ratio <= 5.0):
                r.fail("two-sided ratio %s outside band" % ratio)
                fails.append("bis:ratio")
            if not (c and c > 5000):
                r.fail("claims %s implausibly small for the LBS aggregate"
                       % c)
                fails.append("bis:magnitude")
            r.kv(claims_usd_bn=(pos.get("claims") or {}).get("latest_usd_bn"),
                 liabilities_usd_bn=(pos.get("liabilities") or {})
                 .get("latest_usd_bn"),
                 net_usd_bn=b.get("net_usd_bn"),
                 period=(pos.get("claims") or {}).get("latest_period"),
                 key_used=b.get("key_used"))
            r.log("  aggregation: %s" % b.get("aggregation"))
        else:
            r.warn("BIS aggregate NOT resolved — no fabricated figure. "
                   "attempts=%s" % json.dumps(b.get("attempts") or [])[:500])
            notes.append("bis unresolved")

        z = d.get("stress_z") or {}
        r.section("G6 — z-score composite")
        r.kv(sum_z=z.get("sum_z"), mean_z=z.get("mean_z"),
             reading=z.get("reading"), legs=len(z.get("legs") or []),
             missing=",".join(z.get("legs_missing") or []) or "none")
        for lg in (z.get("legs") or []):
            r.log("  %-30s z %+0.2f" % (lg["leg"], lg["z"]))
        if len(z.get("legs") or []) < 3:
            r.fail("composite has only %d legs" % len(z.get("legs") or []))
            fails.append("z:legs")

        r.section("G7 — declared gaps carry no fabricated value")
        ne = d.get("not_entitled") or []
        r.kv(declared_gaps=len(ne),
             ids=",".join(x.get("id", "?") for x in ne))
        for g in ne:
            if "value" in g:
                r.fail("declared gap %s carries a value" % g.get("id"))
                fails.append("gap:%s" % g.get("id"))

        r.section("G8 — edge acceptance (explicit UA)")
        st, txt = edge("https://justhodl.ai/eurodollar.html")
        r.kv(page_status=st, marker_present=(MARKER in txt), bytes=len(txt))
        if not (st == 200 and MARKER in txt):
            notes.append("edge page pending")
        st2, txt2 = edge("https://justhodl.ai/data/usd-funding.json")
        r.kv(feed_status=st2, feed_bytes=len(txt2),
             feed_ok=('"reference_rates"' in txt2))
        if not (st2 == 200 and '"reference_rates"' in txt2):
            notes.append("edge feed pending (CDN)")

        verdict = "GREEN" if not fails else "RED"
        r.section("Verdict")
        if fails:
            r.fail("FAILED GATES: %s" % ", ".join(fails))
        else:
            r.ok("all gates green" + (" (notes: %s)" % ", ".join(notes)
                                      if notes else ""))
        (ROOT / "aws" / "ops" / "reports" / "4935.json").write_text(
            json.dumps({"verdict": verdict, "fails": fails, "notes": notes,
                        "status": d.get("status"), "errors": d.get("errors"),
                        "sum_z": z.get("sum_z"), "reading": z.get("reading"),
                        "tgcr": rr.get("tgcr"), "bgcr": rr.get("bgcr"),
                        "bis": b, "abcp_share_pct": cp.get("abcp_share_pct"),
                        "quality_spread_30d_bp":
                            cp.get("quality_spread_30d_bp")},
                       indent=1, default=str))
        r.kv(verdict=verdict,
             duration_s=int((datetime.now(timezone.utc) - t0)
                            .total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
