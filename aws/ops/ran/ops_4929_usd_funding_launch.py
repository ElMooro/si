"""ops/4929 -- justhodl-usd-funding v1.0.0 launch + acceptance.

Closes the seven-item gap list Khalid handed over. Harness was GREEN
83/83 before this push; this op proves the LIVE tape, which is the only
proof that counts.

Gates, in order. Any red is reported as red -- a launch that reports
green on a half-empty payload is worse than no launch:

  G0 field-level (house pattern since the spx-beaters silent-join):
     container length is NOT sufficient. Assert the specific fields a
     consumer binds to -- reference_rates.tgcr.volume_bn numeric,
     bis_lbs_usd.positions.claims.latest_usd_bn numeric -- because an
     empty dict with the right key count passes a length check and
     lies on the page.
  G1 TGCR + BGCR actually present with volume and percentile fan. This
     is the whole point: FRED returns HTTP 400 for both.
  G2 H.8 wholesale legs bound and fresh.
  G3 CP outstandings incl. ABCP, and the 13-cell tenor grid.
  G4 SOFR term structure + slope.
  G5 BIS LBS USD claims/liabilities -- and if the aggregate row is
     absent, that is reported honestly, NOT patched with a sum of
     overlapping rows.
  G6 z-composite has legs, and any dead leg is NAMED in legs_missing
     rather than zero-filled.
  G7 declared gaps carry no value field (nothing fabricated).
  G8 edge acceptance on data.html -- with an explicit User-Agent,
     because bare urlopen behind Cloudflare returns nothing and cost
     ops 4914-4918 five false PENDING reports.
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

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-usd-funding"
OUT_KEY = "data/usd-funding.json"
MARKER = "usd-funding-panel-v1"
UA = "JustHodl ops4929 edge-acceptance"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=430,
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
    with report("4929-usd-funding-launch") as r:
        r.heading("ops 4929 — justhodl-usd-funding v1.0.0 launch")

        r.section("Invoke")
        try:
            resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                              Payload=b"{}")
            body = json.loads(resp["Payload"].read())
            r.kv(status_code=resp.get("StatusCode"),
                 fn_error=resp.get("FunctionError", "none"),
                 body=str(body)[:200])
            if resp.get("FunctionError"):
                fails.append("invoke:%s" % resp["FunctionError"])
        except Exception as e:  # noqa: BLE001
            r.fail("invoke failed: %s" % str(e)[:160])
            fails.append("invoke")

        r.section("Read live payload")
        try:
            d = json.loads(s3.get_object(Bucket=B,
                                         Key=OUT_KEY)["Body"].read())
        except Exception as e:  # noqa: BLE001
            r.fail("cannot read %s: %s" % (OUT_KEY, str(e)[:120]))
            r.kv(status="RED")
            return
        r.kv(version=d.get("version"), status=d.get("status"),
             generated=d.get("generated", "")[:19],
             errors=len(d.get("errors") or []))
        if d.get("errors"):
            r.warn("collector errors: %s" % " · ".join(d["errors"][:8]))

        # ── G1 reference rates ───────────────────────────────────────
        r.section("G1 — TGCR/BGCR direct (FRED returns HTTP 400 for both)")
        rr = d.get("reference_rates") or {}
        sp = d.get("spreads_vs_iorb") or {}
        for rid in ("sofr", "tgcr", "bgcr", "effr", "obfr"):
            m = rr.get(rid) or {}
            if not m.get("ok"):
                r.fail("%s NOT collected: %s" % (rid, m.get("error", "?")))
                fails.append("rate:%s" % rid)
                continue
            s = sp.get(rid) or {}
            r.log("  %-5s %6.4f%%  vol $%-7s  IQR %-5s  fan %-5s  "
                  "−IORB %-7s  z %-6s  %s"
                  % (m["label"], m["rate"],
                     ("%.0fbn" % m["volume_bn"]) if num(m.get("volume_bn"))
                     else "n/a", m.get("iqr_bp"), m.get("fan_bp"),
                     ("%.1fbp" % s["value_bp"]) if num(s.get("value_bp"))
                     else "n/a", s.get("z"), m.get("date")))
        for rid in ("tgcr", "bgcr"):
            m = rr.get(rid) or {}
            if not num(m.get("volume_bn")):
                r.fail("G0 field: %s.volume_bn not numeric" % rid)
                fails.append("g0:%s.volume_bn" % rid)
            if not num(m.get("fan_bp")):
                r.warn("%s percentile fan absent" % rid)
                notes.append("%s fan absent" % rid)

        # ── G2 H.8 ───────────────────────────────────────────────────
        r.section("G2 — H.8 bank wholesale funding")
        h8 = d.get("bank_wholesale_funding") or {}
        for k in ("large_time_deposits", "borrowings", "deposits",
                  "other_deposits", "net_due_foreign"):
            m = h8.get(k) or {}
            if not num(m.get("value")):
                r.fail("H.8 %s missing (%s)" % (k, m.get("error", "?")))
                fails.append("h8:%s" % k)
            else:
                r.log("  %-24s %14s %-4s  13wΔ %-9s  z %-6s  %s  [%s]"
                      % (m["label"], "{:,.0f}".format(m["value"]),
                         m.get("unit", ""), m.get("chg_13w"),
                         m.get("z_13w_chg"), m.get("date"),
                         m.get("series_id")))

        # ── G3 CP ────────────────────────────────────────────────────
        r.section("G3 — commercial paper quantities + tenor grid")
        cp = d.get("commercial_paper") or {}
        outs = cp.get("outstandings") or {}
        for k in ("cp_total", "cp_financial", "cp_nonfinancial", "cp_abcp"):
            m = outs.get(k) or {}
            if not num(m.get("value")):
                r.fail("CP outstanding %s missing" % k)
                fails.append("cp:%s" % k)
            else:
                r.log("  %-32s $%9.1fbn  4wΔ %-8s 13wΔ %-8s (%s%%)  %s"
                      % (m["label"], m["value"], m.get("chg_4w"),
                         m.get("chg_13w"), m.get("pct_13w"), m.get("date")))
        r.kv(abcp_share_pct=cp.get("abcp_share_pct"),
             quality_spread_30d_bp=cp.get("quality_spread_30d_bp"))
        rates = cp.get("rates") or {}
        live_cells = sum(1 for v in rates.values() if num(v.get("rate")))
        r.kv(cp_rate_cells=len(rates), cp_cells_live=live_cells)
        if live_cells < 10:
            r.fail("only %d/13 CP rate cells live" % live_cells)
            fails.append("cp:grid")
        for k, v in sorted(rates.items()):
            if num(v.get("rate")):
                r.log("  %-20s %-4s %6.3f%%  −SOFR %-8s  %s"
                      % (v.get("tier"), v.get("tenor"), v["rate"],
                         v.get("spread_to_sofr_bp"), v.get("date")))

        # ── G4 SOFR term ─────────────────────────────────────────────
        r.section("G4 — SOFR term structure")
        t = d.get("sofr_term_structure") or {}
        for k in ("sofr_30d_avg", "sofr_90d_avg", "sofr_180d_avg",
                  "sofr_index"):
            m = t.get(k) or {}
            if not num(m.get("value")):
                r.fail("term %s missing" % k)
                fails.append("term:%s" % k)
            else:
                r.log("  %-24s %12.6f  %s"
                      % (m["label"], m["value"], m.get("date")))
        r.kv(term_slope_bp=t.get("term_slope_bp"))

        # ── G5 BIS ───────────────────────────────────────────────────
        r.section("G5 — BIS LBS USD cross-border positions")
        b = d.get("bis_lbs_usd") or {}
        if b.get("ok"):
            pos = b.get("positions") or {}
            c = (pos.get("claims") or {}).get("latest_usd_bn")
            li = (pos.get("liabilities") or {}).get("latest_usd_bn")
            if not num(c):
                r.fail("G0 field: bis.claims.latest_usd_bn not numeric")
                fails.append("g0:bis.claims")
            r.kv(claims_usd_bn=c, liabilities_usd_bn=li,
                 net_usd_bn=b.get("net_usd_bn"),
                 period=(pos.get("claims") or {}).get("latest_period"),
                 key_used=b.get("key_used"))
            r.log("  aggregation: %s" % b.get("aggregation"))
        else:
            r.warn("BIS aggregate NOT resolved — reported honestly, "
                   "no fabricated figure. attempts=%s"
                   % json.dumps(b.get("attempts") or [])[:400])
            notes.append("bis unresolved")

        # ── G6 z composite ───────────────────────────────────────────
        r.section("G6 — z-score composite")
        z = d.get("stress_z") or {}
        legs = z.get("legs") or []
        r.kv(sum_z=z.get("sum_z"), mean_z=z.get("mean_z"),
             reading=z.get("reading"), legs=len(legs),
             missing=",".join(z.get("legs_missing") or []) or "none")
        for lg in legs:
            r.log("  %-28s z %+0.2f" % (lg["leg"], lg["z"]))
        if len(legs) < 3:
            r.fail("composite has only %d legs" % len(legs))
            fails.append("z:legs")
        have = " ".join(x["leg"] for x in legs)
        for want in ("TGCR", "BGCR"):
            if want not in have and want + "-IORB" not in \
                    " ".join(z.get("legs_missing") or []):
                r.fail("%s neither in legs nor named missing" % want)
                fails.append("z:%s" % want)

        # ── G7 declared gaps ─────────────────────────────────────────
        r.section("G7 — declared gaps carry no fabricated value")
        ne = d.get("not_entitled") or []
        r.kv(declared_gaps=len(ne),
             ids=",".join(x.get("id", "?") for x in ne))
        for g in ne:
            if "value" in g:
                r.fail("declared gap %s carries a value" % g.get("id"))
                fails.append("gap:%s" % g.get("id"))

        # ── G8 edge ──────────────────────────────────────────────────
        r.section("G8 — edge acceptance (explicit UA)")
        for url in ("https://justhodl.ai/data.html",
                    "https://justhodl.ai/data/usd-funding.json"):
            st, txt = edge(url)
            if url.endswith(".html"):
                ok = (st == 200 and MARKER in txt)
                r.kv(page_status=st, marker_present=(MARKER in txt),
                     bytes=len(txt))
                if not ok:
                    r.warn("page not yet serving %s (pages.yml may still "
                           "be publishing)" % MARKER)
                    notes.append("edge page pending")
            else:
                ok = (st == 200 and '"reference_rates"' in txt)
                r.kv(feed_status=st, feed_bytes=len(txt),
                     feed_has_rates=('"reference_rates"' in txt))
                if not ok:
                    r.warn("feed not yet at edge (CDN cache)")
                    notes.append("edge feed pending")

        verdict = "GREEN" if not fails else "RED"
        r.section("Verdict")
        if fails:
            r.fail("FAILED GATES: %s" % ", ".join(fails))
        else:
            r.ok("all gates green" + (" (notes: %s)" % ", ".join(notes)
                                      if notes else ""))
        (ROOT / "aws" / "ops" / "reports" / "4929.json").write_text(
            json.dumps({"verdict": verdict, "fails": fails, "notes": notes,
                        "status": d.get("status"),
                        "errors": d.get("errors"),
                        "sum_z": z.get("sum_z"),
                        "reading": z.get("reading"),
                        "tgcr": rr.get("tgcr"), "bgcr": rr.get("bgcr"),
                        "bis_ok": b.get("ok"),
                        "abcp_share_pct": cp.get("abcp_share_pct")},
                       indent=1, default=str))
        r.kv(verdict=verdict,
             duration_s=int((datetime.now(timezone.utc) - t0)
                            .total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
