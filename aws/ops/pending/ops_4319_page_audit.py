"""ops_4319 -- full-page truth audit: identify the mispriced ticker by
scanning docs for quote.price ~430.10, diff its stored quote+PE against
LIVE FMP, and grep the writer for exactly which fields feed quote,
pe_ttm, and the ownership counts -- evidence before any fix."""
import json, subprocess, sys, urllib.request
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
B = "justhodl-dashboard-live"
def grepper(pat, path, ctx=2):
    try:
        return subprocess.run(["grep", "-n", "-B", str(ctx), "-A",
                               str(ctx + 2), pat, path],
                              capture_output=True, text=True,
                              timeout=20).stdout[:1200]
    except Exception as e:
        return "grep fail: %s" % e
fails = []
with report("4319_page_audit") as r:
    r.heading("ops 4319 -- why.html data-truth audit")
    ar = "aws/lambdas/justhodl-alpha-research/source/lambda_function.py"
    ok = grepper('OUT_KEY', ar, 0)
    r.log("keys:\n%s" % ok)
    import re
    okm = re.search(r'OUT_KEY\s*=\s*"([^"]+)"', ok)
    OUT = okm.group(1) if okm else "data/alpha-research.json"
    doc = json.loads(s3.get_object(Bucket=B, Key=OUT)["Body"].read())
    by = doc.get("by_ticker") or {}
    r.log("doc %s: %d tickers, top keys %s"
          % (OUT, len(by), list(doc)[:6]))
    hit = None
    for tk, rec in by.items():
        p = ((rec.get("quote") or {}).get("price"))
        if p and 425 <= p <= 435:
            hit = (tk, rec)
            break
    if not hit:
        fails.append("no ticker with stored price ~430 found")
    else:
        tk, rec = hit
        q = rec.get("quote") or {}
        v = rec.get("valuation") or {}
        r.ok("MISPRICED NAME IDENTIFIED: %s" % tk)
        r.log("stored quote: %s" % json.dumps(
            {k: q.get(k) for k in ("price", "change_pct", "day_low",
                                   "day_high", "year_low",
                                   "year_high", "as_of", "quote_at",
                                   "ts")})[:300])
        r.log("stored valuation keys: %s · pe_ttm=%s"
              % (list(v)[:14], v.get("pe_ttm")))
        own = rec.get("ownership") or rec.get("institutional") or {}
        r.log("ownership block keys: %s · sample: %s"
              % (list(own)[:10], json.dumps(own)[:260]))
        kd = lam.get_function_configuration(
            FunctionName="justhodl-commodity-curves")
        fk = ((kd.get("Environment") or {}).get("Variables")
              or {}).get("FMP_KEY") or ((kd.get("Environment") or
              {}).get("Variables") or {}).get("FMP_API_KEY")
        lq = json.loads(urllib.request.urlopen(
            "https://financialmodelingprep.com/stable/quote"
            "?symbol=%s&apikey=%s" % (tk, fk), timeout=25).read())
        lq = lq[0] if isinstance(lq, list) and lq else lq
        r.ok("LIVE FMP quote: price=%s chg%%=%s day=%s-%s 52w=%s-%s"
             % (lq.get("price"), lq.get("changePercentage",
                lq.get("changesPercentage")), lq.get("dayLow"),
                lq.get("dayHigh"), lq.get("yearLow"),
                lq.get("yearHigh")))
        r.log("Δ price: stored %s vs live %s -- STALE BY %s"
              % (q.get("price"), lq.get("price"),
                 "days (day-range mismatch)" if
                 abs((q.get("day_low") or 0) -
                     (lq.get("dayLow") or 0)) > 1 else "intraday"))
        try:
            rt = json.loads(urllib.request.urlopen(
                "https://financialmodelingprep.com/stable/ratios-ttm"
                "?symbol=%s&apikey=%s" % (tk, fk), timeout=25).read())
            rt = rt[0] if isinstance(rt, list) and rt else rt
            r.ok("LIVE ratios-ttm: peTTM=%s peg=%s pb=%s ps=%s"
                 % (rt.get("priceToEarningsRatioTTM",
                    rt.get("peRatioTTM")),
                    rt.get("priceToEarningsGrowthRatioTTM",
                    rt.get("pegRatioTTM")),
                    rt.get("priceToBookRatioTTM"),
                    rt.get("priceToSalesRatioTTM")))
            r.log("VERDICT pe_ttm=%s matches which live field? "
                  "(closest wins the indictment)" % v.get("pe_ttm"))
        except Exception as e:
            r.warn("ratios: %s" % str(e)[:80])
    r.section("writer forensics -- where these fields are born")
    r.log("quote build:\n%s" % grepper('"quote"', ar, 2))
    r.log("pe_ttm build:\n%s" % grepper('pe_ttm', ar, 2))
    r.log("ownership counts build:\n%s"
          % grepper('inst_buying\|holders\|inst_selling', ar, 2))
    r.log("freshness/cache policy:\n%s"
          % grepper('fresh\|ttl\|cache', ar, 1)[:900])
    r.section("page-side -- the HOLDERS tile template")
    r.log(grepper('HOLDERS', "why.html", 3)[:900])
    if fails:
        for f in fails:
            r.fail(f)
        sys.exit(1)
    r.ok("AUDIT COMPLETE -- fixes ship with this evidence attached")
