"""ops_4319 v2 -- true-path audit: docs live at equity-research/{T}.json
(CDN worker front, lambda-url writer). Scan the prefix for the ~$430
doc, diff stored quote/PE vs LIVE FMP, and grep the repo for the
engine that writes this prefix -- then its quote/pe/ownership/cache
lines are the indictment."""
import json, re, subprocess, sys, urllib.request
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
B = "justhodl-dashboard-live"

def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=25).stdout[:1400]
    except Exception as e:
        return "sh fail: %s" % e
fails = []
with report("4319_page_audit") as r:
    r.heading("ops 4319 v2 -- the real research docs, audited")
    keys = []
    tok = None
    while True:
        kw = {"Bucket": B, "Prefix": "equity-research/",
              "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        resp = s3.list_objects_v2(**kw)
        keys += [(o["Key"], o["LastModified"], o["Size"])
                 for o in resp.get("Contents", [])]
        tok = resp.get("NextContinuationToken")
        if not tok:
            break
    keys.sort(key=lambda x: x[1], reverse=True)
    r.log("prefix holds %d docs; 6 freshest: %s"
          % (len(keys), [(k.split("/")[-1],
                          str(lm)[:16]) for k, lm, _ in keys[:6]]))
    hit = None
    for k, lm, _ in keys[:120]:
        try:
            doc = json.loads(s3.get_object(
                Bucket=B, Key=k)["Body"].read())
            p = ((doc.get("quote") or {}).get("price"))
            if p and 425 <= p <= 435:
                hit = (k, lm, doc)
                break
        except Exception:
            continue
    if not hit:
        fails.append("no doc with quote.price ~430 among 120 "
                     "freshest")
    else:
        k, lm, doc = hit
        tk = doc.get("ticker") or k.split("/")[-1].split(".")[0]
        q = doc.get("quote") or {}
        v = doc.get("valuation") or {}
        r.ok("MISPRICED DOC: %s (S3 LastModified %s, doc "
             "generated_at=%s)" % (tk, lm, doc.get("generated_at")
                                   or doc.get("as_of")))
        r.log("stored quote: %s" % json.dumps(q)[:340])
        r.log("valuation keys: %s | pe_ttm=%s pe=%s"
              % (list(v)[:16], v.get("pe_ttm"), v.get("pe")))
        own = (doc.get("ownership") or doc.get("institutional")
               or {})
        r.log("ownership: %s" % json.dumps(own)[:340])
        kd = lam.get_function_configuration(
            FunctionName="justhodl-commodity-curves")
        env = ((kd.get("Environment") or {}).get("Variables") or {})
        fk = env.get("FMP_KEY") or env.get("FMP_API_KEY")
        lq = json.loads(urllib.request.urlopen(
            "https://financialmodelingprep.com/stable/quote"
            "?symbol=%s&apikey=%s" % (tk, fk), timeout=25).read())
        lq = lq[0] if isinstance(lq, list) and lq else lq
        r.ok("LIVE quote: price=%s day=%s-%s 52w=%s-%s"
             % (lq.get("price"), lq.get("dayLow"),
                lq.get("dayHigh"), lq.get("yearLow"),
                lq.get("yearHigh")))
        rt = json.loads(urllib.request.urlopen(
            "https://financialmodelingprep.com/stable/ratios-ttm"
            "?symbol=%s&apikey=%s" % (tk, fk), timeout=25).read())
        rt = rt[0] if isinstance(rt, list) and rt else rt
        cands = {kk: rt.get(kk) for kk in rt
                 if isinstance(rt.get(kk), (int, float))
                 and abs(rt[kk] - (v.get("pe_ttm") or -999)) < 0.15}
        r.ok("LIVE ratios: peTTM=%s | fields matching stored "
             "pe_ttm=%s -> %s"
             % (rt.get("priceToEarningsRatioTTM",
                       rt.get("peRatioTTM")),
                v.get("pe_ttm"), cands))
    r.section("writer forensics (repo-wide)")
    who = sh(["grep", "-rln", "equity-research/", "aws/lambdas/"])
    r.log("writers of the prefix:\n%s" % who)
    eng = (who.strip().splitlines() or [""])[0]
    if eng:
        for pat, lbl in ((r'"quote"', "quote build"),
                         ("pe_ttm", "pe_ttm build"),
                         ("holders", "ownership build"),
                         ("cache", "cache policy")):
            r.log("%s:\n%s" % (lbl, sh(["grep", "-n", "-B2", "-A5",
                                         pat, eng])[:1100]))
    if fails:
        for f in fails:
            r.fail(f)
        sys.exit(1)
    r.ok("AUDIT v2 COMPLETE")
