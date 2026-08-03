"""ops_4335 -- the institutional layer sealed: family taxonomy (drift-
proof PRIME v2 = 3 families + >=4 systems), declared-prior evidence
weights, first-seen freshness decay, regime stamps, chase-guard entry
quality -- and the live Convergence Desk page."""
import json, subprocess, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4335_convergence_desk") as r:
    r.heading("ops 4335 -- the desk goes live")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-compound-aggregator"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(50):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-compound-aggregator")
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and lm >= fl:
                ok = True
                break
        except Exception:
            pass
        time.sleep(9)
    if not ok:
        fails.append("deploy floor")
    else:
        lam.invoke(FunctionName="justhodl-compound-aggregator",
                   InvocationType="RequestResponse", Payload=b"{}")
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/compound-signals.json"
        )["Body"].read())
        ranked = d.get("compound") if isinstance(
            d.get("compound"), list) else d.get("ranked") or []
        r0 = ranked[0] if ranked else {}
        r.log("ranked[0] %s: %s" % (r0.get("symbol"), json.dumps(
            {k: r0.get(k) for k in
             ("desk_score", "n_families", "families",
              "evidence_weight", "freshness", "entry_quality",
              "regime", "prime_convergence")}, default=str)[:340]))
        for k in ("desk_score", "n_families", "freshness",
                  "entry_quality", "regime", "evidence_weight"):
            if k not in r0:
                fails.append("rows lack %s" % k)
        bad = [x["symbol"] for x in ranked
               if x.get("prime_convergence") !=
               (x.get("n_systems", 0) >= 4
                and x.get("n_families", 0) == 3)]
        if bad:
            fails.append("prime-v2 inconsistent: %s" % bad[:5])
        else:
            r.ok("prime v2 (3-family) consistent across %d rows"
                 % len(ranked))
        primes = [x for x in ranked
                  if x.get("prime_convergence")]
        r.section("TODAY'S DESK BOARD (top by desk_score)")
        for x in ranked[:6]:
            r.log("%s desk=%s fam=%s/%s fresh=%s entry=%s "
                  "arch=%s"
                  % (x["symbol"], x.get("desk_score"),
                     x.get("n_families"), x.get("n_systems"),
                     x.get("freshness"), x.get("entry_quality"),
                     x.get("archetype")))
        r.log("PRIME today: %s"
              % [x["symbol"] for x in primes[:8]] or "none")
        fsn = json.loads(s3.get_object(
            Bucket=B, Key="data/compound-firstseen.json"
        )["Body"].read())
        r.ok("first-seen state: %d (symbol,system) pairs banked"
             % len(fsn))
    body = ""
    for _ in range(13):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/convergence-desk.html",
                headers={"User-Agent": "ops/4335",
                         "Cache-Control": "no-cache"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "Convergence Desk" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ("Convergence Desk", "desk_score", "families",
               "entry_quality", "chase-guard", "Methodology",
               "family_priors_v1", "PRIME"):
        if mk not in body:
            fails.append("edge missing %s" % mk)
    if "Convergence Desk" in body:
        r.ok("PAGE LIVE: https://justhodl.ai/convergence-desk.html"
             " (%d bytes)" % len(body))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4335 PASS -- the winning engine now has a desk, "
         "and the desk has discipline")
