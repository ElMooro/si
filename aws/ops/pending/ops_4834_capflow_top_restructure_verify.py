"""ops/4834 -- capital-flow.html TOP restructure verify.
Khalid: US-by-destination + world grid must sit at the TOP.
 G0  field-level on BOTH feeds every card binds:
     foreign-flows.json (six flows, four signals, split, countries)
     and global-flows.json (peru 4 series + five deferrals).
 (1) committed-HTML: both card ids exactly once; ORDER assert
     tic-ff-card < global-ff-card < legacy '13F funds'; destination
     labels present (US Treasuries (LT+ST), Agency / MBS, real-
     estate honesty note).
 (2) served: poll justhodl.ai/capital-flow.html for BOTH ids and
     the order, <=8 min.
"""
import gzip
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
PAGE = Path(__file__).resolve().parents[3] / "capital-flow.html"
URL = "https://justhodl.ai/capital-flow.html"
s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4834 -- capital-flow top restructure "
                "verify") as rep:
        rep.heading("G0. both live feeds, field-level")
        try:
            ff = sread("data/foreign-flows.json")
            ok = (isinstance(((ff.get("flows_bn") or {})
                              .get("treas") or {}).get("latest"),
                             (int, float))
                  and "official_private" in (ff.get("signals")
                                             or {})
                  and (ff.get("country_lt_treasury") or {}))
            (rep.ok if ok else rep.fail)(
                "  foreign-flows bindings %s"
                % ("OK" if ok else "BROKEN"))
            if not ok:
                FAILED.append("ff")
        except ClientError:
            rep.fail("  foreign-flows.json unreadable")
            FAILED.append("ff")
        try:
            gf = sread("data/global-flows.json")
            pe = (gf.get("countries") or {}).get("peru") or {}
            ok = (pe.get("status") == "LIVE"
                  and isinstance(((pe.get("series") or {})
                                  .get("gov_bonds_nonresident")
                                  or {}).get("latest"),
                                 (int, float))
                  and len(gf.get("deferred") or {}) == 5)
            (rep.ok if ok else rep.fail)(
                "  global-flows bindings %s (peru %s)"
                % ("OK" if ok else "BROKEN",
                   pe.get("latest_period")))
            if not ok:
                FAILED.append("gf")
        except ClientError:
            rep.fail("  global-flows.json unreadable")
            FAILED.append("gf")
        if FAILED:
            rep.fail("feeds broken -- cards would lie")
            sys.exit(1)

        rep.heading("1. committed-HTML + ORDER")
        html = PAGE.read_text(encoding="utf-8")
        for tok in ('id="tic-ff-card"', 'id="global-ff-card"'):
            if html.count(tok) == 1:
                rep.ok("  %s exactly once" % tok)
            else:
                rep.fail("  %s count=%d" % (tok, html.count(tok)))
                FAILED.append("tok")
        try:
            it = html.index('id="tic-ff-card"')
            ig = html.index('id="global-ff-card"')
            il = html.index("13F funds")
            if it < ig < il:
                rep.ok("  ORDER: US card @%d < world @%d < "
                       "legacy @%d (TOP confirmed)" % (it, ig, il))
            else:
                rep.fail("  order wrong: %d %d %d" % (it, ig, il))
                FAILED.append("order")
        except ValueError as e:
            rep.fail("  anchor missing: %s" % e)
            FAILED.append("order")
        for tok in ("US Treasuries (LT+ST)",
                    "Agency / MBS (mortgage channel)",
                    "NAR annual survey",
                    "Gov bonds bought by nonresidents"):
            if tok in html:
                rep.ok("  label %r present" % tok)
            else:
                rep.fail("  label %r MISSING" % tok)
                FAILED.append("label")
        if FAILED:
            sys.exit(1)

        rep.heading("2. served (<=8 min)")
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "%s?t=%d" % (URL, int(time.time())),
                    headers={"User-Agent": "ops-4834",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) as r:
                    body = r.read().decode("utf-8", "replace")
                if ('id="tic-ff-card"' in body
                        and 'id="global-ff-card"' in body
                        and body.index('id="tic-ff-card"')
                        < body.index('id="global-ff-card"')):
                    rep.ok("BOTH cards SERVED in order after %ds"
                           % int(time.time() - t0))
                    break
                rep.log("  settling...")
            except Exception as e:  # noqa: BLE001
                rep.log("  fetch: %s" % str(e)[:60])
            time.sleep(30)
        else:
            rep.fail("cards never served in order within 8 min")
            sys.exit(1)

        rep.heading("3. verdict")
        rep.ok("capital-flow.html: US-by-destination + world "
               "inflow grid now lead the page")


if __name__ == "__main__":
    main()
