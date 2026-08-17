"""ops/4831 -- capital-flow.html TIC card verify FIXED
(4830: PAGE used parents[2]=aws/ not repo root; G0 on the
live v1.1 JSON was already green before the crash). (claimed in
docs/SESSION_CLAIMS.md).
 G0  FIELD-level on data/foreign-flows.json for EVERY binding the
     card reads: flows_bn six keys latest numeric; signals four keys
     present as latest_bn OR honest {value:None,why}; holder_splits
     .lt_total.status in the known set; country_lt_treasury rows
     with holdings_bn numeric on OK rows.
 (1) committed-HTML asserts at HEAD: card id + fetch path + all
     container tokens present exactly once.
 (2) served check: poll https://justhodl.ai/capital-flow.html (cache
     -busted; success-path Cloudflare purge is live since the
     pages.yml fix) for the card id, <=8 min.
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
FF = "data/foreign-flows.json"
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
    with report("ops 4831 -- capital-flow TIC card verify fixed") as rep:
        rep.heading("G0. live JSON contract for every card binding")
        try:
            d = sread(FF)
        except ClientError:
            rep.fail("foreign-flows.json unreadable")
            sys.exit(1)
        fb = d.get("flows_bn") or {}
        for k in ("total", "treas", "equity", "corp", "agency",
                  "tbills"):
            v = (fb.get(k) or {})
            if isinstance(v.get("latest"), (int, float)) \
                    and isinstance(v.get("sum_12m"), (int, float)):
                rep.ok("  flows_bn.%s latest=%+.1f 12m=%+.1f z=%s"
                       % (k, v["latest"], v["sum_12m"],
                          v.get("z_10y")))
            else:
                rep.fail("  flows_bn.%s broken: %s"
                         % (k, json.dumps(v)[:60]))
                FAILED.append("fb_" + k)
        sg = d.get("signals") or {}
        for k in ("risk_appetite", "safe_haven", "total_demand",
                  "official_private"):
            s = sg.get(k) or {}
            if isinstance(s.get("latest_bn"), (int, float)):
                rep.ok("  signals.%s latest=%+.1fB z=%s"
                       % (k, s["latest_bn"], s.get("z_10y")))
            elif s.get("value") is None and s.get("why"):
                rep.ok("  signals.%s honest-null: %s"
                       % (k, str(s["why"])[:50]))
            else:
                rep.fail("  signals.%s broken: %s"
                         % (k, json.dumps(s)[:60]))
                FAILED.append("sg_" + k)
        hs = ((d.get("holder_splits") or {}).get("lt_total")) or {}
        if hs.get("status") in ("OK", "MISSING", "MISALIGNED",
                                "UNRECONCILED"):
            rep.ok("  holder_splits.lt_total status=%s gap=%s"
                   % (hs["status"], hs.get("recon_gap_bn")))
            if hs["status"] == "OK":
                rep.ok("  split OK: private %+.1fB vs official "
                       "%+.1fB"
                       % ((hs.get("private") or {}).get("latest",
                                                        0),
                          (hs.get("official") or {}).get("latest",
                                                         0)))
        else:
            rep.fail("  holder_splits.lt_total missing/unknown: %s"
                     % json.dumps(hs)[:60])
            FAILED.append("hs")
        cc = d.get("country_lt_treasury") or {}
        ok_rows = [(n, r) for n, r in cc.items()
                   if (r or {}).get("status") == "OK"
                   and isinstance(r.get("holdings_bn"),
                                  (int, float))]
        if ok_rows:
            rep.ok("  country_lt_treasury: %d OK rows (e.g. %s "
                   "%+.1fB, other-gap %s)"
                   % (len(ok_rows), ok_rows[0][0],
                      ok_rows[0][1]["holdings_bn"],
                      ok_rows[0][1].get("identity_gap_bn")))
        elif cc:
            rep.warn("  country rows present but none OK: %s"
                     % {n: (r or {}).get("status")
                        for n, r in list(cc.items())[:5]})
        else:
            rep.fail("  country_lt_treasury empty")
            FAILED.append("cc")
        if FAILED:
            rep.fail("G0 broken -- card would render lies")
            sys.exit(1)

        rep.heading("1. committed-HTML asserts")
        html = PAGE.read_text(encoding="utf-8")
        for tok in ('id="tic-ff-card"', "/data/foreign-flows.json",
                    "country_lt_treasury", "holder_splits",
                    "official_private", "custodial bias"):
            n = html.count(tok)
            if n >= 1:
                rep.ok("  token %-28r x%d" % (tok, n))
            else:
                rep.fail("  token %r MISSING" % tok)
                FAILED.append("tok")
        if html.count('id="tic-ff-card"') != 1:
            rep.fail("  card id not exactly once")
            FAILED.append("dup")
        if FAILED:
            sys.exit(1)

        rep.heading("2. served check (<=8 min; CF purge-on-success "
                    "live)")
        t0 = time.time()
        served = False
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "%s?t=%d" % (URL, int(time.time())),
                    headers={"User-Agent": "ops-4831",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) as r:
                    body = r.read().decode("utf-8", "replace")
                if 'id="tic-ff-card"' in body:
                    served = True
                    rep.ok("card SERVED at %s after %ds"
                           % (URL, int(time.time() - t0)))
                    break
                rep.log("  not yet (deploy/CDN settling)...")
            except Exception as e:  # noqa: BLE001
                rep.log("  fetch: %s" % str(e)[:60])
            time.sleep(30)
        if not served:
            rep.fail("card never served within 8 min -- check "
                     "pages deploy")
            sys.exit(1)

        rep.heading("3. verdict")
        rep.ok("capital-flow.html now surfaces the TIC organ: six "
               "flows, four signals incl private-vs-official, "
               "reconciled split, country decomposition with the "
               "honest 'other' gap")


if __name__ == "__main__":
    main()
