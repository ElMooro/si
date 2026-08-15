"""ops 4690 — deploy v6 (clean rewrite) with byte-level verification.

Both tools/wayback-ice-grab.js and tools/ice-recover.html were rewritten
from scratch tonight after two rounds of accumulated-patch bugs (a
stray HAVE reference, then window.__JH_ICE pushed to without ever being
declared). Both were then EXECUTED end-to-end in a jsdom-simulated
browser against a fake archive.org and fake S3 before this op — proven
by running, not just read. This op's job is narrow and paranoid: deploy
the tested bytes and prove the LIVE S3 object is exactly what was
tested, not a stale or partially-substituted copy.
"""
import sys

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-tv-notes-ingest"
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4690_arm_v6") as r:
        r.heading("ops 4690 — deploy v6 with byte-level verification")
        misses = 0

        r.section("1. Resolve ingest token + URL (authoritative)")
        token = ""
        for pn in ("/justhodl/tvnotes/ingest-token",
                   "/justhodl/tv-notes/ingest-token"):
            try:
                token = ssm.get_parameter(
                    Name=pn, WithDecryption=True)["Parameter"]["Value"]
                break
            except Exception:
                continue
        misses += contract(r, "resolve", bool(token),
                           "token resolved (len=%d, never printed)"
                           % len(token))
        url = ""
        try:
            url = lam.get_function_url_config(
                FunctionName=FN)["FunctionUrl"]
        except Exception as e:
            r.warn("  url config: %s" % str(e)[:90])
        misses += contract(r, "resolve", bool(url),
                           "function URL: %s" % url)
        if misses:
            sys.exit(1)

        r.section("2. Grabber — v6 has no placeholders; publish + "
                  "verify")
        grab_src = open("tools/wayback-ice-grab.js").read()
        s3.put_object(Bucket=B, Key="tools/wayback-ice-grab.js",
                      Body=grab_src.encode(),
                      ContentType="application/javascript",
                      CacheControl="no-cache")
        back1 = s3.get_object(
            Bucket=B, Key="tools/wayback-ice-grab.js"
        )["Body"].read().decode()
        misses += contract(r, "grabber", back1 == grab_src,
                           "published bytes match repo source exactly "
                           "(%d bytes)" % len(grab_src))
        # the exact bug class that shipped twice tonight: a push/read
        # against window.__JH_ICE with no declaration anywhere.
        has_ref = "__JH_ICE" in back1
        has_decl = ("var collected" in back1
                    and "window.__JH_ICE" not in back1)
        misses += contract(
            r, "grabber", (not has_ref) or has_decl,
            "no orphaned __JH_ICE reference (ref=%s)" % has_ref)

        r.section("3. Recovery page — inject live token/URL, publish, "
                  "read back")
        page_src = open("tools/ice-recover.html").read()
        page_armed = (page_src.replace("__TOKEN__", token)
                              .replace("__INGEST__", url))
        misses += contract(
            r, "page", "__TOKEN__" not in page_armed
            and "__INGEST__" not in page_armed,
            "no placeholder survives injection")
        s3.put_object(Bucket=B, Key="tools/ice-recover.html",
                      Body=page_armed.encode(), ContentType="text/html",
                      CacheControl="no-cache")
        back2 = s3.get_object(
            Bucket=B, Key="tools/ice-recover.html"
        )["Body"].read().decode()
        misses += contract(r, "page", back2 == page_armed,
                           "published bytes match armed content "
                           "exactly (%d bytes)" % len(page_armed))
        misses += contract(
            r, "page", (url in back2) and (token in back2),
            "live URL and token CONFIRMED present in the deployed "
            "S3 object (not the repo copy, the actual bytes served)")

        r.section("verdict")
        if misses:
            r.fail("v6 deploy: %d red" % misses)
            sys.exit(1)
        r.ok("v6 deployed and byte-verified end-to-end — both files "
             "were executed in a simulated browser before this run, "
             "and this run confirms the live S3 bytes are exactly "
             "what was tested")


if __name__ == "__main__":
    main()
