"""ops 4606 — Cloudflare token restored: prove purges work again.

The runner's CLOUDFLARE_API_TOKEN was 401ing (ops 4603/4604). Khalid's
token from the April session was re-sealed into the Actions secret via
the GitHub API (sandbox-side, sealed-box — never committed). Rerun after Khalid granted Zone > Cache Purge to the token
(first run: verify+zone green, purge 401 code 10000). This op
is the ground-truth probe: verify the token, resolve the zone, purge
the plumbing surfaces, and confirm the edge answers.
"""
import json
import os
import sys
import time
import urllib.request

from ops_report import report


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def cf(path, method="GET", data=None):
    tok = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    req = urllib.request.Request(
        "https://api.cloudflare.com/client/v4" + path,
        data=json.dumps(data).encode() if data else None, method=method,
        headers={"Authorization": "Bearer " + tok,
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as h:
            return json.loads(h.read()), None
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read()), "HTTP %s" % e.code
        except Exception:
            return None, "HTTP %s" % e.code
    except Exception as e:
        return None, str(e)[:120]


def main():
    misses = 0
    with report("4606_cf_token_probe") as r:
        r.heading("ops 4606 — Cloudflare token restored: purge probe")

        r.section("token verify")
        misses += contract(r, "env",
                           bool(os.environ.get("CLOUDFLARE_API_TOKEN")),
                           "CLOUDFLARE_API_TOKEN present in runner env")
        vj, verr = cf("/user/tokens/verify")
        vstat = ((vj or {}).get("result") or {}).get("status")
        r.kv(verify_success=(vj or {}).get("success"),
             token_status=vstat, verify_err=verr,
             verify_msgs=json.dumps((vj or {}).get("errors") or [])[:200])
        misses += contract(r, "verify",
                           bool((vj or {}).get("success"))
                           and vstat == "active",
                           "token verifies active (status=%s err=%s)"
                           % (vstat, verr))
        if not ((vj or {}).get("success") and vstat == "active"):
            r.fail("Token is DEAD at Cloudflare — Khalid must mint a "
                   "fresh one (dash.cloudflare.com/profile/api-tokens) "
                   "and paste it; I will re-seal the secret.")
            sys.exit(1)

        r.section("zone + purge")
        zj, zerr = cf("/zones?name=justhodl.ai")
        zid = (((zj or {}).get("result") or [{}])[0] or {}).get("id")
        misses += contract(r, "zone", bool(zid),
                           "zone justhodl.ai resolved (id=%s err=%s)"
                           % (zid, zerr))
        purged = False
        if zid:
            pj, perr = cf("/zones/%s/purge_cache" % zid, "POST",
                          {"files": [
                              "https://justhodl.ai/plumbing.html",
                              "https://justhodl.ai/data/"
                              "plumbing-stress.json",
                              "https://justhodl.ai/data.html",
                              "https://justhodl.ai/data/"
                              "provider-catalog.json"]})
            purged = bool((pj or {}).get("success"))
            r.kv(purge_success=purged, purge_err=perr,
                 purge_msgs=json.dumps((pj or {}).get("errors")
                                       or [])[:200])
        misses += contract(r, "purge", purged,
                           "cache purge accepted for the plumbing "
                           "surfaces")

        r.section("edge sanity")
        ok_edge = False
        for att in range(4):
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/plumbing.html?cb=%d"
                    % time.time(),
                    headers={"User-Agent": "ops-4606"})
                with urllib.request.urlopen(req, timeout=30) as h:
                    body = h.read().decode("utf-8", "replace")
                if h.status == 200 and "L0 \u2014 Repo Core" in body:
                    ok_edge = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:80]))
            time.sleep(10)
        misses += contract(r, "edge", ok_edge,
                           "post-purge edge serves plumbing.html with "
                           "the L0 card")

        r.section("verdict")
        if misses:
            r.fail("cf probe: %d red" % misses)
            sys.exit(1)
        r.ok("Cloudflare purge path RESTORED — token active, zone "
             "resolved, purge accepted, edge fresh. Every future ops "
             "purge and pages.yml post-deploy purge now works again. "
             "Note: this token was once pasted in chat (April); when "
             "convenient, roll it and hand me the new value — I will "
             "re-seal the secret the same way.")


if __name__ == "__main__":
    main()
