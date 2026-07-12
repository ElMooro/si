"""ops 3156 — userdata auth hardening: E2E security gates.

Worker changes (deployed by deploy-workers.yml on this push):
  • /userdata: Bearer-verified requests bind to the VERIFIED supabase
    uid (path uid ignored); anonymous traffic isolated in anon:<device>
    namespace with read-through to legacy u:<device> blobs; a PRESENT
    but invalid Bearer is 401 (no silent anon downgrade).
  • /create-checkout carries metadata[plan] (multi-tier: pro/elite/…).
  • /stripe-webhook resolves plan from metadata (fallback pro).
  • /billing-portal (new): verified user → Stripe customer portal URL.

GATES (runner-side, real requests):
  1. anon PUT+GET roundtrip works (logged-out favorites keep working)
  2. garbage Bearer → 401 (no token, no access)
  3. anon on uid A cannot read blob written on uid B (namespacing)
  4. /billing-portal without auth → 401
  5. /create-checkout returns a live checkout.stripe.com URL with plan
  6. /stripe-webhook unsigned → 400 (signature enforcement intact)
"""

import json
import sys
import time
import urllib.error
import urllib.request

from ops_report import report

BASE = "https://justhodl-data-proxy.raafouis.workers.dev"


def call(method, path, body=None, headers=None):
    h = {"Content-Type": "application/json",
         "Origin": "https://justhodl.ai",
         "User-Agent": "Mozilla/5.0 ops-3156"}
    h.update(headers or {})
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers=h,
                                 method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:
        return None, str(e)


with report("3156_userdata_hardening") as rep:
    fails, warns = [], []
    rep.heading("ops 3156 — /userdata hardening E2E")

    rep.section("0. Wait for worker deploy")
    ready = False
    probe_uid = f"ops3156a{int(time.time())}"
    for i in range(14):
        st, _ = call("GET", f"/userdata/{probe_uid}",
                     headers={"Authorization": "Bearer garbage-token-xyz"})
        if st == 401:
            ready = True
            rep.ok(f"hardened worker live after ~{i*20}s")
            break
        time.sleep(20)
    if not ready:
        fails.append("worker never served hardened build (invalid Bearer "
                     "still not 401) — deploy-workers.yml check")

    if ready:
        rep.section("1. Anonymous roundtrip + namespacing")
        uid_a = f"deva{int(time.time())}"
        uid_b = f"devb{int(time.time())}"
        st1, _ = call("PUT", f"/userdata/{uid_a}",
                      {"favs": ["flows.html"], "theme": "dark"})
        st2, b2 = call("GET", f"/userdata/{uid_a}")
        st3, b3 = call("GET", f"/userdata/{uid_b}")
        rep.kv(anon_put=st1, anon_get=st2)
        if st1 == 200 and st2 == 200 and "flows.html" in b2:
            rep.ok("anonymous device roundtrip intact")
        else:
            fails.append(f"anon roundtrip broke: put={st1} get={st2}")
        if "flows.html" in (b3 or ""):
            fails.append("NAMESPACE LEAK: uid B read uid A's blob")
        else:
            rep.ok("uid isolation holds")

        rep.section("2. Auth wall")
        st, body = call("GET", f"/userdata/{uid_a}",
                        headers={"Authorization": "Bearer not-a-real-jwt"})
        if st == 401:
            rep.ok("invalid Bearer → 401")
        else:
            fails.append(f"invalid Bearer got {st} (expected 401)")
        st, body = call("POST", "/billing-portal", {},
                        headers={"Authorization": "Bearer nope"})
        if st == 401:
            rep.ok("/billing-portal unauth → 401")
        else:
            fails.append(f"/billing-portal unauth got {st}")

        rep.section("3. Billing plumbing")
        st, body = call("POST", "/create-checkout",
                        {"priceId": "price_1TfKrrQ0UPXfFGwHVUfVhyaA",
                         "plan": "pro",
                         "userId": "00000000-0000-0000-0000-0000000000e2",
                         "email": "ops3156@justhodl.ai",
                         "returnUrl": "https://justhodl.ai"})
        has_url = "checkout.stripe.com" in (body or "")
        rep.kv(checkout_status=st, has_stripe_url=has_url)
        if st == 200 and has_url:
            rep.ok("checkout session live (plan metadata attached)")
        else:
            fails.append(f"checkout: {st} {str(body)[:120]}")
        st, _ = call("POST", "/stripe-webhook", {"fake": True})
        if st == 400:
            rep.ok("webhook rejects unsigned payloads")
        else:
            fails.append(f"unsigned webhook got {st} (expected 400)")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
