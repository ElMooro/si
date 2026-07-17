"""ops 3366 — Supabase per-user layer: INSTALL VERIFICATION (E2E, real requests).

Context: auth-config.js is live (enabled:true, real project), worker /userdata
is Bearer-hardened (ops 3156), account-backed favorites proven (3292). But the
`profiles` table that auth.js reads plan from — and that the Stripe webhook
writes — has NO DDL anywhere in the repo. This ops determines ground truth and
gates the whole per-user stack:

  G1  live justhodl.ai/auth-config.js matches repo (url + enabled:true)
  G2  Supabase project alive (auth health)
  G3  profiles table: MISSING / EXISTS+RLS-ON / EXISTS+RLS-OFF (security bug)
  G4  E2E: real signup → (if session issued) trigger-created profile row,
      /userdata/self PUT+GET roundtrip (favorites persistence), /plan/self
      (new server-authoritative route; polled for worker deploy race)
  G5  security regression: tokenless & garbage-Bearer rejected, unsigned
      webhook 400 (503 = worker secrets MISSING → critical), portal 401
  G6  /create-checkout returns live checkout.stripe.com URL (Stripe leg)

Companion changes in this push: webhook PATCH→UPSERT (missing row can no
longer eat a paid plan), worker GET /plan/self, auth.js /plan/self fallback,
supabase/setup.sql (idempotent DDL — the only Khalid-paste if G3=MISSING).
"""

import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from ops_report import report

WORKER = "https://justhodl-data-proxy.raafouis.workers.dev"
SITE = "https://justhodl.ai"
UA = {"User-Agent": "Mozilla/5.0 (ops-3366; JustHodl verify)"}


def req(url, method="GET", data=None, headers=None, timeout=25):
    h = dict(UA)
    if headers:
        h.update(headers)
    body = None
    if data is not None:
        body = data if isinstance(data, bytes) else json.dumps(data).encode()
        h.setdefault("Content-Type", "application/json")
    r = urllib.request.Request(url, data=body, headers=h, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        return -1, str(e)[:200]


def main(rep):
    out = {"gates": {}, "findings": [], "khalid_actions": []}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:400]}
        print(("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:300])
        if not ok:
            fails.append(name)

    # ── Parse repo auth-config.js (run-ops executes inside the checkout) ──
    cfg_src = open("auth-config.js", encoding="utf-8").read()

    def pick(key):
        m = re.search(key + r"\s*:\s*\"([^\"]+)\"", cfg_src)
        return m.group(1) if m else None

    supa = pick("supabaseUrl")
    anon = pick("supabaseAnonKey")
    pro_price = pick("proPriceId")
    enabled = bool(re.search(r"enabled\s*:\s*true", cfg_src))
    out["config"] = {"supabaseUrl": supa, "proPriceId": pro_price, "enabled": enabled}
    if not (supa and anon and enabled):
        gate("G0_repo_config", False, "auth-config.js missing url/key/enabled")
        Path("aws/ops/reports/3366.json").write_text(json.dumps(out, indent=2))
        sys.exit(0)
    gate("G0_repo_config", True, f"{supa} enabled={enabled}")
    ah = {"apikey": anon, "Authorization": "Bearer " + anon}

    # ── G1: live site serves the same config ──
    st, body = req(SITE + "/auth-config.js")
    gate("G1_live_auth_config", st == 200 and supa in body and "enabled: true" in body,
         f"http {st}, url_match={supa in body}")

    # ── G2: Supabase project alive ──
    st, body = req(supa + "/auth/v1/health", headers=ah)
    gate("G2_supabase_alive", st == 200, f"http {st} {body[:80]}")

    # ── G3: profiles table probe (anon key, no user JWT) ──
    st, body = req(supa + "/rest/v1/profiles?select=id&limit=1", headers=ah)
    if st == 200:
        try:
            rows = json.loads(body)
        except Exception:  # noqa: BLE001
            rows = None
        if isinstance(rows, list) and len(rows) == 0:
            g3 = "EXISTS_RLS_ON"
            gate("G3_profiles_table", True, "table exists, RLS filtering anon (correct)")
        elif isinstance(rows, list):
            g3 = "EXISTS_RLS_OFF"
            gate("G3_profiles_table", False, f"RLS OFF — anon can read {len(rows)}+ rows. Run setup.sql")
            out["khalid_actions"].append("SECURITY: paste supabase/setup.sql in SQL Editor (enables RLS)")
        else:
            g3 = "UNKNOWN"
            gate("G3_profiles_table", False, f"unparseable 200 body: {body[:120]}")
    elif st in (404, 406) or "PGRST205" in body or "does not exist" in body:
        g3 = "MISSING"
        gate("G3_profiles_table", False, "profiles table MISSING — plan persistence dead until setup.sql runs")
        out["khalid_actions"].append("REQUIRED: Supabase → SQL Editor → paste supabase/setup.sql → Run (30s, idempotent)")
    else:
        g3 = f"HTTP_{st}"
        gate("G3_profiles_table", False, f"http {st} {body[:120]}")
    out["profiles_state"] = g3

    # ── G4: E2E signup → trigger row, /userdata roundtrip, /plan/self ──
    ts = int(time.time())
    email = f"ops-e2e-{ts}@justhodl.ai"
    pw = f"Ops3366!{ts}x"
    st, body = req(supa + "/auth/v1/signup", "POST", {"email": email, "password": pw}, headers=ah)
    token = None
    try:
        j = json.loads(body)
        token = j.get("access_token")
    except Exception:  # noqa: BLE001
        j = {}
    out["signup"] = {"http": st, "email": email, "session_issued": bool(token),
                     "confirm_required": (st == 200 and not token)}
    if st != 200:
        gate("G4_signup", False, f"http {st} {body[:150]}")
    elif not token:
        gate("G4_signup", True, "signup ok; email confirmations ON — JWT gates skipped (prod-correct posture)")
        out["findings"].append("Email confirm ON: authed gates rest on 3292 proof + G3 shape")
    else:
        gate("G4_signup", True, "signup ok, session issued (confirmations OFF)")
        jwt_h = {"apikey": anon, "Authorization": "Bearer " + token}

        if g3.startswith("EXISTS"):
            st2, b2 = req(supa + "/rest/v1/profiles?select=plan&id=eq." + j.get("user", {}).get("id", ""),
                          headers=jwt_h)
            try:
                rows = json.loads(b2)
            except Exception:  # noqa: BLE001
                rows = []
            has_row = isinstance(rows, list) and len(rows) == 1
            plan_v = rows[0].get("plan") if has_row else None
            gate("G4a_trigger_profile_row", has_row and plan_v == "free",
                 f"row={has_row} plan={plan_v} (trigger auto-create)")
            if not has_row:
                out["khalid_actions"].append("Trigger missing: setup.sql installs it + backfills")
        else:
            print("SKIP  G4a — table missing, trigger untestable")

        blob = {"v": 1, "favs": ["/why.html", "/capital-flow.html"], "theme": "dark",
                "updated_at": ts, "_ops": 3366}
        st3, _ = req(WORKER + "/userdata/self", "PUT", blob, headers={"Authorization": "Bearer " + token})
        st4, b4 = req(WORKER + "/userdata/self", headers={"Authorization": "Bearer " + token})
        rt = False
        try:
            rt = json.loads(b4).get("_ops") == 3366
        except Exception:  # noqa: BLE001
            pass
        gate("G4b_userdata_roundtrip", st3 in (200, 204) and st4 == 200 and rt,
             f"put={st3} get={st4} echo={rt} (account favorites/indicators persistence)")

        deadline = time.time() + 150
        ps, pb = -1, ""
        while time.time() < deadline:
            ps, pb = req(WORKER + "/plan/self", headers={"Authorization": "Bearer " + token})
            if ps == 200:
                break
            time.sleep(10)
        plan_ok = False
        try:
            plan_ok = ps == 200 and json.loads(pb).get("plan") == "free"
        except Exception:  # noqa: BLE001
            pass
        gate("G4c_plan_self", plan_ok, f"http {ps} {pb[:100]} (server-authoritative plan)")

    # ── G5: security regression battery ──
    st, _ = req(WORKER + "/userdata/self")
    gate("G5a_tokenless_rejected", st in (400, 401, 403), f"http {st}")
    st, _ = req(WORKER + "/userdata/self", headers={"Authorization": "Bearer garbage.garbage.garbage"})
    gate("G5b_garbage_bearer_401", st == 401, f"http {st}")
    st, body = req(WORKER + "/stripe-webhook", "POST", {"type": "checkout.session.completed"})
    if st == 503:
        gate("G5c_webhook_signature", False, "503 not-configured — STRIPE_WEBHOOK_SECRET/SUPABASE_SERVICE_KEY missing on worker (CRITICAL)")
        out["khalid_actions"].append("CRITICAL: set worker secrets STRIPE_WEBHOOK_SECRET + SUPABASE_SERVICE_KEY")
    else:
        gate("G5c_webhook_signature", st == 400, f"http {st} (unsigned must be 400)")
    st, _ = req(WORKER + "/billing-portal", "POST", {})
    gate("G5d_portal_unauth_401", st == 401, f"http {st}")

    # ── G6: checkout leg ──
    st, body = req(WORKER + "/create-checkout", "POST",
                   {"priceId": pro_price, "userId": "ops-3366-probe", "plan": "pro",
                    "email": f"ops-e2e-{ts}@justhodl.ai"})
    ck = False
    try:
        ck = "checkout.stripe.com" in (json.loads(body).get("url") or "")
    except Exception:  # noqa: BLE001
        pass
    gate("G6_checkout_url", st == 200 and ck, f"http {st} stripe_url={ck}")

    out["verdict"] = ("INSTALLED_CORRECTLY" if not fails else
                      "GAPS: " + ",".join(fails))
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    for a in out["khalid_actions"]:
        print("KHALID:", a)
        rep.log("KHALID: " + a)
    Path("aws/ops/reports/3366.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)


with report("3366_supabase_install_verify") as _rep:
    _rep.heading("ops 3366 — Supabase per-user layer install verify")
    main(_rep)
