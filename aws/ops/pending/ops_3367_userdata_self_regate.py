"""ops 3367 — /userdata/self alias fix: security re-gate.

3366 exposed it: the uid length gate (<8 chars → 400) fired BEFORE Bearer
verification, so the drawer's SYNC_URL /userdata/self ALWAYS got 400 — signed-
in favorites sync was a silent no-op (client swallows errors by design). Fix
in this push: "self" bypasses the length gate, reaches verify, and unauthed
"self" is a hard 401 (no anon:self bucket possible).

Gates (poll worker deploy, then real requests):
  G1  tokenless  /userdata/self            → 401  (was 400 pre-fix)
  G2  garbage Bearer /userdata/self        → 401
  G3  garbage Bearer /userdata/<8+chars>   → 401  (true 3156 property, retest)
  G4  anon device roundtrip /userdata/ops3367devicexx PUT+GET → echo intact
  G5  path traversal-ish /userdata/ab      → 400 (length gate still on)
"""

import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from ops_report import report

WORKER = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = {"User-Agent": "Mozilla/5.0 (ops-3367)"}


def req(url, method="GET", data=None, headers=None, timeout=20):
    h = dict(UA)
    if headers:
        h.update(headers)
    body = None
    if data is not None:
        body = json.dumps(data).encode()
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
    out = {"gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:300]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:250]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    # Poll until the fixed worker is live: tokenless self flips 400→401.
    deadline = time.time() + 180
    st = -1
    while time.time() < deadline:
        st, _ = req(WORKER + "/userdata/self")
        if st == 401:
            break
        time.sleep(10)
    gate("G1_tokenless_self_401", st == 401, f"http {st}")

    st, _ = req(WORKER + "/userdata/self",
                headers={"Authorization": "Bearer aaaa.bbbb.cccc"})
    gate("G2_garbage_bearer_self_401", st == 401, f"http {st}")

    st, _ = req(WORKER + "/userdata/deadbeefcafe01",
                headers={"Authorization": "Bearer aaaa.bbbb.cccc"})
    gate("G3_garbage_bearer_devuid_401", st == 401, f"http {st}")

    ts = int(time.time())
    dev = f"ops3367device{ts % 100000}"
    blob = {"v": 1, "favs": ["/panels.html"], "_ops": 3367}
    s1, _ = req(WORKER + "/userdata/" + dev, "PUT", blob)
    s2, b2 = req(WORKER + "/userdata/" + dev)
    rt = False
    try:
        rt = json.loads(b2).get("_ops") == 3367
    except Exception:  # noqa: BLE001
        pass
    gate("G4_anon_roundtrip", s1 in (200, 204) and s2 == 200 and rt,
         f"put={s1} get={s2} echo={rt}")

    st, _ = req(WORKER + "/userdata/ab")
    gate("G5_short_uid_400", st == 400, f"http {st}")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3367.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)


with report("3367_userdata_self_regate") as _rep:
    _rep.heading("ops 3367 — /userdata/self alias security re-gate")
    main(_rep)
