"""ops 3368 — drawer color tags: live deploy gates.

Feature (this push, jh-nav-drawer.js, additive): 5-color tags (red/orange/
yellow/green/blue) on every drawer row incl. favorites — unlimited pages per
color, multiple colors per page; dots on rows; swatch popover; filter chips
with counts; localStorage jh_tags; per-user union-merge sync through the same
/userdata blob (never-shrink, mirrors favs doctrine). GEN 3335→3368 so the
fresh-guard pulls the new JS on every client. jsdom render-truth harness
PASS_ALL (9 behaviors) pre-push.

Gates:
  G1  live jh-nav-drawer.js serves GEN "3368" + tag markers (poll ≤240s)
  G2  worker /userdata blob preserves the tags field verbatim (PUT+GET)
"""

import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from ops_report import report

SITE = "https://justhodl.ai"
WORKER = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = {"User-Agent": "Mozilla/5.0 (ops-3368)"}


def req(url, method="GET", data=None, headers=None, timeout=25):
    h = dict(UA)
    if headers:
        h.update(headers)
    body = json.dumps(data).encode() if data is not None else None
    if body:
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

    markers = ['GEN = "3368"', "jh_tags", "jhnav-tagbtn", "jhtag-pop",
               "jhtag-chip", "tags: getTags()"]
    deadline = time.time() + 240
    missing, st = markers, -1
    while time.time() < deadline:
        st, body = req(SITE + "/jh-nav-drawer.js?opsv=" + str(int(time.time())))
        missing = [m for m in markers if m not in body] if st == 200 else markers
        if not missing:
            break
        time.sleep(12)
    gate("G1_live_js_markers", not missing, f"http {st} missing={missing}")

    ts = int(time.time())
    dev = f"ops3368tags{ts % 100000}"
    blob = {"v": 1, "favs": ["/why.html"],
            "tags": {"/why.html": ["red", "green"], "/panels.html": ["blue"]},
            "updated_at": ts}
    s1, _ = req(WORKER + "/userdata/" + dev, "PUT", blob)
    s2, b2 = req(WORKER + "/userdata/" + dev)
    ok2 = False
    try:
        ok2 = json.loads(b2).get("tags") == blob["tags"]
    except Exception:  # noqa: BLE001
        pass
    gate("G2_tags_survive_blob", s1 in (200, 204) and s2 == 200 and ok2,
         f"put={s1} get={s2} verbatim={ok2}")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3368.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)


with report("3368_drawer_color_tags") as _rep:
    _rep.heading("ops 3368 — drawer color tags deploy gates")
    main(_rep)
