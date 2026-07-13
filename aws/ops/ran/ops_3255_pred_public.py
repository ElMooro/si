"""ops 3255 — public re-verify after the pages deploy settled: the
PREDICTIONS board literal in served panels.html + the predictions feed
over the CDN with its top call."""
import json
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3255)"}

with report("3255_pred_public") as rep:
    fails = []
    ok_b = ok_f = False
    for i in range(16):
        try:
            h = urllib.request.urlopen(urllib.request.Request(
                f"https://justhodl.ai/panels.html?t={int(time.time())}",
                headers=UA), timeout=15).read().decode("utf-8", "replace")
            if "ops 3254: PREDICTIONS" in h:
                ok_b = True
        except Exception:
            pass
        if ok_b:
            break
        time.sleep(12)
    try:
        P = json.loads(urllib.request.urlopen(urllib.request.Request(
            "https://justhodl.ai/data/wl-predictions.json?t="
            f"{int(time.time())}", headers=UA), timeout=15).read())
        preds = P.get("predictions") or []
        if preds:
            ok_f = True
            r = preds[0]
            rep.ok(f"feed public: {len(preds)} theses — top: "
                   f"{str(r['name'])[:44]} → {r['target']} "
                   f"({r['current_call']})")
    except Exception as e:
        fails.append(f"feed: {str(e)[:60]}")
    if ok_b:
        rep.ok("PREDICTIONS board in served panels.html")
    else:
        fails.append("board literal still absent")
    rep.kv(board=ok_b, feed=ok_f,
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
