"""ops 3491 — macro overlay CLOSE: ISO-week dates + derived 2s10s.

3490's probe exposed the banked 3273 gotcha live: the bridge keys weekly
points as ISO-WEEK ("2026-28") — new Date() on that is Invalid, so the
v2.3 overlays would have rendered NOTHING. v2.4: wk2d() (byte-for-byte
chart-pro parity, unit-proven: 2026-28 -> 2026-07-06 Monday, 53-week
years, ISO-date passthrough) converts every bridge point; T10Y2Y is now
DERIVED client-side (US10Y - US02Y week-aligned, both live on the
bridge); CPI dropped honestly (not in the wl map).

Behavioral gates mirror the page logic in python against the LIVE bridge:
  Y1 fetch US10Y, convert its LAST week key with the same ISO-week math,
     assert the resulting ISO date is within 14 days of today
  Y2 derived 2s10s: week-align US10Y/US02Y, last spread equals
     (10Y_last - 2Y_last) exactly and sits in a sane band (-2..+3)
  Y3 flagship v2.4 live (ops3491 + wk2d + derived:) , CPIYOY absent,
     all prior markers intact; 4-surface node-check
"""
import datetime
import json
import re
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report  # noqa: E402

REPO = Path(__file__).resolve().parents[3]
WLAPI = "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws"


def wk2d(w):
    q = str(w).split("-")
    if len(str(w)) > 7 or len(q) < 2 or not q[1].isdigit():
        return str(w)[:10]
    y, n = int(q[0]), int(q[1])
    d = datetime.date(y, 1, 4)
    dow = d.isoweekday()
    d = d + datetime.timedelta(days=(1 - dow) + (n - 1) * 7)
    return d.isoformat()


def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3491"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read())


def fetch_raw(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3491"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b)
        p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3491_wk2d_close") as rep:
    out = {"ops": 3491, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:440]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:400]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3491 — ISO-week conversion + derived 2s10s (live)")

    try:
        d10 = fetch_json(f"{WLAPI}/?sym={urllib.parse.quote('TVC:US10Y')}")
        last_w, last_v = d10["points"][-1]
        iso = wk2d(last_w)
        age = (datetime.date.today()
               - datetime.date.fromisoformat(iso)).days
        gate("Y1_wk2d_recency", 0 <= age <= 14 and 2 < last_v < 8,
             {"last_week": last_w, "iso": iso, "age_days": age,
              "us10y": last_v})
    except Exception as e:  # noqa: BLE001
        gate("Y1_wk2d_recency", False, str(e)[:260])

    try:
        d02 = fetch_json(f"{WLAPI}/?sym={urllib.parse.quote('TVC:US02Y')}")
        m02 = {w: v for w, v in d02["points"]}
        spread = [(w, v - m02[w]) for w, v in d10["points"]
                  if w in m02 and v is not None and m02[w] is not None]
        lw, lv = spread[-1]
        expect = d10["points"][-1][1] - m02[d10["points"][-1][0]]
        gate("Y2_derived_2s10s",
             len(spread) > 100 and abs(lv - expect) < 1e-9
             and -2 <= lv <= 3,
             {"n": len(spread), "last_week": lw,
              "spread": round(lv, 3),
              "check": f"{d10['points'][-1][1]}-{m02[d10['points'][-1][0]]}"})
    except Exception as e:  # noqa: BLE001
        gate("Y2_derived_2s10s", False, str(e)[:260])

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["core"] = fetch_raw(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch_raw(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch_raw(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch_raw(f"https://justhodl.ai/why.html?cb={cb}")
            if b"ops3491" in got["flag"]:
                break
        except Exception as e:  # noqa: BLE001
            got["err"] = str(e)[:120]
        time.sleep(20)
    checks = [node_ok(got.get("core", b"x=")), node_ok(got.get("cat", b"x="))]
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>",
                   got.get("flag", b""))
    checks.append(node_ok(m1.group(1) if m1 else b"x="))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>',
                   got.get("why", b""))
    checks.append(node_ok(m2.group(1) if m2 else b"x="))
    f = got.get("flag", b"")
    d3 = {"ops3491": b"ops3491" in f, "wk2d": b"wk2d" in f,
          "derived": b"derived:" in f, "cpi_dropped": b"CPIYOY" not in f,
          "node_ok": all(checks), "mx_intact": b"mxbtn" in f,
          "evt_intact": b"evtbtn" in f, "rt_intact": b"rtbtn" in f}
    gate("Y3_flagship_v24", all(d3.values()), d3)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3491.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
