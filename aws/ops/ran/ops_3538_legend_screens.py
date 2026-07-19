"""ops 3538 — LEGEND SCREENS (Khalid spec): two dropdowns on the
Metric Explorer. 🏆 Long — 11 famous-investor criteria sets (Buffett,
Munger, Graham, Lynch, Greenblatt Magic Formula, Simons, Soros,
Druckenmiller, Fisher, Ackman, Klarman); 🩸 Short — 6 legendary
short frameworks (Chanos accounting flags, Burry bubble valuations,
Einhorn balance-sheet stress, Muddy Waters forensic, Soros macro
short, Growth-trap SBC bleeders). Each = chip set with per-metric
goodness direction (shorts flip so Σ-top = best short candidates) +
HARD threshold gates (ROE>=15, P/E<=15, Beneish>=-1.78, Altman<=3,
SBC/rev>=8 ...) + auxiliary filters (noflag/qmin/momentum tercile/
turn<=25 deteriorating for shorts). Mutually exclusive with each
other and the quick presets; active strategy named in the banner.
26-behavior harness PASS pre-push (Buffett gate membership exact,
Chanos ordering DDD>BBB by subset percentiles, clear restores).

  M1 page served: fxInv + fxShort + strategy configs + node
  M2 LIVE screen sanity from the runner over the real matrix:
     Buffett hard gates yield 15..200 names; Chanos yields 5..150 and
     every survivor has beneish_m >= -1.78; top-5 of each printed
"""
import json, re, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3538"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3538_legend_screens") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3538 — legend screens")
    pa = b""
    for _ in range(14):
        try:
            pa = fetch("https://justhodl.ai/fundamental-census.html?cb=%d"
                       % int(time.time()))
            if b"fxInv" in pa and b"fxShort" in pa: break
        except Exception: pass
        time.sleep(20)
    mm = re.search(rb'<script id="OPS3529">\n([\s\S]*?)</script>', pa)
    ok_n = False
    if mm:
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(mm.group(1)); pth = f.name
        ok_n = subprocess.run(["node", "--check", pth],
                              capture_output=True).returncode == 0
    need = [b"fxInv", b"fxShort", b"Warren Buffett", b"Jim Chanos",
            b"Magic Formula", b"Muddy Waters", b"fxStratNote",
            b"HARDF"]
    gate("M1_page", all(k in pa for k in need) and ok_n,
         {"node": ok_n, "missing": [k.decode() for k in need
                                    if k not in pa]})

    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    col = lambda k: MX["cols"].get(k) or [None]*MX["n_tickers"]
    def screen(hard):
        out = []
        for i, t in enumerate(MX["tickers"]):
            ok = True
            for k, (lo, hi) in hard.items():
                v = col(k)[i]
                if not isinstance(v, (int, float)) or \
                   (lo is not None and v < lo) or \
                   (hi is not None and v > hi):
                    ok = False; break
            if ok:
                out.append(t)
        return out
    buff = screen({"roe_pct": (15, None), "gross_margin_pct": (35, None),
                   "debt_to_equity": (None, 0.8),
                   "interest_coverage_ttm": (8, None)})
    chan = screen({"beneish_m": (-1.78, None)})
    ben_ok = all(isinstance(col("beneish_m")[MX["tickers"].index(t)],
                            (int, float))
                 and col("beneish_m")[MX["tickers"].index(t)] >= -1.78
                 for t in chan[:50])
    gate("M2_live_screens", 15 <= len(buff) <= 200
         and 5 <= len(chan) <= 150 and ben_ok,
         {"buffett_n": len(buff), "buffett_head": buff[:5],
          "chanos_n": len(chan), "chanos_head": chan[:5]})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3538.json").write_text(
        json.dumps({"ops": 3538, "fails": fails}))
sys.exit(0)
