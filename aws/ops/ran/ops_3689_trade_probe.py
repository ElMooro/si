"""ops 3689 — RECON for the trade-nowcast layer: [A] CPB World Trade Monitor
(world trade volume + industrial production; xlsx/csv on cpb.nl) [B] Baltic
Dry Index + container freight rates (FRED first — keyless & reliable — then
public alternates). Probe only: report what parses, with samples. No engine
until a source is PROVEN (doctrine: control-in-every-probe)."""
import json, re, sys, urllib.parse, urllib.request
from pathlib import Path
import boto3  # noqa
from _lambda_deploy_helpers import deploy_lambda  # noqa
from ops_report import report

UA = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 Chrome/126.0 Safari/537.36")}
FRED = "2f057499936072679d8843d7fce99989"


def get(u, t=30, cap=600_000):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers=UA),
                                   timeout=t)
        return r.read(cap), None
    except Exception as e:
        return b"", str(e)[:110]


def fred(sid, limit=16):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         f"series_id={sid}&api_key={FRED}&file_type=json"
         f"&sort_order=desc&limit={limit}")
    b, e = get(u, 20)
    if e:
        return {"sid": sid, "err": e}
    try:
        o = json.loads(b)
        obs = [x for x in (o.get("observations") or [])
               if x.get("value") not in (".", "", None)]
        if not obs:
            return {"sid": sid, "err": "no obs"}
        return {"sid": sid, "latest": obs[0]["value"], "date": obs[0]["date"],
                "n": len(obs),
                "yr_ago": (obs[12]["value"] if len(obs) > 12 else None)}
    except Exception as ex:
        return {"sid": sid, "err": str(ex)[:80]}


with report("3689_trade_probe") as rep:
    rep.heading("ops 3689 — CPB WTM + freight-rate source recon")
    out = {"gates": {}}
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3689.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        # ---- [A] CPB World Trade Monitor ----
        cpb = {}
        pages = [
            "https://www.cpb.nl/en/worldtrademonitor",
            "https://www.cpb.nl/en/world-trade-monitor",
            "https://www.cpb.nl/en/data",
        ]
        links = []
        for p in pages:
            b, e = get(p)
            if e:
                cpb[p[-28:]] = "ERR " + e
                continue
            h = b.decode("utf-8", "replace")
            cpb[p[-28:]] = f"ok {len(h)}b title={(re.search(r'<title>([^<]*)', h) or [None,''])[1][:60]}"
            for m in re.finditer(r'href="([^"]+\.(?:xlsx|xls|csv))"', h, re.I):
                u2 = m.group(1)
                if u2.startswith("/"):
                    u2 = "https://www.cpb.nl" + u2
                if u2 not in links:
                    links.append(u2)
        out["cpb_pages"] = cpb
        out["cpb_files"] = links[:8]
        # try first file
        if links:
            b, e = get(links[0], 45, 4_000_000)
            out["cpb_first_file"] = {"url": links[0][-70:],
                                     "bytes": len(b), "err": e,
                                     "zip": b[:2] == b"PK"}
        # ---- [B] freight rates via FRED (keyless, reliable) ----
        cand = {
            "bdi_like": "BALTICDRY",          # may not exist; probe
            "harpex": "HARPEX",
            "container_ppi": "PCU4831114831112",   # deep sea freight PPI
            "truck_ppi": "PCU484121484121",
            "rail_ppi": "PCU4821224821221",
            "imp_price_idx": "IR",             # import price index
            "exp_price_idx": "IQ",
        }
        got = {k: fred(v) for k, v in cand.items()}
        out["fred_probe"] = got
        # public BDI alternates (no key)
        alts = {}
        for nm, u in {
            "tradingeconomics_bdi": "https://tradingeconomics.com/commodity/baltic",
            "stooq_bdiy": "https://stooq.com/q/d/l/?s=bdi&i=d",
        }.items():
            b, e = get(u, 25, 200_000)
            txt = b.decode("utf-8", "replace")[:300] if b else ""
            alts[nm] = {"err": e, "bytes": len(b), "head": txt[:160]}
        out["bdi_alts"] = alts

        ok_a = bool(links)
        ok_b = sum(1 for v in got.values() if v.get("latest")) >= 2
        out["gates"]["G1_cpb"] = {"ok": ok_a,
                                   "detail": f"files={links[:4]} pages={cpb}"}
        out["gates"]["G2_rates"] = {"ok": ok_b,
                                     "detail": json.dumps(got)[:600]
                                     + " | alts=" + json.dumps(alts)[:260]}
        for k, v in out["gates"].items():
            print(("PASS  " if v["ok"] else "FAIL  ") + k + " — "
                  + str(v["detail"])[:700])
        out["verdict"] = ("PASS_ALL" if (ok_a and ok_b) else
                          "GAPS: " + ",".join(k for k, v in out["gates"].items()
                                               if not v["ok"]))
    except Exception:
        out["crash"] = traceback.format_exc()[-1000:]
        out["verdict"] = "CRASH"
        print("CRASH:", out["crash"][-400:])
    Path("aws/ops/reports/3689.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
