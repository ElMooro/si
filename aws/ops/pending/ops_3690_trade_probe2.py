"""ops 3690 — trade-nowcast recon v2. 3689: CPB URLs 404, FRED gave 3 live
trade-price series, BDI alts are HTML shells. Now: [A] find CPB's real WTM
path (sitemap + site search + known data portal), [B] use FRED's series
SEARCH api to discover actual freight/trade IDs (never guess again),
[C] extract BDI value from the tradingeconomics HTML we already fetched OK.
Probe only; controls included."""
import json, re, sys, urllib.parse, urllib.request
from pathlib import Path
import boto3  # noqa
from _lambda_deploy_helpers import deploy_lambda  # noqa
from ops_report import report

UA = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 Chrome/126.0 Safari/537.36")}
FRED = "2f057499936072679d8843d7fce99989"


def get(u, t=30, cap=900_000):
    try:
        return urllib.request.urlopen(
            urllib.request.Request(u, headers=UA), timeout=t).read(cap), None
    except Exception as e:
        return b"", str(e)[:110]


def fsearch(text, limit=8):
    u = ("https://api.stlouisfed.org/fred/series/search?search_text="
         + urllib.parse.quote(text) + f"&api_key={FRED}&file_type=json"
         f"&limit={limit}&order_by=popularity&sort_order=desc")
    b, e = get(u, 25)
    if e:
        return {"err": e}
    try:
        j = json.loads(b)
        return [{"id": s["id"], "title": s["title"][:66],
                 "freq": s.get("frequency_short"),
                 "last": s.get("observation_end")}
                for s in (j.get("seriess") or [])]
    except Exception as ex:
        return {"err": str(ex)[:80]}


with report("3690_trade_probe2") as rep:
    rep.heading("ops 3690 — CPB path hunt + FRED discovery + BDI parse")
    out = {"gates": {}}
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3690.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        # [A] CPB real path
        cpb = {}
        b, e = get("https://www.cpb.nl/sitemap.xml", 30)
        if not e and b:
            locs = re.findall(r"<loc>([^<]+)</loc>", b.decode("utf-8", "replace"))
            cpb["sitemap_n"] = len(locs)
            cpb["wtm_hits"] = [l for l in locs
                               if re.search(r"trade|monitor|wereldhandel",
                                            l, re.I)][:10]
        else:
            cpb["sitemap_err"] = e
        for probe in ("https://www.cpb.nl/en",
                      "https://www.cpb.nl/en/publications",
                      "https://www.cpb.nl/"):
            bb, ee = get(probe, 25, 400_000)
            if ee:
                cpb[probe[-22:]] = "ERR " + ee
                continue
            h = bb.decode("utf-8", "replace")
            hits = [m for m in re.findall(r'href="([^"]+)"', h)
                    if re.search(r"trade|monitor|handel", m, re.I)][:8]
            cpb[probe[-22:]] = {"bytes": len(bb), "trade_links": hits}
        out["cpb"] = cpb

        # [B] FRED discovery (control: 'unemployment' must return rows)
        disc = {}
        for term in ("baltic dry index", "freight rate index",
                     "world trade volume", "container freight",
                     "harpex container", "unemployment rate"):
            disc[term] = fsearch(term)
        out["fred_search"] = disc

        # [C] BDI from tradingeconomics HTML
        bdi = {}
        bb, ee = get("https://tradingeconomics.com/commodity/baltic",
                     25, 400_000)
        if not ee and bb:
            h = bb.decode("utf-8", "replace")
            m = re.search(r'id="p"[^>]*>\s*([\d,\.]+)', h)
            m2 = re.search(r'"last"\s*:\s*([\d\.]+)', h)
            m3 = re.search(r'Baltic[^<]{0,60}?([\d,]{3,6}(?:\.\d+)?)\s*(?:points|index)', h, re.I)
            bdi = {"pattern_p": m.group(1) if m else None,
                   "pattern_last": m2.group(1) if m2 else None,
                   "pattern_txt": m3.group(1) if m3 else None,
                   "sample": re.sub(r"<[^>]+>", " ",
                                    h[:1200])[:300]}
        else:
            bdi = {"err": ee}
        out["bdi_parse"] = bdi

        ok_a = bool(cpb.get("wtm_hits"))
        ctrl = disc.get("unemployment rate")
        ok_b = isinstance(ctrl, list) and len(ctrl) > 0
        ok_c = any(bdi.get(k) for k in ("pattern_p", "pattern_last",
                                         "pattern_txt"))
        out["gates"] = {
            "G1_cpb_path": {"ok": ok_a, "detail": json.dumps(cpb)[:620]},
            "G2_fred_disc": {"ok": ok_b, "detail": json.dumps(disc)[:900]},
            "G3_bdi": {"ok": ok_c, "detail": json.dumps(bdi)[:400]},
        }
        for k, v in out["gates"].items():
            print(("PASS  " if v["ok"] else "FAIL  ") + k + " — "
                  + str(v["detail"])[:820])
        fails = [k for k, v in out["gates"].items() if not v["ok"]]
        out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    except Exception:
        out["crash"] = traceback.format_exc()[-1000:]
        out["verdict"] = "CRASH"
        print("CRASH:", out["crash"][-400:])
    Path("aws/ops/reports/3690.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
