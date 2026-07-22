"""ops 3693 — TW value leg (biggest board gap; blocks the 45.7pp semis
divergence from being staged). Audit-first: read data/asia-leads.json and
report EXACTLY what taiwan_orders + taiwan_exports carry (yoy? level? period?
levels-cache depth?), plus the FRED Taiwan export series as a fallback value
leg. Then boom-stage can use: orders.yoy -> exports.yoy -> levels-derived,
in that order of preference. Probe only; no engine change until proven."""
import json, sys, urllib.parse, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda  # noqa
from ops_report import report

S3C = boto3.client("s3", "us-east-1", config=Config(retries={"max_attempts": 2}))
B = "justhodl-dashboard-live"
FRED = "2f057499936072679d8843d7fce99989"


def fred_search(term, limit=8):
    u = ("https://api.stlouisfed.org/fred/series/search?search_text="
         + urllib.parse.quote(term) + f"&api_key={FRED}&file_type=json"
         f"&limit={limit}&order_by=popularity&sort_order=desc")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=25).read())
        return [{"id": s["id"], "title": s["title"][:62],
                 "freq": s.get("frequency_short"),
                 "last": s.get("observation_end")}
                for s in (j.get("seriess") or [])]
    except Exception as e:
        return {"err": str(e)[:80]}


def fred_obs(sid):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         f"series_id={sid}&api_key={FRED}&file_type=json"
         "&sort_order=desc&limit=20")
    try:
        obs = [x for x in (json.loads(urllib.request.urlopen(u, timeout=25)
                                      .read()).get("observations") or [])
               if x.get("value") not in (".", "", None)]
        if not obs:
            return None
        cur = float(obs[0]["value"])
        d = {"sid": sid, "level": cur, "date": obs[0]["date"], "n": len(obs)}
        if len(obs) > 12:
            pri = float(obs[12]["value"])
            if pri:
                d["yoy_pct"] = round(100 * (cur / pri - 1), 1)
        return d
    except Exception as e:
        return {"sid": sid, "err": str(e)[:80]}


with report("3693_tw_value") as rep:
    rep.heading("ops 3693 — Taiwan value-leg audit")
    out = {"gates": {}}
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3693.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        j = json.loads(S3C.get_object(Bucket=B, Key="data/asia-leads.json")["Body"].read())
        tw_o = j.get("taiwan_orders") or {}
        tw_e = j.get("taiwan_exports") or {}
        out["taiwan_orders"] = {k: tw_o.get(k) for k in
                                ("orders_usd_bn", "yoy_pct", "yoy", "period",
                                 "usd_bn", "error", "stage6_hit", "source",
                                 "label", "levels_cached")}
        out["taiwan_exports"] = {k: tw_e.get(k) for k in
                                 ("level", "yoy_pct", "date", "series_id",
                                  "label", "note")}
        try:
            lv = json.loads(S3C.get_object(
                Bucket=B, Key="asia/tw-orders-levels.json")["Body"].read())
            lvls = lv.get("levels") or {}
            out["levels_cache"] = {"n": len(lvls),
                                    "keys": sorted(lvls)[-8:],
                                    "sample": {k: lvls[k] for k in
                                               sorted(lvls)[-4:]}}
        except Exception as e:
            out["levels_cache"] = {"err": str(e)[:90]}

        out["fred_search_tw"] = fred_search("Taiwan exports")
        for sid in ("XTEXVA01TWM667S", "TWNXTEXVA01NCMLM",
                    "XTEXVA01TWM664N"):
            r = fred_obs(sid)
            if r:
                out.setdefault("fred_candidates", {})[sid] = r

        have_orders_yoy = isinstance(tw_o.get("yoy_pct") or tw_o.get("yoy"),
                                     (int, float))
        have_exp_yoy = isinstance(tw_e.get("yoy_pct"), (int, float))
        have_fred = any(isinstance((v or {}).get("yoy_pct"), (int, float))
                        for v in (out.get("fred_candidates") or {}).values())
        ok = have_orders_yoy or have_exp_yoy or have_fred
        out["gates"]["G1_value_source"] = {"ok": ok, "detail":
            (f"orders_yoy={have_orders_yoy} ({tw_o.get('yoy_pct')}/"
             f"{tw_o.get('yoy')}) exports_yoy={have_exp_yoy} "
             f"({tw_e.get('yoy_pct')}) fred={have_fred} "
             f"levels={out.get('levels_cache')} "
             f"fred_cands={json.dumps(out.get('fred_candidates'))[:300]} "
             f"search={json.dumps(out.get('fred_search_tw'))[:300]}")}
        print(("PASS  " if ok else "FAIL  ") + "G1_value_source — "
              + out["gates"]["G1_value_source"]["detail"][:820])
        out["verdict"] = "PASS_ALL" if ok else "GAPS: G1_value_source"
    except Exception:
        out["crash"] = traceback.format_exc()[-1000:]
        out["verdict"] = "CRASH"
        print("CRASH:", out["crash"][-400:])
    Path("aws/ops/reports/3693.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
