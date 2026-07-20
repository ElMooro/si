"""justhodl-asia-leads v1.0 — the MacroMicro gap-analysis engine, sourced from
FREE PRIMARIES (no middleman): China Total Social Financing (NBS via DBnomics
A_A0L08 — the global credit-impulse lead the CB-balance-sheet stack misses),
Korea exports (FRED monthly; 20-day customs flash queued behind a free BoK
ECOS key), Taiwan exports (FRED, proxy for MOEA export orders — the classic
semis/AI-demand lead; true orders series queued behind endpoint discovery),
and the FRED releases/dates calendar (upcoming high-impact US prints).
Writes data/asia-leads.json. Real data only; blocks degrade independently."""
import json, os, time, urllib.parse, urllib.request
from datetime import datetime, timezone
import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/asia-leads.json"
FRED = os.environ.get("FRED_API_KEY") or "2f057499936072679d8843d7fce99989"
s3 = boto3.client("s3", region_name="us-east-1")
UA = {"User-Agent": "JustHodl research contact@justhodl.ai", "Accept": "application/json"}


def gj(url, timeout=25):
    try:
        raw = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout).read()
        return json.loads(raw)
    except Exception as e:
        print("[asia-leads] fetch fail", url[:80], str(e)[:80])
        return None


def yoy(periods, values, back=12):
    try:
        if len(values) > back and values[-1 - back]:
            return round((values[-1] / values[-1 - back] - 1) * 100, 2)
    except Exception:
        pass
    return None


def mom3(periods, values):
    try:
        if len(values) > 3 and values[-4]:
            return round((values[-1] / values[-4] - 1) * 100, 2)
    except Exception:
        pass
    return None


def china_tsf():
    j = gj("https://api.db.nomics.world/v22/series/NBS/A_A0L08?limit=30&observations=1")
    docs = ((j or {}).get("series") or {}).get("docs") or []
    series, flow_12m = [], None
    for d in docs:
        per, val = d.get("period") or [], d.get("value") or []
        pv = [(p, v) for p, v in zip(per, val) if isinstance(v, (int, float))]
        if not pv:
            continue
        per = [p for p, _ in pv]
        val = [v for _, v in pv]
        row = {"code": d.get("series_code"), "name": (d.get("series_name") or "")[:110],
               "last_period": per[-1], "last_value": val[-1],
               "yoy_pct": yoy(per, val), "chg_3m_pct": mom3(per, val), "n_obs": len(val)}
        nm = (row["name"] or "").lower()
        if "aggregate financing" in nm or "social financing" in nm:
            row["is_headline"] = True
            if len(val) >= 12:
                flow_12m = round(sum(val[-12:]), 1)
        series.append(row)
    return {"source": "NBS via DBnomics A_A0L08 (Social Financing and Its Composition)",
            "series": series, "n_series": len(series),
            "flow_12m_sum_headline": flow_12m,
            "note": ("TSF = the broad China credit tap — the global liquidity lead central-bank "
                     "balance sheets miss. Credit-impulse (Δ12m-flow / GDP) lands v2 once series "
                     "semantics are confirmed against two prints.")}


def fred_block(sid, label, extra_note=""):
    j = gj(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
           f"&api_key={FRED}&file_type=json&observation_start=2015-01-01")
    obs = (j or {}).get("observations") or []
    per = [o["date"] for o in obs if o.get("value") not in (None, ".")]
    val = [float(o["value"]) for o in obs if o.get("value") not in (None, ".")]
    if not val:
        return {"source": f"FRED {sid}", "label": label, "error": "no observations"}
    return {"source": f"FRED {sid}", "label": label,
            "last_period": per[-1], "last_value": val[-1],
            "yoy_pct": yoy(per, val), "chg_3m_pct": mom3(per, val),
            "n_obs": len(val), "history_24m": [{"p": p, "v": v} for p, v in zip(per[-24:], val[-24:])],
            "note": extra_note}


HIGH_IMPACT = ("Consumer Price Index", "Employment Situation", "Gross Domestic Product",
               "Personal Income and Outlays", "FOMC", "Advance Monthly Sales for Retail",
               "Producer Price Index", "Job Openings and Labor Turnover",
               "H.4.1", "Consumer Sentiment", "Employment Cost Index")


def us_calendar(now):
    end = (now.timestamp() + 15 * 86400)
    d1 = datetime.fromtimestamp(end, tz=timezone.utc).strftime("%Y-%m-%d")
    j = gj(f"https://api.stlouisfed.org/fred/releases/dates?api_key={FRED}&file_type=json"
           f"&include_release_dates_with_no_data=true&sort_order=asc"
           f"&realtime_start={now.strftime('%Y-%m-%d')}&realtime_end={d1}&limit=400")
    seen, rows = set(), []
    for r in (j or {}).get("release_dates") or []:
        k = (r.get("date"), r.get("release_name"))
        if k in seen or not r.get("release_name"):
            continue
        seen.add(k)
        hi = any(h.lower() in r["release_name"].lower() for h in HIGH_IMPACT)
        rows.append({"date": r["date"], "release": r["release_name"][:80], "high_impact": hi})
    hi_rows = [r for r in rows if r["high_impact"]]
    return {"source": "FRED releases/dates", "window_days": 15,
            "n_total": len(rows), "high_impact": hi_rows[:40], "all": rows[:120]}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    out = {
        "engine": "asia-leads", "version": "1.0.0",
        "generated_at": now.isoformat(),
        "china_tsf": china_tsf(),
        "korea_exports": fred_block(
            "XTEXVA01KRM667N", "Korea merchandise exports (monthly, NSA)",
            "20-day customs flash (the true nowcast) requires a free Bank of Korea ECOS key — PENDING."),
        "taiwan_exports": fred_block(
            "VALEXPTWM052N", "Taiwan goods exports (monthly)",
            "Proxy for MOEA export ORDERS (orders lead shipments); direct MOEA endpoint discovery queued."),
        "us_calendar": us_calendar(now),
        "methodology": {
            "origin": ("MacroMicro gap analysis 2026-07-20: rejected the paid middleman API; "
                       "sourced the genuinely-missing leads from free primaries instead."),
            "reads": ("China TSF YoY turning up = global credit impulse improving (risk-asset lead ~2-4q); "
                      "Korea + Taiwan export YoY = global tech/semis demand pulse (feeds the AI-infra thesis); "
                      "calendar = upcoming high-impact US prints for the front-run sniffer."),
        },
        "sources": ["DBnomics NBS/A_A0L08", "FRED XTEXVA01KRM667N", "FRED VALEXPTWM052N",
                    "FRED releases/dates"],
        "disclaimer": "Real primary data, research only — not investment advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[asia-leads] tsf_series={out['china_tsf']['n_series']} "
          f"kr_yoy={out['korea_exports'].get('yoy_pct')} tw_yoy={out['taiwan_exports'].get('yoy_pct')} "
          f"cal_hi={len(out['us_calendar']['high_impact'])} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "tsf_series": out["china_tsf"]["n_series"],
        "kr_yoy": out["korea_exports"].get("yoy_pct"),
        "tw_yoy": out["taiwan_exports"].get("yoy_pct"),
        "cal_high_impact": len(out["us_calendar"]["high_impact"])})}
