"""ops 1122 — Diagnose lce.html 'unavailable' rows.

Pulls data/liquidity-credit-engine.json and inspects the failing series
(RESPPALGUONNWW, WGCAL, RESPPNTEPNWW, WRESBAL, WCURCIR, RRPONTSYD, WLRRAL).

Then probes the FRED API directly for each to see if it's a Lambda bug or a
FRED deprecation issue (FRED occasionally retires H.4.1 series IDs).
"""
import json, os, urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
BUCKET = "justhodl-dashboard-live"
FRED_KEY = "2f057499936072679d8843d7fce99989"

s3 = boto3.client("s3", region_name=REGION)

FAILING = ["RESPPALGUONNWW", "WGCAL", "RESPPNTEPNWW", "WRESBAL",
           "WCURCIR", "RRPONTSYD", "WLRRAL"]


def probe_fred(sid):
    """Try fetching FRED data for one series. Returns (status, latest_value_or_err)."""
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit=3")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-LCEDiag/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode("utf-8"))
        obs = data.get("observations", [])
        non_null = [o for o in obs if o.get("value") not in (".", "", None)]
        return {
            "fred_status": "OK",
            "n_obs_returned": len(obs),
            "n_non_null": len(non_null),
            "latest_date": (non_null[0]["date"] if non_null else None),
            "latest_value": (non_null[0]["value"] if non_null else None),
            "first_three": obs[:3],
        }
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            body = ""
        return {"fred_status": f"HTTP {e.code}", "body": body}
    except Exception as e:
        return {"fred_status": "EXC", "err": str(e)[:200]}


def probe_fred_metadata(sid):
    """Get series metadata — useful if FRED renamed or discontinued the series."""
    url = (f"https://api.stlouisfed.org/fred/series"
           f"?series_id={sid}&api_key={FRED_KEY}&file_type=json")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-LCEDiag/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
        s = (data.get("seriess") or [{}])[0]
        return {
            "meta_status": "OK",
            "id": s.get("id"),
            "title": s.get("title"),
            "units": s.get("units_short"),
            "frequency": s.get("frequency_short"),
            "last_updated": s.get("last_updated"),
            "observation_end": s.get("observation_end"),
            "notes": (s.get("notes") or "")[:200],
        }
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            body = ""
        return {"meta_status": f"HTTP {e.code}", "body": body}
    except Exception as e:
        return {"meta_status": "EXC", "err": str(e)[:200]}


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}

    # 1) Read the LCE output JSON
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/liquidity-credit-engine.json")
        lce = json.loads(obj["Body"].read())
        rpt["lce_generated_at"] = lce.get("generated_at")
        series = lce.get("series", {}) or {}
        rpt["lce_n_series"] = len(series)
    except Exception as e:
        rpt["lce_err"] = str(e)[:300]
        series = {}

    # 2) Inspect each failing series — what does the LCE JSON say?
    diagnosis = {}
    for sid in FAILING:
        info = {"in_lce_output": sid in series}
        if sid in series:
            s = series[sid]
            info["lce_keys"] = sorted(s.keys())
            info["lce_available"] = s.get("available")
            info["lce_error"] = s.get("error")
            info["lce__label"] = s.get("_label")
            info["lce_latest_value"] = s.get("latest_value")
            info["lce_signal"] = s.get("signal")
        info["fred_probe"] = probe_fred(sid)
        info["fred_meta"] = probe_fred_metadata(sid)
        diagnosis[sid] = info
    rpt["diagnosis"] = diagnosis
    rpt["finished"] = datetime.now(timezone.utc).isoformat()

    p = os.path.join(REPO_ROOT, "aws/ops/reports/1122.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)

    # Pretty summary
    for sid, info in diagnosis.items():
        f = info.get("fred_probe", {})
        m = info.get("fred_meta", {})
        print(f"\n{sid}")
        print(f"  in_lce_output:  {info.get('in_lce_output')}")
        if info.get("in_lce_output"):
            print(f"  lce_available:  {info.get('lce_available')}")
            print(f"  lce_error:      {info.get('lce_error')}")
            print(f"  lce__label:     {info.get('lce__label')!r}")
        print(f"  fred_probe:     {f.get('fred_status')}  n_obs={f.get('n_obs_returned')} n_non_null={f.get('n_non_null')} latest={f.get('latest_date')}={f.get('latest_value')}")
        print(f"  fred_meta:      {m.get('meta_status')}  title={(m.get('title') or '')[:60]} obs_end={m.get('observation_end')}")


if __name__ == "__main__":
    main()
