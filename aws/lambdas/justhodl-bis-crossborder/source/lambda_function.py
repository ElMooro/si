"""justhodl-bis-crossborder v1.0 — BIS cross-border banking claims engine.

Eurodollar-plumbing extension discovered via World Monitor reverse-
engineering. Their hard-won lesson (documented in their seeds, reused as
fact): WS_LBS_D_PUB publishes NO per-counterparty breakdown on the public
API — WS_CBS_PUB (Consolidated Banking Statistics) DOES, SDMX key shape
Q.S.<PARENT>.4B.F.C.A.A.TO1.A. with the L_CP_COUNTRY dimension in the
response. We pull all-reporters foreign claims by counterparty, compute
QoQ/YoY, and roll offshore-centre and EM-Asia aggregates — the offshore-USD
credit pulse our plumbing board lacked. Quarterly source; weekly check.
Feeds data/bis-crossborder.json. stdlib-only; never fabricates.
"""
import json
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/bis-crossborder.json"
UA = {"User-Agent": "JustHodl research admin@justhodl.ai",
      "Accept": "application/vnd.sdmx.data+json, application/json"}
S3 = boto3.client("s3", region_name="us-east-1")

BIS = ("https://stats.bis.org/api/v1/data/WS_CBS_PUB/"
       "Q.S.5A.4B.F.C.A.A.TO1.A.?lastNObservations=9")

OFFSHORE = {"KY", "HK", "SG", "LU", "BM", "BS", "JE", "GG", "PA", "CW"}
EM_ASIA = {"CN", "IN", "ID", "MY", "TH", "PH", "VN", "KR", "TW"}
WATCH = {"5J": "All countries (total)", "CN": "China", "GB": "UK",
         "JP": "Japan", "KY": "Cayman Is", "HK": "Hong Kong",
         "SG": "Singapore", "LU": "Luxembourg", "IN": "India",
         "BR": "Brazil", "MX": "Mexico", "TR": "Turkey", "KR": "Korea",
         "US": "United States", "DE": "Germany", "FR": "France",
         "SA": "Saudi Arabia", "AE": "UAE", "ID": "Indonesia"}


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    out = {"ok": False, "version": VERSION, "generated_at": now.isoformat(),
           "source": "BIS WS_CBS_PUB — consolidated foreign claims, "
                     "immediate counterparty basis, all reporting banks",
           "units": "USD millions (reported); *_bn/_tn derived",
           "by_counterparty": [], "errors": []}
    try:
        raw = urllib.request.urlopen(
            urllib.request.Request(BIS, headers=UA), timeout=45).read()
        j = json.loads(raw)
    except Exception as e:
        out["errors"].append("fetch: " + str(e)[:120])
        S3.put_object(Bucket=BUCKET, Key=KEY,
                      Body=json.dumps(out).encode(),
                      ContentType="application/json")
        return {"ok": False, "err": out["errors"]}

    data = (j.get("data") or j)
    structure = data.get("structure") or j.get("structure") or {}
    dsets = data.get("dataSets") or j.get("dataSets") or []
    sdims = ((structure.get("dimensions") or {}).get("series")) or []
    odims = ((structure.get("dimensions") or {}).get("observation")) or []
    cp_idx = next((i for i, d in enumerate(sdims)
                   if d.get("id") in ("L_CP_COUNTRY", "CP_COUNTRY",
                                      "COUNTERPARTY_COUNTRY")), None)
    if cp_idx is None or not dsets:
        out["errors"].append(f"shape: cp_idx={cp_idx} dsets={len(dsets)} "
                             f"dims={[d.get('id') for d in sdims][:8]}")
        S3.put_object(Bucket=BUCKET, Key=KEY,
                      Body=json.dumps(out).encode(),
                      ContentType="application/json")
        return {"ok": False, "err": out["errors"]}
    cp_vals = sdims[cp_idx].get("values") or []
    times = [(v.get("id") or v.get("name"))
             for v in (odims[0].get("values") or [])] if odims else []
    series = dsets[0].get("series") or {}

    rows = {}
    for skey, sv in series.items():
        try:
            coords = [int(x) for x in skey.split(":")]
            cp = cp_vals[coords[cp_idx]]
        except Exception:
            continue
        code = cp.get("id")
        obs = sv.get("observations") or {}
        pts = []
        for oi, ov in obs.items():
            try:
                t = times[int(oi)] if times else oi
                v = ov[0]
                if v is not None:
                    pts.append((str(t), float(v)))
            except Exception:
                continue
        if not pts:
            continue
        pts.sort()
        rows[code] = {"name": cp.get("name") or code, "pts": pts}

    def metric(code):
        r = rows.get(code)
        if not r or not r["pts"]:
            return None
        pts = r["pts"]
        period, latest = pts[-1]
        qoq = yoy = None
        if len(pts) >= 2 and pts[-2][1]:
            qoq = round(100 * (latest / pts[-2][1] - 1), 1)
        if len(pts) >= 5 and pts[-5][1]:
            yoy = round(100 * (latest / pts[-5][1] - 1), 1)
        return {"code": code, "name": r["name"], "period": period,
                "latest_bn": round(latest / 1000, 1),
                "qoq_pct": qoq, "yoy_pct": yoy}

    for code in WATCH:
        m = metric(code)
        if m:
            out["by_counterparty"].append(m)
    out["by_counterparty"].sort(
        key=lambda r: -(r["latest_bn"] if r["code"] != "5J" else 0))

    def agg(codes, label):
        tot_now = tot_prev4 = 0.0
        n = 0
        period = None
        for c in codes:
            r = rows.get(c)
            if not r or len(r["pts"]) < 5:
                continue
            tot_now += r["pts"][-1][1]
            tot_prev4 += r["pts"][-5][1]
            period = r["pts"][-1][0]
            n += 1
        if not n or not tot_prev4:
            return None
        return {"label": label, "n": n, "period": period,
                "latest_bn": round(tot_now / 1000, 1),
                "yoy_pct": round(100 * (tot_now / tot_prev4 - 1), 1)}

    out["offshore_centres"] = agg(OFFSHORE & set(rows), "offshore centres")
    out["em_asia"] = agg(EM_ASIA & set(rows), "EM Asia")
    tot = metric("5J")
    if tot:
        out["total"] = {**tot, "latest_tn": round(tot["latest_bn"] / 1000, 2)}
    out["counterparties_in_response"] = len(rows)
    out["ok"] = bool(tot) and len(out["by_counterparty"]) >= 6

    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[bis-cb] cps={len(rows)} total={out.get('total')} "
          f"offshore={out.get('offshore_centres')} "
          f"em_asia={out.get('em_asia')}")
    return {"ok": out["ok"], "total": out.get("total"),
            "cps": len(rows)}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2)[:1500])
