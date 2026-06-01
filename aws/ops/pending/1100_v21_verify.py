"""1100 — invoke auction-crisis-detector + verify v2.1 schema additions."""
import json, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1100_v21_verify.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Invoke
    print("[1100] invoking detector (extra FRED batch — expect 30-50s)…")
    t0 = time.time()
    r = lam.invoke(FunctionName="justhodl-auction-crisis-detector",
                    InvocationType="RequestResponse", Payload=b"{}")
    out["elapsed_s"] = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        out["status_code"] = p.get("statusCode")
        if isinstance(p.get("body"), str):
            try:
                out["summary"] = json.loads(p["body"])
            except Exception:
                out["body_preview"] = p["body"][:300]
    except Exception:
        out["raw"] = body[:500]
    
    # Read updated output
    time.sleep(2)
    print("[1100] reading auction-crisis.json…")
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis.json")
    d = json.loads(obj["Body"].read())
    out["meta"] = {
        "size_kb":        round(obj["ContentLength"]/1024, 1),
        "last_modified":  obj["LastModified"].isoformat(),
        "schema_version": d.get("schema_version"),
        "regime":         d.get("regime"),
        "composite":      d.get("composite_score"),
        "elapsed_v2_sec": d.get("elapsed_v2_sec"),
    }
    
    # Audit forward_calendar for concession fields
    fwd = d.get("forward_calendar") or []
    out["forward_count"] = len(fwd)
    concession_counts = {"HEAVY_CONCESSION": 0, "CONCESSION": 0, "FLAT": 0,
                          "RALLY": 0, "STRONG_RALLY": 0, "NO_DATA": 0}
    samples_with_concession = []
    for f in fwd:
        regime = f.get("concession_regime") or "MISSING"
        concession_counts[regime] = concession_counts.get(regime, 0) + 1
        if f.get("concession_5d_bp") is not None and len(samples_with_concession) < 4:
            samples_with_concession.append({
                "auction_date":            f.get("auction_date"),
                "security_term":           f.get("security_term"),
                "concession_series":       f.get("concession_series"),
                "concession_5d_bp":        f.get("concession_5d_bp"),
                "concession_1d_bp":        f.get("concession_1d_bp"),
                "concession_today_yield":  f.get("concession_today_yield"),
                "concession_regime":       f.get("concession_regime"),
                "concession_interpretation": (f.get("concession_interpretation") or "")[:160],
            })
    out["concession_regime_counts"] = concession_counts
    out["concession_samples"] = samples_with_concession
    
    # Audit recent_auctions for post-issue fields
    recent = d.get("recent_auctions") or []
    out["recent_count"] = len(recent)
    postissue_counts = {"STRONG": 0, "FIRM": 0, "FLAT": 0, "SOFT": 0, "WEAK": 0,
                          "PENDING": 0, "PENDING_5D": 0, "MISSING": 0}
    samples_with_postissue = []
    for a in recent:
        cls = a.get("postissue_classification") or "MISSING"
        postissue_counts[cls] = postissue_counts.get(cls, 0) + 1
        if a.get("postissue_5d_bp") is not None and len(samples_with_postissue) < 5:
            samples_with_postissue.append({
                "issue_date":               a.get("issue_date"),
                "security_term":            a.get("security_term"),
                "high_rate":                a.get("high_rate"),
                "postissue_series":         a.get("postissue_series"),
                "postissue_1d_bp":          a.get("postissue_1d_bp"),
                "postissue_5d_bp":          a.get("postissue_5d_bp"),
                "postissue_30d_bp":         a.get("postissue_30d_bp"),
                "postissue_classification": a.get("postissue_classification"),
            })
    out["postissue_class_counts"] = postissue_counts
    out["postissue_samples"] = samples_with_postissue
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1100] DONE — schema {out['meta']['schema_version']}, "
            f"concession enriched {sum(v for k,v in concession_counts.items() if k != 'NO_DATA' and k != 'MISSING')}, "
            f"postissue enriched {sum(v for k,v in postissue_counts.items() if k != 'PENDING' and k != 'PENDING_5D' and k != 'MISSING')}")


if __name__ == "__main__":
    main()
