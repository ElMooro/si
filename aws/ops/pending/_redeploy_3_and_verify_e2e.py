"""Redeploy asymmetric-scorer + risk-sizer + ai-brief, then verify end-to-end:
  1. Both producers now mirror-write to canonical data/ paths
  2. Brief loads from real schemas and Claude sees rich data in both compressors
"""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def make_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(source_dir):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, source_dir)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def deploy(name, source_dir):
    zb = make_zip(source_dir)
    lam.update_function_code(FunctionName=name, ZipFile=zb)
    for _ in range(15):
        cfg = lam.get_function(FunctionName=name)["Configuration"]
        if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
            return len(zb), cfg.get("LastModified")
        time.sleep(2)
    return len(zb), "?"


def main():
    with report("redeploy_3_and_verify_e2e") as r:
        r.heading("1) Redeploy 3 Lambdas")
        for name, sd in [
            ("justhodl-asymmetric-scorer", "aws/lambdas/justhodl-asymmetric-scorer/source"),
            ("justhodl-risk-sizer",         "aws/lambdas/justhodl-risk-sizer/source"),
            ("justhodl-ai-brief",           "aws/lambdas/justhodl-ai-brief/source"),
        ]:
            try:
                size, ts = deploy(name, sd)
                r.ok(f"  ✓ {name:32s}  zip={size:,}b  modified={ts}")
            except Exception as e:
                r.log(f"  ✗ {name}: {e}")

        r.heading("2) Invoke producers — verify mirror writes")
        for name in ["justhodl-asymmetric-scorer", "justhodl-risk-sizer"]:
            t0 = time.time()
            resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
            body = resp["Payload"].read().decode()
            r.log(f"  {name}: status={resp['StatusCode']}  duration={time.time()-t0:.1f}s")

        r.heading("3) Confirm both legacy + canonical paths exist with same data")
        for legacy, canonical in [
            ("opportunities/asymmetric-equity.json", "data/asymmetric-scorer.json"),
            ("risk/recommendations.json",            "data/risk-sizer.json"),
        ]:
            try:
                a = s3.head_object(Bucket="justhodl-dashboard-live", Key=legacy)
                b = s3.head_object(Bucket="justhodl-dashboard-live", Key=canonical)
                same_size = a["ContentLength"] == b["ContentLength"]
                r.ok(f"  ✓ {legacy:42s} {a['ContentLength']:>8,}b  {a['LastModified'].isoformat()}")
                r.ok(f"  ✓ {canonical:42s} {b['ContentLength']:>8,}b  {b['LastModified'].isoformat()}  same_size={same_size}")
            except Exception as e:
                r.log(f"  ✗ {e}")

        r.heading("4) Trigger AI brief with enriched compressors")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-ai-brief", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:400]}")

        r.heading("5) Verify brief snapshot has rich asymmetric + risk_sizer data")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.json")
            d = json.loads(obj["Body"].read())
            sym = (d.get("snapshot") or {}).get("asymmetric_setups") or {}
            risk = (d.get("snapshot") or {}).get("risk_sizer") or {}

            r.log("")
            r.log("  ASYMMETRIC SETUPS snapshot:")
            r.log(f"    n_setups:                {sym.get('n_setups')}")
            r.log(f"    n_value_traps:           {sym.get('n_value_traps')}")
            r.log(f"    n_quality_passed:        {sym.get('n_quality_passed')}/{sym.get('n_screened')} screened")
            r.log(f"    cutoffs:                 {sym.get('cutoffs_used')}")
            r.log(f"    aaii_signal:             {sym.get('aaii_signal')}")
            r.log(f"    top sectors:             {sym.get('top_3_sectors')}")
            r.log(f"    top 5 setups:")
            for s in (sym.get("top_5_setups") or [])[:5]:
                r.log(f"      • {s.get('symbol'):6s} ({s.get('sector')[:18] if s.get('sector') else '?':18s}) composite={s.get('composite_score')}  dims={s.get('dims_passed')}")

            r.log("")
            r.log("  RISK SIZER snapshot:")
            r.log(f"    regime:                  {risk.get('regime')} (strength {risk.get('regime_strength')})")
            r.log(f"    max_gross_exposure_pct:  {risk.get('max_gross_exposure_pct')}%")
            r.log(f"    current_dd_pct:          {risk.get('current_dd_pct')}%")
            r.log(f"    dd_active_trigger:       {risk.get('dd_active_trigger')}")
            r.log(f"    n_clusters:              {risk.get('n_clusters')}")
            r.log(f"    total_recommended_size:  {risk.get('total_recommended_size_pct')}%")
            r.log(f"    kelly_fraction:          {risk.get('kelly_fraction')}")
            r.log(f"    top 5 sized:")
            for p in (risk.get("top_5_sized") or [])[:5]:
                r.log(f"      • {p.get('symbol'):6s} {p.get('size_pct')}%  kelly={p.get('kelly_raw')}  conv={p.get('conviction')}  cluster={p.get('cluster')}")
            r.log(f"    warnings:                {risk.get('warnings')}")

            r.heading("6) Brief mentions of asymmetric tickers + risk-sizer terms")
            md = d.get("brief_md") or ""
            tickers = [s.get("symbol") for s in (sym.get("top_5_setups") or [])]
            sized_tickers = [p.get("symbol") for p in (risk.get("top_5_sized") or [])]
            search_terms = list(set(tickers + sized_tickers + ["Kelly", "asymmetric", "QARP", "Piotroski", "kelly", "drawdown", "exposure cap", "value trap"]))
            for term in search_terms[:15]:
                if term and term.lower() in md.lower():
                    for line in md.splitlines():
                        if term.lower() in line.lower():
                            r.log(f"    {term:18s}: {line.strip()[:140]}")
                            break
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
