"""Trigger fresh AI brief with eurodollar data populated, verify snapshot tile."""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("rerun_ai_brief_with_eurodollar") as r:
        r.heading("1) Re-invoke ai-brief with new eurodollar data")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-ai-brief", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:400]}")

        r.heading("2) Verify eurodollar in snapshot")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {d.get('generated_at')}")
            r.log(f"  duration_s: {d.get('duration_s')}")
            r.log(f"  brief_md_chars: {len(d.get('brief_md',''))}")
            usage = d.get("usage") or {}
            r.log(f"  usage: in={usage.get('input_tokens')} out={usage.get('output_tokens')}")

            eur = (d.get("snapshot") or {}).get("eurodollar_stress") or {}
            r.log("")
            r.log(f"  Snapshot.eurodollar_stress (was 'not deployed' yesterday):")
            r.log(f"    composite_score: {eur.get('composite_score')}")
            r.log(f"    severity:        {eur.get('severity')}")
            r.log(f"    regime:          {eur.get('regime')}")
            r.log(f"    n_signals_used:  {eur.get('n_signals_used')}/{eur.get('n_signals_total')}")
            r.log(f"    hot_signals:     {[s.get('id') for s in (eur.get('hot_signals') or [])]}")
            r.log(f"    cold_signals:    {[s.get('id') for s in (eur.get('cold_signals') or [])]}")

            # Check brief mentions eurodollar
            md = d.get("brief_md") or ""
            mentions = []
            for term in ["eurodollar", "eurod", "stress", "repo_spread", "SOFR", "STLFSI", "FSI"]:
                if term.lower() in md.lower():
                    # Find context line
                    for line in md.splitlines():
                        if term.lower() in line.lower():
                            mentions.append(f"    {term:14s}: {line.strip()[:120]}")
                            break
            r.log("")
            r.log(f"  Brief mentions of eurodollar terms (first match each):")
            for m in mentions[:8]:
                r.log(m)
            if not mentions:
                r.log("    (no explicit mention — Claude may have woven it into broader stress narrative)")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
