#!/usr/bin/env python3
"""Runner-only. Adds flow/options tape overlays + Beneish haircut to
justhodl-alpha-confluence without counting extra firing factors.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "lambdas/justhodl-alpha-confluence/source/lambda_function.py"

HELPERS = '''
def _s3j(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _tkmap(key):
    doc = _s3j(key)
    tm = doc.get("ticker_map") if isinstance(doc, dict) else None
    return tm if isinstance(tm, dict) else {}


'''

LOAD = '''    fc_map = _tkmap("data/flow-confluence.json")
    oc_map = _tkmap("data/options-confluence.json")
    bad = set()
    bn = _s3j("data/beneish.json")
    for it in (bn.get("manipulators") or bn.get("high_risk") or bn.get("flags") or []):
        if isinstance(it, dict):
            sy = str(it.get("ticker") or it.get("symbol") or "").upper().strip()
            if sy:
                bad.add(sy)
    confluence_records = []
'''

LOOP_OLD = '''        regime_adj_score, regime_adj = apply_regime_adjustment(
            s.get("alpha_score"), s.get("sector"), regime)
        confluence_records.append({
'''

LOOP_NEW = '''        regime_adj_score, regime_adj = apply_regime_adjustment(
            s.get("alpha_score"), s.get("sector"), regime)
        sy = str(s.get("symbol") or "").upper()
        overlays = []
        fp = fc_map.get(sy) or {}
        op = oc_map.get(sy) or {}
        if isinstance(fp, dict) and fp.get("posture"):
            overlays.append("flow:" + str(fp.get("posture")))
        if isinstance(op, dict) and op.get("posture"):
            overlays.append("opt:" + str(op.get("posture")))
        if sy in bad:
            regime_adj_score = max(0.0, (regime_adj_score or 0) * 0.75)
            overlays.append("Beneish flag")
        confluence_records.append({
'''


def main():
    text = TARGET.read_text()
    if "_tkmap(" in text and '"overlays": overlays' in text:
        print("already clean:", TARGET)
        return 0
    if "lam_client = boto3.client(\"lambda\", region_name=\"us-east-1\")\n" not in text:
        raise SystemExit("anchor miss: lam_client")
    if "    confluence_records = []\n" not in text:
        raise SystemExit("anchor miss: records")
    if LOOP_OLD not in text:
        raise SystemExit("anchor miss: loop")
    if '"risk_flags": s.get("risk_flags") or [],\n' not in text:
        raise SystemExit("anchor miss: risk_flags")
    text = text.replace(
        "lam_client = boto3.client(\"lambda\", region_name=\"us-east-1\")\n\n\n",
        "lam_client = boto3.client(\"lambda\", region_name=\"us-east-1\")\nVERSION = \"1.1\"\n\n" + HELPERS,
        1,
    )
    text = text.replace("    confluence_records = []\n", LOAD, 1)
    text = text.replace(LOOP_OLD, LOOP_NEW, 1)
    text = text.replace(
        '            "risk_flags": s.get("risk_flags") or [],\n        })',
        '            "risk_flags": s.get("risk_flags") or [],\n            "overlays": overlays,\n        })',
        1,
    )
    text = text.replace(
        "    confluence_payload = {\n        \"generated_at\":",
        "    confluence_payload = {\n        \"engine\": \"justhodl-alpha-confluence\",\n        \"version\": VERSION,\n        \"generated_at\":",
        1,
    )
    text = text.replace(
        "    regime_payload = {\n        \"generated_at\":",
        "    regime_payload = {\n        \"engine\": \"justhodl-alpha-confluence\",\n        \"version\": VERSION,\n        \"generated_at\":",
        1,
    )
    TARGET.write_text(text)
    print("patched", TARGET)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
