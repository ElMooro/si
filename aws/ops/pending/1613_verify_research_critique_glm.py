"""
aws/ops/pending/1613_verify_research_critique_glm.py
Verify research-critique serves via GLM-5.1. Corrected synthetic payload
(verdict as dict) + correct nested parse (out['critic']).
"""
import json
import boto3

lam = boto3.client("lambda", region_name="us-east-1")

research = {
    "company": {"name": "Test Corp"},
    "verdict": {"rating": "BUY", "conviction_grade": "B+",
                "price_target_12m": 150, "upside_pct": 20},
    "investment_thesis": ["Accelerating revenue", "Operating margin expansion"],
    "risk_factors": ["Valuation rich vs peers", "Customer concentration"],
    "scenarios": {"bull": "30% upside", "base": "10% upside", "bear": "20% downside"},
}
resp = lam.invoke(
    FunctionName="justhodl-research-critique",
    InvocationType="RequestResponse",
    Payload=json.dumps({"ticker": "TEST", "research": research}).encode(),
)
out = json.loads(resp["Payload"].read().decode())
# unwrap if {statusCode, body}
if isinstance(out, dict) and "body" in out and isinstance(out["body"], str):
    try:
        out = json.loads(out["body"])
    except Exception:
        pass
critic = (out or {}).get("critic", {}) if isinstance(out, dict) else {}
prov, model = critic.get("provider"), critic.get("model")
print("provider:", prov)
print("model:", model)
print("cost_usd:", critic.get("cost_usd"))
print("elapsed_s:", critic.get("elapsed_s"))
print("usage:", critic.get("usage"))
crit = (out or {}).get("critique", {}) if isinstance(out, dict) else {}
print("critique disagreement_score:", crit.get("disagreement_score"))
print("critique keys:", list(crit.keys())[:8] if isinstance(crit, dict) else crit)
if prov == "zai" or str(model).startswith("glm"):
    print("\n✅ VERIFIED: research-critique is served by GLM-5.1 in production")
else:
    print(f"\n⚠️  served by {prov}/{model}")
    print("full out snippet:", json.dumps(out)[:500])
