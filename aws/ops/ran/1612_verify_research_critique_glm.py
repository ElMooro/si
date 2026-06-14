"""
aws/ops/pending/1612_verify_research_critique_glm.py
Verify research-critique now serves via GLM-5.1 in production. Invokes the
live Lambda synchronously with synthetic inline research and reports which
provider/model actually answered + a snippet. No secret printed.
"""
import json
import boto3

lam = boto3.client("lambda", region_name="us-east-1")

research = {
    "company": {"name": "Test Corp"},
    "verdict": "BUY",
    "investment_thesis": ["Accelerating revenue", "Operating margin expansion"],
    "risk_factors": ["Valuation rich vs peers", "Customer concentration"],
    "scenarios": {"bull": "30% upside", "base": "10% upside", "bear": "20% downside"},
}
payload = {"ticker": "TEST", "research": research}

resp = lam.invoke(
    FunctionName="justhodl-research-critique",
    InvocationType="RequestResponse",
    Payload=json.dumps(payload).encode(),
)
raw = resp["Payload"].read().decode()
print("Lambda StatusCode:", resp.get("StatusCode"))
try:
    out = json.loads(raw)
    body = out.get("body")
    body = json.loads(body) if isinstance(body, str) else (body or out)
    prov = body.get("provider") or body.get("model") or out.get("provider")
    model = body.get("model") or body.get("model_used")
    print("statusCode:", out.get("statusCode"))
    print("provider:", body.get("provider"))
    print("model:", model)
    print("cost_usd:", body.get("cost_usd"))
    print("elapsed_s:", body.get("elapsed_s"))
    txt = json.dumps(body)[:600]
    print("body snippet:", txt)
    if (body.get("provider") == "zai") or (str(model).startswith("glm")):
        print("\n✅ VERIFIED: research-critique served by GLM-5.1")
    else:
        print(f"\n⚠️  served by {body.get('provider')}/{model} — GLM not primary (check fallback path)")
except Exception as e:
    print("parse error:", repr(e))
    print("raw:", raw[:800])
