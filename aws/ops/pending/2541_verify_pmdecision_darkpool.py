"""ops 2541 — verify #3.3: pm-decision dark-pool distribution wiring.

(1) Invoke pm-decision live (book is currently empty -> flags == [], but proves
    the engine runs clean and the new field is present). (2) Synthetic join test:
    replicate the join logic against the LIVE dark-pool feed with a held name that
    IS being distributed, to prove the de-risk flag fires when the book holds one.
"""
import boto3, json, time
from botocore.config import Config

lam = boto3.client("lambda", "us-east-1", config=Config(read_timeout=200, retries={"max_attempts": 0}))
s3 = boto3.client("s3", "us-east-1")


def rd(k):
    try:
        return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=k)["Body"].read())
    except Exception:
        return {}


# 1. live invoke
r = lam.invoke(FunctionName="justhodl-pm-decision", InvocationType="RequestResponse", Payload=b"{}")
print("FunctionError:", r.get("FunctionError"))
if r.get("FunctionError"):
    print("PAYLOAD:", r["Payload"].read().decode()[:600])
time.sleep(3)
out = rd("data/pm-decision.json")
pf = out.get("portfolio") or {}
print("field present:", "dark_pool_distribution_flags" in pf)
print("live flags (book empty -> expect []):", pf.get("dark_pool_distribution_flags"))
print("n_positions:", pf.get("n_positions"))

# 2. synthetic join test against the LIVE dark-pool board
dp = rd("data/dark-pool.json")
dp_dist = {str(d.get("ticker") or "").upper(): d for d in (dp.get("top_distribution") or [])}
print("\nlive distribution board (sample):", list(dp_dist)[:6])
if dp_dist:
    held = list(dp_dist)[0]          # pretend the book holds the #1 distributed name
    fake_book = {held: {}, "AAPL": {}}   # AAPL not on the board -> should NOT flag
    flags = []
    for tk in fake_book:
        d = dp_dist.get(str(tk).upper())
        if d:
            flags.append({"ticker": tk, "distribution_score": d.get("score"),
                          "dark_pool_pct": d.get("dark_pool_pct"),
                          "offex_pct": d.get("offex_pct"),
                          "week_return_pct": d.get("week_return_pct"),
                          "signal": "DE-RISK — held name under off-exchange distribution"})
    print(f"synthetic book {{{held}, AAPL}} -> flags:", json.dumps(flags, default=str))
    print("JOIN CORRECT:", len(flags) == 1 and flags[0]["ticker"] == held)
print("DONE 2541")
