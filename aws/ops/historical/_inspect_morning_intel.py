"""Inspect what morning-intelligence currently produces."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def main():
    with report("inspect_morning_intel") as r:
        r.heading("morning-intelligence Lambda + outputs")
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-morning-intelligence")
            r.ok(f"  ✓ Lambda mem={cfg['MemorySize']}MB timeout={cfg['Timeout']}s mod={cfg['LastModified'][:19]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        for key in [
            "morning-intelligence/latest.json",
            "morning-brief/latest.json",
            "morning/brief.json",
            "data/morning-brief.json",
            "data/morning-intel.json",
            "intelligence/latest.json",
            "intelligence/brief.json",
        ]:
            try:
                obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
                d = json.loads(obj["Body"].read())
                r.ok(f"  ✓ {key} ({obj['ContentLength']:,}b)")
                if isinstance(d, dict):
                    r.log(f"    keys: {list(d.keys())[:15]}")
            except Exception:
                pass

        r.heading("Anthropic + Telegram + brief Lambdas")
        for n in ["justhodl-morning-intelligence", "justhodl-morning-brief-tg", "justhodl-watchlist-debate", "justhodl-prompt-iterator"]:
            try:
                cfg = lam.get_function_configuration(FunctionName=n)
                env = (cfg.get("Environment") or {}).get("Variables") or {}
                env_keys = list(env.keys())
                r.log(f"  {n}: env_vars={env_keys[:8]}")
            except Exception:
                r.log(f"  ✗ {n}")


if __name__ == "__main__":
    main()
