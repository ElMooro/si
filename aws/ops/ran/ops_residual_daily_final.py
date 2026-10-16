#!/usr/bin/env python3
"""Final residual: force remaining high-frequency items to once daily Mon-Fri + concurrency=1.
"""
import boto3

REGION = "us-east-1"
DAILY = "cron(0 13 ? * MON-FRI *)"
FUNCTIONS = ["justhodl-dex-scanner", "justhodl-student-rsi", "justhodl-chain-resumer", "justhodl-symdir", "justhodl-sec-13f"]

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
scheduler = boto3.client("scheduler", region_name=REGION)

def try_rule(name):
    try:
        info = events.describe_rule(Name=name)
        expr = info.get("ScheduleExpression", "")
        print(f"  rule {name}: {expr}")
        if "rate(" in expr and "minute" in expr:
            events.put_rule(Name=name, ScheduleExpression=DAILY, State="ENABLED", Description="Once daily Mon-Fri final residual")
            print("    -> daily")
    except Exception as e:
        print(f"  rule {name}: {e}")

def try_sched(name):
    try:
        info = scheduler.get_schedule(Name=name)
        expr = info.get("ScheduleExpression", "")
        print(f"  sched {name}: {expr}")
        if "rate(" in expr and "minute" in expr:
            scheduler.update_schedule(Name=name, ScheduleExpression=DAILY, State="ENABLED", FlexibleTimeWindow={"Mode": "OFF"}, Target=info.get("Target"))
            print("    -> daily")
    except Exception as e:
        print(f"  sched {name}: {e}")

def main():
    print("=== Final residual force daily ===")
    for fn in FUNCTIONS:
        print(f"\n--- {fn} ---")
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            print(f"  mem={cfg.get('MemorySize')} timeout={cfg.get('Timeout')}")
            lam.put_function_concurrency(FunctionName=fn, ReservedConcurrentExecutions=1)
            print("  concurrency=1 set")
            for s in ["-15min", "-5min", "-1min", "-30min"]:
                try_rule(fn + s)
                try_rule("justhodl-" + fn.split("-")[-1] + s)
            if fn == "justhodl-symdir":
                for s in ["justhodl-symdir-warm", "justhodl-symdir-fredupdates", "justhodl-symdir-codelists"]:
                    try_sched(s)
            if fn == "justhodl-dex-scanner":
                try_rule("justhodl-dex-scanner-15min")
        except Exception as e:
            print(f"  {fn}: {e}")
    print("=== Final residual done ===")

if __name__ == "__main__":
    main()
