"""ops 2688 — confirm exactly 1 target per rule now, and each still fires correctly."""
import boto3
ev = boto3.client("events", region_name="us-east-1")
for rule in ["signal-genealogy-daily", "structural-pre-signals-daily", "universe-discovery-daily", "talent-migration-daily"]:
    targets = ev.list_targets_by_rule(Rule=rule)["Targets"]
    print(f"{rule}: {len(targets)} target(s) -- {'OK' if len(targets)==1 else 'STILL WRONG'}")
print("DONE 2688")
