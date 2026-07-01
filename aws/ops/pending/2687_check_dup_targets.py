"""ops 2687 — check for duplicate EventBridge targets and clean up if genuinely duplicated."""
import boto3
ev = boto3.client("events", region_name="us-east-1")
for rule in ["signal-genealogy-daily", "structural-pre-signals-daily", "universe-discovery-daily", "talent-migration-daily"]:
    targets = ev.list_targets_by_rule(Rule=rule)["Targets"]
    print(f"\n{rule}: {len(targets)} targets")
    for t in targets:
        print(f"  Id={t['Id']} Arn={t['Arn']}")
    # if genuinely duplicate ARNs under different Ids, remove all but one
    arns_seen = {}
    remove_ids = []
    for t in targets:
        if t["Arn"] in arns_seen:
            remove_ids.append(t["Id"])
        else:
            arns_seen[t["Arn"]] = t["Id"]
    if remove_ids:
        ev.remove_targets(Rule=rule, Ids=remove_ids)
        print(f"  removed duplicate target ids: {remove_ids}")
    else:
        print("  no duplicates (different rules or genuinely distinct targets) -- leaving as-is")
print("\nDONE 2687")
