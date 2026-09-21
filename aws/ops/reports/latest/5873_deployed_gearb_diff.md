# ops 5873 -- deployed justhodl-ai vs checkout

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-09-21T18:15:27+00:00  

## Data

| code_sha | description | last_modified |
|---|---|---|
| P2z65gzMDimnsZvd | AI v2 (review mode): SageMaker front window + Brain pipeline state machine + governed market read; the Perplexity MLOps  | 2026-09-21T18:13:02.000+0000 |

## Log
- `18:15:27` gear_b.py: deployed 780 lines, main 780 lines, diff lines 0
- `18:15:27` lambda_function.py: deployed 2492 lines, main 2492 lines, diff lines 0
## deployed tick(): every line mentioning launch

- `18:15:27`  733          describe_card, region: str = "us-east-1", launch: bool = True) -> Dict[str, Any]:
- `18:15:27`  734     """Hourly: poll jobs, (re)build the dataset if none is eligible, launch at most one SFT inside the caps."""
- `18:15:27`  735     out: Dict[str, Any] = {"at": now_iso(), "polled": [], "built": None, "launched": None, "refusal": None}
- `18:15:27`  742     if any(u.get("status") in ("launching", "InProgress", "Stopping", "unknown") for u in out["polled"]):
- `18:15:27`  745     # the exam launches and decides itself (2026-09-17): trained candidate -> adapter -> frozen exam -> shared contract
- `18:15:27`  753     if isinstance(out.get("examined"), dict) and (out["examined"].get("launched") or out["examined"].get("skipped") == "an exam is in flight"):
- `18:15:27`  756     manifest = latest_unlaunched_manifest(s3, private_bucket)
- `18:15:27`  764     if not manifest.get("ok") or not launch:
- `18:15:27`  765         out["refusal"] = manifest.get("reason") or "launch disabled"
- `18:15:27`  776         out["launched"] = launch_sft(sm, s3, spec=spec, role_arn=role_arn, private_bucket=private_bucket, control=control, policy=policy,
- `18:15:27` ✅ done
