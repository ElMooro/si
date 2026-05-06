"""Tiny smoke test to verify ops workflow is running at all."""
import os, time
out_dir = "aws/ops/reports/latest"
os.makedirs(out_dir, exist_ok=True)
ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
with open(os.path.join(out_dir, "smoke_workflow_alive.md"), "w") as f:
    f.write(f"# smoke workflow test\n\nWorkflow ran at {ts}\n")
print(f"smoke ran at {ts}")
