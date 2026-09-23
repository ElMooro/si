"""ops 5961 -- preference pairs from every burst, then a dataset build that carries them (Claude, 2026-09-22). Direct lane.
1. scripts/factory_pairs.py over all bursts (create-if-absent pair records)
2. one Gear B BUILD (POST /gearb/build semantics via mode=gearb launch=false is a preview; use the build route) and the
   new manifest's pref_pairs, so the next hourly tick (train_mode=auto, min_pairs=50) launches DPO when the count allows
"""
import json, subprocess, sys
from pathlib import Path
import boto3
from botocore.config import Config
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
REPO = HERE.parents[3]; PRI = "justhodl-ai-857687956942"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("5961_build_pairs") as r:
        r.heading("ops 5961 -- pairs from the bursts, then a dataset that carries them")
        r.section("1. Pairs")
        p = subprocess.run([sys.executable, "scripts/factory_pairs.py", "--bursts", "80"], cwd=REPO, capture_output=True, text=True, timeout=1500)
        for line in (p.stdout or "").strip().splitlines()[-4:]:
            r.log(line[:900])
        if p.returncode not in (0, 2):
            r.fail("factory_pairs exited %s: %s" % (p.returncode, (p.stderr or "")[-600:])); sys.exit(1)
        r.section("2. Build (gear_b.build_dataset on the runner, same code as the deployed engine)")
        sys.path.insert(0, str(REPO / "aws/lambdas/justhodl-ai/source")); sys.path.insert(0, str(REPO / "aws/shared"))
        import gear_b as gb
        man = gb.build_dataset(s3, PRI, "justhodl-dashboard-live", gb.load_control(s3, PRI))
        r.kv(ok=man.get("ok"), generation=man.get("generation"), kept=man.get("kept"), pref_pairs=man.get("pref_pairs"), families=man.get("families"),
             seen=json.dumps({k: (man.get("seen") or {}).get(k) for k in ("pairs_attached", "pair_records", "verified_seen")}), reason=man.get("reason"))
        ctl = gb.load_control(s3, PRI)
        r.kv(next_launch_mode=gb.launch_mode(ctl, man), train_mode=ctl.get("train_mode"), min_pairs=ctl.get("min_pairs"))
        r.ok("done")


if __name__ == "__main__":
    main()
