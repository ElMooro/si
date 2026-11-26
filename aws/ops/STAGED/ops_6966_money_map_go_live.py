"""ops 6966 - the money map goes live: the same build and the same gate as the dry run (ops 6964), then the engine's own publish().

Written from the runner with the engine's code (aws/lambdas/justhodl-global-flow-desk/source/flow_map.py): the Lambda is
not invoked, so its once-a-day run (23:37 UTC, Monday to Friday) is not spent. Nothing is written if a check fails.

Objects written: data/global-flow-map.json, data/global-flow-map-funds.json, data/global-flow-map-history.json and
data/global-flow-map/funds/<letter>.json. Safe to run again: it rebuilds and overwrites the same keys.
"""
from pathlib import Path
import importlib.util
import sys

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("ops_6964_money_map_dry_run", HERE / "ops_6964_money_map_dry_run.py")
dry = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = dry
spec.loader.exec_module(dry)

if __name__ == "__main__":
    dry.main(write=True, name="ops_6966_money_map_go_live")
