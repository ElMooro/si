"""Retired engines (Khalid, 2026-10-20) never come back through a deploy: a deleted engine has no aws/lambdas/<fn>/
directory and no live trigger in the manifests; a switched-off engine's declared triggers are DISABLED."""
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[2]
REG = json.loads((ROOT / "config/retired-engines.json").read_text(encoding="utf-8"))
PLAN = json.loads((ROOT / "config/retire-plan-2026-10-20.json").read_text(encoding="utf-8"))


def test_deleted_engines_have_no_deployable_directory():
    for fn in REG["deleted"]:
        assert not (ROOT / "aws/lambdas" / fn).exists(), "%s is retired: its code lives under aws/lambdas-retired/" % fn
        assert (ROOT / "aws/lambdas-retired" / fn).is_dir(), "%s: retired copy missing" % fn


def test_retired_engines_are_in_no_fan_out_tick():
    fan = json.loads((ROOT / "config/fanout-manifest.json").read_text(encoding="utf-8"))
    members = {f for fs in fan["ticks"].values() for f in fs}
    for fn in REG["deleted"] + REG["switched_off"]:
        assert fn not in members, "%s still in a fan-out tick" % fn


def test_switched_off_triggers_are_disabled_in_the_manifest():
    m = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
    state = {(k, e["name"]): e.get("state") for k, sec in (("rule", "rules"), ("sched", "schedules")) for e in m.get(sec, [])}
    for fn, spec in PLAN["switch_off"].items():
        for t in spec["triggers"]:
            if (t["kind"], t["name"]) in state:
                assert state[(t["kind"], t["name"])] == "DISABLED", "%s trigger %s is not DISABLED in the manifest" % (fn, t["name"])


def test_registry_and_plan_agree():
    assert sorted(PLAN["delete"]) == REG["deleted"] and sorted(PLAN["switch_off"]) == REG["switched_off"]


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
    print("retired_engines tests passed:", len(tests))
