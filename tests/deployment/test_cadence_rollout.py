"""cadence_rollout: the ops 6553/6554 driver end to end against fake clients, with the committed plan's own shape."""
from pathlib import Path
import json
import sys
import types

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import cadence_rollout as cr  # noqa: E402
from test_cadence_apply import FakeEvents, FakeScheduler, FakeS3, lam  # noqa: E402


class Rec:
    def __init__(self):
        self.lines, self.rows, self.failed = [], [], []

    def heading(self, t): self.lines.append("# " + t)
    def section(self, t): self.lines.append("## " + t)
    def log(self, t): self.lines.append(t)
    def ok(self, t): self.lines.append("OK " + t)
    def warn(self, t): self.lines.append("WARN " + t)
    def fail(self, t): self.failed.append(t)
    def kv(self, **kw): self.rows.append(kw)


class S3Live(FakeS3):
    """put_object/get_object with raw text for the live bucket, JSON for backups."""
    def __init__(self, texts):
        super().__init__()
        self.texts = dict(texts)

    def put_object(self, Bucket, Key, Body, **kw):
        if Bucket == cr.LIVE:
            self.texts[Key] = Body.decode("utf-8")
            self.order.append(Key)
        else:
            super().put_object(Bucket, Key, Body, **kw)

    def get_object(self, Bucket, Key):
        return {"Body": types.SimpleNamespace(read=lambda: self.texts[Key].encode("utf-8"))}


def test_dry_run_then_apply_reach_the_floor_and_update_the_s3_manifests(monkeypatch=None):
    ev, sc = FakeEvents(), FakeScheduler()
    ev.add("fast", "rate(5 minutes)", targets=[{"Id": "1", "Arn": lam("fn-a")}])
    ev.add("shared", "rate(1 hour)", targets=[{"Id": "1", "Arn": lam("fn-b")}, {"Id": "2", "Arn": lam("fn-c")}])
    ev.add("jhk-tick-hourly", "cron(0 12 * * ? *)", targets=[{"Id": "1", "Arn": lam("justhodl-scheduler")}])
    sc.add("c-daily", "cron(5 6 * * ? *)", fn="fn-c")
    sc.add("gov-tick", "cron(7 * * * ? *)", fn="gov")
    for i in range(400):                                        # the driver refuses a snapshot under 400 records
        ev.add("pad-%03d" % i, "cron(0 1 * * ? *)", targets=[{"Id": "1", "Arn": lam("pad-%d" % i)}])
    plan = {"rule": "once a day", "summary": {"ops": {"retime": 2}},
            "actions": [{"kind": "rule", "name": "fast", "bus": "default", "op": "retime", "functions": ["fn-a"],
                         "before": {"expr": "rate(5 minutes)", "state": "ENABLED"}, "after": {"expr": "cron(3 4 * * ? *)", "state": "ENABLED"}},
                        {"kind": "rule", "name": "shared", "bus": "default", "op": "retime", "functions": ["fn-b", "fn-c"],
                         "remove_functions": ["fn-c"],
                         "before": {"expr": "rate(1 hour)", "state": "ENABLED"}, "after": {"expr": "cron(9 5 * * ? *)", "state": "ENABLED"}}],
            "fanout_removals": {"hourly": ["fn-c"]}}
    floor = {"exempt_triggers": [{"kind": "sched", "name": "gov-tick", "why": "control plane, hourly on purpose"}]}
    fan = {"version": "1.0", "ticks": {"hourly": ["fn-c", "fn-d"]}, "assignment": {"fn-c": {"ticks": ["hourly"], "per_day": 1.0}, "fn-d": {"ticks": ["hourly"], "per_day": 1.0}}}
    manifest = {"rules": [{"expr": "rate(5 minutes)", "kind": "events", "name": "fast", "state": "ENABLED", "targets": []},
                          {"expr": "rate(1 hour)", "kind": "events", "name": "shared", "state": "ENABLED",
                           "targets": [{"arn": lam("fn-b"), "id": "1"}, {"arn": lam("fn-c"), "id": "2"}]}], "schedules": []}
    s3 = S3Live({"config/fanout-manifest.json": json.dumps(fan, indent=1, ensure_ascii=False) + "\n",
                 "config/schedule-manifest.json": json.dumps(manifest, indent=1)})
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "config").mkdir()
        (root / "config/cadence-plan.json").write_text(json.dumps(plan), encoding="utf-8")
        (root / "config/cadence-floor.json").write_text(json.dumps(floor), encoding="utf-8")
        (root / "config/fanout-manifest.json").write_text("repo copy differs on purpose", encoding="utf-8")
        (root / "config/schedule-manifest.json").write_text("repo copy differs on purpose", encoding="utf-8")
        fake_boto = types.SimpleNamespace(client=lambda svc, **kw: {"events": ev, "scheduler": sc, "s3": s3}[svc])
        sys.modules["boto3"] = fake_boto
        sys.modules.setdefault("botocore", types.ModuleType("botocore"))
        sys.modules["botocore.config"] = types.SimpleNamespace(Config=lambda **kw: None)
        old_root = cr.ROOT
        cr.ROOT = root
        try:
            r = Rec()
            cr.run(r, dry_run=True)
            assert not r.failed and any("dry run clean: 2 items ready" in l for l in r.lines), r.lines[-6:]
            assert not ev.calls and not sc.calls and s3.order == [], "dry run wrote nothing"
            r = Rec()
            cr.run(r, dry_run=False)
            assert not r.failed, r.failed
        finally:
            cr.ROOT = old_root
            del sys.modules["boto3"]
    assert ev.rules[("default", "fast")]["ScheduleExpression"] == "cron(3 4 * * ? *)"
    assert [t["Id"] for t in ev.targets[("default", "shared")]] == ["1"], "fn-c runs from c-daily; dropped from the shared rule"
    assert sc.items[("default", "gov-tick")]["ScheduleExpression"] == "cron(7 * * * ? *)", "exempt, untouched"
    new_fan = json.loads(s3.texts["config/fanout-manifest.json"])
    assert new_fan["ticks"]["hourly"] == ["fn-d"] and "fn-c" not in new_fan["assignment"]
    new_man = json.loads(s3.texts["config/schedule-manifest.json"])
    assert new_man["rules"][0]["expr"] == "cron(3 4 * * ? *)" and [t["id"] for t in new_man["rules"][1]["targets"]] == ["1"]
    backups = [k for k in s3.objects]
    assert any(k.endswith("rule/default/fast.json") for k in backups) and any("manifest/fanout-manifest.json" in k for k in backups)
    assert any(k.endswith("after-snapshot.json") for k in backups)
    assert any("DIFFERS from the committed repo file" in l for l in r.lines)


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
    print("cadence_rollout tests passed:", len(tests))
