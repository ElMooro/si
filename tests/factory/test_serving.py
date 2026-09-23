"""Promote -> serve (2026-09-23): Gear B merges a new champion once; the serve script is a no-op until merged weights wait."""
import json, runpy, sys, types, unittest
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared')); sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source'))


class FakeS3:
    def __init__(self, rows=None): self.rows = dict(rows or {})
    def get_object(self, Bucket, Key):
        if Key not in self.rows: raise KeyError("NoSuchKey")
        return {"Body": types.SimpleNamespace(read=lambda: self.rows[Key])}
    def put_object(self, Bucket, Key, Body, **kw): self.rows[Key] = Body


class ServingTests(unittest.TestCase):
    def test_merge_pending_waits_for_a_champion_then_records_merged_weights(self):
        import gear_b as gb
        gb.get_json = lambda s3, b, k: json.loads(s3.rows[k]) if k in s3.rows else None
        gb.put_json = lambda s3, b, k, o: s3.put_object(Bucket=b, Key=k, Body=json.dumps(o).encode())
        s3 = FakeS3()
        self.assertIsNone(gb.merge_pending(None, s3, private_bucket="p", control={}, role_arn="r", pricing=None))
        s3.rows[gb.CHAMPION_KEY] = json.dumps({"generation": "gen-27", "adapter": "factory/champions/gen-27/adapter/"}).encode()
        s3.rows["factory/models/served/gen-27/merge_manifest.json"] = json.dumps({"files": {"model.safetensors": {}}}).encode()
        out = gb.merge_pending(None, s3, private_bucket="p", control={}, role_arn="r", pricing=None)
        self.assertEqual(out, {"generation": 27, "merged": True})
        self.assertEqual(json.loads(s3.rows[gb.CHAMPION_KEY])["merged_prefix"], "s3://p/factory/models/served/gen-27/")

    def test_serve_script_is_a_noop_without_merged_weights_or_when_already_serving(self):
        M = runpy.run_path(str(ROOT / 'scripts/factory_serve_champion.py'))
        fake_boto = types.SimpleNamespace(client=lambda name, **kw: FakeS3() if name == "s3" else types.SimpleNamespace())
        sys.modules["boto3"] = fake_boto
        try:
            self.assertEqual(M["main"]([]), 0)                                                   # nothing promoted
        finally:
            sys.modules.pop("boto3", None)


    def test_spot_falls_back_to_on_demand_after_a_capacity_stop(self):
        import gear_b as gb
        from datetime import datetime, timezone, timedelta
        rows = {}
        gb.list_keys = lambda s3, b, p, n=0: [k for k in rows if k.startswith(p)]
        gb.get_json = lambda s3, b, k: rows.get(k)
        self.assertTrue(gb.use_spot(None, "p", {}))
        self.assertFalse(gb.use_spot(None, "p", {"spot": False}))
        rows[gb.CAPACITY_PREFIX + "jh-gearb-gen27.json"] = {"at": datetime.now(timezone.utc).isoformat()}
        self.assertFalse(gb.use_spot(None, "p", {}))                                   # a recent capacity stop -> on-demand
        rows[gb.CAPACITY_PREFIX + "jh-gearb-gen27.json"] = {"at": (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()}
        self.assertTrue(gb.use_spot(None, "p", {}))                                    # cooled down -> spot again
        self.assertEqual(gb._stop(3600, False), {"MaxRuntimeInSeconds": 3600})         # on-demand never carries MaxWait
        self.assertEqual(gb._stop(3600, True)["MaxWaitTimeInSeconds"], 7200)


if __name__ == '__main__':
    unittest.main()
