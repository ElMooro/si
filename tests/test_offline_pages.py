"""Whole invented site, real baker subprocesses, no live/provider requests."""
from pathlib import Path
import hashlib
import json
import os
import re
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import bake_engine_directory as directory
import bake_right_rail as rail
from run_offline_bake import BAKERS


class OfflinePages(unittest.TestCase):
    def site(self, root):
        root.mkdir(exist_ok=True)
        (root / "engine-manifest.json").write_text(json.dumps({"engines": [
            {"engine": "invented-primary", "keys": ["data/invented.json", "portfolio/invented.json"],
             "key_patterns": ["data/invented-history/*.json"], "unresolved_writes": [{"reason": "invented dynamic path"}]},
            {"engine": "invented-empty", "keys": []},
        ]}), encoding="utf-8")
        (root / "engines.html").write_text('<html><body><script>window.__jhEngineData=__JH_ENGINE_DATA__;</script></body></html>', encoding="utf-8")
        page = '<html><head></head><body>' + ' ' * 2100 + '<script>fetch("data/invented.json")</script></body></html>'
        (root / "defcon.html").write_text(page, encoding="utf-8")
        (root / "nav-manifest.json").write_text(json.dumps({"categories": [{"pages": [{"href": "/defcon.html", "title": "Invented research"}]}]}), encoding="utf-8")
        # Exercise the retired layout too: offline must skip before reading feeds.
        (root / "index.html").write_text('<html>JH COMMAND CENTER v2.0<body><span id="tp-spx">—</span></body></html>', encoding="utf-8")
        (root / "data.html").write_text('<html><body><details id="ai-playbook">Invented provider page</details></body></html>', encoding="utf-8")

    def test_all_real_cli_bakers_are_offline_and_preserve_unknowns(self):
        with TemporaryDirectory() as td:
            root = Path(td); self.site(root)
            original = {name: (root / name).read_bytes() for name in ("index.html", "data.html")}
            for baker in sorted(BAKERS):
                target = root / "index.html" if baker == "bake_homepage" else root
                result = subprocess.run([sys.executable, str(ROOT / "scripts/run_offline_bake.py"), baker, str(target), "--offline"],
                                        cwd=ROOT, capture_output=True, text=True, encoding="utf-8", env={**os.environ, "PYTHONUTF8": "1"})
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            for name, raw in original.items(): self.assertEqual((root / name).read_bytes(), raw)
            data = json.loads(re.search(r'window\.__jhEngineData=(.*?);</script>', (root / "engines.html").read_text(encoding="utf-8")).group(1))
            self.assertEqual(data["build_mode"], "offline"); self.assertFalse(data["availability_checked"])
            row = next(row for row in data["rows"] if row["name"] == "invented-primary")
            self.assertEqual(row["status"], "wired-unverified-feed"); self.assertIsNone(row["n_present"])
            self.assertEqual(row["n_unchecked"], 2); self.assertEqual(row["n_referenced"], 1)
            self.assertEqual(row["unresolved_writes"], [{"reason": "invented dynamic path"}])
            for output in row["outputs"][:2]:
                self.assertEqual(output["state"], "not_checked_during_build")
                for field in ("present", "fresh", "valid", "age_h"): self.assertIsNone(output[field])
            self.assertEqual(row["outputs"][2]["state"], "pattern_requires_index")
            payload = json.loads(re.search(r"window\.__jhRail=(.*?);</script>", (root / "defcon.html").read_text(encoding="utf-8")).group(1))
            self.assertEqual(payload["build_mode"], "offline"); self.assertIsNone(payload["feeds"][0]["modified_at"])
            self.assertFalse(payload["availability_checked"])
            registry = json.loads((root / "config/engine-registry.json").read_bytes())
            self.assertEqual(set(registry["engines"]), {"invented-primary", "invented-empty"})

    def test_library_defaults_never_request_registry_payload_or_metadata(self):
        with TemporaryDirectory() as td:
            root = Path(td); self.site(root)
            with patch.object(directory, "get", side_effect=AssertionError("no request")), patch("urllib.request.urlopen", side_effect=AssertionError("no request")):
                entries, asof = directory.load_entries(root)
                self.assertEqual(len(entries), 2); self.assertIsNone(asof)
                directory.main(root); rail.main(str(root))

    def test_missing_or_invalid_build_manifest_cannot_fall_back_to_repository(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            for raw in (None, b"not json", b'{"engines": []}'):
                if raw is not None: (root / "engine-manifest.json").write_bytes(raw)
                with self.assertRaises(ValueError): directory.load_entries(root)

    def test_swallowed_network_and_child_process_attempts_still_fail(self):
        for event in ("urllib.Request", "socket.__new__", "socket.connect", "socket.getaddrinfo", "subprocess.Popen", "os.system"):
            # Audit events exercise denial before any real I/O or subprocess.
            code = 'import sys;sys.path.insert(0,sys.argv[1]);from run_offline_bake import OfflineBoundary;b=OfflineBoundary();sys.addaudithook(b.audit)\ntry:sys.audit(sys.argv[2],"INVENTED_NOT_A_REAL_DESTINATION")\nexcept RuntimeError:pass\nb.verify()'
            result = subprocess.run([sys.executable, "-c", code, str(ROOT / "scripts"), event], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0); self.assertIn("attempted forbidden I/O", result.stderr)
            self.assertNotIn("INVENTED_NOT_A_REAL_DESTINATION", result.stderr)

    def test_workflow_uses_guard_for_all_bakers_and_preserves_site_schedule(self):
        workflow = (ROOT / ".github/workflows/pages.yml").read_text(encoding="utf-8")
        for baker in BAKERS:
            self.assertIn("scripts/run_offline_bake.py " + baker, workflow)
            self.assertNotIn("run: python3 scripts/" + baker + ".py", workflow)
        self.assertEqual(workflow.count(" --offline"), 4)
        self.assertIn("cron: '*/15 * * * *'", workflow)

    def test_engine_directory_copy_does_not_turn_unknown_count_into_zero(self):
        source = (ROOT / "engines.html").read_text(encoding="utf-8")
        self.assertIn('row.n_present == null ? "availability unchecked"', source)
        self.assertIn("LIVE AVAILABILITY NOT CHECKED", source)
        self.assertNotIn("Availability and source age are checked during deployment", source)


if __name__ == "__main__": unittest.main()
