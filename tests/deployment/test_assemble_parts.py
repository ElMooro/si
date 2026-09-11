"""Deploy lane v2 — multipart uploads reassemble only when the declared hash matches."""
from __future__ import annotations

import hashlib
import json
import runpy
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE = runpy.run_path(str(ROOT / "scripts/assemble_parts.py"))


def upload(root: Path, name: str, data: bytes, chunk: int = 7, **overrides):
    folder = root / "aws/ops/patchers/parts" / name
    folder.mkdir(parents=True)
    parts = []
    for i in range(0, len(data), chunk):
        pname = f"part-{len(parts) + 1:03d}"
        (folder / pname).write_bytes(data[i:i + chunk]); parts.append(pname)
    manifest = {"target": "aws/lambdas/justhodl-x/source/lambda_function.py", "parts": parts,
                "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
    manifest.update(overrides)
    (folder / "manifest.json").write_text(json.dumps(manifest))
    return folder


def test_complete_upload_is_assembled_verified_and_parts_removed():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); data = b"def lambda_handler(e, c):\n    return 42\n" * 30
        folder = upload(root, "u1", data)
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "assembled" and out[0]["bytes"] == len(data)
        assert (root / "aws/lambdas/justhodl-x/source/lambda_function.py").read_bytes() == data
        assert not folder.exists()


def test_incomplete_upload_is_skipped_not_failed():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); data = b"x" * 50
        folder = upload(root, "u2", data)
        (folder / "part-003").unlink()
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "incomplete" and out[0]["missing"] == ["part-003"]
        assert not (root / "aws/lambdas/justhodl-x/source/lambda_function.py").exists()


def test_hash_mismatch_never_writes_the_target():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); data = b"y" * 50
        upload(root, "u3", data, sha256="0" * 64)
        try:
            MODULE["assemble_all"](root)
        except MODULE["PartsError"] as exc:
            assert "sha256 mismatch" in str(exc)
        else:
            raise AssertionError("mismatched upload was accepted")
        assert not (root / "aws/lambdas/justhodl-x/source/lambda_function.py").exists()


def test_targets_outside_the_allowed_tree_are_refused():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for bad in ("../etc/passwd", "/etc/passwd", ".github/workflows/evil.yml"):
            upload(root, "u-" + hashlib.md5(bad.encode()).hexdigest()[:6], b"z" * 10, target=bad)
        try:
            MODULE["assemble_all"](root)
        except MODULE["PartsError"]:
            pass
        else:
            raise AssertionError("unsafe target accepted")


def test_bytes_only_manifest_is_accepted_but_python_must_compile():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); data = b"def lambda_handler(e, c):\n    return 1\n"
        folder = upload(root, "u5", data, sha256=None)
        del_manifest = json.loads((folder / "manifest.json").read_text()); del_manifest.pop("sha256")
        (folder / "manifest.json").write_text(json.dumps(del_manifest))
        assert MODULE["assemble_all"](root)[0]["status"] == "assembled"
        bad = b"def lambda_handler(e, c:\n    return 1\n"
        folder = upload(root, "u6", bad)
        try:
            MODULE["assemble_all"](root)
        except MODULE["PartsError"] as exc:
            assert "does not compile" in str(exc)
        else:
            raise AssertionError("non-compiling python was written")


def test_split_then_assemble_round_trips_a_40kb_engine():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); (root / "aws/lambdas/justhodl-big/source").mkdir(parents=True)
        src = root / "aws/lambdas/justhodl-big/source/lambda_function.py"
        body = ("import json\n" + "".join(f"\ndef helper_{i}(event):\n    return json.dumps({{'n': {i}}})\n" for i in range(700))
                + "\ndef lambda_handler(event, context):\n    return helper_0(event)\n").encode()
        src.write_bytes(body); assert len(body) > 40000
        splitter = runpy.run_path(str(ROOT / "scripts/split_parts.py"))
        folder = splitter["split"](src, "aws/lambdas/justhodl-big/source/lambda_function.py", 16000, "", root)
        assert all((folder / p).stat().st_size <= 16000 for p in json.loads((folder / "manifest.json").read_text())["parts"])
        src.write_bytes(b"# stub\n")
        assert MODULE["assemble_all"](root)[0]["status"] == "assembled"
        assert src.read_bytes() == body
