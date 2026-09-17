"""Deploy lane v3 — multipart uploads a no-shell lane can drive: markers prove each part, nothing needs a hash,
and a bad upload becomes a readable STATUS/receipt instead of a written stub or a silent red run."""
from __future__ import annotations

import hashlib
import json
import runpy
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE = runpy.run_path(str(ROOT / "scripts/assemble_parts.py"))
SPLITTER = runpy.run_path(str(ROOT / "scripts/split_parts.py"))
TARGET = "aws/lambdas/justhodl-x/source/lambda_function.py"
ENGINE = "import json\n\n" + "".join(f"def helper_{i}(event):\n    return json.dumps({{'n': {i}}})\n\n" for i in range(40)) \
         + "def lambda_handler(event, context):\n    return helper_0(event)\n"


def llm_upload(root: Path, name: str, text: str, parts: int = 3, markers: bool = True, manifest: dict | None = None,
               target: str = TARGET, drop_end_on: int | None = None, skip_part: int | None = None):
    """Write parts the way a model does: whole lines, @@PART/@@END markers, manifest last."""
    folder = root / "aws/ops/patchers/parts" / name
    folder.mkdir(parents=True)
    lines = text.splitlines(keepends=True)
    per = -(-len(lines) // parts)
    for i in range(parts):
        chunk = "".join(lines[i * per:(i + 1) * per])
        if not chunk:
            continue
        n = i + 1
        if skip_part == n:
            continue
        body = chunk
        if markers:
            body = f"@@PART {n}/{parts}@@\n" + chunk + ("" if drop_end_on == n else f"@@END {n}@@\n")
        (folder / f"part-{n:03d}").write_text(body, encoding="utf-8")
    if manifest is not None:
        m = {"target": target, "complete": True, "note": "test"}
        m.update(manifest)
        (folder / "manifest.json").write_text(json.dumps(m))
    return folder


def receipt(root: Path, name: str) -> dict:
    return json.loads((root / "aws/ops/patchers/parts/_receipts" / f"{name}.json").read_text())


def result(root: Path, name: str) -> dict:
    """assemble_all reports every upload it sees -- rejected folders stay -- so pick by upload id."""
    return next(r for r in MODULE["assemble_all"](root) if r["upload"] == name)


def test_marker_upload_without_any_hash_is_assembled_exactly_and_receipted():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = llm_upload(root, "u1", ENGINE, parts=4, manifest={"first_line": "import json", "last_line": "return helper_0(event)"})
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "assembled" and out[0]["parts"] == 4 and out[0]["check"] == "py_compile", out
        assert (root / TARGET).read_text() == ENGINE
        assert not folder.exists()
        assert receipt(root, "u1")["sha256"] == hashlib.sha256(ENGINE.encode()).hexdigest()


def test_manifest_written_last_is_the_go_signal():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = llm_upload(root, "u2", ENGINE, parts=3, manifest=None)
        assert MODULE["assemble_all"](root) == []                      # no manifest: still uploading, nothing recorded
        (folder / "manifest.json").write_text(json.dumps({"target": TARGET, "complete": False}))
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "incomplete" and not (root / TARGET).exists()
        assert not (root / "aws/ops/patchers/parts/_receipts").exists()
        (folder / "manifest.json").write_text(json.dumps({"target": TARGET, "complete": True}))
        assert MODULE["assemble_all"](root)[0]["status"] == "assembled"


def test_truncated_part_is_rejected_with_status_and_nothing_written():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = llm_upload(root, "u3", ENGINE, parts=3, manifest={}, drop_end_on=2)
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "rejected" and "part-002" in out[0]["reason"] and "cut off" in out[0]["reason"]
        assert not (root / TARGET).exists() and folder.exists()
        status = json.loads((folder / "STATUS.json").read_text())
        assert status["status"] == "rejected" and receipt(root, "u3")["reason"] == status["reason"]


def test_missing_middle_part_is_a_gap_not_a_shorter_file():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        llm_upload(root, "u4", ENGINE, parts=4, manifest={}, skip_part=3)
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "rejected" and "part-003" in out[0]["reason"]
        assert not (root / TARGET).exists()


def test_marker_numbers_must_match_position_and_total():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = llm_upload(root, "u5", ENGINE, parts=3, manifest={})
        p2 = (folder / "part-002").read_text().replace("@@PART 2/3@@", "@@PART 2/5@@")
        (folder / "part-002").write_text(p2)
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "rejected" and "2/5" in out[0]["reason"]


def test_first_and_last_line_catch_a_wrong_or_missing_edge_part():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        llm_upload(root, "u6", ENGINE, parts=3, manifest={"last_line": "return helper_39(event)"})
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "rejected" and "last_line" in out[0]["reason"]
        assert not (root / TARGET).exists()


def test_parts_without_trailing_newline_still_join_on_line_boundaries():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = root / "aws/ops/patchers/parts/u7"; folder.mkdir(parents=True)
        (folder / "part-001").write_text("@@PART 1/2@@\nimport json\ndef a():\n    return 1\n@@END 1@@")     # no final newline anywhere
        (folder / "part-002").write_text("@@PART 2/2@@\r\ndef lambda_handler(e, c):\r\n    return a()\r\n@@END 2@@\r\n")  # CRLF
        (folder / "manifest.json").write_text(json.dumps({"target": TARGET, "complete": True}))
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "assembled", out
        assert (root / TARGET).read_text() == "import json\ndef a():\n    return 1\ndef lambda_handler(e, c):\n    return a()\n"


def test_python_that_does_not_compile_is_rejected_not_written():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        llm_upload(root, "u8", ENGINE.replace("def lambda_handler(event, context):", "def lambda_handler(event, context:"), parts=2, manifest={})
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "rejected" and "does not compile" in out[0]["reason"]
        assert not (root / TARGET).exists()


def test_json_and_html_targets_get_their_own_checks():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        good = json.dumps({"sources": [{"id": i, "v": "x" * 40} for i in range(20)]}, indent=1) + "\n"
        llm_upload(root, "u9", good, parts=3, manifest={}, target="aws/lambdas/justhodl-x/source/registry.json")
        assert MODULE["assemble_all"](root)[0]["status"] == "assembled"
        llm_upload(root, "u10", good[:-3] + "\n", parts=2, manifest={}, target="aws/lambdas/justhodl-x/source/broken.json")
        out = result(root, "u10")
        assert out["status"] == "rejected" and "JSON" in out["reason"]
        page = "<!doctype html>\n<html>\n<head><title>t</title></head>\n<body>\n" + "<p>row</p>\n" * 30 + "<script>\nconsole.log(1)\n</script>\n</body>\n</html>\n"
        llm_upload(root, "u11", page, parts=2, manifest={}, target="lane-test.html")
        assert result(root, "u11")["status"] == "assembled"
        llm_upload(root, "u12", page.replace("</html>\n", ""), parts=2, manifest={}, target="lane-test2.html")
        out = result(root, "u12")
        assert out["status"] == "rejected" and "whole document" in out["reason"]


def test_shrink_guard_needs_an_explicit_shrink_ok():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / TARGET).parent.mkdir(parents=True)
        (root / TARGET).write_text(ENGINE + "\n# padding\n" * 400)
        small = "import json\n\ndef lambda_handler(event, context):\n    return json.dumps({'ok': True})\n"
        llm_upload(root, "u13", small, parts=1, manifest={})
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "rejected" and "shrink_ok" in out[0]["reason"]
        assert (root / TARGET).read_text().startswith(ENGINE)
        llm_upload(root, "u14", small, parts=1, manifest={"shrink_ok": True})
        assert result(root, "u14")["status"] == "assembled"
        assert (root / TARGET).read_text() == small


def test_targets_outside_the_allowed_tree_are_rejected():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for i, bad in enumerate(("../etc/passwd", "/etc/passwd", ".github/workflows/evil.yml", "aws/ops/pending/ops_9999_x.py")):
            llm_upload(root, f"bad{i}", ENGINE, parts=1, manifest={}, target=bad)
        out = MODULE["assemble_all"](root)
        assert len(out) == 4 and all(r["status"] == "rejected" for r in out), out
        assert not (root / ".github").exists() and not (root / "aws/ops/pending").exists()


def test_v2_manifest_with_hash_still_works_and_a_wrong_hash_is_rejected():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); data = ENGINE.encode()
        folder = root / "aws/ops/patchers/parts/v2ok"; folder.mkdir(parents=True)
        names = []
        for i in range(0, len(data), 500):
            n = f"part-{len(names) + 1:03d}"; (folder / n).write_bytes(data[i:i + 500]); names.append(n)
        (folder / "manifest.json").write_text(json.dumps({"target": TARGET, "parts": names, "join": "bytes",
                                                         "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}))
        assert MODULE["assemble_all"](root)[0]["status"] == "assembled" and (root / TARGET).read_bytes() == data
        folder = root / "aws/ops/patchers/parts/v2bad"; folder.mkdir(parents=True)
        (folder / "part-001").write_bytes(data)
        (folder / "manifest.json").write_text(json.dumps({"target": "aws/lambdas/justhodl-y/source/lambda_function.py",
                                                         "parts": ["part-001"], "join": "bytes", "sha256": "0" * 64}))
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "rejected" and "sha256 mismatch" in out[0]["reason"]
        assert not (root / "aws/lambdas/justhodl-y").exists()


def test_check_mode_writes_nothing_and_records_nothing():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = llm_upload(root, "u15", ENGINE, parts=2, manifest={})
        out = MODULE["assemble_all"](root, write=False)
        assert out[0]["status"] == "ready" and folder.exists() and not (root / TARGET).exists()
        assert not (root / "aws/ops/patchers/parts/_receipts").exists()


def test_split_then_assemble_round_trips_a_40kb_engine_with_markers():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); (root / "aws/lambdas/justhodl-big/source").mkdir(parents=True)
        src = root / "aws/lambdas/justhodl-big/source/lambda_function.py"
        body = ("import json\n" + "".join(f"\ndef helper_{i}(event):\n    return json.dumps({{'n': {i}}})\n" for i in range(700))
                + "\ndef lambda_handler(event, context):\n    return helper_0(event)\n").encode()
        src.write_bytes(body); assert len(body) > 40000
        folder = SPLITTER["split"](src, "aws/lambdas/justhodl-big/source/lambda_function.py", 12000, "", root)
        manifest = json.loads((folder / "manifest.json").read_text())
        parts = sorted(folder.glob("part-*"))
        assert manifest["join"] == "lines" and manifest["parts"] == len(parts) and manifest["complete"] is True
        assert all(p.stat().st_size <= 12000 for p in parts)
        assert (folder / "part-001").read_text().startswith("@@PART 1/") and (folder / "part-001").read_text().rstrip().endswith("@@END 1@@")
        src.write_bytes(b"# stub\n")
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "assembled", out
        assert src.read_bytes() == body


def test_parts_over_the_connector_limit_are_refused_before_trust():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = root / "aws/ops/patchers/parts/u16"; folder.mkdir(parents=True)
        (folder / "part-001").write_text("@@PART 1/1@@\n" + "x = 1\n" * 8000 + "@@END 1@@\n")
        (folder / "manifest.json").write_text(json.dumps({"target": TARGET, "complete": True}))
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "rejected" and "connector limit" in out[0]["reason"]


def test_one_write_cancels_an_abandoned_upload():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = llm_upload(root, "u17", ENGINE, parts=3, manifest=None)
        (folder / "manifest.json").write_text(json.dumps({"cancel": True}))
        out = MODULE["assemble_all"](root)
        assert out[0]["status"] == "cancelled" and not folder.exists() and not (root / TARGET).exists()
        assert receipt(root, "u17")["status"] == "cancelled"
