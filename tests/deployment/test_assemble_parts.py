"""Deploy lane v3 — multipart uploads a no-shell lane can drive: markers prove each part, nothing needs a hash,
and a bad upload becomes a readable STATUS/receipt instead of a written stub or a silent red run."""
from __future__ import annotations

import hashlib
import json
import runpy
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
MODULE = runpy.run_path(str(ROOT / "scripts/assemble_parts.py"))
BATCH_MODULE = runpy.run_path(str(ROOT / "scripts/assemble_batch.py"))
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
    for kind, key in (("parts", name), ("batch", name), ("batch", "test-" + name)):
        path = root / f"aws/ops/patchers/{kind}/_receipts/{key}.json"
        if path.is_file():
            return json.loads(path.read_text())
    raise AssertionError(f"missing receipt: {name}")


def result(root: Path, name: str) -> dict:
    for module, key in ((MODULE, "upload"), (BATCH_MODULE, "batch")):
        for item in module["assemble_all"](root):
            if item[key] in (name, "test-" + name):
                return item
    raise AssertionError(f"missing result: {name}")


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
        (folder / "part-002").write_bytes(b"@@PART 2/2@@\r\ndef lambda_handler(e, c):\r\n    return a()\r\n@@END 2@@\r\n")  # exact CRLF on every platform
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


def test_v3_unmarked_content_is_rejected_even_when_it_compiles():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = llm_upload(root, "no-markers", ENGINE, parts=2, markers=False, manifest={})
        out = result(root, "no-markers")
        assert out["status"] == "rejected" and "@@PART" in out["reason"], out
        assert not (root / TARGET).exists()
        assert json.loads((folder / "STATUS.json").read_text())["status"] == "rejected"


def test_v3_end_marker_without_start_is_rejected():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = llm_upload(root, "end-only", ENGINE, parts=1, manifest={})
        part = folder / "part-001"
        part.write_text(part.read_text().split("\n", 1)[1], encoding="utf-8")
        out = result(root, "end-only")
        assert out["status"] == "rejected" and "@@PART" in out["reason"], out
        assert not (root / TARGET).exists()


def test_raw_bytes_without_hash_or_count_cannot_bypass_markers():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = llm_upload(root, "raw-unverified", ENGINE, parts=1, markers=False, manifest={"join": "bytes"})
        out = result(root, "raw-unverified")
        assert out["status"] == "rejected" and "sha256" in out["reason"], out
        assert not (root / TARGET).exists() and folder.exists()


def test_explicit_parts_cannot_override_incomplete_v3_manifest():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = llm_upload(root, "not-ready", ENGINE, parts=1,
                            manifest={"complete": False, "parts": ["part-001"]})
        out = result(root, "not-ready")
        assert out["status"] == "incomplete", out
        assert not (root / TARGET).exists() and folder.exists()


def test_legacy_raw_bytes_with_only_count_still_round_trips():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        data = ENGINE.encode("utf-8")
        folder = root / "aws/ops/patchers/parts/count-only"
        folder.mkdir(parents=True)
        (folder / "part-001").write_bytes(data)
        (folder / "manifest.json").write_text(json.dumps({
            "target": TARGET, "parts": ["part-001"], "join": "bytes", "bytes": len(data),
        }))
        out = result(root, "count-only")
        assert out["status"] == "assembled", out
        assert (root / TARGET).read_bytes() == data


def batch_upload(root, name, members, complete=True):
    """Exercise the public v4 contract that Grok and ChatGPT share."""
    folder = root / BATCH_MODULE["BATCH_DIR"] / ("test-" + name)
    files = []
    for member_id, target, text, options in members:
        part_dir = folder / "parts" / target
        part_dir.mkdir(parents=True, exist_ok=True)
        lines = text.splitlines(keepends=True)
        per = -(-len(lines) // 2)
        for n in (1, 2):
            body = f"@@PART {n}/2@@\n" + "".join(lines[(n-1)*per:n*per]) + f"@@END {n}@@\n"
            (part_dir / f"part-{n:03d}").write_bytes(body.encode())
        files.append({"target": target, **options})
    (folder / "manifest.json").write_text(json.dumps({"complete": complete, "files": files}), encoding="utf-8")
    return folder


def test_batch_has_one_go_signal_and_commits_all_members_together():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        helper = "aws/shared/large_helper.py"
        folder = batch_upload(root, "release", [("engine", TARGET, ENGINE, {}), ("helper", helper, ENGINE, {})], False)
        assert result(root, "release")["status"] == "incomplete"
        assert not (root / TARGET).exists() and not (root / helper).exists()
        m = json.loads((folder / "manifest.json").read_text())
        m["complete"] = True
        (folder / "manifest.json").write_text(json.dumps(m))
        checked = BATCH_MODULE["assemble_all"](root, write=False)[0]
        assert checked["status"] == "ready" and not (root / TARGET).exists()
        out = result(root, "release")
        assert out["status"] == "assembled" and len(out["files"]) == 2, out
        assert (root / TARGET).read_text() == (root / helper).read_text() == ENGINE
        assert not folder.exists() and receipt(root, "release")["batch"]


def test_bad_batch_member_leaves_every_original_untouched_then_can_retry():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        original = root / TARGET
        original.parent.mkdir(parents=True)
        original.write_bytes(ENGINE.encode())
        updated = ENGINE + "\n# reviewed update\n"
        helper = "aws/shared/helper.py"
        folder = batch_upload(root, "retry", [("engine", TARGET, updated, {}), ("helper", helper, ENGINE, {})])
        part = folder / "parts" / helper / "part-002"
        whole = part.read_bytes()
        part.write_bytes(whole.replace(b"@@END 2@@", b""))
        out = result(root, "retry")
        assert out["status"] == "rejected" and "helper" in out["reason"] and "part-002" in out["reason"], out
        assert original.read_bytes() == ENGINE.encode() and not (root / helper).exists()
        part.write_bytes(whole)
        assert result(root, "retry")["status"] == "assembled"
        assert original.read_bytes() == updated.encode()


def test_batch_duplicate_targets_and_missing_members_are_rejected():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        folder = batch_upload(root, "duplicate", [("a", TARGET, ENGINE, {}), ("b", TARGET, ENGINE, {})])
        assert "duplicate batch target" in result(root, "duplicate")["reason"]
        m = json.loads((folder / "manifest.json").read_text())
        m["files"][1]["target"] = "aws/shared/missing.py"
        (folder / "manifest.json").write_text(json.dumps(m))
        assert "not uploaded" in result(root, "duplicate")["reason"]
        assert not (root / TARGET).exists()


def test_batch_config_twins_must_match_before_any_source_is_written():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        twin = "config/policy.json"
        bundle = "aws/lambdas/justhodl-x/source/policy.json"
        policy = json.dumps({"sources": list(range(100))}, indent=1) + "\n"
        folder = batch_upload(root, "twins", [("canonical", twin, policy, {}), ("bundled", bundle, policy.replace("99", "98"), {})])
        assert "bundled config mismatch" in result(root, "twins")["reason"]
        assert not (root / twin).exists() and not (root / bundle).exists()
        for name in ("part-001", "part-002"):
            (folder / "parts" / bundle / name).write_bytes((folder / "parts" / twin / name).read_bytes())
        assert result(root, "twins")["status"] == "assembled"
        assert (root / twin).read_bytes() == (root / bundle).read_bytes()


def test_changing_a_config_checks_all_existing_bundles():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        policy = json.dumps({"sources": list(range(100))}, indent=1) + "\n"
        twin = "config/policy.json"
        first = "aws/lambdas/justhodl-x/source/policy.json"
        second = "aws/lambdas/justhodl-y/source/policy.json"
        for name in (twin, first, second):
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(policy.encode())
        changed = policy.replace("99", "98")
        batch_upload(root, "missing-twin", [("config", twin, changed, {}), ("x", first, changed, {})])
        out = result(root, "missing-twin")
        assert out["status"] == "rejected" and second in out["reason"], out
        assert (root / twin).read_bytes() == policy.encode()
        llm_upload(root, "single", changed, manifest={}, target=first)
        assert result(root, "single")["status"] == "rejected"


def test_optional_contents_api_base_sha_rejects_stale_overwrites():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        target = root / TARGET
        target.parent.mkdir(parents=True)
        target.write_bytes(ENGINE.encode())
        sha = hashlib.sha1(b"blob " + str(len(ENGINE.encode())).encode() + b"\0" + ENGINE.encode()).hexdigest()
        llm_upload(root, "stale", ENGINE + "\n# change\n", manifest={"base_blob_sha": sha})
        target.write_bytes((ENGINE + "\n# another lane\n").encode())
        out = result(root, "stale")
        assert out["status"] == "rejected" and "changed since" in out["reason"], out
        assert target.read_bytes().endswith(b"# another lane\n")
        target.write_bytes(ENGINE.encode())
        assert result(root, "stale")["status"] == "assembled"


def test_create_only_base_sha_and_idempotent_retry():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        llm_upload(root, "create", ENGINE, manifest={"base_blob_sha": None})
        assert result(root, "create")["status"] == "assembled"
        llm_upload(root, "noop", ENGINE, manifest={"base_blob_sha": None})
        out = result(root, "noop")
        assert out["status"] == "assembled" and out["changed"] is False
        llm_upload(root, "overwrite", ENGINE + "\n# changed\n", manifest={"base_blob_sha": None})
        assert result(root, "overwrite")["status"] == "rejected"


def test_two_ready_uploads_cannot_silently_overwrite_each_other():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        batch_upload(root, "first", [("engine", TARGET, ENGINE, {})])
        batch_upload(root, "second", [("engine", TARGET, ENGINE + "\n# later\n", {})])
        out = BATCH_MODULE["assemble_all"](root)
        assert len(out) == 2 and all(r["status"] == "rejected" and "overlapping" in r["reason"] for r in out), out
        assert not (root / TARGET).exists()


def test_legacy_part_lists_cannot_read_outside_the_upload_or_repeat_a_part():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for i, names in enumerate((["../part-001"], ["part-001", "part-001"], ["/etc/passwd"])):
            llm_upload(root, f"unsafe-{i}", ENGINE, parts=1, manifest={"parts": names, "join": "bytes", "bytes": 100})
        out = MODULE["assemble_all"](root)
        assert all(r["status"] == "rejected" and "unique part-NNN" in r["reason"] for r in out), out


def test_portable_targets_cannot_hide_traversal_in_backslashes():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for i, target in enumerate(("aws/shared/../bad.py", "aws/shared/..\\bad.py", "aws/shared/C:bad.py", "aws//shared/bad.py")):
            llm_upload(root, f"path-{i}", ENGINE, manifest={}, target=target)
        assert all(r["status"] == "rejected" for r in MODULE["assemble_all"](root))


def test_failed_second_file_write_cannot_leave_a_partial_batch():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        target = root / TARGET
        target.parent.mkdir(parents=True)
        target.write_bytes(ENGINE.encode())
        helper = "aws/shared/helper.py"
        batch_upload(root, "io-failure", [("engine", TARGET, ENGINE + "\n# changed\n", {}), ("helper", helper, ENGINE, {})])
        replace = MODULE["os"].replace

        def fail_helper(src, dest):
            if Path(dest) == root / helper:
                raise OSError("simulated disk failure")
            return replace(src, dest)

        with patch.object(MODULE["os"], "replace", side_effect=fail_helper):
            try:
                BATCH_MODULE["assemble_all"](root)
            except OSError as exc:
                assert "simulated disk failure" in str(exc)
            else:
                raise AssertionError("I/O failures must stop the workflow before any commit")
        assert target.read_bytes() == ENGINE.encode() and not (root / helper).exists()
        assert not list(root.rglob("*.upload"))


def test_splitter_round_trips_megabyte_batch_and_unique_upload_ids():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        body = ENGINE.encode() * 500
        assert len(body) > 1_000_000
        (root / "candidate.py").write_bytes(body)
        helper = "aws/shared/helper.py"
        folder = SPLITTER["split_batch"]([
            {"source": "candidate.py", "target": TARGET}, {"source": "candidate.py", "target": helper}
        ], root=root)
        assert all(p.stat().st_size <= 12000 for p in folder.rglob("part-*"))
        assert not list(folder.glob("*/manifest.json"))
        out = result(root, folder.name)
        assert out["status"] == "assembled", out
        assert (root / TARGET).read_bytes() == (root / helper).read_bytes() == body
        one = SPLITTER["split"](root / "candidate.py", TARGET, 12000, "", root)
        two = SPLITTER["split"](root / "candidate.py", TARGET, 12000, "", root)
        assert one != two and one.is_dir() and two.is_dir()


def test_splitter_rejects_invalid_chunk_sizes_before_creating_an_upload():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        src = root / "candidate.py"
        src.write_text(ENGINE)
        for size in (0, -1, 40, 40000):
            try:
                SPLITTER["split"](src, TARGET, size, "", root)
            except ValueError as exc:
                assert "chunk" in str(exc)
            else:
                raise AssertionError(f"accepted invalid chunk size: {size}")
        assert not (root / MODULE["PARTS_DIR"]).exists()


def test_javascript_is_rejected_when_node_validation_is_unavailable():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        llm_upload(root, "js", "const a = 1;\n" * 20, manifest={}, target="large.js")
        with patch.object(MODULE["shutil"], "which", return_value=None):
            out = result(root, "js")
        assert out["status"] == "rejected" and "node is required" in out["reason"], out


def test_assembly_receipt_links_to_exact_apply_handoff():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        llm_upload(root, "traceable", ENGINE, manifest={})
        with patch.dict(MODULE["os"].environ, {"GITHUB_RUN_ID": "123456"}):
            assert result(root, "traceable")["status"] == "assembled"
        proof = receipt(root, "traceable")
        assert proof["handoff_ref"] == "ops-evidence" and proof["handoff_path"] == "aws/ops/reports/apply-lane/123456.json"
