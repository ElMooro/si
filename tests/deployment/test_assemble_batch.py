"""Deploy lane v4 — a batch lands several related files together or not at all, with one receipt."""
from __future__ import annotations

import json
import runpy
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE = runpy.run_path(str(ROOT / "scripts/assemble_batch.py"))
ENGINE = "import json\nfrom fmp_client import quote\n\n" + "".join(f"def helper_{i}(event):\n    return json.dumps({{'n': {i}}})\n\n" for i in range(40)) \
         + "def lambda_handler(event, context):\n    return quote(helper_0(event))\n"
HELPER = "import json\n\n\ndef quote(x):\n    return json.dumps({'q': x})\n"
CONFIG = json.dumps({"function_name": "justhodl-x", "runtime": "python3.12", "handler": "lambda_function.lambda_handler",
                     "timeout": 60, "memory": 256, "env": {"S3_BUCKET": "justhodl-dashboard-live"}}, indent=1) + "\n"
FN = "aws/lambdas/justhodl-x"


def whole(folder: Path, target: str, text: str):
    p = folder / "files" / target
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def parts(folder: Path, target: str, text: str, n: int = 3, drop_end_on: int | None = None):
    d = folder / "parts" / target
    d.mkdir(parents=True, exist_ok=True)
    lines = text.splitlines(keepends=True)
    per = -(-len(lines) // n)
    for i in range(n):
        chunk = "".join(lines[i * per:(i + 1) * per])
        if not chunk:
            continue
        body = f"@@PART {i + 1}/{n}@@\n" + chunk + ("" if drop_end_on == i + 1 else f"@@END {i + 1}@@\n")
        (d / f"part-{i + 1:03d}").write_text(body, encoding="utf-8")


def manifest(folder: Path, **m):
    base = {"complete": True, "lane": "test", "note": "t"}
    base.update(m)
    (folder / "manifest.json").write_text(json.dumps(base))


def batch(root: Path, bid: str = "test-20260917t150000z-stock") -> Path:
    f = root / "aws/ops/patchers/batch" / bid
    f.mkdir(parents=True)
    return f


def result(root: Path, bid: str) -> dict:
    return next(r for r in MODULE["assemble_all"](root) if r["batch"] == bid)


def test_engine_helper_and_config_land_together_with_one_receipt():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); f = batch(root)
        parts(f, f"{FN}/source/lambda_function.py", ENGINE)
        whole(f, f"{FN}/source/fmp_client.py", HELPER)
        whole(f, f"{FN}/config.json", CONFIG)
        manifest(f, files=[{"target": f"{FN}/source/lambda_function.py", "first_line": "import json", "last_line": "return quote(helper_0(event))"},
                           {"target": f"{FN}/source/fmp_client.py"}, {"target": f"{FN}/config.json"}])
        r = result(root, f.name)
        assert r["status"] == "assembled" and r["functions"] == ["justhodl-x"] and len(r["files"]) == 3, r
        assert (root / FN / "source/lambda_function.py").read_text() == ENGINE
        assert (root / FN / "source/fmp_client.py").read_text() == HELPER
        assert json.loads((root / FN / "config.json").read_text())["memory"] == 256
        assert not f.exists()
        rec = json.loads((root / "aws/ops/patchers/batch/_receipts" / f"{f.name}.json").read_text())
        assert {x["target"] for x in rec["files"]} == {f"{FN}/source/lambda_function.py", f"{FN}/source/fmp_client.py", f"{FN}/config.json"}
        assert {x["how"] for x in rec["files"]} == {"parts", "whole"}


def test_nothing_is_written_when_any_file_fails_and_every_problem_is_reported():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); f = batch(root)
        parts(f, f"{FN}/source/lambda_function.py", ENGINE, drop_end_on=2)          # cut-off part
        whole(f, f"{FN}/source/fmp_client.py", HELPER.replace("def quote(x):", "def quote(x:"))  # syntax error
        whole(f, f"{FN}/config.json", CONFIG)                                         # fine
        manifest(f)
        r = result(root, f.name)
        assert r["status"] == "rejected"
        assert "lambda_function.py" in r["reason"] and "cut off" in r["reason"]
        assert "fmp_client.py" in r["reason"] and "does not compile" in r["reason"]
        assert not (root / FN).exists(), "the good config.json must not land alone"
        assert json.loads((f / "STATUS.json").read_text())["status"] == "rejected"


def test_manifest_and_uploads_must_agree():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); f = batch(root)
        whole(f, f"{FN}/source/fmp_client.py", HELPER)
        whole(f, f"{FN}/source/extra.py", HELPER)
        manifest(f, files=[{"target": f"{FN}/source/fmp_client.py"}, {"target": f"{FN}/source/missing.py"}])
        r = result(root, f.name)
        assert r["status"] == "rejected" and "missing.py" in r["reason"] and "extra.py" in r["reason"]
        assert not (root / FN).exists()


def test_incomplete_batch_does_nothing_until_manifest_says_complete():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); f = batch(root)
        whole(f, f"{FN}/source/fmp_client.py", HELPER)
        assert result(root, f.name)["status"] == "incomplete"
        manifest(f, complete=False)
        assert result(root, f.name)["status"] == "incomplete" and not (root / FN).exists()
        assert not (root / "aws/ops/patchers/batch/_receipts").exists()
        manifest(f, complete=True)
        assert result(root, f.name)["status"] == "assembled"


def test_batch_ids_are_validated_and_never_reused():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        f = batch(root, "Bad Id")
        whole(f, f"{FN}/source/fmp_client.py", HELPER); manifest(f)
        r = MODULE["assemble_all"](root)[0]
        assert r["status"] == "rejected" and "invalid" in r["reason"]
        g = batch(root, "grok-20260917t150000z-stock")
        whole(g, f"{FN}/source/fmp_client.py", HELPER); manifest(g)
        assert result(root, g.name)["status"] == "assembled"
        g2 = batch(root, "grok-20260917t150000z-stock")                # same id again
        whole(g2, f"{FN}/source/fmp_client.py", HELPER + "# v2\n"); manifest(g2)
        r = result(root, g2.name)
        assert r["status"] == "rejected" and "already used" in r["reason"]
        assert (root / FN / "source/fmp_client.py").read_text() == HELPER


def test_allowed_trees_include_config_and_ops_scripts_but_never_workflows():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); f = batch(root)
        reg = json.dumps({"sources": [{"id": i} for i in range(30)]}, indent=1) + "\n"
        whole(f, "config/fusion-registry.v1.json", reg)
        whole(f, f"{FN}/source/fusion-registry.v1.json", reg)
        ops = "import sys\n\n\ndef main():\n    print('gate')\n\n\nif __name__ == '__main__':\n    main()\n    sys.exit(0)\n"
        whole(f, "aws/ops/pending/ops_9999_gate.py", ops)
        manifest(f)
        r = result(root, f.name)
        assert r["status"] == "assembled", r
        assert r["config"] == ["config/fusion-registry.v1.json"] and r["ops_scripts"] == ["aws/ops/pending/ops_9999_gate.py"]
        assert r["functions"] == ["justhodl-x"]
        g = batch(root, "test-20260917t150001z-evil")
        whole(g, ".github/workflows/evil.yml", "on: push\n"); manifest(g)
        r = result(root, g.name)
        assert r["status"] == "rejected" and not (root / ".github").exists()


def test_whole_files_are_normalised_and_shrink_guarded():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / FN / "source").mkdir(parents=True)
        (root / FN / "source/lambda_function.py").write_text(ENGINE * 3)
        f = batch(root)
        whole(f, f"{FN}/source/fmp_client.py", "import json\r\n\r\ndef quote(x):\r\n    return json.dumps(x)")   # CRLF, no final newline
        manifest(f)
        assert result(root, f.name)["status"] == "assembled"
        assert (root / FN / "source/fmp_client.py").read_text() == "import json\n\ndef quote(x):\n    return json.dumps(x)\n"
        g = batch(root, "test-20260917t150002z-shrink")
        whole(g, f"{FN}/source/lambda_function.py", "import json\n\n\ndef lambda_handler(event, context):\n    return json.dumps({'ok': 1})\n")
        manifest(g)
        r = result(root, g.name)
        assert r["status"] == "rejected" and "shrink_ok" in r["reason"]
        h = batch(root, "test-20260917t150003z-shrink-ok")
        whole(h, f"{FN}/source/lambda_function.py", "import json\n\n\ndef lambda_handler(event, context):\n    return json.dumps({'ok': 1})\n")
        manifest(h, shrink_ok=True)
        assert result(root, h.name)["status"] == "assembled"


def test_check_mode_is_read_only():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); f = batch(root)
        whole(f, f"{FN}/config.json", CONFIG); manifest(f)
        r = MODULE["assemble_all"](root, write=False)[0]
        assert r["status"] == "ready" and f.exists() and not (root / FN).exists()
