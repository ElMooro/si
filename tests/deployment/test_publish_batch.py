"""Exercise the sender, interruption recovery and its real receiver without network/AWS calls."""
import hashlib
import json
from pathlib import Path
import runpy
import tempfile

ROOT = Path(__file__).resolve().parents[2]
PUBLISH = runpy.run_path(str(ROOT / "scripts/publish_batch.py"))
SPLIT = runpy.run_path(str(ROOT / "scripts/split_parts.py"))
BATCH = runpy.run_path(str(ROOT / "scripts/assemble_batch.py"))
ENGINE_TARGET = "aws/lambdas/justhodl-upload-test/source/lambda_function.py"
HELPER_TARGET = "aws/lambdas/justhodl-upload-test/source/helper.py"
BODY = ("import json\n" + "".join(f"def helper_{i}(event):\n    return json.dumps({{'number': {i}}})\n\n" for i in range(1200))).encode()
HELPER = b"def useful_helper():\n    return 42\n"


class FakeGitHub:
    def __init__(self):
        self.data, self.writes, self.reads = {}, [], []
        self.fail_after = None
        self.lose_ack = None
        self.after_manifest = None
        self.inventory_hook = None

    def read(self, path, ref=None):
        self.reads.append((ref or "main", path))
        data = self.data.get((ref or "main", path))
        return (data, PUBLISH["blob_sha"](data)) if data is not None else None

    def files(self, prefix):
        if self.inventory_hook:
            self.inventory_hook(self)
        return {path for (ref, path) in self.data if ref == "main" and path.startswith(prefix + "/")}

    def create(self, path, data):
        if self.fail_after is not None and len(self.writes) >= self.fail_after:
            raise PUBLISH["UploadError"]("simulated interruption or permission denial")
        assert ("main", path) not in self.data, "sender must not blindly overwrite an existing file"
        self.writes.append(path)
        self.data["main", path] = data
        if path.endswith("/manifest.json") and self.after_manifest:
            self.after_manifest(self)
        if self.lose_ack in (path, "next"):
            self.lose_ack = None
            raise PUBLISH["RetryableError"]("response lost after successful write")
        return {"content": {"sha": PUBLISH["blob_sha"](data)}, "commit": {"sha": "a" * 40}}


def prepare(root):
    (root / "new-engine.py").write_bytes(BODY)
    (root / "helper.py").write_bytes(HELPER)
    return SPLIT["split_batch"]([
        {"source": "new-engine.py", "target": ENGINE_TARGET},
        {"source": "helper.py", "target": HELPER_TARGET},
    ], root=root, lane="codex")


def upload(client, folder, root):
    return PUBLISH["upload"](client, folder, root=root, pause=lambda _: None, progress=lambda _: None)


def assert_stopped(action, contains):
    try:
        action()
    except PUBLISH["UploadError"] as exc:
        assert contains in str(exc), exc
    else:
        raise AssertionError("expected sender to withhold publication")


def assembled_receipt(client, folder, root, remove_parts=True):
    prefix, _, _, expected = PUBLISH["local_plan"](folder, root)
    receipt = {"status": "assembled", "files": [{"target": k, "sha256": v} for k, v in expected.items()],
               "handoff_ref": "ops-evidence", "handoff_path": "aws/ops/reports/apply-lane/123.json"}
    if remove_parts:
        for key in list(client.data):
            if key[0] == "main" and key[1].startswith(prefix + "/"):
                del client.data[key]
    client.data["main", PUBLISH["BATCH_ROOT"] + "/_receipts/" + folder.name + ".json"] = json.dumps(receipt).encode()
    return receipt


def test_one_operation_uploads_a_large_engine_and_helper_then_manifest_and_receiver_matches():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        assert len(BODY) > 57000
        outcome = upload(client, folder, root)
        prefix, payloads, _, _ = PUBLISH["local_plan"](folder, root)
        assert outcome["state"] == "submitted" and "deployment_verified" not in outcome
        assert client.writes[-1] == prefix + "/manifest.json"
        assert set(client.writes[:-1]) == set(payloads)
        assert all(len(data) <= 12000 for data in payloads.values())
        assert any("/files/" in path for path in payloads) and any("/parts/" in path for path in payloads)
        assert all(client.reads.count(("main", path)) >= 2 for path in payloads)
        receiver = root / "receiver"
        receiver.mkdir()
        for (ref, path), data in client.data.items():
            dest = receiver / path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)
        results = BATCH["assemble_all"](receiver)
        assert len(results) == 1 and results[0]["status"] == "assembled", results
        assert (receiver / ENGINE_TARGET).read_bytes() == BODY
        assert (receiver / HELPER_TARGET).read_bytes() == HELPER


def test_interrupted_upload_resumes_only_missing_payloads_and_never_sends_manifest_early():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        client.fail_after = 2
        assert_stopped(lambda: upload(client, folder, root), "interruption")
        assert len(client.writes) == 2 and not any(p.endswith("manifest.json") for p in client.writes)
        already_written = list(client.writes)
        client.fail_after = None
        assert upload(client, folder, root)["state"] == "submitted"
        assert all(client.writes.count(path) == 1 for path in already_written)
        assert client.writes[-1].endswith("/manifest.json")


def test_lost_payload_write_response_is_resolved_by_readback_without_duplicate_write():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        client.lose_ack = "next"
        assert upload(client, folder, root)["state"] == "submitted"
        assert len(client.writes) == len(set(client.writes))


def test_forbidden_first_write_stops_the_release_without_a_manifest():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        client.fail_after = 0
        assert_stopped(lambda: upload(client, folder, root), "permission denial")
        assert client.writes == []


def test_mismatched_existing_payload_is_not_overwritten_or_finalized():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        _, payloads, _, _ = PUBLISH["local_plan"](folder, root)
        path = next(iter(payloads))
        client.data["main", path] = b"another lane or a truncated write"
        assert_stopped(lambda: upload(client, folder, root), "Remote payload differs")
        assert client.writes == []


def test_extra_remote_file_withholds_the_manifest():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        prefix, _, _, _ = PUBLISH["local_plan"](folder, root)
        client.data["main", prefix + "/parts/unexpected/part-001"] = b"unlisted"
        assert_stopped(lambda: upload(client, folder, root), "Unexpected remote batch")
        assert not client.writes


def test_final_readback_barrier_detects_changes_after_individual_payload_verification():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        _, payloads, _, _ = PUBLISH["local_plan"](folder, root)
        first = next(iter(payloads))
        def tamper(remote):
            if len(remote.writes) == len(payloads):
                remote.data["main", first] = b"changed while upload was finishing"
        client.inventory_hook = tamper
        assert_stopped(lambda: upload(client, folder, root), "changed before finalization")
        assert not any(path.endswith("manifest.json") for path in client.writes)


def test_repeat_submitted_batch_does_not_write_it_again():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        upload(client, folder, root)
        writes = list(client.writes)
        assert upload(client, folder, root)["state"] == "submitted"
        assert writes == client.writes


def test_existing_go_signal_with_missing_parts_is_not_reported_as_a_complete_upload():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        prefix, _, manifest, _ = PUBLISH["local_plan"](folder, root)
        client.data["main", prefix + "/manifest.json"] = manifest
        assert_stopped(lambda: upload(client, folder, root), "before all matching payloads")
        assert client.writes == []


def test_lost_manifest_response_after_receiver_assembly_does_not_recreate_the_batch():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        prefix, _, _, _ = PUBLISH["local_plan"](folder, root)
        client.lose_ack = prefix + "/manifest.json"
        client.after_manifest = lambda remote: assembled_receipt(remote, folder, root)
        assert upload(client, folder, root)["state"] == "assembled"
        assert client.writes.count(prefix + "/manifest.json") == 1
        assert not any(path.startswith(prefix + "/") for ref, path in client.data)


def test_previously_assembled_different_content_cannot_be_claimed_as_this_release():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        receipt = assembled_receipt(client, folder, root)
        receipt["files"][0]["sha256"] = "0" * 64
        path = PUBLISH["BATCH_ROOT"] + "/_receipts/" + folder.name + ".json"
        client.data["main", path] = json.dumps(receipt).encode()
        assert_stopped(lambda: upload(client, folder, root), "already assembled different content")
        assert not client.writes


def test_assembly_and_handoff_do_not_pretend_to_be_aws_deployment_proof():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); folder = prepare(root); client = FakeGitHub()
        receipt = assembled_receipt(client, folder, root)
        assert PUBLISH["release_status"](client, folder, root)["state"] == "awaiting_handoff"
        client.data["ops-evidence", receipt["handoff_path"]] = json.dumps({
            "status": "dispatched_unverified", "result_sha": "b" * 40, "deploy_run_id": "456"}).encode()
        state = PUBLISH["release_status"](client, folder, root)
        assert state["state"] == "dispatched_unverified" and state["deployment_verified"] is False
        assert state["result_sha"] == "b" * 40


def test_required_verifiers_receive_exact_source_sha_and_every_requested_data_key():
    calls = []
    checks = [("justhodl-one", "data/one.json"), ("justhodl-two", "data/two.json")]
    def push(args):
        calls.append(("push", args)); return 0
    def release(args):
        calls.append(("release", args)); return 0
    result = PUBLISH["verify_requested"]("c" * 40, checks, push=push, release=release)
    assert result["state"] == "requested_checks_verified"
    assert calls == [("push", ["c" * 40, "--wait", "0"]),
                     ("release", ["justhodl-one", "--commit", "c" * 40, "--data", "data/one.json"]),
                     ("release", ["justhodl-two", "--commit", "c" * 40, "--data", "data/two.json"])]


def test_pending_failed_or_missing_release_proof_never_returns_verified():
    checks = [("justhodl-one", "data/one.json")]
    def forbidden(_):
        raise AssertionError("release must wait for workflow verification")
    assert PUBLISH["verify_requested"]("d" * 40, checks, push=lambda _: 2, release=forbidden)["state"] == "awaiting_deployment"
    assert PUBLISH["verify_requested"]("d" * 40, checks, push=lambda _: 1, release=forbidden)["state"] == "deployment_failed"
    for code in (1, 2):
        state = PUBLISH["verify_requested"]("d" * 40, checks, push=lambda _: 0, release=lambda _: code)
        assert state["state"] == "awaiting_release_proof" and state["unverified"] == ["justhodl-one"]
