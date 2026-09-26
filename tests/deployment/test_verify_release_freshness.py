"""Publication checks must work through the reviewed-artifact public gateway."""
import importlib.util
import json
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr
from unittest.mock import patch
import io

spec = importlib.util.spec_from_file_location("release_freshness", Path(__file__).resolve().parents[2] / "scripts/verify_release.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
NOW = 1789664400  # 2026-09-17T17:00:00Z


def check(payload, headers=None):
    return module.data_freshness(headers or {}, json.dumps(payload).encode(), 26, now=NOW)


def test_gateway_without_headers_uses_real_json_publication():
    fresh, detail = check({"generated_at": "2026-09-17T16:22:16Z", "quality": {"status": "incomplete"}})
    assert fresh and "quality=incomplete" in detail


def test_last_good_rewrite_does_not_refresh_old_observations():
    fresh, _ = check({"generated_at": "2026-09-10T12:00:00Z"}, {"Last-Modified": "Thu, 17 Sep 2026 16:00:00 GMT"})
    assert not fresh
    fresh, _ = check({"generated_at": "2026-09-17T16:00:00Z", "meta": {"generated_at": "2026-09-10T12:00:00Z"}})
    assert not fresh


def test_bad_quality_and_invalid_publication_cannot_pass():
    cases = [{}, {"generated_at": "broken"}, {"generated_at": "2026-09-17"},
             {"generated_at": "2026-09-19T12:00:00Z"}, {"ok": False}]
    cases += [{"generated_at": "2026-09-17T16:00:00Z", "quality": {"status": s}}
              for s in ("stale", "invalid", "unavailable")]
    for case in cases:
        try:
            check(case)
        except ValueError:
            continue
        raise AssertionError(f"unusable payload passed: {case}")


def test_legacy_header_and_nested_metadata_are_supported():
    assert check({}, {"Last-Modified": "Thu, 17 Sep 2026 16:00:00 GMT"})[0]
    assert check({"meta": {"generated_at": "2026-09-17T16:00:00+00:00"}})[0]


def test_native_contract_rejects_fresh_predecessors_and_wrong_versions():
    for contract in (None, "fifx-vol-research.v0", True, {"contract": "fifx-vol-research.v1"}):
        packet = {"generated_at": "2026-09-17T16:00:00Z", "contract": contract}
        try:
            module.data_freshness({}, json.dumps(packet).encode(), 26, now=NOW, expected_contract="fifx-vol-research.v1")
        except ValueError as exc:
            assert "native output contract mismatch" in str(exc)
        else:
            raise AssertionError("Predecessor or malformed contract passed")
    packet["contract"] = "fifx-vol-research.v1"
    assert module.data_freshness({}, json.dumps(packet).encode(), 26, now=NOW, expected_contract=packet["contract"])[0]
    packet["generated_at"] = "2026-09-10T16:00:00Z"
    assert not module.data_freshness({}, json.dumps(packet).encode(), 26, now=NOW, expected_contract=packet["contract"])[0]


def test_release_command_cannot_promote_a_fresh_old_packet_to_native_proof():
    receipt = {"deployed_at": "2026-09-17T16:30:00Z", "commit": "a"*40,
               "code_sha256": "test", "zip_bytes": 123, "source": {}}
    packet = {"generated_at": "2026-09-17T16:00:00Z"}
    def fetch(key):
        return {}, json.dumps(packet if key == "data/fifx-vol.json" else receipt).encode()
    args = ["justhodl-fifx-vol-migration", "--commit", "a"*40, "--data", "data/fifx-vol.json", "--data-contract", "fifx-vol-research.v1"]
    out = io.StringIO()
    with patch.object(module, "fetch", side_effect=fetch), patch.object(module.time, "time", return_value=NOW), redirect_stdout(out):
        assert module.main(args) == 1
        assert "NOT VERIFIED" in out.getvalue()
        packet["contract"] = "fifx-vol-research.v1"
        assert module.main(args) == 0
        assert "replay remain separate" in out.getvalue()


def test_contract_only_flag_cannot_be_silently_ignored():
    for args in (["engine", "--data-contract", "v1"], ["engine", "--data", "data/x.json", "--data-contract", " "]):
        with patch.object(module, "fetch") as fetch, redirect_stderr(io.StringIO()):
            try:
                module.main(args)
            except SystemExit as exc:
                assert exc.code == 2
            else:
                raise AssertionError("Invalid contract check reached networking")
            fetch.assert_not_called()
