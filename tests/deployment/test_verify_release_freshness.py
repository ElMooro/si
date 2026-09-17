"""Publication checks must work through the reviewed-artifact public gateway."""
import importlib.util
import json
from pathlib import Path

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
