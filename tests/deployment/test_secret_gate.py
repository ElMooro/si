"""The secret gate reports only locations, including embedded URL credentials."""
import hashlib
import runpy
from pathlib import Path

scan = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/check_secrets.py"))["findings"]


def test_retired_secret_matches_embedded_url_without_exposing_value():
    fake = "exampleRetiredCredentialValue123456789"
    result = scan("url = 'https://provider.invalid?apikey=" + fake + "'", {hashlib.sha256(fake.encode()).hexdigest()})
    assert result == [(1, "retired-provider-credential")]
    assert fake not in repr(result)


def test_managed_configuration_and_placeholder_are_allowed():
    assert scan('managed_secret(("FRED_KEY",), ("/justhodl/fred/api-key",))', set()) == []
