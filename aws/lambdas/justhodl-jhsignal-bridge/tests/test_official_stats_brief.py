"""official_stats_brief fusion leg. Registry merge must land in all three copies."""
import json
from jh_fixtures import REPO, ts
import jhsignal as J


def _spec(registry):
    rows = [e for e in registry.doc["engines"] if e["engine_id"] == "official_stats_brief"]
    assert len(rows) == 1, "official_stats_brief must appear exactly once — merge config/engine-registry.v1.json into both lambda copies"
    return rows[0]


def _brief(gdp="4.4164", curve="0.89", status="LIVE", mode="official_stats"):
    return {
        "schema": "brief-1.0",
        "mode": mode,
        "status": status,
        "generated_at": ts(1),
        "source": "justhodl-brief-compiler",
        "fields": {
            "gdpnow": gdp,
            "gdpnow_date": "2026-07-01",
            "t10y3m": curve,
            "t10y3m_date": "2026-09-11",
            "nowcast_status": "LIVE",
        },
    }


def test_official_stats_row_shadow_market_and_no_drift(registry):
    spec = _spec(registry)
    assert spec["engine_family"] == "MARKET" and spec["status"] == "shadow"
    assert spec["criticality"] == "NONCRITICAL"
    assert spec["adapter"] == "official_stats_brief"
    assert "official_stats_nowcast" in spec["signal_types"]
    assert "official_stats_brief" not in registry.critical_engines()
    frag = json.loads((REPO / "config" / "brief-engine-official-stats.json").read_text())
    assert frag == spec
    cfg = (REPO / "config" / "engine-registry.v1.json").read_bytes()
    for fn in ("justhodl-jhsignal-bridge", "justhodl-jh-fusion"):
        assert (REPO / "aws" / "lambdas" / fn / "source" / "engine-registry.v1.json").read_bytes() == cfg, fn


def test_official_stats_live_gdpnow_is_one_market_signal(registry, universe, now):
    from jh_adapters import adapter_for
    res = adapter_for(_spec(registry), universe, now=now).parse_existing_output(_brief(), {"last_modified": ts(0)})
    assert res.source_status == "OK" and len(res.signals) == 1 and res.skipped == 0, res.diagnostics
    s = res.signals[0]
    assert s["entity_id"] == "market:US_EQUITY"
    assert s["signal_type"] == "official_stats_nowcast"
    assert s["category"] == "price_confirmation"
    assert abs(s["score"] - ((4.4164 - 2.0) / 4.0)) < 1e-6
    assert abs(s["confidence"] - 0.7) < 1e-9
    assert J.validate(s) == []


def test_official_stats_held_or_missing_gdp_is_not_a_zero(registry, universe, now):
    from jh_adapters import adapter_for
    spec = _spec(registry)
    for doc in (
        _brief(status="HELD"),
        _brief(mode="plumbing"),
        {"schema": "brief-1.0", "mode": "official_stats", "status": "LIVE", "fields": {}},
    ):
        res = adapter_for(spec, universe, now=now).parse_existing_output(doc, {"last_modified": ts(0)})
        assert res.signals == [], (doc.get("status"), doc.get("mode"), res.diagnostics)


def test_canary_context_does_not_change_official_stats_signals(registry, universe, now, monkeypatch):
    """Real producer → adapter; synthetic warehouse only, legacy gate preserved."""
    import brief_compiler
    import brief_contract
    from jh_adapters import adapter_for
    from datetime import datetime, timedelta
    import socket
    import uuid
    monkeypatch.setattr(uuid, "uuid4", lambda: uuid.UUID("00000000-0000-4000-8000-000000000001"))
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: (_ for _ in ()).throw(AssertionError('network forbidden')))
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return now
    monkeypatch.setattr(brief_contract, 'datetime', Clock)
    monkeypatch.setattr(brief_compiler, '_put', lambda *a: None)
    monkeypatch.setattr(brief_compiler, '_now', lambda: now)
    for legacy_asof in [now.isoformat(), (now - timedelta(days=400)).isoformat()]:
        legacy = {'generated_at': legacy_asof, 'series': {'atlantafed': {'last': {'GDPNOW': '4.4164'}}}}
        canary = {'schema_version': '2.0', 'generated_at': now.isoformat(),
                  'GDPNOW': {'value': -100, 'as_of': now.isoformat()}}
        monkeypatch.setattr(brief_compiler, '_load', lambda s3, key: (
            legacy if key == 'data/fed-nowcast-join.json' else canary, legacy_asof, None))
        result = brief_compiler.compile_official_stats(None)
        assert result['status'] == ('LIVE' if legacy_asof == now.isoformat() else 'HELD')
        assert result['evidence']['measurements']['GDPNOW']['reported_value'] == -100
        predecessor_shape = dict(result)
        predecessor_shape.pop('evidence')
        adapter = adapter_for(_spec(registry), universe, now=now)
        before = adapter.parse_existing_output(predecessor_shape, {'last_modified': now.isoformat()})
        after = adapter.parse_existing_output(result, {'last_modified': now.isoformat()})
        assert after.signals == before.signals
        if result['status'] == 'HELD':
            assert after.signals == []
        else:
            assert len(after.signals) == 1
            assert abs(after.signals[0]['score'] - ((4.4164 - 2.0) / 4.0)) < 1e-6
