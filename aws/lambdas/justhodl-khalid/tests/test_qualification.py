"""Offline parity, publication identity and unresolved-specification checks."""
import ast
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import types
from qualification import publish_qualification, BACKEND_RULES, REQUESTED_RULES
from scoring import score_candidate
from test_scoring import base_row, valid_khalid_risk_artifact

SOURCE = Path(__file__).resolve().parents[1] / "source"
NOW = datetime(2026, 10, 1, 4, tzinfo=timezone.utc)


def predecessor_scoring():
    tree = ast.parse((SOURCE / "scoring.py").read_text(encoding="utf-8"))
    tree.body = [n for n in tree.body if not (isinstance(n, ast.ImportFrom) and n.module == "qualification")]
    removed = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for i in range(len(node.keys)-1, -1, -1):
                if isinstance(node.keys[i], ast.Constant) and node.keys[i].value == "backend_gate_observation":
                    del node.keys[i]; del node.values[i]; removed += 1
    assert removed == 1
    assert hashlib.sha256(ast.dump(tree, include_attributes=False).encode()).hexdigest() == "8042b10740ffad3d7fd4c566418c17f11b5e30f1cdd82529f699073087a1a6b8"
    module = types.ModuleType("pre_qualification_scoring")
    exec(compile(tree, str(SOURCE / "scoring.py"), "exec"), module.__dict__)
    return module


def publication(action="READY_TO_SNIPE", fresh=True, count=1):
    raw = base_row(); raw["entry_triggered"] = True
    scored = score_candidate(raw, asset_class="STOCK", risk_allows_entries=True)
    rows = []
    native = []
    for i in range(count):
        r = copy.deepcopy(scored); r["ticker"] = "TEST" + (str(i) if i else "")
        r["backend_gate_observation"]["ticker"] = r["ticker"]
        native.append(r)
        row = copy.deepcopy(r); row["action"] = action; rows.append(row)
    output = {"generated_at": NOW.isoformat(), "opportunity_radar": rows,
              "source_health": [{"name": "fortress", "key": "data/fortress.json", "status": "FRESH" if fresh else "STALE",
                                 "as_of": NOW.isoformat(), "critical": True, "max_age_h": 26, "s3_modified_at": None}]}
    output["source_health"].append({**output["source_health"][0], "name": "khalid_risk", "key": "data/khalid-risk.json"})
    before = copy.deepcopy(output)
    publish_qualification(output, native)
    compare = copy.deepcopy(output); del compare["qualification_evidence"]
    assert compare == before
    return output


def test_scoring_every_preexisting_statement_and_decision_preserved():
    baseline = predecessor_scoring()
    changes = [{}, {"entry_triggered": True}, {"volume_dryup": None}, {"volume_dryup": .81},
               {"vs_ema200_pct": 2}, {"rsi": 46}, {"confidence": .59}, {"dilution_yoy_pct": 11},
               {"adv_usd_20d": 100}, {"flow_score": None, "industry_inflow_major": False},
               {"higher_lows": 0, "ad_divergence": False}, {"trade_plan": {}}, {"confidence": "unknown"}]
    for asset in ["STOCK", "ETF", "BOND", "CRYPTO", "COMMODITY"]:
        for allowed in [True, False]:
            for patch in changes:
                raw = {**base_row(), **patch}; before = copy.deepcopy(raw)
                a = baseline.score_candidate(raw, asset_class=asset, risk_allows_entries=allowed)
                b = score_candidate(raw, asset_class=asset, risk_allows_entries=allowed)
                del b["backend_gate_observation"]
                assert a == b and raw == before


def test_full_handler_parity_only_additive_publication():
    import lambda_function as candidate
    text = (SOURCE / "lambda_function.py").read_text(encoding="utf-8")
    text = text.replace("from qualification import publish_qualification\n", "").replace("    publish_qualification(output, ranked)\n", "")
    text = text.replace('        "risk_authority_diagnostics": __import__("risk_diagnostics").project(risk_artifact),\n', "")
    assert hashlib.sha256(text.encode()).hexdigest() == "6af9e26abfe5a798e51e969cb9aeeff751248d992b54cacc09acf003c207caad"
    module = types.ModuleType("pre_qualification_handler"); module.__file__ = str(SOURCE / "lambda_function.py")
    exec(compile(text, module.__file__, "exec"), module.__dict__)
    for risk in [valid_khalid_risk_artifact(NOW.isoformat()), {}]:
        for trigger in [True, False]:
            raw = base_row(); raw["entry_triggered"] = trigger
            feeds = {"fortress": {"board": [raw], "etfs": [], "ledger": []}, "khalid_risk": risk}
            metas = {k: {"last_modified": NOW.isoformat(), "error": None} for k in feeds}
            a = module.build_output(feeds, metas, NOW, []); b = candidate.build_output(feeds, metas, NOW, [])
            del b["risk_authority_diagnostics"]
            del b["qualification_evidence"]; assert a == b


def test_final_lifecycle_state_wins_and_missing_sources_unavailable():
    for action, expected in [("READY_TO_SNIPE", "PASS"), ("TRACKING", "FAIL"), ("BUILDING_BASE", "FAIL")]:
        output = publication(action)
        b = output["qualification_evidence"]["rows"][0]["existing_backend_qualification"]
        assert b["status"] == expected and b["reported_action"] == action and b["scoring_action"] == "READY_TO_SNIPE"
    assert publication(fresh=False)["qualification_evidence"]["rows"][0]["existing_backend_qualification"]["status"] == "UNAVAILABLE"
    missing = publication(); native = missing["opportunity_radar"]
    missing["source_health"] = [h for h in missing["source_health"] if h["name"] != "khalid_risk"]
    publish_qualification(missing, native)
    assert missing["qualification_evidence"]["rows"][0]["existing_backend_qualification"]["status"] == "UNAVAILABLE"
    output = publication(); publish_qualification(output, [])
    assert output["qualification_evidence"]["rows"][0]["existing_backend_qualification"]["status"] == "UNAVAILABLE"


def test_requested_never_complete_no_fabricated_clocks_and_full_definitions():
    output = publication(); p = output["qualification_evidence"]
    assert len(p["backend_definitions"]) == len(BACKEND_RULES) == 13
    assert len(p["requested_criteria"]) == len(REQUESTED_RULES) == 23
    for row in p["rows"]:
        q = row["requested_strategy_qualification"]
        assert q["status"] == "UNRESOLVED" and q["complete"] is False
        assert row["artifact_revision"] == p["revision"]
        assert q["clocks"]["available_at"] is None and q["clocks"]["effective_at"] is None
    assert all(c["status"] == "UNRESOLVED" and c["value"] is None for c in p["requested_criteria"])
    assert "SPY is not silently substituted" in next(c["definition"] for c in p["requested_criteria"] if c["id"] == "resilience")
    assert "63 native sessions" in next(c["definition"] for c in p["requested_criteria"] if c["id"] == "support_3m")


def test_payload_budget_shared_definitions_and_revision_changes():
    a = publication(); b = publication("TRACKING")
    assert a["qualification_evidence"]["revision"] != b["qualification_evidence"]["revision"]
    p = publication(count=1000)["qualification_evidence"]
    assert len(json.dumps(p, separators=(",", ":")).encode()) < 3_500_000


if __name__ == "__main__":
    print(json.dumps(publication(), indent=2))
