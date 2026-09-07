"""aws/shared/jh_registry.py -- engine registry (phase 2) + feature flags (phase 50)
+ pilot universe, for the JustHodl Intelligence Network.

The registry is PERSISTED in config/engine-registry.v1.json (shipped inside
every Lambda zip that needs it and mirrored to S3 by the bridge so pages can
read it) rather than hard-coded in engines. This module validates it on
load, so a malformed registry fails loudly at cold start instead of
silently dropping engines.

Feature flags come from config/jh-fusion-flags.json (defaults) overlaid by
the SSM parameter /justhodl/fusion/flags (a JSON object) when present, so a
flag can be flipped without a deploy. Shadow mode is the default: fusion
outputs are published for comparison and never feed the existing sizing
engine until FUSION_SHADOW_MODE is turned off deliberately.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from jhsignal import ENGINE_FAMILIES, EVIDENCE_CLUSTERS, CATEGORIES, HORIZONS, JHSignalError

CRITICALITIES = ("NONCRITICAL", "IMPORTANT", "CRITICAL")
STATUSES = ("active", "shadow", "disabled")

_REGISTRY_REQUIRED = (
    "engine_id", "engine_name", "engine_family", "description", "owner", "producer",
    "artifact", "version", "status", "criticality", "default_horizon",
    "expected_update_frequency", "freshness_ttl_seconds", "half_life_days",
    "entities_supported", "dependencies", "historical_backtest_available",
    "calibration_available", "hard_veto_capable", "soft_veto_capable",
    "signal_schema_version", "adapter", "signal_types",
)

DEFAULT_FLAGS: Dict[str, Any] = {
    "FUSION_SIGNAL_BUS_ENABLED": True,
    "FUSION_STATE_DDB_ENABLED": True,
    "FUSION_ARCHIVE_ENABLED": True,
    "FUSION_VETO_ENABLED": True,
    "FUSION_REGIME_WEIGHTING_ENABLED": True,
    "FUSION_RELIABILITY_WEIGHTING_ENABLED": True,
    "FUSION_CORRELATION_ADJUST_ENABLED": True,
    "FUSION_SHADOW_MODE": True,
    "FUSION_UI_ENABLED": True,
    "FUSION_EMERGENT_ENTITIES_ENABLED": False,
    "FUSION_MAX_PROPAGATION_DEPTH": 3,
    "FUSION_PER_SIGNAL_EVENTS": "changes_only",
    "FUSION_MAX_ENTITIES": 400,
    "FUSION_DISABLED_ENGINES": [],
    "FUSION_DISABLED_ENTITIES": []
}


def _candidate_paths(name: str) -> List[Path]:
    here = Path(__file__).resolve().parent
    return [
        here / name,                                    # bundled next to the Lambda code
        here / "config" / name,
        here.parents[1] / "config" / name,              # aws/shared -> repo/config
        Path(os.getcwd()) / "config" / name,
        Path(os.environ.get("JH_CONFIG_DIR", "/var/task")) / name,
    ]


def _load_json_file(name: str) -> Dict[str, Any]:
    for p in _candidate_paths(name):
        try:
            if p.is_file():
                return json.loads(p.read_text())
        except Exception as exc:  # a corrupt file must not look like "absent"
            raise JHSignalError([f"{p}: {exc}"])
    raise JHSignalError([f"config file {name} not found in {[str(p) for p in _candidate_paths(name)]}"])


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
class EngineRegistry:
    def __init__(self, doc: Dict[str, Any]):
        problems = validate_registry(doc)
        if problems:
            raise JHSignalError(problems)
        self.doc = doc
        self.engines: Dict[str, Dict[str, Any]] = {e["engine_id"]: e for e in doc["engines"]}

    @classmethod
    def load(cls, name: str = "engine-registry.v1.json") -> "EngineRegistry":
        return cls(_load_json_file(name))

    def get(self, engine_id: str) -> Dict[str, Any]:
        if engine_id not in self.engines:
            raise JHSignalError([f"engine {engine_id!r} not in registry"])
        return self.engines[engine_id]

    def active(self, disabled: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        dis = set(disabled or [])
        return [e for e in self.doc["engines"] if e["status"] in ("active", "shadow") and e["engine_id"] not in dis]

    def by_family(self, family: str) -> List[Dict[str, Any]]:
        return [e for e in self.doc["engines"] if e["engine_family"] == family]

    def cluster_for(self, engine_id: str, signal_type: str) -> str:
        e = self.get(engine_id)
        st = e["signal_types"].get(signal_type)
        if not st:
            raise JHSignalError([f"engine {engine_id} does not declare signal_type {signal_type}"])
        return st["cluster"]

    def category_for(self, engine_id: str, signal_type: str) -> str:
        return self.get(engine_id)["signal_types"][signal_type]["category"]

    def critical_engines(self) -> List[str]:
        return [e["engine_id"] for e in self.doc["engines"] if e["criticality"] == "CRITICAL" and e["status"] != "disabled"]


def validate_registry(doc: Any) -> List[str]:
    p: List[str] = []
    if not isinstance(doc, dict):
        return ["registry must be an object"]
    if doc.get("schema_version") != "ENGINE-REGISTRY-1.0":
        p.append("schema_version must be ENGINE-REGISTRY-1.0")
    engines = doc.get("engines")
    if not isinstance(engines, list) or not engines:
        return p + ["engines must be a non-empty list"]
    seen = set()
    for i, e in enumerate(engines):
        tag = f"engines[{i}]"
        if not isinstance(e, dict):
            p.append(f"{tag} must be an object"); continue
        for k in _REGISTRY_REQUIRED:
            if k not in e:
                p.append(f"{tag} missing {k}")
        eid = e.get("engine_id")
        if eid in seen:
            p.append(f"{tag} duplicate engine_id {eid}")
        seen.add(eid)
        if e.get("engine_family") not in ENGINE_FAMILIES:
            p.append(f"{tag} engine_family {e.get('engine_family')!r} not in {ENGINE_FAMILIES}")
        if e.get("criticality") not in CRITICALITIES:
            p.append(f"{tag} criticality {e.get('criticality')!r} invalid")
        if e.get("status") not in STATUSES:
            p.append(f"{tag} status {e.get('status')!r} invalid")
        if e.get("default_horizon") not in HORIZONS:
            p.append(f"{tag} default_horizon invalid")
        if not isinstance(e.get("freshness_ttl_seconds"), int) or e.get("freshness_ttl_seconds", 0) < 60:
            p.append(f"{tag} freshness_ttl_seconds must be int >= 60")
        hl = e.get("half_life_days")
        if not isinstance(hl, (int, float)) or hl <= 0:
            p.append(f"{tag} half_life_days must be > 0")
        if not isinstance(e.get("artifact"), str) or not e.get("artifact", "").startswith("data/"):
            p.append(f"{tag} artifact must be an S3 key under data/")
        st = e.get("signal_types")
        if not isinstance(st, dict) or not st:
            p.append(f"{tag} signal_types must be a non-empty object")
        else:
            for name, spec in st.items():
                if not isinstance(spec, dict) or spec.get("cluster") not in EVIDENCE_CLUSTERS:
                    p.append(f"{tag}.signal_types.{name} cluster invalid")
                elif spec.get("category") not in CATEGORIES:
                    p.append(f"{tag}.signal_types.{name} category invalid")
    return p


# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------
_flag_cache: Optional[Dict[str, Any]] = None


def load_flags(*, ssm_client=None, use_ssm: bool = True, force: bool = False) -> Dict[str, Any]:
    """Defaults <- config/jh-fusion-flags.json <- SSM /justhodl/fusion/flags.

    Never raises on an SSM problem (flags are best-effort); a malformed SSM
    document is ignored and logged, the file defaults stand.
    """
    global _flag_cache
    if _flag_cache is not None and not force:
        return _flag_cache
    flags = dict(DEFAULT_FLAGS)
    try:
        flags.update(_load_json_file("jh-fusion-flags.json").get("flags") or {})
    except JHSignalError:
        pass
    if use_ssm and os.environ.get("JH_FLAGS_SSM", "1") == "1":
        try:
            if ssm_client is None:
                import boto3
                ssm_client = boto3.client("ssm", region_name=os.environ.get("AWS_REGION", "us-east-1"))
            resp = ssm_client.get_parameter(Name=os.environ.get("JH_FLAGS_PARAM", "/justhodl/fusion/flags"))
            override = json.loads(resp["Parameter"]["Value"])
            if isinstance(override, dict):
                flags.update(override)
                flags["_ssm_override"] = sorted(override.keys())
        except Exception as exc:  # parameter absent or unreadable -> defaults
            flags["_ssm_override"] = f"none ({type(exc).__name__})"
    _flag_cache = flags
    return flags


def flag(name: str, default: Any = None) -> Any:
    return load_flags().get(name, default)


# ---------------------------------------------------------------------------
# Pilot universe
# ---------------------------------------------------------------------------
def load_universe(name: str = "jh-fusion-universe.json") -> Dict[str, Any]:
    doc = _load_json_file(name)
    ents = doc.get("entities")
    if not isinstance(ents, list) or not ents:
        raise JHSignalError(["universe.entities must be a non-empty list"])
    for e in ents:
        if not isinstance(e, dict) or ":" not in str(e.get("entity_id", "")):
            raise JHSignalError([f"universe entity malformed: {e!r}"])
    return doc


def universe_index(doc: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """symbol -> entity spec (entity_type, entity_id, aliases...)."""
    out: Dict[str, Dict[str, Any]] = {}
    for e in doc["entities"]:
        et, sym = e["entity_id"].split(":", 1)
        spec = dict(e); spec["entity_type"] = et; spec["symbol"] = sym
        out[sym] = spec
        for a in e.get("aliases") or []:
            out[str(a).upper()] = spec
    return out
