"""Dependency-free governed evidence fusion read model.

This module does not discover inputs from a manifest and does not emit an
aggregate verdict. It validates an explicit allowlist, creates canonical
packets, removes stale evidence from active use, deduplicates correlated source
families, and surfaces disagreements/vetoes with complete lineage.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any

VERSION="1.0.0"
MAX_FUTURE_SKEW_HOURS=5/60


def number(value: Any) -> float | None:
    if value is None or isinstance(value,bool): return None
    try:
        parsed=float(value); return parsed if math.isfinite(parsed) else None
    except (TypeError,ValueError): return None


def get(payload: Any,path: str) -> Any:
    value=payload
    for part in path.split("."):
        if not isinstance(value,dict): return None
        value=value.get(part)
    return value


def parse_time(value: Any) -> datetime | None:
    if not value: return None
    try:
        dt=datetime.fromisoformat(str(value).replace("Z","+00:00"))
    except (TypeError,ValueError):
        try: dt=datetime.strptime(str(value)[:10],"%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (TypeError,ValueError): return None
    if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def age_hours(value: Any,now: datetime) -> float | None:
    dt=parse_time(value)
    return round((now.astimezone(timezone.utc)-dt).total_seconds()/3600,2) if dt else None


def contract_error(spec: dict,payload: dict) -> str | None:
    contract=spec.get("contract") or {}
    if not isinstance(payload,dict) or not payload: return "artifact missing or empty"
    missing=[path for path in contract.get("required") or [] if get(payload,path) is None]
    if missing: return "missing required fields: "+", ".join(missing)
    alternatives=contract.get("required_any") or []
    if alternatives and not any(get(payload,path) is not None for path in alternatives): return "missing all alternative fields: "+", ".join(alternatives)
    for path,expected in (contract.get("types") or {}).items():
        value=get(payload,path)
        if value is None: continue
        valid=(expected=="number" and isinstance(value,(int,float)) and not isinstance(value,bool) and number(value) is not None) or (expected=="string" and isinstance(value,str) and bool(value.strip())) or (expected=="dict" and isinstance(value,dict)) or (expected=="list" and isinstance(value,list)) or (expected=="boolean" and isinstance(value,bool))
        if not valid: return f"{path} must be {expected}"
    for path,limits in (contract.get("ranges") or {}).items():
        value=number(get(payload,path))
        if value is None or not limits[0]<=value<=limits[1]: return f"{path} must be finite and within {limits[0]}..{limits[1]}"
    if spec.get("adapter")=="settlement_fails":
        treasury=get(payload,"treasury") or {}
        if treasury.get("scope")!="US_TREASURY_INCLUDING_TIPS" or treasury.get("unit")!="USD_bn_par":
            return "treasury scope/unit contract invalid"
        if treasury.get("complete") is not True:
            return "treasury completeness must be true"
        ftd=number(treasury.get("ftd_bn")); ftr=number(treasury.get("ftr_bn")); gross=number(treasury.get("gross_bn"))
        if None in (ftd,ftr,gross) or abs(ftd+ftr-gross)>0.02:
            return "treasury gross must equal finite ftd + ftr"
    return None


def graph_cycles(registry: dict) -> list[list[str]]:
    graph={str(k):list(v or []) for k,v in (registry.get("known_graph_nodes") or {}).items()}
    sources=registry.get("sources") or []
    for source in sources:
        graph.setdefault(source["id"],[]).extend(source.get("depends_on") or [])
    output_id=str(registry.get("output_id","engine_fusion"))
    graph.setdefault(output_id,[]).extend(str(source["id"]) for source in sources)
    cycles=[]; state={}; stack=[]
    def visit(node: str):
        if state.get(node)==1:
            start=stack.index(node) if node in stack else 0
            cycle=stack[start:]+[node]
            if cycle not in cycles: cycles.append(cycle)
            return
        if state.get(node)==2: return
        state[node]=1; stack.append(node)
        for dep in graph.get(node,[]): visit(str(dep))
        stack.pop(); state[node]=2
    for node in list(graph): visit(node)
    return cycles


def _timestamp(spec: dict,payload: dict,meta: dict) -> Any:
    for path in spec.get("timestamp_paths") or []:
        value=get(payload,path)
        if value is not None: return value
    return meta.get("last_modified")


def _direction(score: float,policy: dict) -> str:
    thresholds=policy.get("thresholds") or {}
    if score>=number(thresholds.get("risk_off") or 65): return "RISK_OFF"
    if score<=number(thresholds.get("risk_on") or 35): return "RISK_ON"
    return "NEUTRAL"


def adapt(spec: dict,payload: dict,policy: dict) -> tuple[float|None,str|None,dict]:
    adapter=spec.get("adapter"); detail={}
    if adapter=="risk_gate":
        composite=number(payload.get("composite")); posture=str(payload.get("posture") or "").upper()
        score=max(0,min(100,50-composite*25)) if composite is not None else None
        direction={"RISK_ON":"RISK_ON","NEUTRAL":"NEUTRAL","RISK_OFF":"RISK_OFF","SEVERE":"RISK_OFF"}.get(posture)
        detail={"posture":posture,"sizing_multiplier":number(payload.get("sizing_multiplier"))}
    elif adapter=="crisis":
        score=number(payload.get("master_crisis_score")); direction=_direction(score,policy) if score is not None else None; detail={"defcon_level":number(payload.get("defcon_level"))}
    elif adapter=="settlement_fails":
        treasury=get(payload,"treasury") or {}; score=number(treasury.get("score")); direction={"CALM":"RISK_ON","ELEVATED":"NEUTRAL","STRESS":"RISK_OFF","CRISIS":"RISK_OFF"}.get(str(treasury.get("regime") or "").upper()); detail={"regime":treasury.get("regime"),"ftd_bn":number(treasury.get("ftd_bn")),"ftr_bn":number(treasury.get("ftr_bn")),"gross_bn":number(treasury.get("gross_bn")),"unit":treasury.get("unit")}
    elif adapter=="credit":
        score=number(payload.get("composite")); score=score if score is not None else number(payload.get("composite_score")); direction=_direction(score,policy) if score is not None else None
    elif adapter=="eurodollar":
        score=number(payload.get("composite_score")); direction=_direction(score,policy) if score is not None else None; detail={"severity":payload.get("severity") or payload.get("regime")}
    elif adapter=="dollar":
        score=number(payload.get("dollar_pressure")); direction=_direction(score,policy) if score is not None else None; detail={"regime":payload.get("regime")}
    elif adapter=="liquidity":
        regime=str(payload.get("regime") or "").upper(); score={"EXPANDING":15.0,"NEUTRAL":40.0,"CONTRACTING":70.0}.get(regime); direction={"EXPANDING":"RISK_ON","NEUTRAL":"NEUTRAL","CONTRACTING":"RISK_OFF"}.get(regime); detail={"regime":regime,"global_impulse_13w_pct":number(payload.get("global_impulse_13w_pct"))}
    elif adapter=="cycle":
        synthesis=get(payload,"synthesis") or {}; raw=number(synthesis.get("score")); score=max(0,min(100,50-raw)) if raw is not None else None; posture=str(synthesis.get("posture") or "").upper(); direction="RISK_OFF" if posture=="RISK-OFF" else "RISK_ON" if posture=="RISK-ON" else _direction(score,policy) if score is not None else None; detail={"posture":posture,"raw_score":raw}
    elif adapter=="fifx_vol":
        state=str(get(payload,"migration.state") or "").upper(); score={"CALM":15.0,"UPSTREAM_BREWING":60.0,"MIGRATING":75.0,"BROAD_STRESS":90.0}.get(state); direction=_direction(score,policy) if score is not None else None; detail={"state":state}
    elif adapter=="bond_warroom":
        values=[number(get(payload,path)) for path in ("heartbeat.score","equity_risk.score","eurodollar_shortage.score")]; values=[v for v in values if v is not None]; score=max(values) if values else None; direction=_direction(score,policy) if score is not None else None; detail={"heartbeat":number(get(payload,"heartbeat.score")),"equity_transmission":number(get(payload,"equity_risk.score")),"shortage":number(get(payload,"eurodollar_shortage.score"))}
    else: return None,None,{"excluded":True}
    return score,direction,detail


def _evidence_id(spec: dict,as_of: Any,score: Any,direction: Any,detail: dict) -> str:
    canonical=json.dumps([spec["id"],spec["artifact"],as_of,score,direction,detail],sort_keys=True,separators=(",",":"),allow_nan=False)
    return "ev1:"+hashlib.sha256(canonical.encode()).hexdigest()[:24]


def make_packet(spec: dict,payload: dict,meta: dict,policy: dict,now: datetime) -> tuple[dict,dict]:
    as_of=_timestamp(spec,payload,meta); age=age_hours(as_of,now); error=meta.get("error") or contract_error(spec,payload)
    if meta.get("error") or not payload: freshness="MISSING"
    elif error: freshness="INVALID"
    elif age is None: freshness="UNKNOWN"; error="no parseable timestamp"
    elif age < -MAX_FUTURE_SKEW_HOURS: freshness="INVALID"; error=f"timestamp is {abs(age):.1f}h in the future"
    elif age>float(spec["max_age_hours"]): freshness="STALE"; error=f"{age:.1f}h old; SLA {spec['max_age_hours']}h"
    else: freshness="FRESH"
    score,direction,detail=adapt(spec,payload,policy) if not spec.get("exclude_from_scoring") and not error else (None,None,{"excluded":bool(spec.get("exclude_from_scoring"))})
    active=freshness=="FRESH" and score is not None and direction is not None and not spec.get("exclude_from_scoring")
    packet={"evidence_id":_evidence_id(spec,as_of,score,direction,detail),"source_id":spec["id"],"subject":"GLOBAL","domain":spec["domain"],"direction":direction,"score":round(score,2) if score is not None else None,"confidence":number(spec.get("confidence")),"freshness":freshness,"age_h":age,"as_of":as_of,"provenance":{"artifact":spec["artifact"],"producer":spec["producer"],"adapter":spec.get("adapter"),"registry_version":VERSION},"ancestry":[{"id":dep,"relationship":"declared_upstream"} for dep in spec.get("depends_on") or []],"source_family":spec["source_family"],"evidence_level":spec["evidence_level"],"role":spec["role"],"independence_eligible":bool(spec.get("independence_eligible")),"active":active,"detail":detail,"error":error}
    trace={"source_id":spec["id"],"artifact":spec["artifact"],"freshness":freshness,"as_of":as_of,"age_h":age,"max_age_h":spec["max_age_hours"],"active":active,"error":error,"role":spec["role"]}
    return packet,trace


def dedupe_packets(packets: list[dict]) -> tuple[list[dict],list[dict]]:
    groups={}
    for packet in packets: groups.setdefault((packet["subject"],packet["domain"],packet["source_family"]),[]).append(packet)
    selected=[]; dropped=[]
    role_rank={"root_evidence":3,"synthesized_view":2,"consumer_view":1}
    for key,items in groups.items():
        ranked=sorted(items,key=lambda p:(role_rank.get(p.get("role"),0),number(p.get("confidence")) or 0,-(number(p.get("age_h")) or 0)),reverse=True)
        selected.append(ranked[0])
        for duplicate in ranked[1:]: dropped.append({"evidence_id":duplicate["evidence_id"],"source_id":duplicate["source_id"],"kept_evidence_id":ranked[0]["evidence_id"],"key":{"subject":key[0],"domain":key[1],"source_family":key[2]},"reason":"correlated source-family duplicate"})
    return selected,dropped


def _disagreements(packets: list[dict]) -> list[dict]:
    by_domain={}
    for p in packets: by_domain.setdefault(p["domain"],[]).append(p)
    out=[]
    for domain,items in by_domain.items():
        directions={p["direction"] for p in items}
        if "RISK_ON" in directions and "RISK_OFF" in directions:
            out.append({"domain":domain,"directions":sorted(directions),"evidence_ids":[p["evidence_id"] for p in items],"sources":[p["source_id"] for p in items],"resolution":"UNRESOLVED_VISIBLE_CONFLICT"})
    return out


def _subscriptions(config: dict,packets: list[dict]) -> dict:
    output={}
    for consumer,spec in (config.get("subscriptions") or {}).items():
        selected=[p for p in packets if p["source_id"] in set(spec.get("sources") or []) and p["domain"] in set(spec.get("domains") or [])]
        output[consumer]=[{"evidence_id":p["evidence_id"],"source_id":p["source_id"],"domain":p["domain"],"direction":p["direction"],"score":p["score"],"confidence":p["confidence"],"as_of":p["as_of"],"policy":spec.get("policy")} for p in selected[:int(spec.get("limit") or 10)]]
    return output


def validate_output(payload: dict,schema: dict) -> None:
    missing=[key for key in schema.get("required") or [] if key not in payload]
    if missing: raise ValueError("missing fusion output keys: "+", ".join(missing))
    if payload.get("schema_version")!=VERSION or payload.get("status") not in schema.get("statuses",[]): raise ValueError("invalid fusion schema/status")
    for packet in payload.get("packets") or []:
        p_missing=[key for key in schema.get("packet_required") or [] if key not in packet]
        if p_missing: raise ValueError("packet missing keys: "+", ".join(p_missing))
        if packet["direction"] not in schema["directions"] or packet["freshness"]!="FRESH" or packet["active"] is not True: raise ValueError("active packet contract invalid")
        if number(packet["score"]) is None or not 0<=packet["score"]<=100 or number(packet["confidence"]) is None or not 0<=packet["confidence"]<=1: raise ValueError("packet score/confidence invalid")
    json.dumps(payload,allow_nan=False)


def build_output(registry: dict,subscriptions: dict,policy: dict,schema: dict,feeds: dict[str,dict],metas: dict[str,dict],now: datetime|None=None) -> dict:
    if registry.get("dynamic_manifest_ingestion") is not False: raise ValueError("dynamic manifest ingestion is forbidden")
    cycles=graph_cycles(registry)
    if cycles: raise ValueError("fusion dependency graph cycle: "+json.dumps(cycles))
    now=now or datetime.now(timezone.utc); all_packets=[]; trace=[]
    for spec in registry.get("sources") or []:
        packet,row=make_packet(spec,feeds.get(spec["id"]) or {},metas.get(spec["id"]) or {},policy,now); all_packets.append(packet); trace.append(row)
    active=[p for p in all_packets if p["active"]]; inactive=[p for p in all_packets if not p["active"]]
    deduped,dropped=dedupe_packets(active); disagreements=_disagreements(deduped)
    hard=number((policy.get("thresholds") or {}).get("hard_veto") or 80)
    vetoes=[{"evidence_id":p["evidence_id"],"source_id":p["source_id"],"domain":p["domain"],"active":True,"reason":f"{p['source_id']} is RISK_OFF at {p['score']:.0f}/100","effect":"TIGHTEN_OR_VETO_ONLY","may_loosen_risk":False} for p in deduped if p["direction"]=="RISK_OFF" and p["score"]>=hard]
    allowlisted=[s for s in registry.get("sources") or [] if not s.get("exclude_from_scoring")]
    status="NO_ACTIVE_EVIDENCE" if not deduped else "DEGRADED" if inactive else "OK"
    payload={"engine":"justhodl-engine-fusion","schema_version":VERSION,"version":VERSION,"generated_at":now.astimezone(timezone.utc).isoformat(),"status":status,"authoritative_verdict":None,"packets":deduped,"inactive_packets":inactive,"coverage":{"allowlisted_sources":len(allowlisted),"fresh_active_before_dedupe":len(active),"active_after_dedupe":len(deduped),"ratio":round(len(deduped)/len(allowlisted),4) if allowlisted else 0,"independent_root_evidence":sum(p["independence_eligible"] for p in deduped),"synthesized_views":sum(not p["independence_eligible"] for p in deduped),"freshness":{"fresh":sum(t["freshness"]=="FRESH" for t in trace),"stale":sum(t["freshness"]=="STALE" for t in trace),"missing":sum(t["freshness"]=="MISSING" for t in trace),"invalid":sum(t["freshness"]=="INVALID" for t in trace),"unknown":sum(t["freshness"]=="UNKNOWN" for t in trace)}},"dedupe":{"dropped_count":len(dropped),"dropped":dropped},"disagreements":disagreements,"vetoes":vetoes,"subscriptions":_subscriptions(subscriptions,deduped),"trace":trace,"methodology":{"allowlist":"config/fusion-registry.v1.json only; engine manifest is never read","decision":"No aggregate verdict. Packets, disagreements, source health and vetoes remain visible.","lineage":"Every packet carries a deterministic root evidence ID, provenance and declared ancestry.","independence":"L2/L3 synthesized or consumer views are never counted as independent root evidence.","risk":"Fusion can enrich/tighten consumers but may never loosen or override a risk veto.","cycle_policy":"Reject the entire output before writes if the declared DAG contains a cycle."}}
    validate_output(payload,schema); return payload
