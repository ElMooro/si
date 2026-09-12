"""Five-mode brief compiler + verdict projection. S3 warehouse only. No vendor HTTP."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from brief_contract import (
    BRIEF_SCHEMA, TTL_HOURS, freshness, project_verdict, validate_brief,
)

B = "justhodl-dashboard-live"
HEAVY_IN = {"HEAVY_INFLOW", "ROTATION_IN"}
HEAVY_OUT = {"HEAVY_OUTFLOW", "ROTATION_OUT"}


def _now():
    return datetime.now(timezone.utc)


def _iso(dt=None):
    return (dt or _now()).isoformat()


def _load(s3, key: str) -> Tuple[Optional[dict], Optional[str], Optional[str]]:
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        body = json.loads(obj["Body"].read())
        lm = obj["LastModified"].astimezone(timezone.utc).isoformat()
        return body, lm, None
    except Exception as e:
        return None, None, str(e)[:180]


def _put(s3, key: str, doc: dict) -> None:
    s3.put_object(
        Bucket=B, Key=key,
        Body=json.dumps(doc, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def _inp(required, lm, as_of, ttl, err):
    return {
        "required": required,
        "last_modified": lm,
        "as_of": as_of,
        "freshness": freshness(as_of, ttl) if as_of else "EXPIRED",
        "error": err,
    }


def _finalize(s3, key, mode, source, inputs, fields, why, required_ok):
    brief = {
        "schema": BRIEF_SCHEMA,
        "mode": mode,
        "status": "LIVE" if required_ok else "HELD",
        "generated_at": _iso(),
        "source": source,
        "inputs": inputs,
        "fields": fields,
        "why": why,
    }
    err = validate_brief(brief)
    if brief["status"] == "LIVE" and err:
        brief["status"] = "HELD"
        brief["held_reason"] = err
    _put(s3, key, brief)
    return brief


def _ofr_val(doc, name):
    row = (doc or {}).get(name) if isinstance(doc, dict) else None
    if isinstance(row, dict):
        return row.get("value"), row.get("as_of") or row.get("observed")
    return None, None


def compile_plumbing(s3, source="brief-compiler"):
    ttl = TTL_HOURS["plumbing"]
    doc, lm, err = _load(s3, "data/plumbing-stress.json")
    as_of = (doc or {}).get("generated_at") or (doc or {}).get("as_of") or lm
    fields = {}
    if isinstance(doc, dict):
        fields = {
            "composite_score": doc.get("composite_score"),
            "composite_label": doc.get("composite_label"),
            "n_indicators": doc.get("n_indicators"),
            "n_with_data": doc.get("n_with_data"),
            "as_of": as_of,
            "layer_scores": {
                k: (v.get("score") if isinstance(v, dict) else v)
                for k, v in (doc.get("layers") or {}).items()
            } or doc.get("layer_scores"),
        }
    ofr, olm, oerr = _load(s3, "data/ofr-funding.json")
    o_asof = (ofr or {}).get("as_of") or olm
    sofr_v, sofr_d = _ofr_val(ofr, "sofr")
    tri_v, tri_d = _ofr_val(ofr, "triparty_rate")
    dvp_v, dvp_d = _ofr_val(ofr, "dvp_rate")
    gcf_v, gcf_d = _ofr_val(ofr, "gcf_rate")
    fields["ofr_sofr"] = sofr_v
    fields["ofr_sofr_date"] = sofr_d
    fields["ofr_triparty_rate"] = tri_v
    fields["ofr_triparty_date"] = tri_d
    fields["ofr_dvp_rate"] = dvp_v
    fields["ofr_gcf_rate"] = gcf_v
    fields["ofr_fresh_fields"] = (ofr or {}).get("fresh_fields") if isinstance(ofr, dict) else None
    fields["ofr_available_fields"] = (ofr or {}).get("available_fields") if isinstance(ofr, dict) else None
    ok = not err and doc and freshness(as_of, ttl) != "EXPIRED"
    why = "plumbing-stress composite_label=%s score=%s | OFR SOFR=%s triparty=%s dvp=%s gcf=%s fresh=%s" % (
        fields.get("composite_label"), fields.get("composite_score"),
        sofr_v, tri_v, dvp_v, gcf_v, fields.get("ofr_fresh_fields"))
    return _finalize(
        s3, "data/plumbing-brief.json", "plumbing", source,
        {
            "data/plumbing-stress.json": _inp(True, lm, as_of, ttl, err),
            "data/ofr-funding.json": _inp(False, olm, o_asof, ttl, oerr),
        },
        fields,
        why,
        ok,
    )


def compile_official_stats(s3, source="brief-compiler"):
    ttl = TTL_HOURS["official_stats"]
    doc, lm, err = _load(s3, "data/fed-nowcast-join.json")
    as_of = (doc or {}).get("generated_at") or lm
    series = (doc or {}).get("series") or {}
    atl = ((series.get("atlantafed") or {}).get("last") or {})
    cle = ((series.get("clevelandfed") or {}).get("last") or {})
    fields = {
        "gdpnow": atl.get("GDPNOW"),
        "gdpnow_date": atl.get("observation_date"),
        "t10y3m": cle.get("T10Y3M") or cle.get("t10y3m"),
        "t10y3m_date": cle.get("observation_date"),
        "nowcast_status": (doc or {}).get("status"),
    }
    ok = not err and doc and freshness(as_of, ttl) != "EXPIRED"
    return _finalize(
        s3, "data/official-stats-brief.json", "official_stats", source,
        {"data/fed-nowcast-join.json": _inp(True, lm, as_of, ttl, err)},
        fields,
        "GDPNow %s on %s; T10Y3M %s on %s" % (
            fields.get("gdpnow"), fields.get("gdpnow_date"),
            fields.get("t10y3m"), fields.get("t10y3m_date")),
        ok,
    )


def compile_market_tape(s3, source="brief-compiler"):
    ttl = TTL_HOURS["market_tape"]
    a, alm, aerr = _load(s3, "data/warm/us-equities-daily/latest-summary.json")
    b, blm, berr = _load(s3, "data/etf-flows.json")
    a_asof = (a or {}).get("as_of") or (a or {}).get("generated_at") or alm
    b_asof = (b or {}).get("generated_at") or blm
    by = (b or {}).get("by_etf") or {}
    n_in = n_out = n_other = 0
    if isinstance(by, dict):
        for row in by.values():
            if not isinstance(row, dict):
                continue
            fs = str(row.get("flow_signal") or "").upper()
            if fs in HEAVY_IN:
                n_in += 1
            elif fs in HEAVY_OUT:
                n_out += 1
            else:
                n_other += 1
    fields = {
        "session": (a or {}).get("session") or (a or {}).get("as_of_date"),
        "n_tickers": (a or {}).get("n_tickers") or (a or {}).get("n"),
        "equities_as_of": a_asof,
        "n_etfs": len(by) if isinstance(by, dict) else None,
        "heavy_inflow_n": n_in,
        "heavy_outflow_n": n_out,
        "other_flow_n": n_other,
        "etf_generated_at": b_asof,
        "breadth_basis": "uncapped",
        "breadth_pct": ((n_in - n_out) / (n_in + n_out)) if (n_in + n_out) else None,
    }
    ok = (not aerr and a and freshness(a_asof, ttl) != "EXPIRED"
          and not berr and b and freshness(b_asof, ttl) != "EXPIRED")
    return _finalize(
        s3, "data/market-tape-brief.json", "market_tape", source,
        {
            "data/warm/us-equities-daily/latest-summary.json": _inp(True, alm, a_asof, ttl, aerr),
            "data/etf-flows.json": _inp(True, blm, b_asof, ttl, berr),
        },
        fields,
        "session=%s n_tickers=%s etfs=%s heavy in/out/other %s/%s/%s basis=uncapped" % (
            fields.get("session"), fields.get("n_tickers"), fields.get("n_etfs"),
            n_in, n_out, n_other),
        ok,
    )


def _stale_names(raw):
    out = []
    for x in raw or []:
        if isinstance(x, str):
            out.append(x)
        elif isinstance(x, dict):
            out.append(x.get("fund_key") or x.get("name") or "")
    return [n for n in out if n]


def compile_positioning(s3, source="brief-compiler"):
    ttl = TTL_HOURS["positioning"]
    a, alm, aerr = _load(s3, "data/13f-positions.json")
    u, ulm, uerr = _load(s3, "data/finviz-universe.json")
    a_asof = (a or {}).get("generated_at") or alm
    n_acc = n_dist = n_flat = n_val = 0
    by = {}
    if isinstance(u, dict):
        by = u.get("by_ticker") or u.get("tickers") or {}
        if isinstance(by, list):
            by = {(r.get("ticker") or r.get("Ticker") or "").upper(): r for r in by if isinstance(r, dict)}
    if isinstance(by, dict):
        for r in by.values():
            if not isinstance(r, dict):
                continue
            v = r.get("inst_trans_pct")
            if v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            n_val += 1
            if fv > 0:
                n_acc += 1
            elif fv < 0:
                n_dist += 1
            else:
                n_flat += 1
    fields = {}
    if isinstance(a, dict):
        fields["as_of_quarter"] = a.get("as_of_quarter")
        fields["funds_total"] = a.get("funds_total")
        fields["funds_parsed"] = a.get("funds_parsed")
        fields["stale_funds"] = _stale_names(a.get("stale_funds"))
    fields.update({
        "accumulating": n_acc,
        "distributing": n_dist,
        "flat": n_flat,
        "n_with_inst_trans": n_val,
        "breadth_pct": ((n_acc - n_dist) / (n_acc + n_dist)) if (n_acc + n_dist) else None,
        "breadth_basis": "uncapped",
    })
    ok = not aerr and a and freshness(a_asof, ttl) != "EXPIRED"
    return _finalize(
        s3, "data/positioning-brief.json", "positioning", source,
        {
            "data/13f-positions.json": _inp(True, alm, a_asof, ttl, aerr),
            "data/finviz-universe.json": _inp(False, ulm, (u or {}).get("generated_at") or ulm, ttl, uerr),
        },
        fields,
        "13F quarter %s funds=%s stale=%s | inst breadth (uncapped) buy/sell/flat %s/%s/%s of %s" % (
            fields.get("as_of_quarter"), fields.get("funds_total"),
            ",".join(fields.get("stale_funds") or []) or "none",
            n_acc, n_dist, n_flat, n_val),
        ok,
    )


def compile_event(s3, source="brief-compiler"):
    ttl = TTL_HOURS["event"]
    sig, slm, serr = _load(s3, "data/finviz-signals.json")
    s_asof = (sig or {}).get("generated_at") or slm
    fields = {}
    if isinstance(sig, dict):
        fields["n_screens"] = sig.get("n_screens") or sig.get("n")
        fields["status_signals"] = sig.get("status")
        conf = sig.get("confluence") or {}
        if isinstance(conf, dict):
            fields["confluence_keys"] = list(conf.keys())[:12]
            sizes = []
            for v in conf.values():
                if isinstance(v, list):
                    sizes.append(len(v))
                elif isinstance(v, dict) and isinstance(v.get("tickers"), list):
                    sizes.append(len(v["tickers"]))
            fields["confluence_n"] = sum(sizes) if sizes else None
    ok = not serr and sig and freshness(s_asof, ttl) != "EXPIRED"
    return _finalize(
        s3, "data/event-brief.json", "event", source,
        {"data/finviz-signals.json": _inp(True, slm, s_asof, ttl, serr)},
        fields,
        "finviz-signals n_screens=%s confluence_n=%s" % (
            fields.get("n_screens"), fields.get("confluence_n")),
        ok,
    )


def compile_verdict(s3, source="brief-compiler"):
    fusion, lm, err = _load(s3, "data/jh-fusion.json")
    verdict = project_verdict(fusion if not err else {"_error": err})
    _put(s3, "data/verdict.json", verdict)
    return verdict


MODES = {
    "plumbing": compile_plumbing,
    "official_stats": compile_official_stats,
    "market_tape": compile_market_tape,
    "positioning": compile_positioning,
    "event": compile_event,
    "verdict": compile_verdict,
}


def run(s3, mode: str, source="brief-compiler") -> Dict[str, Any]:
    fn = MODES.get(mode)
    if fn is None:
        raise ValueError("unknown mode %s" % mode)
    return fn(s3, source=source)
