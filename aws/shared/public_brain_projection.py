"""Pure public publication boundary shared by engines and privacy migration.

Full notes and personalized prose remain in IAM-private originals. This module
removes only known private-text paths, keeping market/risk measurements intact.
It performs no IO and never logs the input.
"""
from copy import deepcopy
import re


def _rows(value):
    return list(value.values()) if isinstance(value, dict) else value if isinstance(value, list) else []


def _walk(value):
    if isinstance(value, dict):
        yield value
        for child in list(value.values()):
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def public_notes_block(note):
    if not isinstance(note, dict):
        return {}
    return {**{k: note[k] for k in ("n_notes", "stance", "stance_score", "latest_at", "levels", "note_ids") if k in note},
            "note_text_private": True}


def notes_public(kind, doc):
    out = {k: doc[k] for k in ("generated_at", "version", "n_notes", "n_tickers", "n_macro_notes", "theme_counts", "llm_views") if k in doc}
    out["private_text"] = True
    if kind == "notes-index":
        out["index"] = {ticker: {**{k: row[k] for k in ("n_notes", "stance_score", "stance", "last_note_at", "levels", "themes", "note_ids") if k in row},
                                    "private_text": True} for ticker, row in doc.get("index", {}).items()}
    elif kind == "notes-themes":
        out["themes"] = {theme: {"n_notes": row.get("n_notes"), "avg_stance": row.get("avg_stance"),
                                   "recent": [{"note_id": n.get("note_id"), "at": n.get("at"), "private_text": True} for n in row.get("recent", [])]}
                         for theme, row in doc.get("themes", {}).items()}
    else:
        raise ValueError("unknown notes projection")
    return out


def playbook_public(doc):
    curve = (doc.get("flagship") or {}).get("yield_curve") or {}
    return {**{k: doc[k] for k in ("generated_at", "source_notes", "n_rules", "families") if k in doc},
            "flagship": {"yield_curve": {k: curve[k] for k in ("series", "latest", "most_recent_inversion_onset", "months_elapsed", "khalid_lag_months", "lag_marker_date", "status") if k in curve}},
            "private_text": True,
            "rules": [{**{k: r[k] for k in ("id", "symbol", "family", "params") if k in r}, "text_private": True}
                      for r in doc.get("rules", [])]}


def brief_public(doc):
    return {"engine": "my-brief", "generated_at": doc.get("generated_at"), "brief": None,
            "brief_available": bool(doc.get("brief")), "private_text": True,
            "note": "Sign in as the Brain owner to read the personalized brief."}


def devils_public(doc, allowed_tickers=()):
    rows = [{"ticker": c["ticker"],
             "risk_level": c.get("risk_level") if c.get("risk_level") in {"low", "medium", "high"} else "unknown",
             "rule_violation": bool(c.get("violates_your_rule")), "private_text": True,
             "bear_case": "Private review available to the signed-in Brain owner."}
            for c in doc.get("cases", []) if isinstance(c, dict)
            and c.get("ticker") in allowed_tickers and isinstance(c.get("ticker"), str) and re.fullmatch(r"[A-Z0-9.^=-]{1,20}", c["ticker"])]
    return {"engine": "devils-advocate", "generated_at": doc.get("generated_at"), "cases": rows,
            "by_ticker": {c["ticker"]: c for c in rows}, "private_text": True,
            "n_rule_violations": sum(c["rule_violation"] for c in rows),
            "note": "Sign in as the Brain owner to read the personalized review."}


def vault_search_rows(payload):
    items = (payload or {}).get("symbols") or []
    if isinstance(items, dict):
        items = [dict(value, symbol=symbol) if isinstance(value, dict) else {"symbol": symbol}
                 for symbol, value in items.items()]
    rows = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict) or item.get("status") != "LIVE" or not item.get("symbol"):
            continue
        symbol = str(item["symbol"])
        exchanges = item.get("exchanges") or []
        if not isinstance(exchanges, list):
            exchanges = [exchanges]
        rows.append({"id": "tradingview-vault-live:" + symbol, "title": symbol,
                     "kind": "instrument_ref", "search": " ".join(str(x) for x in (
                         item.get("category"), item.get("source"), item.get("resolved_via"),
                         " ".join(exchanges)) if x is not None)[:1000], "hot": True})
    return rows


def sanitize_public(key, document, *, vault=None):
    """Return a copy; canonical and historical root-alias keys share one contract."""
    if not isinstance(document, dict):
        raise ValueError("public artifact must be an object")
    out = deepcopy(document)
    name = key.removeprefix("data/")
    if name == "brain-compiler.json":
        for row in out.get("claims", []):
            row.pop("claim", None)
            row["claim_text_private"] = True
        for row in out.get("build_queue", []):
            row.pop("sample_claims", None)
            row.setdefault("note_ids", list(dict.fromkeys(c["note_id"] for c in out.get("claims", [])
                if c.get("note_id") and row.get("concept") in c.get("concepts", []))))
            row["claim_text_private"] = True
    elif name == "tv-workbench.json":
        for row in _rows(out.get("symbols")):
            row["notes"] = [{"note_id": n.get("note_id"), "ts": n.get("ts"), "text_private": True} for n in row.get("notes", [])]
        out["note"] = "Notes are private. Note IDs and counts link this public workbench to the authenticated Brain."
    elif name == "canary-warroom.json":
        out["brain_playbook"] = [{"note_id": n.get("note_id"), "cat": n.get("cat"), "pinned": n.get("pinned"), "text_private": True}
                                 for n in out.get("brain_playbook", [])]
    elif name in {"tradingview.json", "domain-barometers.json"}:
        for row in _rows(out.get("symbols")):
            row.pop("note_snippet", None)
            row["note_text_private"] = True
            if name == "domain-barometers.json" and (str(row.get("tier", "")).startswith(("T1a", "T2"))
                    or "his wording" in str(row.get("evidence", "")) or "deciding terms" in str(row.get("evidence", ""))):
                row["evidence"] = "Classification uses private note references; see the authenticated Brain for source text."
    elif name == "best-setups.json":
        for row in _walk(out):
            if isinstance(row.get("brain_aligned"), str) and row["brain_aligned"]:
                row["brain_aligned"] = "Aligned with private sector policy" if "sector" in row["brain_aligned"].lower() else "Aligned with private theme policy"
            if isinstance(row.get("khalid_note"), dict):
                row["khalid_note"]["view"] = None
                row["khalid_note"]["view_private"] = True
            for field in ("playbook_ctx", "_playbook_ctx", "playbook_context"):
                if isinstance(row.get(field), list):
                    row[field] = [{**{k: n[k] for k in ("id", "symbol", "family", "hit_rate", "score") if k in n}, "text_private": True}
                                  for n in row[field] if isinstance(n, dict)]
    elif name == "master-allocation.json":
        for row in _walk(out):
            if isinstance(row.get("brain_posture"), dict):
                source = row["brain_posture"]
                intensity = source.get("intensity", source.get("value", 0)) or 0
                source["regime"] = "DEFENSIVE" if intensity > 0 else "RISK_ON" if intensity < 0 else "NEUTRAL"
                source["source_text_private"] = True
    elif name == "position-sizing.json":
        mult = out.get("posture_mult")
        posture = "aggressive" if mult == 1.3 else "defensive" if mult == 0.6 else "balanced"
        out["risk_posture"] = posture
        regime = out.get("regime") or {}
        for row in out.get("sized_positions", []):
            row["rationale"] = (f"conv {row.get('conviction')} × {posture} posture ({mult}×) × regime "
                f"{regime.get('bond_vol')}/{regime.get('plumbing')}/gamma ({regime.get('combined_mult')}×)")
    elif name == "engine-conflicts.json":
        for row in _walk(out):
            if row.get("type") == "CONVICTION vs YOUR RULES":
                row["bear"] = "Private rule review flagged a conflict; see the authenticated review for details."
                row.setdefault("private_review_ref", {"engine": "devils-advocate", "ticker": row.get("ticker")})
    elif name == "sizing.json":
        # Prior producer versions mixed actual holdings into public model sizing.
        # Keep historical computed risk numbers, but remove the private book and
        # ticker references. Do not relabel old book-adjusted numbers model-only.
        legacy = not (out.get("holdings") is None and out.get("holdings_publication") == "REDACTED_ACCOUNT_PRIVATE"
                      and out.get("book_status") in {"READY", "BLOCKED"})
        had_book = bool(out.get("holdings"))
        out["holdings"] = None
        for row in out.get("recommendations", []):
            flags = row.get("overlap_flags") or []
            had_book = had_book or any(isinstance(v, str) and v.startswith("book:") for v in flags)
            row["overlap_flags"] = [v for v in flags if not (isinstance(v, str) and v.startswith("book:"))]
        if legacy or had_book or out.get("historical_book_context_removed"):
            out["book_status"] = "BLOCKED"
            out["historical_book_context_removed"] = True
            out["execution_eligible"] = False
            out["publication_note"] = "Personal holdings and book references withheld; historical sizing awaits account reconciliation."
        out["holdings_publication"] = "REDACTED_ACCOUNT_PRIVATE"
    elif name == "search/providers/tradingview_vault_live.json.gz":
        if not isinstance(vault, dict):
            raise ValueError("vault source required to rebuild search fields")
        by_id = {r["id"]: r["search"] for r in vault_search_rows(vault)}
        for row in out.get("rows", []):
            if not isinstance(row, list) or len(row) != 7:
                raise ValueError("unexpected search shard row contract")
            if row[2] == "instrument_ref":
                row[3] = by_id.get(row[0], "")
    elif key.startswith("equity-research/") and key.endswith(".json"):
        if "khalid_notes" in out:
            out["khalid_notes"] = public_notes_block(out["khalid_notes"])
    else:
        raise ValueError("unregistered public artifact")
    return out
