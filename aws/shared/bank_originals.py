"""Bank rung for the received-original research engines (the reviewed-contract engines: eps-revision-velocity,
momentum-leaders, earnings-pead, estimate-revisions, pump-positioning).

Those engines retain every provider response whole and publish how it was received; they request FMP through their own
redirect-refusing openers, so the fleet's urlopen shim never touches them, and under the FMP bandwidth block they can only
"preserve previous publication".  This module asks the warehouse bank for the SAME FMP /stable URL and returns the
answer the way those engines retain originals: the bytes (FMP's own shape, produced by fmp_bankfirst.answer only when the
bank's table is fresh for that endpoint family), with the bank's provenance - the S3 object the answer came from, when the
bank pulled it from the provider, and the warehouse data date - so the engine records a warehouse-received original, never
a live response.  Rules the engines apply with it:

* a bank original is admitted only when its vintage (the bank's pulled_at, else as_of) is NEWER than the previous retained
  original for the same request (`newer_than`); older content would otherwise read as a reversal;
* the live rung stays the fallback for everything the bank does not answer (quotes are never answered);
* nothing here opens a network connection, holds a credential or writes.
"""
from datetime import datetime, timezone

TRANSPORT = "warehouse_bank"


def _iso(value):
    if not value:
        return None
    try:
        t = datetime.fromisoformat(str(value)[:19].replace("Z", ""))
        return t.replace(tzinfo=timezone.utc).isoformat()
    except Exception:  # noqa: BLE001
        return None


def vintage(provenance):
    """UTC datetime of the bank's pull for a provenance block, else None."""
    stamp = (provenance or {}).get("pulled_at") or (provenance or {}).get("as_of")
    iso = _iso(stamp)
    return datetime.fromisoformat(iso) if iso else None


def newer_than(provenance, previous_received_at):
    """True when the bank original's vintage is strictly later than an earlier original's receipt (None = no earlier one)."""
    if previous_received_at is None:
        return True
    v = vintage(provenance)
    if v is None:
        return False
    try:
        prev = datetime.fromisoformat(str(previous_received_at).replace("Z", "+00:00"))
    except Exception:  # noqa: BLE001
        return False
    if prev.tzinfo is None:
        prev = prev.replace(tzinfo=timezone.utc)
    return v > prev.astimezone(timezone.utc)


def bank_original(url, now=None):
    """{'raw': bytes, 'bank': {'object', 'pulled_at', 'as_of'}, 'received_at': iso, 'transport': 'warehouse_bank'} or None.

    None means the bank does not answer this URL exactly (unknown family, stale table, a never-banked head such as quote,
    or the shim stepped aside for this invocation) - the caller goes live as before."""
    try:
        import fmp_bank
        import fmp_bankfirst
    except Exception:  # noqa: BLE001 - the bank modules are packaged with every engine; absence means no bank rung
        return None
    try:
        fmp_bank.reset_last_answer()
        raw = fmp_bankfirst.answer(url)
    except Exception:  # noqa: BLE001 - a bank fault never blocks the live rung
        return None
    if not raw:
        return None
    prov = fmp_bank.last_answer() if hasattr(fmp_bank, "last_answer") else {}
    received = (now or datetime.now(timezone.utc)).isoformat()
    return {"raw": raw, "transport": TRANSPORT, "received_at": received,
            "bank": {"object": prov.get("key"), "pulled_at": _iso(prov.get("pulled_at")), "as_of": _iso(prov.get("as_of"))}}
