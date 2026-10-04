"""Market bank custody metadata; this is not an upstream equivalence certificate."""
import gzip
import base64
import hashlib
import json
import math
from datetime import datetime, timezone


class ProviderDenied(RuntimeError):
    """An access or rate-limit response ends this request without fallback."""


def stop_on_denial(error, provider):
    if isinstance(error, ProviderDenied):
        raise error
    if getattr(error, "code", None) in (401, 403, 429):
        raise ProviderDenied(provider + " access/rate-limit denied; no fallback attempted") from None


def validate_upgrade(header, key):
    lines = header.decode("ascii", "strict").split("\r\n")
    status = lines[0].split(" ")
    if len(status) < 2 or status[1] != "101":
        code = status[1] if len(status) > 1 else "invalid"
        # All explicit HTTP refusals terminate host/provider fallback.
        raise ProviderDenied("TradingView upgrade refused (HTTP " + code + "); no fallback attempted")
    values = {}
    for line in lines[1:]:
        if not line:
            continue
        name, separator, value = line.partition(":")
        if not separator or name.lower() in values:
            raise ValueError("Invalid/duplicate WebSocket upgrade header")
        values[name.lower()] = value.strip()
    expected = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
    if (values.get("sec-websocket-accept") != expected
            or values.get("upgrade", "").lower() != "websocket"
            or "upgrade" not in [x.strip().lower() for x in values.get("connection", "").split(",")]
            or "sec-websocket-extensions" in values):
        raise ValueError("WebSocket upgrade was not validated")


def read_bank(client, bucket, key):
    try:
        raw = client.get_object(Bucket=bucket, Key=key)["Body"].read(8_000_001)
    except Exception as error:
        if getattr(error, "response", {}).get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return {}
        raise
    if len(raw) > 8_000_000:
        raise ValueError("Existing market bank exceeds inspection bound")
    # Decompression is bounded independently from the compressed body.
    import io
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
        body = stream.read(32_000_001)
    if len(body) > 32_000_000:
        raise ValueError("Existing market bank exceeds decoded bound")
    old = json.loads(body)
    if not isinstance(old, dict) or not isinstance(old.get("bars"), list):
        raise ValueError("Existing market bank is malformed; refusing replacement")
    return old


def valid_bar(row):
    if not isinstance(row, list) or len(row) not in (5, 6):
        return False
    if any(type(x) not in (int, float) or not math.isfinite(x) for x in row[:5]):
        return False
    if row[0] != int(row[0]) or not -2208988800 <= row[0] <= 7258118400:
        return False
    if row[2] < max(row[1], row[3], row[4]) or row[3] > min(row[1], row[2], row[4]):
        return False
    return len(row) == 5 or row[5] is None or (type(row[5]) in (int, float) and math.isfinite(row[5]) and row[5] >= 0)


def merged_document(old, rows, symbol, tv_symbol, provider):
    if not rows or not all(valid_bar(row) for row in rows):
        raise ValueError("Invalid or empty new history; existing bank preserved")
    if len({row[0] for row in rows}) != len(rows):
        raise ValueError("Duplicate new timestamps; existing bank preserved")
    previous = old.get("bars", [])
    if not isinstance(previous, list) or not all(valid_bar(row) for row in previous):
        raise ValueError("Malformed predecessor bars; existing bank preserved")
    if len({row[0] for row in previous}) != len(previous):
        raise ValueError("Duplicate predecessor timestamps; existing bank preserved")
    labels = old.get("bar_sources")
    trusted_labels = (old.get("market_history_quality", {}).get("contract") == "market-bank-custody.v1"
                      and isinstance(labels, list) and len(labels) == len(previous)
                      and all(isinstance(x, str) and x for x in labels))
    bars = {row[0]: row for row in previous}
    sources = {row[0]: labels[i] if trusted_labels else "legacy:unverified" for i, row in enumerate(previous)}
    revised = sum(row[0] in bars and bars[row[0]] != row for row in rows)
    for row in rows:
        bars[row[0]], sources[row[0]] = row, provider
    times = sorted(bars)
    unique = sorted(set(sources.values()))
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    date = lambda t: datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
    out = dict(old)
    out.update(symbol=symbol, tv_symbol=tv_symbol, source=unique[0] if len(unique) == 1 else "mixed-bank:unqualified",
               as_of=now, n=len(times), first_date=date(times[0]), last_date=date(times[-1]),
               pulled_now=len(rows), bars=[bars[t] for t in times], bar_sources=[sources[t] for t in times])
    out["market_history_quality"] = {
        "contract":"market-bank-custody.v1", "status":"provider_history_unqualified",
        "sources":unique, "legacy_rows":sum(sources[t] == "legacy:unverified" for t in times),
        "new_rows":len(rows), "revised_rows":revised,
        "retrieved_rows_sha256":hashlib.sha256(json.dumps(rows,separators=(",",":"),allow_nan=False).encode()).hexdigest(),
        "hash_scope":"parsed provider rows; not original upstream response",
        "provider_identity_verified":False, "cross_provider_equivalence_verified":False,
        "full_history_verified":False, "raw_upstream_replay_verified":False,
        "point_in_time_verified":False,
        "reason":"Per-bar producer labels retained; original upstream receipts, instrument equivalence, adjustments and full history remain unverified. Existing timestamps may receive corrections; this is not an immutable vintage archive."}
    return out
