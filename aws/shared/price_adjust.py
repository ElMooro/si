"""justhodl price-adjust — shared corporate-action adjustment helpers (7/10).

Pure functions, zero third-party deps. Consumes the per-ticker artifacts
written by the justhodl-corporate-actions Lambda:

    data/corporate-actions/{TICKER}.json
        {ticker, generated_at, schema_version, methodology, events, factors,
         disagreements}
        factors: ascending [[date, cumulative_factor], ...] where
        cumulative_factor(date) = product of event factors with
        event_date > date... (see build_factors in the producer Lambda)

Adjustment convention (backward-adjusted, multiplicative, basis "latest"):
  * A price bar on date d is multiplied by the cumulative factor of all
    actions whose ex/effective date is strictly AFTER d. The bar on the
    ex-date itself is never adjusted (it already reflects post-action
    prices).
  * Volume is divided by the same factor (share counts scale inversely
    to price on splits).
  * Cash dividends are included in the same cumulative series, so the
    adjusted close is dividend-inclusive (total-return basis).

Event price factors (producer-side):
  * split from:to  -> to / from        (2:1 split  -> 0.5)
  * cash dividend  -> (pre_ex_close - cash_amount) / pre_ex_close
                     ($1 on $100 -> 0.99)
  * spinoffs / unverifiable events are recorded but EXCLUDED from the
    factor product (flagged via factor_gaps in the artifact).
"""

from __future__ import annotations

import bisect
import json

SCHEMA_VERSION = "1.0"
ACTIONS_PREFIX = "data/corporate-actions/"


def load_actions(s3_client, bucket, ticker):
    """Read data/corporate-actions/{TICKER}.json from S3.

    Returns the parsed artifact dict, or {} on any error (missing key,
    bad JSON, wrong shape). Fail-soft by design: a missing artifact
    means "no adjustment data", which every helper treats as factor 1.0.
    """
    key = "%s%s.json" % (ACTIONS_PREFIX, str(ticker).upper().strip())
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        doc = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:  # noqa: BLE001 - fail-soft read; any error -> {}
        print("[price_adjust] load_actions %s failed: %s" % (ticker, type(e).__name__))
        return {}
    return doc if isinstance(doc, dict) else {}


def _factor_rows(actions):
    """Validated ascending [[date, factor], ...] rows, or []."""
    try:
        rows = (actions or {}).get("factors") or []
    except AttributeError:
        return []
    clean = []
    for row in rows:
        try:
            d, f = row[0], float(row[1])
        except (TypeError, ValueError, IndexError):
            continue
        if d and f > 0:
            clean.append([str(d), f])
    clean.sort(key=lambda r: r[0])
    return clean


def factor_on(actions, date):
    """Cumulative adjustment factor applicable to a bar on `date`.

    Looks up the first stored row with row_date > date (bisect_right)
    and returns its cumulative factor; 1.0 when there are no actions or
    no actions after `date`. `date` is an ISO "YYYY-MM-DD" string.
    """
    rows = _factor_rows(actions)
    if not rows or not date:
        return 1.0
    dates = [r[0] for r in rows]
    idx = bisect.bisect_right(dates, str(date))
    if idx >= len(rows):
        return 1.0
    return rows[idx][1]


def rebase_bars(bars, actions, to_basis="latest"):
    """Return NEW adjusted bars; the input list/dicts are never mutated.

    Each bar dict must carry open/high/low/close/date (ISO strings) and
    may carry volume. OHLC are multiplied by factor_on(actions, date);
    volume is divided by the same factor. Bars without a date keep
    factor 1.0.

    to_basis: only "latest" is supported (backward-adjusted to the most
    recent corporate-action basis). Anything else raises ValueError.
    """
    if to_basis != "latest":
        raise ValueError("unsupported to_basis=%r (only 'latest')" % (to_basis,))
    out = []
    for bar in bars or []:
        if not isinstance(bar, dict):
            continue
        f = factor_on(actions, bar.get("date")) if bar.get("date") else 1.0
        if not f:
            f = 1.0
        new = dict(bar)
        for k in ("open", "high", "low", "close"):
            try:
                new[k] = float(bar[k]) * f if bar.get(k) is not None else None
            except (TypeError, ValueError):
                new[k] = bar.get(k)
        try:
            new["volume"] = float(bar["volume"]) / f if bar.get("volume") is not None else None
        except (TypeError, ValueError, ZeroDivisionError):
            new["volume"] = bar.get("volume")
        out.append(new)
    return out


def total_return_bars(bars, actions):
    """Dividend-inclusive adjusted bars (total-return basis).

    Cash-dividend factors live in the SAME cumulative series as splits
    (see module docstring), so a total-return series is exactly what
    rebase_bars produces: the adjusted close already reflects reinvested
    dividends. This helper exists to make the call-site intent explicit —
    price charts and backtests should prefer total_return_bars, while
    rebase_bars is the mechanical primitive.
    """
    return rebase_bars(bars, actions, to_basis="latest")
