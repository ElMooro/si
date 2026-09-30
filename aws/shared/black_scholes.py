"""Black-Scholes-Merton option pricing with zero third-party dependencies.

Bloomberg parity 5/10: in-house IV/Greeks for CBOE delayed option chains.
Pure math only -- no I/O, no boto3, no scipy. Safe to bundle into any
Lambda package via the aws/shared mechanism.

Conventions:
- r, q, sigma are decimals (0.05 = 5%).
- T is in years (act/365).
- Greeks: theta is per-day (annual/365), vega is per 1 vol point
  (annual/100), rho is per 1 rate point (annual/100).
- Edge cases (T <= 0, sigma <= 0) return limit values, never raise.
"""
from __future__ import annotations

import math
import re
from datetime import date

_SQRT2 = math.sqrt(2.0)

# OCC symbol: variable-length underlying (space-padded to 6 in the strict
# spec, but CBOE's delayed-quotes API ships it unpadded, e.g.
# 'SPY261130C00770000'), then YYMMDD, then C/P, then strike in
# thousandths of a dollar (8 digits).
_OCC_RE = re.compile(r"^([A-Z0-9.\-/ ]+?)(\d{6})([CP])(\d{8})$")


def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function, via erf.

    No scipy dependency; math.erf is exact to double precision.
    """
    return 0.5 * (1.0 + math.erf(x / _SQRT2))


def _norm_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _d1(S: float, K: float, T: float, r: float, sigma: float, q: float) -> float:
    """Black-Scholes d1."""
    return (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (
        sigma * math.sqrt(T)
    )


def _d2(S: float, K: float, T: float, r: float, sigma: float, q: float) -> float:
    """Black-Scholes d2."""
    return _d1(S, K, T, r, sigma, q) - sigma * math.sqrt(T)


def bs_price(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    kind: str = "call",
) -> float:
    """Black-Scholes-Merton option price.

    Args:
        S: spot price of the underlying.
        K: strike price.
        T: time to expiry in years.
        r: continuously compounded risk-free rate (decimal).
        sigma: volatility (decimal).
        q: continuously compounded dividend yield (decimal).
        kind: 'call' or 'put'.

    Returns:
        Option price. For T <= 0 returns intrinsic value; for sigma <= 0
        returns discounted intrinsic (the sigma -> 0 limit).
    """
    if kind not in ("call", "put"):
        raise ValueError("kind must be 'call' or 'put'")
    if S <= 0 or K <= 0:
        raise ValueError("S and K must be positive")
    if T <= 0:
        # At expiry: plain intrinsic value.
        if kind == "call":
            return float(max(S - K, 0.0))
        return float(max(K - S, 0.0))
    if sigma <= 0:
        # Zero-vol limit: discounted intrinsic.
        disc_s = S * math.exp(-q * T)
        disc_k = K * math.exp(-r * T)
        return (
            max(disc_s - disc_k, 0.0)
            if kind == "call"
            else max(disc_k - disc_s, 0.0)
        )
    d1 = _d1(S, K, T, r, sigma, q)
    d2 = _d2(S, K, T, r, sigma, q)
    if kind == "call":
        return (
            S * math.exp(-q * T) * norm_cdf(d1)
            - K * math.exp(-r * T) * norm_cdf(d2)
        )
    return (
        K * math.exp(-r * T) * norm_cdf(-d2)
        - S * math.exp(-q * T) * norm_cdf(-d1)
    )


def _bs_vega(S: float, K: float, T: float, r: float, sigma: float, q: float) -> float:
    """Annual vega (price sensitivity per 1.0 = 100 vol points of sigma)."""
    d1 = _d1(S, K, T, r, sigma, q)
    return S * math.exp(-q * T) * _norm_pdf(d1) * math.sqrt(T)


def implied_vol(
    price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    q: float = 0.0,
    kind: str = "call",
) -> float | None:
    """Solve implied volatility from a market price.

    Newton-Raphson starting at sigma=0.3 (max 100 iterations, 1e-8 price
    tolerance), falling back to bisection on [0.001, 5.0] when NR stalls
    (vega < 1e-10) or diverges.

    Returns:
        Implied vol as a decimal, or None when the price is outside
        arbitrage bounds (below discounted intrinsic, above the asset
        bound) or no convergence is achieved. Never raises on bad
        input; raises only for invalid kind/S/K (programmer errors).
    """
    if kind not in ("call", "put"):
        raise ValueError("kind must be 'call' or 'put'")
    if S <= 0 or K <= 0 or T <= 0 or price < 0:
        return None
    disc_s = S * math.exp(-q * T)
    disc_k = K * math.exp(-r * T)
    if kind == "call":
        lower = max(disc_s - disc_k, 0.0)
        upper = disc_s
    else:
        lower = max(disc_k - disc_s, 0.0)
        upper = disc_k
    # Outside arbitrage bounds -> no implied vol exists.
    if price < lower - 1e-9 or price > upper + 1e-9:
        return None
    # Price pinned at the lower bound -> vol collapses to ~0.
    if abs(price - lower) < 1e-9:
        return 0.0

    # Newton-Raphson.
    sigma = 0.3
    for _ in range(100):
        model = bs_price(S, K, T, r, sigma, q, kind)
        vega = _bs_vega(S, K, T, r, sigma, q)
        diff = model - price
        if abs(diff) < 1e-8:
            return sigma
        if vega < 1e-10:
            break
        nxt = sigma - diff / vega
        if nxt <= 0.0 or nxt > 10.0 or math.isnan(nxt):
            break
        if abs(nxt - sigma) < 1e-10:
            return nxt
        sigma = nxt

    # Bisection fallback on [0.001, 5.0].
    lo, hi = 0.001, 5.0
    f_lo = bs_price(S, K, T, r, lo, q, kind) - price
    f_hi = bs_price(S, K, T, r, hi, q, kind) - price
    if f_lo * f_hi > 0:
        return None
    for _ in range(100):
        mid = 0.5 * (lo + hi)
        f_mid = bs_price(S, K, T, r, mid, q, kind) - price
        if abs(f_mid) < 1e-8:
            return mid
        if f_lo * f_mid <= 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return 0.5 * (lo + hi)


def bs_greeks(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    kind: str = "call",
) -> dict:
    """Black-Scholes Greeks.

    Returns:
        dict with delta, gamma, theta, vega, rho. Theta is per-day
        (annual/365); vega is per 1 vol point (annual/100); rho is per
        1 rate point (annual/100). For T <= 0, gamma/vega/theta/rho are
        0 and delta is the exercise step function (0.5 at the money).
    """
    if kind not in ("call", "put"):
        raise ValueError("kind must be 'call' or 'put'")
    if S <= 0 or K <= 0:
        raise ValueError("S and K must be positive")
    if T <= 0 or sigma <= 0:
        if kind == "call":
            delta = 1.0 if S > K else (0.5 if S == K else 0.0)
        else:
            delta = -1.0 if S < K else (0.5 if S == K else 0.0)
        return {"delta": delta, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "rho": 0.0}

    d1 = _d1(S, K, T, r, sigma, q)
    d2 = _d2(S, K, T, r, sigma, q)
    pdf_d1 = _norm_pdf(d1)
    sqrt_t = math.sqrt(T)
    disc_s = math.exp(-q * T)
    disc_k = math.exp(-r * T)

    if kind == "call":
        delta = disc_s * norm_cdf(d1)
        theta_annual = (
            -(S * pdf_d1 * sigma * disc_s) / (2.0 * sqrt_t)
            - r * K * disc_k * norm_cdf(d2)
            + q * S * disc_s * norm_cdf(d1)
        )
        rho_annual = K * T * disc_k * norm_cdf(d2)
    else:
        delta = disc_s * (norm_cdf(d1) - 1.0)
        theta_annual = (
            -(S * pdf_d1 * sigma * disc_s) / (2.0 * sqrt_t)
            + r * K * disc_k * norm_cdf(-d2)
            - q * S * disc_s * norm_cdf(-d1)
        )
        rho_annual = -K * T * disc_k * norm_cdf(-d2)
    gamma = disc_s * pdf_d1 / (S * sigma * sqrt_t)
    vega_annual = S * disc_s * pdf_d1 * sqrt_t

    return {
        "delta": delta,
        "gamma": gamma,
        "theta": theta_annual / 365.0,
        "vega": vega_annual / 100.0,
        "rho": rho_annual / 100.0,
    }


def parse_occ_symbol(occ: str) -> dict:
    """Parse an OCC option symbol into its components.

    Accepts both the strict 21-char space-padded form
    ('SPY   261130C00770000') and CBOE's unpadded form
    ('SPY261130C00770000'): variable-length underlying + YYMMDD (6) +
    C/P (1) + strike in thousandths of a dollar (8 digits).

    Example: 'SPY261130C00770000' -> underlying 'SPY', expiry
    2026-11-30, kind 'call', strike 770.0.

    Raises:
        ValueError on malformed symbols.
    """
    if not isinstance(occ, str):
        raise ValueError(f"invalid OCC symbol type: {type(occ).__name__}")
    match = _OCC_RE.match(occ.strip().upper())
    if not match:
        raise ValueError(f"invalid OCC symbol: {occ!r}")
    underlying, yymmdd, cp, strike_raw = match.groups()
    underlying = underlying.strip()
    if not underlying:
        raise ValueError(f"empty underlying in OCC symbol: {occ!r}")
    yy, mm, dd = yymmdd[0:2], yymmdd[2:4], yymmdd[4:6]
    try:
        expiry = date(2000 + int(yy), int(mm), int(dd))
    except ValueError as exc:
        raise ValueError(f"invalid expiry in OCC symbol: {occ!r}") from exc
    return {
        "underlying": underlying,
        "expiry": expiry,
        "kind": "call" if cp == "C" else "put",
        "strike": int(strike_raw) / 1000.0,
    }
