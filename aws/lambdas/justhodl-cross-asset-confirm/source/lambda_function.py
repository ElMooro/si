"""
justhodl-cross-asset-confirm
============================
CROSS-ASSET CONFIRMATION FILTER (CACF)

A meta-layer that VETOES signals when bonds + FX + credit aren't confirming
the regime the signal implies. Brevan Howard's "Don't fight the market
structure" formalized.

CORE IDEA:
----------
An equity LONG signal is fighting tape when:
  - Bonds are catching a bid (flight-to-quality)
  - DXY is spiking (USD risk-off)
  - HY OAS is widening (credit stress)
  - VIX is rising

An equity SHORT signal is fighting tape when the opposite is true.

For every signal in signal-board.json, this engine adds:
  - cross_asset_confirmed: bool
  - confirmation_score: -2..+2  (-2 strong veto, +2 strong confirm)
  - regime_state: RISK_ON | RISK_OFF | MIXED | TRANSITIONING
  - veto_reasons: [list of which assets disagreed]

COMPONENTS MONITORED:
---------------------
  Bonds:    UST 10Y direction (1D, 5D) + 2s10s slope change
  FX:       DXY 1D + 5D direction
  Credit:   HY OAS direction (BAMLH0A0HYM2 1D, 5D)
  Vol:      VIX level + 1D change
  Breadth:  S&P 500 advance-decline (FRED proxy or skipped if unavailable)

OUTPUT:
  data/cross-asset-confirm.json:
    {
      "generated_at": "...",
      "regime_state": "RISK_ON|RISK_OFF|MIXED|TRANSITIONING",
      "components": {
        "bonds": {"state": "...", "raw": {...}, "z": ...},
        "fx": {...}, "credit": {...}, "vol": {...}
      },
      "signal_overlays": [
        {"signal_key": "...", "direction": "LONG|SHORT", "raw_score": ...,
         "cross_asset_confirmed": true/false, "confirmation_score": -2..+2,
         "veto_reasons": [...]}
      ]
    }

SCHEDULE: every 3 hours (matches signal-board cadence)
"""
import os
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1073)

VERSION = "1.0.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
OUT_KEY = os.environ.get('OUT_KEY', 'data/cross-asset-confirm.json')
SIGNAL_BOARD_KEY = os.environ.get('SIGNAL_BOARD_KEY', 'data/signal-board.json')

FRED_KEY = os.environ.get('FRED_API_KEY', '2f057499936072679d8843d7fce99989')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

s3 = boto3.client('s3', region_name=REGION)


# ──────────────────────────────────────────────────────────────────────
# CROSS-ASSET STATE COMPONENTS
# ──────────────────────────────────────────────────────────────────────

FRED_SERIES = {
    'UST_10Y': 'DGS10',
    'UST_2Y': 'DGS2',
    'DXY': 'DTWEXBGS',          # broad trade-weighted dollar
    'HY_OAS': 'BAMLH0A0HYM2',   # HY option-adjusted spread
    'VIX': 'VIXCLS',             # VIX close
    'IG_OAS': 'BAMLC0A0CMTRIV',  # IG total return as proxy (OAS series available too)
}


def http_get_json(url, timeout=15, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-CACF/1.0'})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            last = e
            if i < retries:
                time.sleep(0.5 * (i + 1))
    raise last


def fred_recent(series_id, limit=30):
    """Get last N observations from FRED, newest first."""
    url = (f"https://api.stlouisfed.org/fred/series/observations?"
           f"series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&limit={limit}&sort_order=desc")
    data = http_get_json(url)
    out = []
    for o in data.get('observations', []):
        v = o.get('value')
        if v and v != '.':
            try:
                out.append((o['date'], float(v)))
            except ValueError:
                pass
    return out


def change_over_days(series, n_days):
    """Current - Nth observation."""
    if not series or len(series) < 2:
        return None
    current = series[0][1]
    if len(series) > n_days:
        past = series[n_days][1]
        return current - past
    return current - series[-1][1]


def compute_bonds_state():
    """UST 10Y direction + 2s10s slope."""
    try:
        ust10 = fred_recent('DGS10', 30)
        ust2 = fred_recent('DGS2', 30)
        if not ust10 or not ust2:
            return {'state': 'NO_DATA', 'reason': 'missing fred series'}
        
        d1 = change_over_days(ust10, 1)
        d5 = change_over_days(ust10, 5)
        slope_now = ust10[0][1] - ust2[0][1]
        slope_5d_ago = ust10[5][1] - ust2[5][1] if len(ust10) > 5 and len(ust2) > 5 else slope_now
        slope_change = slope_now - slope_5d_ago
        
        # Interpretation:
        # Yields UP (d1>0, d5>0) = risk-on (growth bid) or supply pressure (depends)
        # Yields DOWN = risk-off (flight to safety) OR Fed-cut pricing
        # Slope STEEPENING = late-cycle / inflation expectation
        # Slope FLATTENING = recession risk
        
        if d5 is not None and d5 < -0.15:  # 10Y down >15bps in 5D
            state = 'BOND_BID_RISK_OFF'
        elif d5 is not None and d5 > +0.15:
            state = 'BOND_SOLD_RISK_ON'
        else:
            state = 'NEUTRAL'
        
        return {
            'state': state,
            'ust10_current_pct': round(ust10[0][1], 2),
            'ust10_1d_bps': round(d1 * 100, 1) if d1 is not None else None,
            'ust10_5d_bps': round(d5 * 100, 1) if d5 is not None else None,
            'ust2s10s_bps': round(slope_now * 100, 1),
            'ust2s10s_5d_change_bps': round(slope_change * 100, 1),
        }
    except Exception as e:
        return {'state': 'ERROR', 'error': str(e)[:150]}


def compute_fx_state():
    """DXY direction."""
    try:
        dxy = fred_recent('DTWEXBGS', 30)
        if not dxy:
            return {'state': 'NO_DATA'}
        d1 = change_over_days(dxy, 1)
        d5 = change_over_days(dxy, 5)
        if d5 is not None and d5 > 1.0:  # DXY up >1pt in 5D
            state = 'DOLLAR_BID_RISK_OFF'
        elif d5 is not None and d5 < -1.0:
            state = 'DOLLAR_SOLD_RISK_ON'
        else:
            state = 'NEUTRAL'
        return {
            'state': state,
            'dxy_current': round(dxy[0][1], 2),
            'dxy_1d_change': round(d1, 2) if d1 is not None else None,
            'dxy_5d_change': round(d5, 2) if d5 is not None else None,
        }
    except Exception as e:
        return {'state': 'ERROR', 'error': str(e)[:150]}


def compute_credit_state():
    """HY OAS direction — widening = stress, tightening = risk-on."""
    try:
        hy = fred_recent('BAMLH0A0HYM2', 60)
        if not hy:
            return {'state': 'NO_DATA'}
        d1 = change_over_days(hy, 1)
        d5 = change_over_days(hy, 5)
        d20 = change_over_days(hy, 20)
        current = hy[0][1]
        
        if d5 is not None and d5 > 0.25:  # OAS widened >25bps in 5D
            state = 'CREDIT_WIDENING_STRESS'
        elif d5 is not None and d5 < -0.25:
            state = 'CREDIT_TIGHTENING_RISK_ON'
        else:
            state = 'NEUTRAL'
        return {
            'state': state,
            'hy_oas_pct': round(current, 2),
            'hy_oas_1d_bps': round(d1 * 100, 1) if d1 is not None else None,
            'hy_oas_5d_bps': round(d5 * 100, 1) if d5 is not None else None,
            'hy_oas_20d_bps': round(d20 * 100, 1) if d20 is not None else None,
        }
    except Exception as e:
        return {'state': 'ERROR', 'error': str(e)[:150]}


def compute_vol_state():
    """VIX level + 1D change."""
    try:
        vix = fred_recent('VIXCLS', 30)
        if not vix:
            return {'state': 'NO_DATA'}
        current = vix[0][1]
        d1 = change_over_days(vix, 1)
        d5 = change_over_days(vix, 5)
        
        if current > 30:
            state = 'VOL_HIGH_STRESS'
        elif current > 20:
            state = 'VOL_ELEVATED'
        elif current < 14:
            state = 'VOL_COMPLACENT'
        else:
            state = 'VOL_NORMAL'
        return {
            'state': state,
            'vix_current': round(current, 2),
            'vix_1d_change': round(d1, 2) if d1 is not None else None,
            'vix_5d_change': round(d5, 2) if d5 is not None else None,
        }
    except Exception as e:
        return {'state': 'ERROR', 'error': str(e)[:150]}


# ──────────────────────────────────────────────────────────────────────
# REGIME SYNTHESIS
# ──────────────────────────────────────────────────────────────────────

def synthesize_regime(bonds, fx, credit, vol):
    """Combine 4 components into a single regime state."""
    # Score: +1 risk-on, -1 risk-off, 0 neutral
    scores = []
    
    b_state = bonds.get('state', '')
    if 'RISK_ON' in b_state:
        scores.append(('bonds', +1))
    elif 'RISK_OFF' in b_state:
        scores.append(('bonds', -1))
    else:
        scores.append(('bonds', 0))
    
    f_state = fx.get('state', '')
    if 'RISK_ON' in f_state:
        scores.append(('fx', +1))
    elif 'RISK_OFF' in f_state:
        scores.append(('fx', -1))
    else:
        scores.append(('fx', 0))
    
    c_state = credit.get('state', '')
    if 'RISK_ON' in c_state or 'TIGHTENING' in c_state:
        scores.append(('credit', +1))
    elif 'STRESS' in c_state or 'WIDENING' in c_state:
        scores.append(('credit', -1))
    else:
        scores.append(('credit', 0))
    
    v_state = vol.get('state', '')
    if 'STRESS' in v_state:
        scores.append(('vol', -1))
    elif 'COMPLACENT' in v_state:
        scores.append(('vol', +1))
    else:
        scores.append(('vol', 0))
    
    total = sum(s for _, s in scores)
    pos = sum(1 for _, s in scores if s > 0)
    neg = sum(1 for _, s in scores if s < 0)
    
    if total >= 3:
        regime = 'RISK_ON_STRONG'
    elif total >= 1:
        regime = 'RISK_ON'
    elif total <= -3:
        regime = 'RISK_OFF_STRONG'
    elif total <= -1:
        regime = 'RISK_OFF'
    elif pos >= 2 and neg >= 2:
        regime = 'MIXED'
    else:
        regime = 'NEUTRAL'
    
    return {
        'regime': regime,
        'composite_score': total,
        'component_scores': dict(scores),
    }


# ──────────────────────────────────────────────────────────────────────
# SIGNAL OVERLAY
# ──────────────────────────────────────────────────────────────────────

def confirm_signal(signal, regime_synth):
    """Determine if a signal is confirmed by the cross-asset regime."""
    # Try to extract direction from signal
    sig_dir = None
    
    # Signal-board format varies. Try several fields.
    dir_field = (signal.get('direction') or signal.get('posture') or
                 signal.get('state') or signal.get('signal') or '')
    raw_score = signal.get('score') or signal.get('raw_score') or signal.get('composite') or 0
    
    dir_str = str(dir_field).upper()
    if any(x in dir_str for x in ['LONG', 'BULL', 'BUY', 'POSITIVE', 'RISK_ON']) or (isinstance(raw_score, (int, float)) and raw_score > 0):
        sig_dir = 'LONG'
    elif any(x in dir_str for x in ['SHORT', 'BEAR', 'SELL', 'NEGATIVE', 'RISK_OFF']) or (isinstance(raw_score, (int, float)) and raw_score < 0):
        sig_dir = 'SHORT'
    else:
        sig_dir = 'NEUTRAL'
    
    regime = regime_synth['regime']
    composite = regime_synth['composite_score']
    
    # Confirmation logic:
    # LONG signal + RISK_ON regime = confirmed
    # SHORT signal + RISK_OFF regime = confirmed
    # Opposite combos = veto
    
    confirmed = False
    confirmation_score = 0
    veto_reasons = []
    
    if sig_dir == 'LONG':
        if 'RISK_ON' in regime:
            confirmed = True
            confirmation_score = min(2, composite)
        elif 'RISK_OFF' in regime:
            confirmed = False
            confirmation_score = max(-2, composite)
            for comp, score in regime_synth['component_scores'].items():
                if score < 0:
                    veto_reasons.append(comp)
        else:  # MIXED / NEUTRAL
            confirmed = True  # don't veto on neutral
            confirmation_score = 0
    elif sig_dir == 'SHORT':
        if 'RISK_OFF' in regime:
            confirmed = True
            confirmation_score = min(2, -composite)
        elif 'RISK_ON' in regime:
            confirmed = False
            confirmation_score = max(-2, -composite)
            for comp, score in regime_synth['component_scores'].items():
                if score > 0:
                    veto_reasons.append(comp)
        else:
            confirmed = True
            confirmation_score = 0
    else:
        confirmed = True  # neutral signals are passthrough
        confirmation_score = 0
    
    return {
        'direction': sig_dir,
        'cross_asset_confirmed': confirmed,
        'confirmation_score': confirmation_score,
        'veto_reasons': veto_reasons,
    }


def load_signal_board():
    """Load signal-board for signal overlays."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=SIGNAL_BOARD_KEY)
        return json.loads(obj['Body'].read().decode())
    except Exception as e:
        print(f"[signal-board] not loaded: {e}")
        return None


def extract_signals_from_board(board):
    """Extract per-engine signals from signal-board.json (schema varies)."""
    if not board or not isinstance(board, dict):
        return []
    signals = []
    
    # Try common patterns
    engines = board.get('engines') or board.get('signals') or board.get('per_engine')
    if isinstance(engines, dict):
        for name, sig in engines.items():
            if isinstance(sig, dict):
                sig['_engine_name'] = name
                signals.append(sig)
            else:
                signals.append({'_engine_name': name, 'state': sig})
    elif isinstance(engines, list):
        for sig in engines:
            signals.append(sig)
    
    # Fall back to top-level keys if no nested structure found
    if not signals:
        for k, v in board.items():
            if isinstance(v, dict) and ('state' in v or 'score' in v or 'posture' in v):
                v['_engine_name'] = k
                signals.append(v)
    
    return signals


# ──────────────────────────────────────────────────────────────────────
# TELEGRAM
# ──────────────────────────────────────────────────────────────────────

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': TELEGRAM_CHAT_ID, 'text': msg[:4000],
            'parse_mode': 'Markdown', 'disable_web_page_preview': 'true',
        }).encode()
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"[telegram] {e}")
        return False


# ──────────────────────────────────────────────────────────────────────
# MAIN HANDLER
# ──────────────────────────────────────────────────────────────────────

def lambda_handler(event=None, context=None):
    started = time.time()
    run_ts = datetime.now(timezone.utc)
    print(f"[cacf] v{VERSION} starting")
    
    # 1. Compute cross-asset state components
    bonds = compute_bonds_state()
    fx = compute_fx_state()
    credit = compute_credit_state()
    vol = compute_vol_state()
    
    # 2. Synthesize regime
    regime_synth = synthesize_regime(bonds, fx, credit, vol)
    print(f"[cacf] regime: {regime_synth['regime']} (score {regime_synth['composite_score']})")
    
    # 3. Overlay on signal board
    board = load_signal_board()
    signals = extract_signals_from_board(board) if board else []
    print(f"[cacf] {len(signals)} signals to overlay")
    
    overlays = []
    n_vetoed = 0
    n_confirmed = 0
    for sig in signals:
        confirm = confirm_signal(sig, regime_synth)
        overlays.append({
            'engine': sig.get('_engine_name') or sig.get('engine') or sig.get('name', '?'),
            **confirm,
            'raw_signal_state': sig.get('state') or sig.get('posture') or sig.get('signal'),
            'raw_score': sig.get('score') or sig.get('composite') or sig.get('raw_score'),
        })
        if not confirm['cross_asset_confirmed']:
            n_vetoed += 1
        elif confirm['confirmation_score'] > 0:
            n_confirmed += 1
    
    payload = {
        'version': VERSION,
        'generated_at': run_ts.isoformat(),
        'elapsed_s': round(time.time() - started, 2),
        'regime': regime_synth['regime'],
        'composite_score': regime_synth['composite_score'],
        'components': {
            'bonds': bonds,
            'fx': fx,
            'credit': credit,
            'vol': vol,
        },
        'signal_overlays': overlays,
        'overlay_summary': {
            'n_signals': len(overlays),
            'n_confirmed': n_confirmed,
            'n_vetoed': n_vetoed,
            'n_neutral': len(overlays) - n_confirmed - n_vetoed,
        },
        'methodology': {
            'bonds': 'UST 10Y direction + 2s10s slope from FRED DGS10/DGS2',
            'fx': 'DXY direction from FRED DTWEXBGS',
            'credit': 'HY OAS direction from FRED BAMLH0A0HYM2',
            'vol': 'VIX level + change from FRED VIXCLS',
            'regime_synthesis': 'composite score = bonds + fx + credit + vol, each in {-1,0,+1}',
            'confirmation_rule': 'LONG + RISK_ON = confirmed; LONG + RISK_OFF = veto (& vice versa)',
        },
    }
    
    # Persist
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType='application/json',
                  CacheControl='max-age=180, public')
    
    # Telegram digest only when many vetoes (interesting signal)
    if n_vetoed >= 3:
        msg = (f"*🚦 CROSS-ASSET CONFIRMATION* — regime *{regime_synth['regime']}*\n"
               f"{n_vetoed} signal(s) being vetoed by structure.\n\n"
               + "\n".join(f"  • `{o['engine']}` {o['direction']} → veto ({', '.join(o['veto_reasons'])})"
                          for o in overlays if not o['cross_asset_confirmed'])[:1500])
        try:
            send_telegram(msg)
            payload['telegram_sent'] = True
        except Exception:
            payload['telegram_sent'] = False
    
    print(f"[cacf] done — regime={regime_synth['regime']}, {n_confirmed} confirmed / {n_vetoed} vetoed")
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({
            'ok': True,
            'regime': regime_synth['regime'],
            'composite_score': regime_synth['composite_score'],
            'n_overlays': len(overlays),
            'n_vetoed': n_vetoed,
            'elapsed_s': round(time.time() - started, 2),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
