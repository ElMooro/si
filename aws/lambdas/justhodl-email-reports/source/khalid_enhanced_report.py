import json
import boto3
import urllib.request
from datetime import datetime

ses = boto3.client('ses', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

def lambda_handler(event, context):
    # Fetch Fed data for metrics
    fed_url = 'https://mjqyipzzwjcmx44irtvijecswm0nkikf.lambda-url.us-east-1.on.aws/'
    with urllib.request.urlopen(fed_url, timeout=10) as response:
        fed_data = json.loads(response.read())
    
    summary = fed_data.get('summary', {})
    
    # Extract all metrics with full detail
    metrics = {
        'vix': float(summary.get('VIXCLS', {}).get('latest_value', 15)),
        'vix_week': float(summary.get('VIXCLS', {}).get('week_change', 0)),
        'vix_month': float(summary.get('VIXCLS', {}).get('month_change', 0)),
        'sp500': float(summary.get('SP500', {}).get('latest_value', 6600)),
        'sp500_week': float(summary.get('SP500', {}).get('week_change', 0)),
        'sp500_month': float(summary.get('SP500', {}).get('month_change', 0)),
        'fed_balance': float(summary.get('WALCL', {}).get('latest_value', 6600000)),
        'fed_week': float(summary.get('WALCL', {}).get('week_change', 0)),
        'fed_month': float(summary.get('WALCL', {}).get('month_change', 0)),
        'high_yield': float(summary.get('BAMLH0A0HYM2', {}).get('latest_value', 2.7)),
        'hy_week': float(summary.get('BAMLH0A0HYM2', {}).get('week_change', 0)),
        'hy_month': float(summary.get('BAMLH0A0HYM2', {}).get('month_change', 0)),
        'ten_year': float(summary.get('DGS10', {}).get('latest_value', 4)),
        'ten_year_week': float(summary.get('DGS10', {}).get('week_change', 0)),
        'fed_funds': float(summary.get('DFF', {}).get('latest_value', 4)),
        'spread': float(summary.get('T10Y2Y', {}).get('latest_value', 0.5)),
        'm2': float(summary.get('M2SL', {}).get('latest_value', 22000)),
        'm2_month': float(summary.get('M2SL', {}).get('month_change', 0)),
        'rrp': float(summary.get('RRPONTSYD', {}).get('latest_value', 30)),
        'rrp_week': float(summary.get('RRPONTSYD', {}).get('week_change', 0)),
        'dollar': float(summary.get('DTWEXBGS', {}).get('latest_value', 120)),
        'stress': float(summary.get('STLFSI3', {}).get('latest_value', -1.7))
    }
    
    # Calculate Khalid Index with detailed components
    khalid = calculate_khalid_index(metrics)
    
    # Generate comprehensive analysis
    analysis = generate_full_analysis(metrics, khalid)
    
    # Create beautiful HTML report
    html = create_html_report(metrics, khalid, analysis)
    
    # Save to S3
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3.put_object(
        Bucket='multi-agent-system-1758294711',
        Key=f'reports/khalid_{timestamp}.html',
        Body=html,
        ContentType='text/html'
    )
    
    # Send email
    ses.send_email(
        Source='PLACEHOLDER@example.com',
        Destination={'ToAddresses': ['PLACEHOLDER@example.com']},
        Message={
            'Subject': {'Data': f"Khalid {khalid['index']:.0f} | {khalid['regime']} | Full Analysis"},
            'Body': {'Html': {'Data': html[:500000]}}
        }
    )
    
    return {'statusCode': 200, 'body': json.dumps({'khalid': khalid['index']})}

def calculate_khalid_index(m):
    """Calculate Khalid Index with all components"""
    base = 50.0
    
    # VIX Component (0-25 points)
    vix = m['vix']
    if vix < 12:
        vix_score = 0
        vix_analysis = "Ultra-low volatility - Market complacency at extreme"
    elif vix < 20:
        vix_score = (vix - 12) / 8 * 10
        vix_analysis = f"Normal volatility ({vix:.1f}) - Healthy market conditions"
    elif vix < 30:
        vix_score = 10 + (vix - 20) / 10 * 10
        vix_analysis = f"Elevated volatility ({vix:.1f}) - Rising market stress"
    else:
        vix_score = 20 + min((vix - 30) / 20 * 5, 5)
        vix_analysis = f"Extreme volatility ({vix:.1f}) - Panic conditions"
    
    # Credit Component (0-25 points)
    hy = m['high_yield']
    if hy < 2.5:
        hy_score = 0
        hy_analysis = f"Ultra-tight spreads ({hy:.2f}%) - Maximum risk appetite"
    elif hy < 4:
        hy_score = (hy - 2.5) / 1.5 * 10
        hy_analysis = f"Normal spreads ({hy:.2f}%) - Healthy credit markets"
    elif hy < 6:
        hy_score = 10 + (hy - 4) / 2 * 10
        hy_analysis = f"Widening spreads ({hy:.2f}%) - Credit stress building"
    else:
        hy_score = 20 + min((hy - 6) / 4 * 5, 5)
        hy_analysis = f"Blown-out spreads ({hy:.2f}%) - Credit crisis conditions"
    
    total = base + vix_score + hy_score
    
    # Determine regime
    if total < 40:
        regime = "RISK ON"
        color = "#22c55e"
    elif total < 60:
        regime = "NEUTRAL"
        color = "#3b82f6"
    elif total < 75:
        regime = "CAUTION"
        color = "#f59e0b"
    else:
        regime = "RISK OFF"
        color = "#ef4444"
    
    return {
        'index': total,
        'vix_component': vix_score,
        'hy_component': hy_score,
        'vix_analysis': vix_analysis,
        'hy_analysis': hy_analysis,
        'regime': regime,
        'color': color
    }

def generate_full_analysis(m, k):
    """Generate comprehensive market analysis"""
    
    analyses = {}
    
    # VIX Analysis
    vix_change = m['vix_month']
    if vix_change > 20:
        analyses['vix'] = f"VIX surged {vix_change:.1f}% this month - Fear spike indicates potential bottom formation"
    elif vix_change < -20:
        analyses['vix'] = f"VIX collapsed {abs(vix_change):.1f}% this month - Complacency rising, potential top"
    else:
        analyses['vix'] = f"VIX relatively stable ({vix_change:+.1f}% monthly) - Normal volatility regime"
    
    # Yield Curve
    spread = m['spread']
    if spread < -0.5:
        analyses['curve'] = f"Deeply inverted at {spread:.2f}% - Strong recession signal, typically 12-18 months lead"
    elif spread < 0:
        analyses['curve'] = f"Mildly inverted at {spread:.2f}% - Early recession warning activated"
    elif spread < 0.5:
        analyses['curve'] = f"Flat curve at {spread:.2f}% - Late cycle dynamics, growth slowing"
    else:
        analyses['curve'] = f"Normal curve at {spread:.2f}% - Healthy expansion, no recession risk"
    
    # Fed Balance Sheet
    fed_change = m['fed_month']
    fed_t = m['fed_balance'] / 1000
    if fed_change > 2:
        analyses['fed'] = f"Fed expanding aggressively (+{fed_change:.1f}% to ${fed_t:.1f}T) - Major liquidity injection"
    elif fed_change < -2:
        analyses['fed'] = f"Fed contracting ({fed_change:.1f}% to ${fed_t:.1f}T) - Quantitative tightening draining liquidity"
    else:
        analyses['fed'] = f"Fed stable at ${fed_t:.1f}T - Neutral policy stance"
    
    # Market Momentum
    sp_month = m['sp500_month']
    sp_week = m['sp500_week']
    if sp_month > 5 and sp_week > 2:
        analyses['momentum'] = f"Strong uptrend (+{sp_month:.1f}% monthly, +{sp_week:.1f}% weekly) - Bullish momentum intact"
    elif sp_month < -5:
        analyses['momentum'] = f"Bearish trend ({sp_month:.1f}% monthly) - Selling pressure dominant"
    else:
        analyses['momentum'] = f"Sideways action ({sp_month:+.1f}% monthly) - Market in consolidation"
    
    # Liquidity
    rrp = m['rrp']
    m2_change = m['m2_month']
    if rrp < 100 and m2_change > 0:
        analyses['liquidity'] = f"Abundant liquidity (RRP ${rrp:.0f}B, M2 +{m2_change:.1f}%) - Highly supportive"
    elif rrp > 500:
        analyses['liquidity'] = f"Liquidity trapped (RRP ${rrp:.0f}B) - Cash parked at Fed instead of markets"
    else:
        analyses['liquidity'] = f"Normal liquidity conditions - Neither headwind nor tailwind"
    
    # Recommendations
    idx = k['index']
    if idx < 40:
        analyses['action'] = """
        BULLISH POSITIONING:
        • Increase equity exposure to 80-90%
        • Focus on growth/tech sectors
        • Use dips to add positions
        • Consider leveraged ETFs with stops
        • Reduce cash/bonds to minimum"""
    elif idx < 60:
        analyses['action'] = """
        BALANCED APPROACH:
        • Maintain 60-70% equity exposure
        • Take profits on extended positions
        • Build 20-30% cash reserves
        • Diversify across sectors
        • Prepare shopping list for pullback"""
    elif idx < 75:
        analyses['action'] = """
        DEFENSIVE STANCE:
        • Reduce to 30-40% equity exposure
        • Focus on defensive sectors
        • Raise cash to 40-50%
        • Consider protective puts
        • Avoid new long positions"""
    else:
        analyses['action'] = """
        MAXIMUM DEFENSE:
        • Cut equity exposure to 0-20%
        • Move to cash/treasuries
        • Consider inverse ETFs
        • Wait for capitulation signals
        • Prepare to buy at lower levels"""
    
    return analyses

def create_html_report(m, k, a):
    """Create beautiful HTML report"""
    
    return f"""<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
* {{margin: 0; padding: 0; box-sizing: border-box;}}
body {{font-family: -apple-system, system-ui, sans-serif; background: #0f172a;}}
.header {{
    background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
    padding: 60px 20px;
    text-align: center;
    color: white;
    position: relative;
    overflow: hidden;
}}
.header::before {{
    content: '';
    position: absolute;
    top: -50%;
    left: -50%;
    width: 200%;
    height: 200%;
    background: radial-gradient(circle, rgba(255,255,255,0.1) 1px, transparent 1px);
    background-size: 50px 50px;
    animation: move 20s linear infinite;
}}
@keyframes move {{
    to {{transform: translate(50px, 50px);}}
}}
.khalid-index {{
    font-size: 140px;
    font-weight: 900;
    text-shadow: 0 10px 40px rgba(0,0,0,0.3);
    position: relative;
    z-index: 1;
}}
.regime {{
    display: inline-block;
    background: {k['color']};
    padding: 12px 40px;
    border-radius: 50px;
    font-size: 24px;
    font-weight: bold;
    margin: 20px 0;
}}
.container {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
}}
.card {{
    background: white;
    border-radius: 16px;
    padding: 30px;
    margin: 20px 0;
    box-shadow: 0 10px 40px rgba(0,0,0,0.1);
}}
.metric-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 20px;
    margin: 20px 0;
}}
.metric {{
    background: linear-gradient(135deg, #f8fafc, #f1f5f9);
    padding: 20px;
    border-radius: 12px;
    border-left: 4px solid #6366f1;
}}
.metric-value {{
    font-size: 32px;
    font-weight: bold;
    color: #1e293b;
}}
.metric-label {{
    color: #64748b;
    margin-top: 5px;
    font-size: 14px;
}}
.metric-change {{
    font-size: 16px;
    margin-top: 5px;
}}
.positive {{color: #22c55e;}}
.negative {{color: #ef4444;}}
.analysis {{
    background: #f8fafc;
    border-left: 4px solid #6366f1;
    padding: 20px;
    margin: 15px 0;
    border-radius: 8px;
}}
.analysis h3 {{
    color: #1e293b;
    margin-bottom: 10px;
}}
.analysis p {{
    color: #475569;
    line-height: 1.7;
}}
pre {{
    background: #1e293b;
    color: #e2e8f0;
    padding: 20px;
    border-radius: 8px;
    overflow-x: auto;
    white-space: pre-wrap;
}}
</style>
</head>
<body>

<div class="header">
<h1 style="position: relative; z-index: 1; font-size: 36px; margin-bottom: 20px;">
KHALID GLOBAL LIQUIDITY INTELLIGENCE
</h1>
<div class="khalid-index">{k['index']:.1f}</div>
<div class="regime">{k['regime']}</div>
<p style="position: relative; z-index: 1; font-size: 18px; margin-top: 20px;">
{datetime.now().strftime('%A, %B %d, %Y at %I:%M %p ET')}
</p>
</div>

<div class="container">

<div class="card">
<h2>📊 KEY MARKET METRICS</h2>
<div class="metric-grid">
    <div class="metric">
        <div class="metric-value">{m['vix']:.2f}</div>
        <div class="metric-label">VIX Fear Index</div>
        <div class="metric-change {'positive' if m['vix_month'] < 0 else 'negative'}">
            M: {m['vix_month']:+.1f}% | W: {m['vix_week']:+.1f}%
        </div>
    </div>
    <div class="metric">
        <div class="metric-value">{m['sp500']:,.0f}</div>
        <div class="metric-label">S&P 500</div>
        <div class="metric-change {'positive' if m['sp500_month'] > 0 else 'negative'}">
            M: {m['sp500_month']:+.1f}% | W: {m['sp500_week']:+.1f}%
        </div>
    </div>
    <div class="metric">
        <div class="metric-value">{m['ten_year']:.2f}%</div>
        <div class="metric-label">10-Year Treasury</div>
        <div class="metric-change">
            W: {m['ten_year_week']:+.1f}%
        </div>
    </div>
    <div class="metric">
        <div class="metric-value">{m['spread']:.2f}%</div>
        <div class="metric-label">2s10s Spread</div>
        <div class="metric-change {'negative' if m['spread'] < 0 else 'positive'}">
            {'INVERTED' if m['spread'] < 0 else 'NORMAL'}
        </div>
    </div>
</div>
</div>

<div class="card">
<h2>🎯 KHALID INDEX BREAKDOWN</h2>
<div class="analysis">
    <h3>VIX Component: +{k['vix_component']:.1f} points</h3>
    <p>{k['vix_analysis']}</p>
</div>
<div class="analysis">
    <h3>Credit Spread Component: +{k['hy_component']:.1f} points</h3>
    <p>{k['hy_analysis']}</p>
</div>
<div class="analysis">
    <h3>Total Score: {k['index']:.1f}/100</h3>
    <p>Base (50) + VIX ({k['vix_component']:.1f}) + Credit ({k['hy_component']:.1f}) = {k['index']:.1f}</p>
</div>
</div>

<div class="card">
<h2>🔍 MARKET ANALYSIS</h2>
<div class="analysis">
    <h3>Volatility Assessment</h3>
    <p>{a['vix']}</p>
</div>
<div class="analysis">
    <h3>Yield Curve Signal</h3>
    <p>{a['curve']}</p>
</div>
<div class="analysis">
    <h3>Fed Policy Impact</h3>
    <p>{a['fed']}</p>
</div>
<div class="analysis">
    <h3>Market Momentum</h3>
    <p>{a['momentum']}</p>
</div>
<div class="analysis">
    <h3>Liquidity Conditions</h3>
    <p>{a['liquidity']}</p>
</div>
</div>

<div class="card">
<h2>⚡ ACTION PLAN</h2>
<pre>{a['action']}</pre>
</div>

</div>
</body>
</html>"""
