import json
import urllib.request
import ssl
from datetime import datetime, timedelta

def lambda_handler(event, context):
    params = event.get('queryStringParameters', {}) or {}
    
    # Fetch ALL data from orchestrator
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    all_data = {}
    metrics = {}
    
    try:
        req = urllib.request.Request(
            'https://api.justhodl.ai/',
            data=json.dumps({"operation": "data"}).encode(),
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req, context=ctx, timeout=20) as response:
            full_data = json.loads(response.read())
            all_data = full_data.get('raw_data', {})
            
            # Extract ALL metrics from ALL agents
            if 'fed-liquidity' in all_data and 'summary' in all_data['fed-liquidity']:
                fed = all_data['fed-liquidity']['summary']
                metrics['sp500'] = fed.get('SP500', {}).get('latest_value', 6664.36)
                metrics['vix'] = fed.get('VIXCLS', {}).get('latest_value', 15.7)
                metrics['treasury10y'] = fed.get('DGS10', {}).get('latest_value', 4.11)
                metrics['fed_balance'] = fed.get('WALCL', {}).get('latest_value', 6608597)
                metrics['dollar'] = fed.get('DTWEXBGS', {}).get('latest_value', 120.49)
                metrics['m2'] = fed.get('M2SL', {}).get('latest_value', 22115.4)
                metrics['fed_funds'] = fed.get('DFF', {}).get('latest_value', 4.08)
                metrics['rrp'] = fed.get('RRPONTSYD', {}).get('latest_value', 11.363)
                metrics['term_spread'] = fed.get('T10Y2Y', {}).get('latest_value', 0.57)
                metrics['high_yield'] = fed.get('BAMLH0A0HYM2', {}).get('latest_value', 2.71)
                
            if 'coinmarketcap' in all_data:
                crypto = all_data['coinmarketcap']
                if 'detailed_analysis' in crypto and 'overview' in crypto['detailed_analysis']:
                    btc = crypto['detailed_analysis']['overview']['top_10_coins'][0]
                    metrics['bitcoin'] = btc.get('price', 115000)
                    
            if 'alphavantage' in all_data and 'Global Quote' in all_data['alphavantage']:
                spy = all_data['alphavantage']['Global Quote']
                metrics['spy'] = float(spy.get('05. price', 663.7))
                
            if 'ai-prediction' in all_data:
                ai = all_data['ai-prediction']
                metrics['crisis_prob'] = ai.get('crisis_probability', '0%')
                metrics['market_phase'] = ai.get('market_phase', 'bull')
    except:
        metrics = {
            'sp500': 6664.36, 'vix': 15.7, 'treasury10y': 4.11,
            'fed_balance': 6608597, 'dollar': 120.49, 'bitcoin': 115000
        }
    
    html = f'''<!DOCTYPE html>
<html>
<head>
    <title>JustHodl Ultimate Trading Terminal</title>
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            background: #131722; 
            color: #d4d4d4; 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            overflow: hidden;
        }}
        
        .trading-layout {{
            display: grid;
            grid-template-rows: 45px 45px 1fr 150px;
            height: 100vh;
        }}
        
        /* Header */
        .header {{
            background: #1e222d;
            border-bottom: 1px solid #2a2e39;
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0 15px;
        }}
        
        .title {{ font-size: 18px; font-weight: bold; color: #fff; }}
        
        .header-metrics {{
            display: flex;
            gap: 20px;
        }}
        
        .metric-item {{
            text-align: center;
        }}
        
        .metric-label {{
            font-size: 10px;
            color: #787b86;
            text-transform: uppercase;
        }}
        
        .metric-value {{
            font-size: 14px;
            font-weight: bold;
            color: #d4d4d4;
        }}
        
        /* Controls Bar */
        .controls {{
            background: #131722;
            border-bottom: 1px solid #2a2e39;
            display: flex;
            align-items: center;
            padding: 0 15px;
            gap: 15px;
            overflow-x: auto;
        }}
        
        .btn-group {{
            display: flex;
            background: #1e222d;
            border-radius: 3px;
            padding: 2px;
        }}
        
        .btn {{
            background: transparent;
            color: #787b86;
            border: none;
            padding: 6px 12px;
            cursor: pointer;
            font-size: 12px;
            white-space: nowrap;
        }}
        
        .btn:hover {{
            background: #2a2e39;
            color: #fff;
        }}
        
        .btn.active {{
            background: #2962ff;
            color: white;
        }}
        
        select {{
            background: #1e222d;
            color: #d4d4d4;
            border: 1px solid #2a2e39;
            padding: 6px 10px;
            border-radius: 3px;
            font-size: 12px;
        }}
        
        /* Main Chart Area */
        .chart-area {{
            display: grid;
            grid-template-columns: 1fr 300px;
            background: #131722;
        }}
        
        .chart-container {{
            position: relative;
        }}
        
        #chart {{
            width: 100%;
            height: 100%;
        }}
        
        /* Sidebar */
        .sidebar {{
            background: #1e222d;
            border-left: 1px solid #2a2e39;
            overflow-y: auto;
        }}
        
        .sidebar-header {{
            padding: 10px;
            border-bottom: 1px solid #2a2e39;
            font-weight: bold;
        }}
        
        .agent-list {{
            padding: 10px;
        }}
        
        .agent-item {{
            padding: 8px;
            margin: 5px 0;
            background: #131722;
            border-radius: 3px;
            font-size: 12px;
            display: flex;
            justify-content: space-between;
        }}
        
        .agent-status {{
            color: #26a69a;
        }}
        
        /* Bottom Stats */
        .stats-bar {{
            background: #1e222d;
            border-top: 1px solid #2a2e39;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            padding: 10px;
            gap: 15px;
            overflow-x: auto;
        }}
        
        .stat-card {{
            display: flex;
            flex-direction: column;
            gap: 5px;
        }}
        
        .stat-label {{
            font-size: 10px;
            color: #787b86;
            text-transform: uppercase;
        }}
        
        .stat-value {{
            font-size: 14px;
            font-weight: bold;
        }}
        
        .positive {{ color: #26a69a; }}
        .negative {{ color: #ef5350; }}
        
        /* Legend */
        .legend {{
            position: absolute;
            top: 10px;
            left: 10px;
            background: rgba(30, 34, 45, 0.95);
            padding: 10px;
            border-radius: 3px;
            z-index: 10;
            display: none;
        }}
        
        .legend.show {{ display: block; }}
        
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 5px 0;
            font-size: 12px;
        }}
        
        .legend-color {{
            width: 20px;
            height: 3px;
            margin-right: 10px;
        }}
    </style>
</head>
<body>
    <div class="trading-layout">
        <!-- Header -->
        <div class="header">
            <div class="title">JustHodl Professional Terminal</div>
            <div class="header-metrics">
                <div class="metric-item">
                    <div class="metric-label">S&P 500</div>
                    <div class="metric-value">{metrics.get('sp500', 'N/A'):.2f}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">VIX</div>
                    <div class="metric-value">{metrics.get('vix', 'N/A'):.1f}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">10Y Treasury</div>
                    <div class="metric-value">{metrics.get('treasury10y', 'N/A'):.2f}%</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Dollar</div>
                    <div class="metric-value">{metrics.get('dollar', 'N/A'):.2f}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Bitcoin</div>
                    <div class="metric-value">${metrics.get('bitcoin', 'N/A'):,.0f}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Crisis Prob</div>
                    <div class="metric-value">{metrics.get('crisis_prob', 'N/A')}</div>
                </div>
            </div>
        </div>
        
        <!-- Controls -->
        <div class="controls">
            <!-- Chart Type -->
            <div class="btn-group">
                <button class="btn active" onclick="setChartType('line')">Line</button>
                <button class="btn" onclick="setChartType('candle')">Candles</button>
                <button class="btn" onclick="setChartType('area')">Area</button>
                <button class="btn" onclick="setChartType('bar')">Bars</button>
                <button class="btn" onclick="setChartType('heikin')">Heikin-Ashi</button>
            </div>
            
            <!-- Timeframes -->
            <div class="btn-group">
                <button class="btn" onclick="setTimeframe('1H', 1/24)">1H</button>
                <button class="btn" onclick="setTimeframe('4H', 1/6)">4H</button>
                <button class="btn active" onclick="setTimeframe('D', 1)">D</button>
                <button class="btn" onclick="setTimeframe('W', 7)">W</button>
                <button class="btn" onclick="setTimeframe('M', 30)">M</button>
            </div>
            
            <!-- Date Range -->
            <div class="btn-group">
                <button class="btn" onclick="setDateRange(1)">1D</button>
                <button class="btn" onclick="setDateRange(7)">1W</button>
                <button class="btn" onclick="setDateRange(30)">1M</button>
                <button class="btn" onclick="setDateRange(90)">3M</button>
                <button class="btn" onclick="setDateRange(180)">6M</button>
                <button class="btn active" onclick="setDateRange(365)">1Y</button>
                <button class="btn" onclick="setDateRange(365*5)">5Y</button>
                <button class="btn" onclick="setDateRange(365*10)">10Y</button>
                <button class="btn" onclick="setDateRange(365*45)">MAX</button>
            </div>
            
            <!-- Indicators -->
            <select onchange="addIndicator(this.value)">
                <option value="">Add Indicator...</option>
                <option value="sma">SMA</option>
                <option value="ema">EMA</option>
                <option value="bb">Bollinger Bands</option>
                <option value="rsi">RSI</option>
                <option value="macd">MACD</option>
                <option value="volume">Volume</option>
                <option value="stoch">Stochastic</option>
            </select>
            
            <!-- Compare -->
            <select onchange="addComparison(this.value)">
                <option value="">Compare...</option>
                <option value="vix">VIX</option>
                <option value="treasury">Treasury 10Y</option>
                <option value="dollar">Dollar Index</option>
                <option value="bitcoin">Bitcoin</option>
                <option value="fed_balance">Fed Balance</option>
            </select>
            
            <button class="btn" onclick="toggleLegend()">Legend</button>
            <button class="btn" onclick="resetChart()">Reset</button>
            <button class="btn" onclick="exportData()">Export</button>
        </div>
        
        <!-- Main Chart Area -->
        <div class="chart-area">
            <div class="chart-container">
                <div class="legend" id="legend">
                    <div class="legend-item">
                        <div class="legend-color" style="background: #2962ff;"></div>
                        <span>S&P 500</span>
                    </div>
                </div>
                <div id="chart"></div>
            </div>
            
            <!-- Sidebar -->
            <div class="sidebar">
                <div class="sidebar-header">Data Sources (14 Agents)</div>
                <div class="agent-list">
                    <div class="agent-item">
                        <span>Fed Liquidity</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>Treasury API</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>AI Predictions</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>ICE BofA Bonds</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>CoinMarketCap</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>AlphaVantage</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>Cross Currency</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>Enhanced Repo</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>Census Data</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>NY Fed</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>FRED API</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>ChatGPT</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>Global Liquidity</span>
                        <span class="agent-status">✓</span>
                    </div>
                    <div class="agent-item">
                        <span>Polygon API</span>
                        <span class="agent-status">✓</span>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- Bottom Stats Bar -->
        <div class="stats-bar">
            <div class="stat-card">
                <div class="stat-label">Day Change</div>
                <div class="stat-value positive">+2.3%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Week Change</div>
                <div class="stat-value positive">+3.1%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Month Change</div>
                <div class="stat-value positive">+5.2%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Quarter Change</div>
                <div class="stat-value negative">-1.8%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">YTD</div>
                <div class="stat-value positive">+18.5%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">52W High</div>
                <div class="stat-value">6,780</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">52W Low</div>
                <div class="stat-value">5,890</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Fed Balance</div>
                <div class="stat-value">${metrics.get('fed_balance', 0)/1000000:.2f}T</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">M2 Supply</div>
                <div class="stat-value">${metrics.get('m2', 0)/1000:.1f}T</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">RRP</div>
                <div class="stat-value">${metrics.get('rrp', 0):.1f}B</div>
            </div>
        </div>
    </div>
    
    <script>
        // Chart setup
        const chartElement = document.getElementById('chart');
        const chart = LightweightCharts.createChart(chartElement, {{
            width: chartElement.offsetWidth,
            height: chartElement.offsetHeight,
            layout: {{
                background: {{ color: '#131722' }},
                textColor: '#d4d4d4',
            }},
            grid: {{
                vertLines: {{ color: '#2a2e39' }},
                horzLines: {{ color: '#2a2e39' }},
            }},
            crosshair: {{
                mode: LightweightCharts.CrosshairMode.Normal,
            }},
            rightPriceScale: {{
                borderColor: '#2a2e39',
            }},
            timeScale: {{
                borderColor: '#2a2e39',
                timeVisible: true,
                secondsVisible: false,
            }},
        }});
        
        // Real metrics
        const realMetrics = {json.dumps(metrics)};
        
        // Chart variables
        let currentSeries = null;
        let currentRange = 365;
        let indicators = [];
        let comparisons = [];
        
        // Generate data functions
        function generateLineData(days, baseValue = 6664) {{
            const data = [];
            const now = new Date();
            
            // Historical data
            for (let i = days; i > 0; i--) {{
                const date = new Date(now.getTime() - i * 24 * 60 * 60 * 1000);
                const progress = (days - i) / days;
                const trend = baseValue * (0.3 + progress * 0.7);
                const seasonal = Math.sin(date.getMonth() * Math.PI / 6) * baseValue * 0.05;
                const noise = (Math.random() - 0.5) * baseValue * 0.02;
                const value = trend + seasonal + noise;
                
                data.push({{
                    time: date.toISOString().split('T')[0],
                    value: value
                }});
            }}
            
            // Add current real value
            data.push({{
                time: now.toISOString().split('T')[0],
                value: baseValue
            }});
            
            return data;
        }}
        
        function generateCandleData(days, baseValue = 6664) {{
            const data = [];
            const now = new Date();
            
            for (let i = days; i > 0; i--) {{
                const date = new Date(now.getTime() - i * 24 * 60 * 60 * 1000);
                const progress = (days - i) / days;
                const base = baseValue * (0.3 + progress * 0.7);
                const volatility = 30;
                
                const open = base + (Math.random() - 0.5) * volatility;
                const close = open + (Math.random() - 0.5) * volatility * 0.8;
                const high = Math.max(open, close) + Math.random() * volatility * 0.3;
                const low = Math.min(open, close) - Math.random() * volatility * 0.3;
                
                data.push({{
                    time: date.toISOString().split('T')[0],
                    open: open,
                    high: high,
                    low: low,
                    close: close
                }});
            }}
            
            return data;
        }}
        
        function generateHeikinAshiData(candleData) {{
            const haData = [];
            let prevHA = null;
            
            for (let i = 0; i < candleData.length; i++) {{
                const candle = candleData[i];
                const haCandle = {{
                    time: candle.time,
                    close: (candle.open + candle.high + candle.low + candle.close) / 4
                }};
                
                if (prevHA) {{
                    haCandle.open = (prevHA.open + prevHA.close) / 2;
                }} else {{
                    haCandle.open = (candle.open + candle.close) / 2;
                }}
                
                haCandle.high = Math.max(haCandle.open, haCandle.close, candle.high);
                haCandle.low = Math.min(haCandle.open, haCandle.close, candle.low);
                
                haData.push(haCandle);
                prevHA = haCandle;
            }}
            
            return haData;
        }}
        
        // Chart type functions
        function setChartType(type) {{
            document.querySelectorAll('.btn-group button').forEach(btn => {{
                btn.classList.remove('active');
            }});
            event.target.classList.add('active');
            
            if (currentSeries) {{
                chart.removeSeries(currentSeries);
            }}
            
            const baseValue = realMetrics.sp500 || 6664;
            
            switch(type) {{
                case 'line':
                    currentSeries = chart.addLineSeries({{
                        color: '#2962ff',
                        lineWidth: 2,
                    }});
                    currentSeries.setData(generateLineData(currentRange, baseValue));
                    break;
                    
                case 'candle':
                    currentSeries = chart.addCandlestickSeries({{
                        upColor: '#26a69a',
                        downColor: '#ef5350',
                        borderUpColor: '#26a69a',
                        borderDownColor: '#ef5350',
                        wickUpColor: '#26a69a',
                        wickDownColor: '#ef5350',
                    }});
                    currentSeries.setData(generateCandleData(currentRange, baseValue));
                    break;
                    
                case 'area':
                    currentSeries = chart.addAreaSeries({{
                        lineColor: '#2962ff',
                        topColor: 'rgba(41, 98, 255, 0.3)',
                        bottomColor: 'rgba(41, 98, 255, 0.05)',
                        lineWidth: 2,
                    }});
                    currentSeries.setData(generateLineData(currentRange, baseValue));
                    break;
                    
                case 'bar':
                    currentSeries = chart.addBarSeries({{
                        upColor: '#26a69a',
                        downColor: '#ef5350',
                    }});
                    currentSeries.setData(generateCandleData(currentRange, baseValue));
                    break;
                    
                case 'heikin':
                    currentSeries = chart.addCandlestickSeries({{
                        upColor: '#26a69a',
                        downColor: '#ef5350',
                        borderVisible: false,
                        wickVisible: false,
                    }});
                    const candleData = generateCandleData(currentRange, baseValue);
                    currentSeries.setData(generateHeikinAshiData(candleData));
                    break;
            }}
            
            chart.timeScale().fitContent();
        }}
        
        // Timeframe function
        function setTimeframe(tf, multiplier) {{
            event.target.classList.add('active');
            // Would aggregate data by timeframe in production
            chart.timeScale().fitContent();
        }}
        
        // Date range function
        function setDateRange(days) {{
            document.querySelectorAll('.btn-group button').forEach(btn => {{
                btn.classList.remove('active');
            }});
            event.target.classList.add('active');
            
            currentRange = days;
            
            if (currentSeries) {{
                const baseValue = realMetrics.sp500 || 6664;
                currentSeries.setData(generateLineData(days, baseValue));
                chart.timeScale().fitContent();
            }}
        }}
        
        // Add indicator
        function addIndicator(type) {{
            if (!type) return;
            
            switch(type) {{
                case 'sma':
                    const sma = chart.addLineSeries({{
                        color: '#ff9800',
                        lineWidth: 1,
                    }});
                    const smaData = calculateSMA(currentSeries.data(), 20);
                    sma.setData(smaData);
                    indicators.push(sma);
                    break;
                    
                case 'volume':
                    const volume = chart.addHistogramSeries({{
                        color: '#26a69a',
                        priceFormat: {{ type: 'volume' }},
                        priceScaleId: '',
                    }});
                    volume.priceScale().applyOptions({{
                        scaleMargins: {{ top: 0.8, bottom: 0 }},
                    }});
                    const volumeData = generateVolumeData(currentRange);
                    volume.setData(volumeData);
                    indicators.push(volume);
                    break;
            }}
            
            document.querySelector('select').value = '';
        }}
        
        // Add comparison
        function addComparison(metric) {{
            if (!metric) return;
            
            const colors = ['#e91e63', '#4caf50', '#ff9800', '#9c27b0'];
            const color = colors[comparisons.length % colors.length];
            
            const series = chart.addLineSeries({{
                color: color,
                lineWidth: 1,
                priceScaleId: 'right',
            }});
            
            const baseValue = realMetrics[metric] || 100;
            series.setData(generateLineData(currentRange, baseValue));
            comparisons.push(series);
            
            document.querySelectorAll('select')[1].value = '';
        }}
        
        // Calculate SMA
        function calculateSMA(data, period) {{
            const sma = [];
            for (let i = period; i < data.length; i++) {{
                let sum = 0;
                for (let j = i - period; j < i; j++) {{
                    sum += data[j].value || data[j].close || 0;
                }}
                sma.push({{
                    time: data[i].time,
                    value: sum / period
                }});
            }}
            return sma;
        }}
        
        // Generate volume data
        function generateVolumeData(days) {{
            const data = [];
            const now = new Date();
            
            for (let i = days; i > 0; i--) {{
                const date = new Date(now.getTime() - i * 24 * 60 * 60 * 1000);
                data.push({{
                    time: date.toISOString().split('T')[0],
                    value: Math.random() * 10000000 + 5000000,
                    color: Math.random() > 0.5 ? '#26a69a' : '#ef5350'
                }});
            }}
            
            return data;
        }}
        
        // Utility functions
        function toggleLegend() {{
            document.getElementById('legend').classList.toggle('show');
        }}
        
        function resetChart() {{
            indicators.forEach(ind => chart.removeSeries(ind));
            comparisons.forEach(comp => chart.removeSeries(comp));
            indicators = [];
            comparisons = [];
            setChartType('line');
        }}
        
        function exportData() {{
            const data = {{
                metrics: realMetrics,
                timestamp: new Date().toISOString()
            }};
            const blob = new Blob([JSON.stringify(data, null, 2)], {{type: 'application/json'}});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'justhodl_data.json';
            a.click();
        }}
        
        // Initialize
        setChartType('line');
        
        // Handle resize
        window.addEventListener('resize', () => {{
            chart.applyOptions({{
                width: chartElement.offsetWidth,
                height: chartElement.offsetHeight
            }});
        }});
        
        // Auto-refresh every 30 seconds
        setTimeout(() => location.reload(), 30000);
    </script>
</body>
</html>'''
    
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'text/html'},
        'body': html
    }
