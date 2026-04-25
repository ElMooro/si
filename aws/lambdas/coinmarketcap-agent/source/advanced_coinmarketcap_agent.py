import json
import urllib3
from datetime import datetime, timedelta
from typing import Dict, List, Any
import time
import hashlib
import hmac
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics
import math

# Initialize
logger = {'info': print, 'error': print}
http = urllib3.PoolManager()

class AdvancedCoinMarketCapAgent:
    """Complete Advanced Crypto Intelligence System"""
    
    def __init__(self):
        self.api_key = '17ba8e87-53f0-46f4-abe5-014d9cd99597'
        self.base_url = 'https://pro-api.coinmarketcap.com'
        self.cache = {}
        self.cache_ttl = 60
        self.last_update = {}
        
        # Your other APIs for correlation
        self.apis = {
            'liquidity': 'https://r9ywtw4dj3.execute-api.us-east-1.amazonaws.com/prod',
            'treasury': 'https://i1hgpjotq7.execute-api.us-east-1.amazonaws.com/prod',
            'polygon': 'https://fjf6t3ne4h.execute-api.us-east-1.amazonaws.com/prod',
            'ai': 'https://z7dm1ulht7.execute-api.us-east-1.amazonaws.com/prod'
        }
        
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make API request with caching"""
        cache_key = f"{endpoint}_{str(params)}"
        
        # Check cache
        if cache_key in self.cache:
            if time.time() - self.last_update.get(cache_key, 0) < self.cache_ttl:
                return self.cache[cache_key]
        
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept-Encoding': 'gzip, deflate'
        }
        
        url = f'{self.base_url}{endpoint}'
        if params:
            from urllib.parse import urlencode
            url = f'{url}?{urlencode(params)}'
        
        try:
            response = http.request('GET', url, headers=headers)
            data = json.loads(response.data.decode('utf-8'))
            
            if response.status == 200:
                self.cache[cache_key] = data
                self.last_update[cache_key] = time.time()
                return data
            return None
        except Exception as e:
            return None
    
    # 1. MARKET REGIME DETECTION
    def identify_market_regime(self) -> Dict:
        """Classify current crypto market state"""
        data = self._make_request('/v1/cryptocurrency/listings/latest', 
                                 {'limit': 200, 'convert': 'USD'})
        
        if not data or 'data' not in data:
            return {'regime': 'unknown', 'confidence': 0}
        
        coins = data['data']
        
        # Calculate regime indicators
        positive_count = sum(1 for c in coins[:100] 
                           if c['quote']['USD'].get('percent_change_24h', 0) > 0)
        avg_change = sum(c['quote']['USD'].get('percent_change_24h', 0) 
                        for c in coins[:100]) / 100
        
        btc_change = coins[0]['quote']['USD'].get('percent_change_24h', 0)
        btc_dominance = self._get_btc_dominance()
        
        # Volume analysis
        total_volume = sum(c['quote']['USD'].get('volume_24h', 0) for c in coins[:20])
        avg_volume_30d = sum(c['quote']['USD'].get('volume_change_24h', 0) 
                            for c in coins[:20]) / 20 if coins else 0
        
        # Determine regime
        regime = 'neutral'
        confidence = 0.5
        signals = []
        
        if avg_change > 5 and positive_count > 70:
            regime = 'euphoria'
            confidence = 0.9
            signals.append('🔴 Extreme greed detected - consider profit taking')
        elif avg_change > 2 and positive_count > 60:
            regime = 'bull_trend'
            confidence = 0.8
            signals.append('📈 Bullish momentum confirmed')
        elif avg_change < -5 and positive_count < 30:
            regime = 'capitulation'
            confidence = 0.9
            signals.append('🟢 Extreme fear - potential buying opportunity')
        elif avg_change < -2 and positive_count < 40:
            regime = 'bear_trend'
            confidence = 0.8
            signals.append('📉 Bearish momentum dominant')
        elif abs(avg_change) < 1:
            regime = 'consolidation'
            confidence = 0.7
            signals.append('📊 Range-bound market - wait for breakout')
        
        # Alt season detection
        if btc_dominance < 40:
            signals.append('🎯 Alt season active - diversify into quality alts')
        elif btc_dominance > 60:
            signals.append('₿ Bitcoin dominance high - flight to quality')
        
        return {
            'regime': regime,
            'confidence': confidence,
            'indicators': {
                'breadth': f"{positive_count}% positive",
                'momentum': f"{avg_change:.2f}% avg change",
                'btc_dominance': f"{btc_dominance:.1f}%",
                'volume_trend': 'increasing' if avg_volume_30d > 0 else 'decreasing'
            },
            'signals': signals,
            'risk_level': self._calculate_risk_level(regime, avg_change),
            'recommended_position_size': self._get_position_size(regime)
        }
    
    # 2. WHALE MOVEMENT DETECTION
    def detect_whale_activity(self) -> Dict:
        """Monitor large movements and unusual activity"""
        data = self._make_request('/v1/cryptocurrency/listings/latest',
                                 {'limit': 100, 'convert': 'USD'})
        
        if not data or 'data' not in data:
            return {'whales_active': False}
        
        coins = data['data']
        whale_movements = []
        
        for coin in coins[:50]:
            quote = coin['quote']['USD']
            volume_24h = quote.get('volume_24h', 0)
            market_cap = quote.get('market_cap', 0)
            
            if market_cap > 0:
                volume_to_mcap = (volume_24h / market_cap) * 100
                
                # Detect unusual volume (whale indicator)
                if volume_to_mcap > 50:  # >50% of market cap traded
                    whale_movements.append({
                        'symbol': coin['symbol'],
                        'name': coin['name'],
                        'volume_to_mcap_ratio': f"{volume_to_mcap:.1f}%",
                        'price_change_24h': quote.get('percent_change_24h', 0),
                        'signal': '🐋 Whale activity detected',
                        'interpretation': 'accumulation' if quote.get('percent_change_24h', 0) > 0 
                                        else 'distribution'
                    })
        
        # Stablecoin analysis
        stablecoin_flow = self._analyze_stablecoin_flows(coins)
        
        return {
            'whales_active': len(whale_movements) > 0,
            'whale_movements': whale_movements[:10],
            'stablecoin_analysis': stablecoin_flow,
            'market_impact': 'high' if len(whale_movements) > 5 else 'moderate',
            'recommendation': self._get_whale_recommendation(whale_movements)
        }
    
    # 3. CRISIS DETECTION SYSTEM
    def crypto_crisis_detection(self) -> Dict:
        """Early warning system for crypto black swans"""
        crisis_indicators = {
            'stablecoin_depeg': False,
            'exchange_risk': False,
            'liquidation_cascade': False,
            'correlation_spike': False,
            'volume_collapse': False
        }
        
        risk_score = 0
        warnings = []
        
        # Get market data
        data = self._make_request('/v1/cryptocurrency/listings/latest',
                                 {'limit': 200, 'convert': 'USD'})
        
        if data and 'data' in data:
            coins = data['data']
            
            # Check stablecoins
            for coin in coins:
                if coin['symbol'] in ['USDT', 'USDC', 'DAI', 'BUSD']:
                    price = coin['quote']['USD']['price']
                    if abs(price - 1.0) > 0.02:  # >2% depeg
                        crisis_indicators['stablecoin_depeg'] = True
                        risk_score += 30
                        warnings.append(f"⚠️ {coin['symbol']} depegged: ${price:.3f}")
            
            # Check correlation (all moving together = crisis)
            changes = [c['quote']['USD'].get('percent_change_24h', 0) for c in coins[:50]]
            if changes:
                avg_change = statistics.mean(changes)
                std_dev = statistics.stdev(changes) if len(changes) > 1 else 0
                
                if std_dev < 2 and abs(avg_change) > 10:  # Low dispersion, high movement
                    crisis_indicators['correlation_spike'] = True
                    risk_score += 25
                    warnings.append(f"🔴 Correlation spike: {std_dev:.1f} std dev")
                
                # Volume collapse detection
                total_volume = sum(c['quote']['USD'].get('volume_24h', 0) for c in coins[:20])
                if total_volume < 50_000_000_000:  # <$50B total volume top 20
                    crisis_indicators['volume_collapse'] = True
                    risk_score += 20
                    warnings.append("📉 Volume collapse detected")
                
                # Liquidation cascade risk
                if avg_change < -15:  # >15% average drop
                    crisis_indicators['liquidation_cascade'] = True
                    risk_score += 35
                    warnings.append(f"💥 Liquidation cascade risk: {avg_change:.1f}% avg drop")
        
        # Generate alert level
        alert_level = 'low'
        if risk_score > 70:
            alert_level = 'critical'
        elif risk_score > 50:
            alert_level = 'high'
        elif risk_score > 30:
            alert_level = 'medium'
        
        return {
            'risk_score': risk_score,
            'alert_level': alert_level,
            'crisis_indicators': crisis_indicators,
            'warnings': warnings,
            'action_required': risk_score > 50,
            'recommended_actions': self._get_crisis_actions(risk_score)
        }
    
    # 4. SMART MONEY FLOW ANALYSIS
    def track_smart_money(self) -> Dict:
        """Follow institutional patterns"""
        data = self._make_request('/v1/cryptocurrency/listings/latest',
                                 {'limit': 100, 'convert': 'USD'})
        
        smart_money_signals = []
        
        if data and 'data' in data:
            coins = data['data']
            
            # Analyze top coins for institutional patterns
            for coin in coins[:20]:
                quote = coin['quote']['USD']
                
                # Gradual accumulation pattern
                if (quote.get('percent_change_7d', 0) > 10 and 
                    quote.get('percent_change_24h', 0) < 3 and
                    quote.get('volume_24h', 0) > quote.get('volume_24h', 1) * 0.8):
                    
                    smart_money_signals.append({
                        'coin': coin['symbol'],
                        'pattern': 'accumulation',
                        'confidence': 'high',
                        'signal': f"🎯 Smart money accumulating {coin['symbol']}"
                    })
                
                # Distribution pattern
                elif (quote.get('percent_change_7d', 0) < -10 and
                      quote.get('volume_24h', 0) > quote.get('volume_24h', 1) * 1.5):
                    
                    smart_money_signals.append({
                        'coin': coin['symbol'],
                        'pattern': 'distribution',
                        'confidence': 'medium',
                        'signal': f"📤 Smart money distributing {coin['symbol']}"
                    })
        
        return {
            'smart_money_active': len(smart_money_signals) > 0,
            'signals': smart_money_signals[:5],
            'institutional_sentiment': self._calculate_institutional_sentiment(smart_money_signals),
            'follow_trades': [s['coin'] for s in smart_money_signals if s['pattern'] == 'accumulation']
        }
    
    # 5. CORRELATION ANALYSIS
    def analyze_correlations(self) -> Dict:
        """Analyze crypto correlations with traditional markets"""
        data = self._make_request('/v1/cryptocurrency/listings/latest',
                                 {'limit': 50, 'convert': 'USD'})
        
        correlations = {
            'btc_market_correlation': 'calculating...',
            'alt_btc_correlation': 'calculating...',
            'sector_rotations': []
        }
        
        if data and 'data' in data:
            coins = data['data']
            
            # BTC vs Alts
            btc_change = coins[0]['quote']['USD'].get('percent_change_24h', 0)
            alt_changes = [c['quote']['USD'].get('percent_change_24h', 0) for c in coins[1:20]]
            avg_alt_change = statistics.mean(alt_changes) if alt_changes else 0
            
            # Determine correlation
            if btc_change > 0 and avg_alt_change > btc_change:
                correlations['alt_btc_correlation'] = 'alts_outperforming'
                correlations['signal'] = '🚀 Alt season conditions'
            elif btc_change < 0 and avg_alt_change < btc_change:
                correlations['alt_btc_correlation'] = 'alts_underperforming'
                correlations['signal'] = '₿ Flight to Bitcoin'
            else:
                correlations['alt_btc_correlation'] = 'mixed'
                correlations['signal'] = '📊 Mixed market'
            
            # Sector analysis
            sectors = self._analyze_sectors(coins)
            correlations['sector_rotations'] = sectors
        
        return correlations
    
    # 6. MARKET SENTIMENT ANALYSIS
    def analyze_sentiment(self) -> Dict:
        """Comprehensive sentiment analysis"""
        data = self._make_request('/v1/cryptocurrency/listings/latest',
                                 {'limit': 100, 'convert': 'USD'})
        
        sentiment_score = 50  # Neutral
        
        if data and 'data' in data:
            coins = data['data']
            
            # Calculate Fear & Greed components
            positive_coins = sum(1 for c in coins[:100] 
                               if c['quote']['USD'].get('percent_change_24h', 0) > 0)
            
            avg_change = statistics.mean([c['quote']['USD'].get('percent_change_24h', 0) 
                                         for c in coins[:100]])
            
            # Sentiment calculation
            breadth_score = (positive_coins / 100) * 100
            momentum_score = min(100, max(0, 50 + (avg_change * 5)))
            
            sentiment_score = (breadth_score + momentum_score) / 2
            
        # Determine sentiment level
        if sentiment_score > 80:
            sentiment = 'extreme_greed'
            action = '🔴 Consider taking profits'
        elif sentiment_score > 65:
            sentiment = 'greed'
            action = '⚠️ Be cautious with new positions'
        elif sentiment_score < 20:
            sentiment = 'extreme_fear'
            action = '🟢 Strong buying opportunity'
        elif sentiment_score < 35:
            sentiment = 'fear'
            action = '💚 Look for quality entries'
        else:
            sentiment = 'neutral'
            action = '📊 Wait for clear direction'
        
        return {
            'sentiment_score': sentiment_score,
            'sentiment': sentiment,
            'action': action,
            'components': {
                'market_breadth': f"{positive_coins}% positive",
                'momentum': f"{avg_change:.2f}% avg change"
            }
        }
    
    # 7. OPPORTUNITY SCANNER
    def scan_opportunities(self) -> Dict:
        """Find trading opportunities across the market"""
        data = self._make_request('/v1/cryptocurrency/listings/latest',
                                 {'limit': 200, 'convert': 'USD'})
        
        opportunities = {
            'oversold_bounces': [],
            'momentum_plays': [],
            'breakout_candidates': [],
            'mean_reversion': []
        }
        
        if data and 'data' in data:
            coins = data['data']
            
            for coin in coins[:100]:
                quote = coin['quote']['USD']
                symbol = coin['symbol']
                
                # Oversold bounce candidates
                if quote.get('percent_change_24h', 0) < -15:
                    opportunities['oversold_bounces'].append({
                        'symbol': symbol,
                        'change_24h': quote.get('percent_change_24h', 0),
                        'setup': 'oversold_bounce',
                        'risk_reward': 'high'
                    })
                
                # Momentum plays
                elif (quote.get('percent_change_24h', 0) > 10 and
                      quote.get('percent_change_7d', 0) > 20):
                    opportunities['momentum_plays'].append({
                        'symbol': symbol,
                        'change_24h': quote.get('percent_change_24h', 0),
                        'change_7d': quote.get('percent_change_7d', 0),
                        'setup': 'momentum_continuation'
                    })
                
                # Volume breakouts
                volume_to_mcap = (quote.get('volume_24h', 0) / 
                                quote.get('market_cap', 1)) * 100
                if volume_to_mcap > 30 and abs(quote.get('percent_change_24h', 0)) < 5:
                    opportunities['breakout_candidates'].append({
                        'symbol': symbol,
                        'volume_spike': f"{volume_to_mcap:.1f}%",
                        'setup': 'volume_breakout_pending'
                    })
        
        # Sort and limit results
        for key in opportunities:
            opportunities[key] = opportunities[key][:5]
        
        return opportunities
    
    # 8. COMPREHENSIVE MARKET REPORT
    def generate_market_intelligence(self) -> Dict:
        """Generate complete market intelligence report"""
        
        # Run all analyses in parallel
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self.identify_market_regime): 'regime',
                executor.submit(self.detect_whale_activity): 'whales',
                executor.submit(self.crypto_crisis_detection): 'crisis',
                executor.submit(self.track_smart_money): 'smart_money',
                executor.submit(self.analyze_correlations): 'correlations',
                executor.submit(self.analyze_sentiment): 'sentiment',
                executor.submit(self.scan_opportunities): 'opportunities',
                executor.submit(self._get_market_overview): 'overview'
            }
            
            results = {}
            for future in as_completed(futures):
                key = futures[future]
                try:
                    results[key] = future.result()
                except Exception as e:
                    results[key] = {'error': str(e)}
        
        # Generate executive summary
        executive_summary = self._generate_executive_summary(results)
        
        # Calculate composite risk score
        risk_score = self._calculate_composite_risk(results)
        
        # Generate actionable recommendations
        recommendations = self._generate_recommendations(results)
        
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'executive_summary': executive_summary,
            'risk_score': risk_score,
            'recommendations': recommendations,
            'detailed_analysis': results,
            'data_source': 'CoinMarketCap PRO API - Real-time data',
            'next_update': (datetime.utcnow() + timedelta(minutes=5)).isoformat()
        }
    
    # Helper methods
    def _get_btc_dominance(self) -> float:
        data = self._make_request('/v1/global-metrics/quotes/latest')
        if data and 'data' in data:
            return data['data'].get('btc_dominance', 50)
        return 50
    
    def _calculate_risk_level(self, regime: str, avg_change: float) -> str:
        if regime in ['euphoria', 'capitulation']:
            return 'extreme'
        elif abs(avg_change) > 10:
            return 'high'
        elif abs(avg_change) > 5:
            return 'medium'
        return 'low'
    
    def _get_position_size(self, regime: str) -> str:
        sizes = {
            'euphoria': '25% (reduce exposure)',
            'bull_trend': '75% (trending market)',
            'consolidation': '50% (neutral)',
            'bear_trend': '25% (preserve capital)',
            'capitulation': '50% (selective buying)',
            'neutral': '50%'
        }
        return sizes.get(regime, '50%')
    
    def _analyze_stablecoin_flows(self, coins: List) -> Dict:
        stablecoin_data = {}
        for coin in coins:
            if coin['symbol'] in ['USDT', 'USDC', 'DAI', 'BUSD']:
                stablecoin_data[coin['symbol']] = {
                    'price': coin['quote']['USD']['price'],
                    'volume_24h': coin['quote']['USD']['volume_24h'],
                    'market_cap': coin['quote']['USD']['market_cap']
                }
        
        total_stable_mcap = sum(s['market_cap'] for s in stablecoin_data.values())
        
        return {
            'total_stablecoin_mcap': total_stable_mcap,
            'data': stablecoin_data,
            'interpretation': 'risk_on' if total_stable_mcap < 150_000_000_000 else 'risk_off'
        }
    
    def _get_whale_recommendation(self, whale_movements: List) -> str:
        if not whale_movements:
            return 'No significant whale activity detected'
        
        accumulation = sum(1 for w in whale_movements if w['interpretation'] == 'accumulation')
        distribution = sum(1 for w in whale_movements if w['interpretation'] == 'distribution')
        
        if accumulation > distribution:
            return '🟢 Whales accumulating - bullish signal'
        elif distribution > accumulation:
            return '🔴 Whales distributing - bearish signal'
        return '📊 Mixed whale activity - stay neutral'
    
    def _get_crisis_actions(self, risk_score: int) -> List[str]:
        if risk_score > 70:
            return [
                '🚨 IMMEDIATE: Reduce all positions by 75%',
                '🛡️ Move to stablecoins or cash',
                '⛔ Do not catch falling knives',
                '📊 Wait for volatility to subside'
            ]
        elif risk_score > 50:
            return [
                '⚠️ Reduce positions by 50%',
                '🛡️ Hedge with puts or shorts',
                '💰 Increase cash reserves',
                '👀 Monitor closely for deterioration'
            ]
        elif risk_score > 30:
            return [
                '📉 Reduce leverage if any',
                '🎯 Tighten stop losses',
                '💵 Keep 30% in cash',
                '📊 Be selective with new positions'
            ]
        return ['✅ Normal market conditions - follow your plan']
    
    def _calculate_institutional_sentiment(self, signals: List) -> str:
        if not signals:
            return 'neutral'
        
        accumulation = sum(1 for s in signals if s['pattern'] == 'accumulation')
        distribution = sum(1 for s in signals if s['pattern'] == 'distribution')
        
        if accumulation > distribution + 2:
            return 'bullish'
        elif distribution > accumulation + 2:
            return 'bearish'
        return 'neutral'
    
    def _analyze_sectors(self, coins: List) -> List[Dict]:
        sectors = {
            'DeFi': ['UNI', 'AAVE', 'SUSHI', 'COMP', 'MKR', 'CRV', 'YFI'],
            'Layer1': ['ETH', 'SOL', 'AVAX', 'NEAR', 'FTM', 'ALGO', 'ONE'],
            'Layer2': ['MATIC', 'ARB', 'OP', 'LRC', 'IMX'],
            'Exchange': ['BNB', 'CRO', 'FTT', 'KCS', 'HT', 'OKB'],
            'Gaming': ['AXS', 'SAND', 'MANA', 'ENJ', 'GALA', 'IMX'],
            'AI': ['FET', 'AGIX', 'OCEAN', 'NMR', 'CTXC'],
            'Meme': ['DOGE', 'SHIB', 'PEPE', 'FLOKI', 'ELON']
        }
        
        sector_performance = []
        
        for sector_name, symbols in sectors.items():
            sector_coins = [c for c in coins if c['symbol'] in symbols]
            if sector_coins:
                avg_change = statistics.mean([c['quote']['USD'].get('percent_change_24h', 0) 
                                             for c in sector_coins])
                sector_performance.append({
                    'sector': sector_name,
                    'performance_24h': f"{avg_change:.2f}%",
                    'trend': 'bullish' if avg_change > 2 else 'bearish' if avg_change < -2 else 'neutral'
                })
        
        return sorted(sector_performance, key=lambda x: float(x['performance_24h'].strip('%')), reverse=True)
    
    def _get_market_overview(self) -> Dict:
        data = self._make_request('/v1/cryptocurrency/listings/latest',
                                 {'limit': 10, 'convert': 'USD'})
        
        if not data or 'data' not in data:
            return {}
        
        top_10 = []
        for coin in data['data']:
            quote = coin['quote']['USD']
            top_10.append({
                'rank': coin['cmc_rank'],
                'symbol': coin['symbol'],
                'price': quote['price'],
                'change_24h': quote['percent_change_24h'],
                'market_cap': quote['market_cap'],
                'volume_24h': quote['volume_24h']
            })
        
        global_data = self._make_request('/v1/global-metrics/quotes/latest')
        
        overview = {
            'top_10_coins': top_10,
            'global_metrics': {}
        }
        
        if global_data and 'data' in global_data:
            metrics = global_data['data']
            overview['global_metrics'] = {
                'total_market_cap': metrics['quote']['USD']['total_market_cap'],
                'total_volume_24h': metrics['quote']['USD']['total_volume_24h'],
                'btc_dominance': metrics['btc_dominance'],
                'eth_dominance': metrics['eth_dominance'],
                'active_cryptocurrencies': metrics['active_cryptocurrencies']
            }
        
        return overview
    
    def _generate_executive_summary(self, results: Dict) -> str:
        regime = results.get('regime', {}).get('regime', 'unknown')
        risk = results.get('crisis', {}).get('alert_level', 'medium')
        sentiment = results.get('sentiment', {}).get('sentiment', 'neutral')
        whale_active = results.get('whales', {}).get('whales_active', False)
        
        summary = f"""
MARKET STATUS: {regime.upper()} | Risk: {risk.upper()} | Sentiment: {sentiment.upper()}
        
Key Insights:
- Market regime indicates {regime} conditions with {results.get('regime', {}).get('confidence', 0)*100:.0f}% confidence
- {"🐋 Whale activity detected - large players are moving" if whale_active else "Normal trading volumes"}
- Crisis detection system shows {risk} risk level
- Smart money is {results.get('smart_money', {}).get('institutional_sentiment', 'neutral')}
- Best opportunities in: {', '.join([k for k, v in results.get('opportunities', {}).items() if v][:3])}

Immediate Action Required: {"YES - See recommendations" if risk in ['high', 'critical'] else "No - Normal conditions"}
        """
        return summary.strip()
    
    def _calculate_composite_risk(self, results: Dict) -> int:
        risk_score = 0
        
        # Crisis risk
        crisis_score = results.get('crisis', {}).get('risk_score', 0)
        risk_score += crisis_score * 0.4
        
        # Regime risk
        regime = results.get('regime', {}).get('regime', 'neutral')
        if regime in ['euphoria', 'capitulation']:
            risk_score += 30
        elif regime in ['bull_trend', 'bear_trend']:
            risk_score += 20
        
        # Sentiment risk
        sentiment_score = results.get('sentiment', {}).get('sentiment_score', 50)
        if sentiment_score > 80 or sentiment_score < 20:
            risk_score += 25
        
        return min(100, int(risk_score))
    
    def _generate_recommendations(self, results: Dict) -> List[str]:
        recommendations = []
        
        risk_level = results.get('crisis', {}).get('alert_level', 'low')
        regime = results.get('regime', {}).get('regime', 'neutral')
        sentiment = results.get('sentiment', {}).get('sentiment', 'neutral')
        opportunities = results.get('opportunities', {})
        
        # Risk-based recommendations
        if risk_level in ['high', 'critical']:
            recommendations.extend(results.get('crisis', {}).get('recommended_actions', []))
        
        # Regime-based recommendations
        if regime == 'euphoria':
            recommendations.append('📊 Take profits on winners')
            recommendations.append('🛡️ Set trailing stops')
        elif regime == 'capitulation':
            recommendations.append('💰 Start accumulating quality assets')
            recommendations.append('📈 Dollar-cost average into positions')
        elif regime == 'bull_trend':
            recommendations.append('🚀 Ride the trend with proper stops')
            recommendations.append('💎 Hold winners, cut losers')
        
        # Opportunity-based recommendations
        if opportunities.get('oversold_bounces'):
            coins = [o['symbol'] for o in opportunities['oversold_bounces'][:3]]
            recommendations.append(f"🟢 Oversold bounce candidates: {', '.join(coins)}")
        
        if opportunities.get('momentum_plays'):
            coins = [o['symbol'] for o in opportunities['momentum_plays'][:3]]
            recommendations.append(f"📈 Momentum plays: {', '.join(coins)}")
        
        # Smart money recommendations
        smart_money = results.get('smart_money', {})
        if smart_money.get('follow_trades'):
            recommendations.append(f"🎯 Smart money accumulating: {', '.join(smart_money['follow_trades'][:3])}")
        
        return recommendations[:7]  # Limit to 7 most important


def lambda_handler(event, context):
    """Lambda handler with all advanced features"""
    
    # CORS headers
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
    }
    
    # Handle preflight
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'message': 'CORS preflight successful'})
        }
    
    try:
        # Initialize agent
        agent = AdvancedCoinMarketCapAgent()
        
        # Parse request
        body = json.loads(event.get('body', '{}')) if event.get('body') else {}
        action = body.get('action', 'market_intelligence')
        
        # Route to appropriate function
        if action == 'market_intelligence':
            # FULL INTELLIGENCE REPORT - ALL FEATURES
            response_data = agent.generate_market_intelligence()
            
        elif action == 'regime':
            response_data = agent.identify_market_regime()
            
        elif action == 'whales':
            response_data = agent.detect_whale_activity()
            
        elif action == 'crisis':
            response_data = agent.crypto_crisis_detection()
            
        elif action == 'smart_money':
            response_data = agent.track_smart_money()
            
        elif action == 'correlations':
            response_data = agent.analyze_correlations()
            
        elif action == 'sentiment':
            response_data = agent.analyze_sentiment()
            
        elif action == 'opportunities':
            response_data = agent.scan_opportunities()
            
        elif action == 'quick_analysis':
            # Quick analysis combining key metrics
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(agent.identify_market_regime): 'regime',
                    executor.submit(agent.crypto_crisis_detection): 'crisis',
                    executor.submit(agent.analyze_sentiment): 'sentiment',
                    executor.submit(agent.scan_opportunities): 'opportunities'
                }
                
                quick_results = {}
                for future in as_completed(futures):
                    key = futures[future]
                    quick_results[key] = future.result()
                
                response_data = {
                    'timestamp': datetime.utcnow().isoformat(),
                    'quick_analysis': quick_results
                }
        
        else:
            # List available actions
            response_data = {
                'error': f'Unknown action: {action}',
                'available_actions': [
                    'market_intelligence - Complete analysis with all features',
                    'regime - Market regime detection',
                    'whales - Whale movement tracking',
                    'crisis - Crisis detection system',
                    'smart_money - Institutional flow analysis',
                    'correlations - Cross-market correlations',
                    'sentiment - Market sentiment analysis',
                    'opportunities - Trading opportunity scanner',
                    'quick_analysis - Fast overview'
                ],
                'example': 'POST with {"action": "market_intelligence"}'
            }
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(response_data, default=str)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }

# Test locally
if __name__ == "__main__":
    # Test the agent
    agent = AdvancedCoinMarketCapAgent()
    print("Testing Advanced CoinMarketCap Agent...")
    print("=" * 50)
    
    # Test market intelligence
    result = agent.generate_market_intelligence()
    print(json.dumps(result, indent=2))
