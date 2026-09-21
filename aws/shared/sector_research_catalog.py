"""Declared current US ETF wrappers; no historical constituent or factor mandate."""
SECTORS={'XLK':'Technology','XLF':'Financials','XLV':'Health Care','XLY':'Consumer Discretionary','XLP':'Consumer Staples','XLE':'Energy','XLI':'Industrials','XLU':'Utilities','XLB':'Materials','XLRE':'Real Estate','XLC':'Communication Services'}
SUBSECTORS={'XLK':'SMH','XLV':'XBI','XLF':'KRE','XLE':'XOP'}
BROAD=('SPY','QQQ','IWM','DIA')
CROSS=('TLT','IEF','HYG','LQD','GLD','SLV','DBC','USO','UUP')
SYMBOLS=tuple(SECTORS)+tuple(SUBSECTORS.values())+BROAD+CROSS
RATIOS=(('XLF','XLU','Financials / Utilities'),('XLY','XLP','Discretionary / Staples'),('XLK','XLE','Technology / Energy'),('IWM','SPY','Small cap / Broad US'),('QQQ','SPY','Nasdaq 100 / Broad US'),('SMH','XLK','Semiconductors / Technology'),('XBI','XLV','Biotechnology / Health Care'),('KRE','XLF','Regional / Broad Financials'),('HYG','IEF','High yield / Treasury'),('TLT','SPY','Long Treasury / Broad US'),('GLD','SPY','Gold / Broad US'),('DBC','SPY','Commodities / Broad US'),('XLB','XLU','Materials / Utilities'),('USO','SPY','Oil wrapper / Broad US'))
HORIZONS=(1,5,21,63,126)
PROVIDER_DEFINITION='https://massive.com/docs/rest/stocks/aggregates/custom-bars'
ADJUSTMENT_DEFINITION='https://massive.com/knowledge-base/categories/faq'
ISSUER_DEFINITION='https://www.ssga.com/us/en/individual/etfs/state-street-technology-select-sector-spdr-etf-xlk'
