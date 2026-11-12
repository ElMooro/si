"""Native issuer snapshots and histories for funds outside the iShares / State Street / ProShares workbooks.

Most of these issuers publish today's NAV, shares outstanding and net assets on their own fund page or data API but no
downloadable share history. The engine therefore accrues each day's reviewed snapshot into a retained, content-addressed
ledger (etf-snapshot-ledger.v1) and computes flows from the ledger exactly as it does from an issuer workbook. Every
ledger row names the retained original it was parsed from; nothing is inferred from vendors, and no date is invented:
a snapshot whose own as-of dates disagree is rejected for that day rather than dated by the acquisition clock.
Dry-run proof against the live sources before the first deploy: ops 6801 (2026-11-12), 20 snapshots + 3 histories.
"""
from datetime import date,datetime,timedelta
from decimal import Decimal,InvalidOperation,localcontext
import html,io,json,re,zipfile,xml.etree.ElementTree as ET
import etf_native as n

LEDGER_CONTRACT='etf-snapshot-ledger.v1'
LEDGER_MAX_ROWS=6000

# ---- registry ----------------------------------------------------------------------------------------------------
GLOBALX={'ARGT':'argt','BOTZ':'botz','DRIV':'driv','FINX':'finx','LIT':'lit','URA':'ura'}
VANECK={'GDX':('gold-miners-etf-gdx','351321','233083'),'SMH':('semiconductor-etf-smh','351342','233107'),
        'NLR':('uranium-nuclear-energy-etf-nlr','351334','233098'),'VNM':('vietnam-etf-vnm','351345','233110')}
FIRST_TRUST=('CIBR',)
ROBO=('ROBO',)
INVESCO=('BKLN','DBA','DBC','QQQ','RSP','TAN')
USCF=('UNG','USO')
AMPLIFY={'HACK':'amplify-etfs-data-feed'}
DWS={'ASHR':'ASHR-harvest-csi-300-china-a-shares-etf'}
SPDR_GOLD=('GLD',)
GLOBALX_URL='https://www.globalxetfs.com/funds/{slug}'
VANECK_PAGE='https://www.vaneck.com/us/en/investments/{slug}/overview/'
VANECK_BLOCK='https://www.vaneck.com/Main/FundDatasetBlock/Get/?blockId={block}&pageId={page}&ticker={ticker}'
FT_SUMMARY='https://www.ftportfolios.com/Retail/Etf/EtfSummary.aspx?Ticker={ticker}'
ROBO_URL='https://roboglobaletfs.com/{slug}'
INVESCO_PRICES='https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/{ticker}/prices?idType=ticker&variationType=priceListing&productType=ETF'
INVESCO_HEADERS={'Accept':'application/json','Origin':'https://www.invesco.com','Referer':'https://www.invesco.com/'}
USCF_KEY_JS='https://www.uscfinvestments.com/site-template/assets/javascript/api_key.php'
USCF_DAILY='https://secure.alpsinc.com/MarketingAPI/api/v1/dailyprice/{ticker}'
AMPLIFY_KEY_JS='https://amplifyetfs.com/wp-content/plugins/amplify-data/js/amplify-firestore.js'
AMPLIFY_DOC='https://firestore.googleapis.com/v1/projects/{project}/databases/(default)/documents/funds/{ticker}/daily/{day}'
AMPLIFY_DAYS=40   # weekdays requested per run (the collection itself cannot be listed: the issuer's rules allow get, not list)
DWS_EXPORT='https://etf.deutsche-am.com/api/pdp/en-us/Export/etf/{ticker}/Performance'
GLD_ARCHIVE='https://api.spdrgoldshares.com/api/v1/historical-archive?product=gld&exchange=NYSE&lang=en'
# transient public web keys: where the issuer's own site publishes them and how each is sent (never retained or logged)
WEB_KEYS={'amplify_firestore':{'url':AMPLIFY_KEY_JS,'pattern':r'apiKey\s*:\s*["\']([A-Za-z0-9_-]+)["\']','param':'key'},
          'uscf_alps':{'url':USCF_KEY_JS,'pattern':r'token\s*=\s*["\']([A-Za-z0-9._-]+)["\']','header':'Authorization','prefix':'Bearer '}}

# Funds whose issuer route was reviewed (ops 6796-6800, 2026-11-12) and found to publish no daily share figure the engine
# may retain: reported as a reviewed limit, not as an unreviewed gap.
REVIEWED_LIMITS={**{t:'issuer_publishes_month_end_shares_only' for t in ('BND','EDV','MGK','VCIT','VCSH','VEA','VGIT','VGLT','VGSH','VIG','VOO','VTI','VTIP','VTV','VUG','VWO','VYM')},
    'ARKK':'issuer_site_bot_challenge','SCHD':'issuer_site_refuses_automated_clients','SCHG':'issuer_site_refuses_automated_clients','SCHP':'issuer_site_refuses_automated_clients',
    'GBTC':'issuer_site_bot_challenge','FBTC':'no_issuer_route_reviewed','VXX':'exchange_traded_note_without_fund_share_publication'}

def snapshot_funds():
    """Tickers served by an accruing daily ledger, in deterministic order."""
    return sorted(set(GLOBALX)|set(VANECK)|set(FIRST_TRUST)|set(ROBO)|set(INVESCO)|set(USCF))

def history_funds():
    """Tickers whose issuer publishes a complete daily share history the engine re-reads whole on every run."""
    return sorted(set(AMPLIFY)|set(DWS)|set(SPDR_GOLD))

def weekdays_back(today,count):
    """The last `count` weekdays ending today (ISO dates, oldest first); exchange holidays simply answer 404."""
    out=[];day=today
    while len(out)<count:
        if day.weekday()<5:out.append(day.isoformat())
        day-=timedelta(days=1)
    return list(reversed(out))

def requests_for(ticker,today=None):
    """(label, url, auth, headers) acquisitions for one fund. auth is None or a WEB_KEYS name: a transient public web key
    the store reads at run time from the issuer's own site (never retained, logged or committed). `today` (a date)
    is needed only for issuers whose documents are addressed by day."""
    if ticker in GLOBALX:return [('globalx_'+ticker,GLOBALX_URL.format(slug=GLOBALX[ticker]),None,None)]
    if ticker in VANECK:
        slug,block,page=VANECK[ticker]
        return [('vaneck_page_'+ticker,VANECK_PAGE.format(slug=slug),None,None),('vaneck_block_'+ticker,VANECK_BLOCK.format(block=block,page=page,ticker=ticker),None,None)]
    if ticker in FIRST_TRUST:return [('firsttrust_'+ticker,FT_SUMMARY.format(ticker=ticker),None,None)]
    if ticker in ROBO:return [('robo_'+ticker,ROBO_URL.format(slug=ticker.lower()),None,None)]
    if ticker in INVESCO:return [('invesco_'+ticker,INVESCO_PRICES.format(ticker=ticker),None,INVESCO_HEADERS)]
    if ticker in USCF:return [('uscf_'+ticker,USCF_DAILY.format(ticker=ticker),'uscf_alps',{'Accept':'application/json','Origin':'https://www.uscfinvestments.com'})]
    if ticker in AMPLIFY:
        if today is None:raise ValueError('Amplify documents are addressed by day')
        return [('amplify_'+ticker+'_'+day,AMPLIFY_DOC.format(project=AMPLIFY[ticker],ticker=ticker,day=day),'amplify_firestore',None) for day in weekdays_back(today,AMPLIFY_DAYS)]
    if ticker in DWS:return [('dws_'+ticker,DWS_EXPORT.format(ticker=ticker),None,None)]
    if ticker in SPDR_GOLD:return [('spdrgold_'+ticker,GLD_ARCHIVE,None,None)]
    raise KeyError(ticker)

# ---- helpers -----------------------------------------------------------------------------------------------------
def _text(fragment):
    return html.unescape(re.sub(r'\s+',' ',re.sub(r'<[^>]+>',' ',fragment))).strip()

def _money(value):
    text=str(value).strip()
    if not re.fullmatch(r'\$?-?[\d,]+(?:\.\d+)?',text):raise ValueError('currency amount differs: '+text[:40])
    return n.dec(text.lstrip('$'))

def _count(value):
    text=str(value).strip()
    if not re.fullmatch(r'[\d,]+(?:\.\d+)?',text):raise ValueError('share count differs: '+text[:40])
    return n.dec(text)

def _day(value,fmt):
    return datetime.strptime(value.strip(),fmt).date().isoformat()

def whole(amount):
    """A share count the issuer serialised with a fractional zero tail (14423603.0000) is the whole number it names."""
    if amount!=amount.to_integral_value():raise ValueError('fractional share count')
    return amount.quantize(Decimal(1))

def published(value,places):
    """A number stored as a binary double (473811188526.530029) read at the column's published decimal precision."""
    amount=n.dec(value)
    if places<0:raise ValueError('precision differs')
    return amount.quantize(Decimal(1).scaleb(-places))

def _row(date,nav,shares,aum,nav_text,shares_text,aum_text,source,extra=None):
    """One ledger row in the native-history row contract. Net assets are retained only when exact (an issuer's rounded
    display such as '$2.99 billion' is kept as text, not as a decimal that would fail the precision residual)."""
    if nav<=0 or shares<=0 or (aum is not None and aum<0):raise ValueError('invalid issuer balance')
    qsh=n.quantum(shares_text.replace(',','').lstrip('$'));qn=n.quantum(nav_text.replace(',','').lstrip('$'))
    row={'date':date,'nav_decimal':n.ds(nav),'shares_decimal':n.ds(shares),'net_assets_decimal':n.ds(aum),
         'distribution_decimal':None,'distribution_status':'not_in_this_source',
         'share_display_quantum_decimal':n.ds(qsh),'nav_display_quantum_decimal':n.ds(qn),'source_row':0,'source':source}
    if aum is not None:
        with localcontext() as ctx:
            ctx.prec=50
            qa=n.quantum(aum_text.replace(',','').lstrip('$'));residual=aum-nav*shares;sensitivity=nav*qsh+shares*qn+qsh*qn+qa
        row.update(aum_residual_decimal=n.ds(residual),aum_display_precision_sensitivity_decimal=n.ds(sensitivity),aum_within_display_precision=abs(residual)<=sensitivity)
    else:row.update(net_assets_display=aum_text,aum_residual_decimal=None,aum_display_precision_sensitivity_decimal=None,aum_within_display_precision=None)
    if extra:row.update(extra)
    return row

def _ref(ref):
    ev=ref['evidence'];return {'url':ref['url'],'acquired_at':ref['acquired_at'],'key':ev['key'],'sha256':ev['sha256']}

# ---- Global X ------------------------------------------------------------------------------------------------------
def _globalx_card(text,title):
    m=re.search(r'>'+re.escape(title)+r'</h3>\s*<p class="[^"]*">As of ([A-Za-z]{3} \d{1,2}, \d{4})</p>(.*?)</ul>',text,re.S)
    if not m:raise ValueError('Global X card missing: '+title)
    items={};counts={}
    for label,value in re.findall(r'<span class="text-xs font-semibold text-muted-foreground">([^<]+)</span>.*?<span class="text-sm font-bold text-background text-right">([^<]+)</span>',m.group(2),re.S):
        label=_text(label);items.setdefault(label,_text(value));counts[label]=counts.get(label,0)+1
    if any(counts.get(k,0)>1 for k in ('NAV','Shares Outstanding','Net Assets','Ticker')):raise ValueError('Global X card label repeated')
    return _day(m.group(1),'%b %d, %Y'),items

def globalx_snapshot(raw,ticker,ref):
    text=raw.decode('utf-8','strict')
    nav_date,prices=_globalx_card(text,'ETF Prices');details_date,details=_globalx_card(text,'Fund Details')
    if details.get('Ticker')!=ticker:raise ValueError('Global X page ticker differs')
    if nav_date!=details_date:raise ValueError('Global X NAV and fund-details as-of dates differ')
    nav=_money(prices['NAV']);shares=_count(details['Shares Outstanding'])
    return _row(nav_date,nav,shares,None,prices['NAV'],details['Shares Outstanding'],details.get('Net Assets'),[_ref(ref)],
        {'issuer_cusip':details.get('CUSIP'),'issuer_isin':details.get('ISIN')})

# ---- VanEck --------------------------------------------------------------------------------------------------------
def _vaneck_items(text):
    out={}
    for li in re.findall(r'<li[^>]*>(.*?)</li>',text,re.S):
        t=re.search(r'item-title[^>]*>\s*([^<]+)',li);v=re.search(r'<div class="item-value">(.*?)</div>',li,re.S);d=re.search(r'class="as-of-date">as of ([A-Za-z]+ \d{1,2}, \d{4})</span>',li)
        if t and v and d:
            label=_text(t.group(1));value=_text(re.sub(r'<span class="item-change">.*',"",v.group(1),flags=re.S))
            if label not in out:out[label]=(value,_day(d.group(1),'%B %d, %Y'))
    return out

def vaneck_snapshot(page_raw,block_raw,ticker,page_ref,block_ref):
    items=_vaneck_items(page_raw.decode('utf-8','strict'))
    if 'NAV' not in items:raise ValueError('VanEck dated NAV missing')
    nav_text,nav_date=items['NAV'];nav=_money(nav_text.replace(' ',''))
    block=n.json_original(block_raw)
    if not isinstance(block,dict) or str(block.get('NAV','')).replace(' ','')!='$'+nav_text.replace(' ','').lstrip('$'):raise ValueError('VanEck dataset block NAV differs from the dated page NAV')
    holdings=block.get('HoldingsList') or []
    asof=str((holdings[0] if holdings else {}).get('AsOfDate',''))[:10]
    if asof!=nav_date:raise ValueError('VanEck dataset block as-of differs from the page NAV date')
    shares_text=str(block['Shares Outstanding']);shares=_count(shares_text)
    aum_text=str(block.get('Total Net Assets',''))
    return _row(nav_date,nav,shares,None,nav_text,shares_text,aum_text,[_ref(page_ref),_ref(block_ref)],
        {'issuer_cusip':block.get('CUSIP'),'issuer_isin':block.get('ISIN'),'shares_date_basis':'dataset block whose holdings as-of equals the page NAV as-of; the block carries no own share date'})

# ---- First Trust ---------------------------------------------------------------------------------------------------
def firsttrust_snapshot(raw,ticker,ref):
    text=raw.decode('utf-8','strict')
    header=re.search(r'id="FundNavigation_lblPageHeader"[^>]*>([^<]*)</span>',text)
    if not header or not header.group(1).strip().endswith('('+ticker+')'):raise ValueError('First Trust page identity differs')
    m=re.search(r'Current Fund Data \(as of (\d{1,2}/\d{1,2}/\d{4})\)</div>(.*?)</table>',text,re.S)
    if not m:raise ValueError('First Trust current fund data block missing')
    items={}
    for label,value in re.findall(r'<td class="CEFFieldLabel"[^>]*>(.*?)</td><td class="CEFPagesBody"[^>]*>(.*?)</td>',m.group(2),re.S):
        label=re.sub(r'<sup>.*?</sup>|<span.*?</span>','',label,flags=re.S);items.setdefault(_text(label),_text(value))
    nav=_money(items['Closing NAV']);shares=_count(items['Outstanding Shares']);aum=_money(items['Total Net Assets'])
    return _row(_day(m.group(1),'%m/%d/%Y'),nav,shares,aum,items['Closing NAV'],items['Outstanding Shares'],items['Total Net Assets'],[_ref(ref)],{'fund_name':header.group(1).strip()})

# ---- ROBO Global (Nuxt devalue payload) ------------------------------------------------------------------------------
def _devalue(data,value,depth=0):
    if depth>6:raise ValueError('ROBO payload nesting differs')
    if isinstance(value,int) and not isinstance(value,bool):
        if not 0<=value<len(data):raise ValueError('ROBO payload reference differs')
        value=data[value]
    if isinstance(value,dict):return {k:_devalue(data,v,depth+1) for k,v in value.items()}
    if isinstance(value,list):return [_devalue(data,v,depth+1) for v in value[:50]]
    return value

def robo_snapshot(raw,ticker,ref):
    text=raw.decode('utf-8','strict')
    m=re.search(r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>',text,re.S)
    if not m:raise ValueError('ROBO Nuxt payload missing')
    data=json.loads(html.unescape(m.group(1)))
    if not isinstance(data,list) or len(data)>20000:raise ValueError('ROBO payload structure differs')
    navs=[]
    for v in data:
        kind=v.get('componentType') if isinstance(v,dict) else None
        if isinstance(kind,int) and not isinstance(kind,bool) and 0<=kind<len(data) and data[kind]=='NavsComponent':navs.append(v)
    if len(navs)!=1:raise ValueError('ROBO NavsComponent count differs')
    component=_devalue(data,navs[0]);fin=component['finData']
    if fin.get('fund_ticker')!=ticker or fin.get('data_date')!=component.get('date'):raise ValueError('ROBO fund identity or data date differs')
    nav=_money(fin['nav']);shares=_count(fin['shares_outstanding']);aum=_money(fin['net_assets'])
    return _row(_day(fin['data_date'],'%m/%d/%Y'),nav,shares,aum,fin['nav'],fin['shares_outstanding'],fin['net_assets'],[_ref(ref)],{'fund_name':fin.get('fund_name'),'issuer_cusip':component.get('cusip')})

# ---- Amplify (Firestore daily documents) -----------------------------------------------------------------------------
def _firestore_number(field):
    if not isinstance(field,dict) or len(field)!=1:raise ValueError('Firestore field shape differs')
    kind,value=next(iter(field.items()))
    if kind not in ('doubleValue','integerValue'):raise ValueError('Firestore numeric field differs')
    return n.dec(value)

def amplify_history(pages,ticker,refs,at):
    """Rows from retained Firestore documents of funds/<ticker>/daily (single documents, or listing pages holding
    several). Documents are keyed by their ISO date."""
    rows={};seen=set()
    for raw,ref in zip(pages,refs):
        doc=n.json_original(raw)
        for d in (doc.get('documents',[]) if 'documents' in doc else [doc]):
            name=d['name'].rsplit('/',1)[-1]
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',name) or '/funds/'+ticker+'/daily/' not in d['name']:raise ValueError('Amplify document identity differs')
            if name in seen:raise ValueError('Amplify duplicate daily document')
            seen.add(name);f=d.get('fields',{})
            nav=_firestore_number(f['NAV']);shares=_firestore_number(f['SharesOutstanding']);aum=_firestore_number(f['NetAssets'])
            rows[name]=_row(name,nav,shares,aum,n.ds(nav),n.ds(shares),n.ds(aum),[{**_ref(ref),'document':d['name'].split('/documents/')[-1]}],
                {'document_update_time':d.get('updateTime'),'issuer_cusip':(f.get('CUSIP') or {}).get('stringValue')})
    if not rows:raise ValueError('Amplify daily documents missing')
    ordered=[rows[k] for k in sorted(rows,reverse=True)]
    for i,row in enumerate(ordered,1):row['source_row']=i
    return {'rows':n.validate_dates(ordered,at),'scope':'Issuer Firestore daily documents (NAV, shares outstanding, net assets) for the weekdays requested in this run; a day the issuer holds no document for is absent, never filled.'}

# ---- Invesco (dng-api price listing) -----------------------------------------------------------------------------------
def invesco_snapshot(raw,ticker,ref):
    doc=n.json_original(raw)
    if not isinstance(doc,dict) or doc.get('cusip')!=ticker or doc.get('currency')!='USD':raise ValueError('Invesco price listing identity differs')
    nav=n.dec(doc['nav']);shares=whole(n.dec(doc['sharesOutstanding']));aum=published(doc['marketValue'],2)
    return _row(_day(doc['effectiveDate'],'%Y-%m-%d'),nav,shares,aum,n.ds(nav),n.ds(shares),n.ds(aum),[_ref(ref)],
        {'net_assets_field':'marketValue','closing_price_decimal':n.ds(n.dec(doc.get('closingPrice'),True))})

# ---- USCF (ALPS MarketingAPI daily price, bearer token from the issuer's own site) ------------------------------------
def uscf_snapshot(raw,ticker,ref):
    doc=n.json_original(raw)
    if not isinstance(doc,list) or len(doc)!=1 or doc[0].get('symbol')!=ticker:raise ValueError('ALPS daily price identity differs')
    d=doc[0];nav=n.dec(d['navextended']);shares=whole(n.dec(d['so']));aum=published(d['navtotal'],2)
    return _row(_day(str(d['displaydate'])[:10],'%Y-%m-%d'),nav,shares,aum,n.ds(nav),n.ds(shares),n.ds(aum),[_ref(ref)],
        {'nav_field':'navextended','displayed_nav_decimal':n.ds(published(d['nav'],2)),'issuer_cusip':d.get('cusip'),'issuer_isin':d.get('isin')})

# ---- spreadsheet workbooks (DWS export, SPDR Gold archive) -------------------------------------------------------------
XLSX_NS={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main','r':'http://schemas.openxmlformats.org/officeDocument/2006/relationships'}

def xlsx_rows(raw,sheet_index=0,max_rows=LEDGER_MAX_ROWS):
    """Cell values of one worksheet of an .xlsx workbook: {row index: {column letter: text}}. Shared strings are
    resolved; formulas, external declarations and unreviewed cell types are refused."""
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members=archive.infolist()
        if len(members)>80 or sum(v.file_size for v in members)>48*1024*1024 or len({v.filename for v in members})!=len(members):raise ValueError('spreadsheet archive bound')
        workbook=archive.read('xl/workbook.xml');rels=archive.read('xl/_rels/workbook.xml.rels')
        texts=[workbook,rels]+([archive.read('xl/sharedStrings.xml')] if 'xl/sharedStrings.xml' in archive.namelist() else [])
        sheets=ET.fromstring(workbook).findall('s:sheets/s:sheet',XLSX_NS)
        if not 0<=sheet_index<len(sheets):raise ValueError('worksheet index differs')
        rid=sheets[sheet_index].get('{'+XLSX_NS['r']+'}id')
        target=next(v.get('Target') for v in ET.fromstring(rels) if v.get('Id')==rid)
        path=target.lstrip('/') if target.startswith('/') else 'xl/'+target
        sheet=archive.read(path);texts.append(sheet)
        if any(b'<!DOCTYPE' in v.upper() or b'<!ENTITY' in v.upper() for v in texts):raise ValueError('external XML declarations rejected')
        strings=[''.join(v.itertext()) for v in ET.fromstring(texts[2]).findall('s:si',XLSX_NS)] if len(texts)==4 else []
    out={}
    for row in ET.fromstring(sheet).findall('s:sheetData/s:row',XLSX_NS):
        index=int(row.get('r'))
        if index in out or not 1<=index<=max_rows:raise ValueError('spreadsheet row identity differs')
        values={}
        for cell in row.findall('s:c',XLSX_NS):
            address=cell.get('r','');column=re.sub(r'\d+','',address)
            if not re.fullmatch(r'[A-Z]{1,2}'+str(index),address) or column in values:raise ValueError('spreadsheet cell identity differs')
            kind=cell.get('t');value=cell.findtext('s:v',default='',namespaces=XLSX_NS)
            if cell.find('s:f',XLSX_NS) is not None:
                if kind=='str':value=value   # a cached formula text (the GLD disclaimer sheet); never a number
                else:raise ValueError('spreadsheet formula rejected')
            elif kind=='s' and value:
                si=int(value)
                if not 0<=si<len(strings):raise ValueError('spreadsheet shared string index differs')
                value=strings[si]
            elif kind=='inlineStr':value=''.join(cell.itertext())
            elif kind not in (None,'n','s','str'):raise ValueError('unreviewed spreadsheet cell type')
            values[column]=value
        out[index]=values
    return out

def excel_serial(value):
    text=str(value).strip()
    if not re.fullmatch(r'\d{4,6}',text):raise ValueError('Excel serial date differs')
    return (date(1899,12,30)+timedelta(days=int(text))).isoformat()

def dws_history(raw,ticker,ref,at):
    cells=xlsx_rows(raw)
    if cells.get(5,{}).get('B')!='Fund Name:' or cells.get(6,{}).get('B')!='Ticker:' or cells.get(6,{}).get('C')!=ticker or cells.get(7,{}).get('B')!='As of:':raise ValueError('DWS workbook identity differs')
    if [cells[9].get(c) for c in 'BCDEF']!=['Date','NAV','AUM','Outstanding shares','Dividends paid']:raise ValueError('DWS workbook columns differ')
    as_of=_day(cells[7]['C'],'%b %d, %Y');rows=[];ended=False
    for index in sorted(cells):
        if index<=9:continue
        values=cells[index];day=values.get('B','')
        if not re.fullmatch(r'\d{4,6}',day):
            if any(values.get(c,'') for c in 'CDE'):raise ValueError('unrecognized populated DWS row')
            ended=True;continue
        if ended:raise ValueError('observations resume after end of DWS table')
        nav=n.dec(values.get('C'),True);aum=n.dec(values.get('D'),True);shares=n.dec(values.get('E'),True);dividend=n.dec(values.get('F'),True)
        if nav is None or shares is None or aum is None:raise ValueError('DWS row incomplete')
        row=_row(excel_serial(day),nav,shares,aum,n.ds(nav),n.ds(shares),n.ds(aum),[_ref(ref)],{'source_row':index})
        row.update(distribution_decimal=n.ds(dividend),distribution_status='reported' if dividend is not None else 'not_reported');rows.append(row)
    if not rows or rows[-1]['date']>as_of:raise ValueError('DWS as-of differs from the last observation')
    rows.sort(key=lambda v:v['date'],reverse=True)
    return {'rows':n.validate_dates(rows,at),'scope':'Issuer NAV-history workbook (date, NAV, AUM, outstanding shares, dividends paid); whole current vintage as exported by the issuer.','source_columns':['Date','NAV','AUM','Outstanding shares','Dividends paid'],'as_of':as_of}

GLD_HEADER=['Date','Closing Price','Ounces of Gold per Share','NAV/Share at 10:30am NYT','Indicative Price per Share at 4:15pm NYT','Mid point of bid/ask spread at 4:15pm NYT','Premium/Discount of GLD Mid Point vs Indicative Price at 4:15pm NYT','Daily Share Volume','Total Ounces of Gold in the Trust','Tonnes of Gold','Total Net Asset Value in the Trust']

def gld_history(raw,ticker,ref,at):
    """SPDR Gold Trust archive: the trust publishes NAV per share (6 dp) and total net asset value (2 dp) but no share
    count; the share count is their quotient rounded to the share. The published precisions bound that quotient to a
    few shares (the archive's own figures place it within about four shares of the trust's 100,000-share baskets), so
    each row carries a ten-share display quantum rather than the one share a printed count would claim."""
    cells=xlsx_rows(raw,1,max_rows=12000)
    header=[cells.get(1,{}).get(c,'') for c in 'ABCDEFGHIJK']
    if header[:4]!=GLD_HEADER[:4] or header[8]!=GLD_HEADER[8] or header[10]!=GLD_HEADER[10]:raise ValueError('SPDR Gold archive columns differ')
    rows=[]
    with localcontext() as ctx:
        ctx.prec=50
        for index in sorted(cells):
            if index==1:continue
            values=cells[index];day=values.get('A','')
            if not re.fullmatch(r'\d{2}-[A-Za-z]{3}-\d{4}',day):raise ValueError('SPDR Gold archive date differs')
            nav_text=values.get('D','');tnav_text=values.get('K','');ounces_text=values.get('I','')
            if any(not re.fullmatch(r'-?[\d.]+(?:[Ee][+-]?\d+)?',v or '') for v in (nav_text,tnav_text,ounces_text)):continue   # 'US Holiday' and other non-observation rows
            nav=published(nav_text,6);tnav=published(tnav_text,2);ounces=published(ounces_text,2)
            if nav<=0 or tnav<=0:continue
            exact=tnav/nav;shares=exact.quantize(Decimal(1))
            row=_row(_day(day,'%d-%b-%Y'),nav,shares,tnav,n.ds(nav),n.ds(shares),n.ds(tnav),[_ref(ref)],
                {'source_row':index,'shares_basis':'total net asset value in the trust / NAV per share, rounded to the share','share_quotient_decimal':n.ds(exact.quantize(Decimal('0.0001'))),
                 'share_display_quantum_decimal':'10','total_ounces_decimal':n.ds(ounces),'ounces_per_share_decimal':n.ds(published(values.get('C'),8)) if values.get('C') else None})
            with localcontext() as inner:
                inner.prec=50;residual=tnav-nav*shares;sensitivity=nav*Decimal(10)+shares*Decimal('0.000001')+Decimal('0.01')
                row.update(aum_residual_decimal=n.ds(residual),aum_display_precision_sensitivity_decimal=n.ds(sensitivity),aum_within_display_precision=abs(residual)<=sensitivity)
            rows.append(row)
    rows.sort(key=lambda v:v['date'],reverse=True)
    return {'rows':n.validate_dates(rows,at),'scope':'SPDR Gold Trust historical archive (NAV per share, total net asset value, ounces); whole current vintage; share count derived as stated per row.','source_columns':GLD_HEADER}

# ---- ledger accrual --------------------------------------------------------------------------------------------------
def accrue(previous,snapshot,ticker,issuer,observed_at):
    """Append a day's reviewed snapshot to the retained ledger. A row already present for the snapshot date is never
    overwritten: a differing later observation is recorded as a restatement beside the first-observed row."""
    if previous is not None:
        if previous.get('contract')!=LEDGER_CONTRACT or previous.get('ticker')!=ticker:raise ValueError('ledger identity differs')
        rows=list(previous['rows']);restatements=list(previous.get('restatements',[]))
    else:rows=[];restatements=[]
    dates=[v['date'] for v in rows]
    if dates!=sorted(dates) or len(set(dates))!=len(dates) or len(rows)>LEDGER_MAX_ROWS:raise ValueError('ledger order or duplicates differ')
    if snapshot is not None:
        stamped={**snapshot,'observed_at':observed_at}
        if snapshot['date'] not in dates:
            rows.append(stamped);rows.sort(key=lambda v:v['date'])
        else:
            held=next(v for v in rows if v['date']==snapshot['date'])
            if any(held.get(k)!=snapshot.get(k) for k in ('nav_decimal','shares_decimal','net_assets_decimal')):
                restatements.append({'date':snapshot['date'],'held':{k:held.get(k) for k in ('nav_decimal','shares_decimal','net_assets_decimal')},
                    'observed':{k:snapshot.get(k) for k in ('nav_decimal','shares_decimal','net_assets_decimal')},'observed_at':observed_at,'source':snapshot['source']})
    for i,row in enumerate(rows,1):row['source_row']=i
    return {'contract':LEDGER_CONTRACT,'ticker':ticker,'issuer':issuer,'rows':rows,'restatements':restatements[-200:],
        'policy':'One reviewed issuer snapshot per observed as-of date, appended on the day it is observed; a later differing observation for a held date is listed as a restatement and never replaces the held row.'}

def parse_snapshot(ticker,originals,read,at):
    """The day's snapshot for one registry fund from its retained originals, or a ValueError naming the defect."""
    issuer=issuer_of(ticker);requests=requests_for(ticker)
    if ticker in VANECK:
        (pl,pu,_,_),(bl,bu,_,_)=requests
        return issuer,vaneck_snapshot(n.original(originals[pl],read,at,pu),n.original(originals[bl],read,at,bu),ticker,originals[pl],originals[bl])
    label,url,_,_=requests[0];raw=n.original(originals[label],read,at,url);ref=originals[label]
    if ticker in GLOBALX:return issuer,globalx_snapshot(raw,ticker,ref)
    if ticker in FIRST_TRUST:return issuer,firsttrust_snapshot(raw,ticker,ref)
    if ticker in ROBO:return issuer,robo_snapshot(raw,ticker,ref)
    if ticker in INVESCO:return issuer,invesco_snapshot(raw,ticker,ref)
    if ticker in USCF:return issuer,uscf_snapshot(raw,ticker,ref)
    raise KeyError(ticker)

def parse_history(ticker,originals,read,at):
    issuer=issuer_of(ticker)
    if ticker in AMPLIFY:
        prefix='amplify_'+ticker+'_';base=AMPLIFY_DOC.format(project=AMPLIFY[ticker],ticker=ticker,day='')
        refs=[originals[k] for k in sorted(originals) if k.startswith(prefix) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',k[len(prefix):])]
        if not refs:raise KeyError(prefix)
        for ref in refs:
            if not re.fullmatch(re.escape(base)+r'\d{4}-\d{2}-\d{2}',ref['url']):raise ValueError('Amplify document identity differs')
        return issuer,amplify_history([n.original(ref,read,at,ref['url']) for ref in refs],ticker,refs,at)
    label,url,_,_=requests_for(ticker)[0];raw=n.original(originals[label],read,at,url);ref=originals[label]
    if ticker in DWS:return issuer,dws_history(raw,ticker,ref,at)
    if ticker in SPDR_GOLD:return issuer,gld_history(raw,ticker,ref,at)
    raise KeyError(ticker)

def issuer_of(ticker):
    for group,name in ((GLOBALX,'Global X'),(VANECK,'VanEck'),(FIRST_TRUST,'First Trust'),(ROBO,'ROBO Global'),(INVESCO,'Invesco'),(USCF,'USCF'),
                       (AMPLIFY,'Amplify'),(DWS,'DWS Xtrackers'),(SPDR_GOLD,'SPDR Gold Trust')):
        if ticker in group:return name
    return None
