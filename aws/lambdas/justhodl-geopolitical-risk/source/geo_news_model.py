"""Dated RSS observations, not geopolitical probabilities or market pricing.

Every configured feed and parsed entry is accounted for. Title grouping is a
mechanical duplicate diagnostic, not verification of a unique real-world event.
"""
from datetime import datetime,timedelta,timezone
from email.utils import parsedate_to_datetime
from collections import Counter
import gzip,hashlib,json,re,unicodedata,urllib.parse,xml.etree.ElementTree as ET,zlib
from io import BytesIO

CONTRACT='geopolitical-news-research.v1'
GROUPS=('geopolitics','analysis','crisis','security','official','policy')
FLAGS=('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')
TERMS=re.compile(r'\b(?:war|invasion|invad\w*|strike|airstrike|missile|attack|sanction\w*|conflict|troops|military|nuclear|coup|assassinat\w*|escalat\w*|ceasefire|casualt\w*|killed|bombing|offensive|retaliat\w*|blockade|incursion|warhead|deployment|mobiliz\w*|shelling|drone strike)\b',re.I)
MARKET=re.compile(r'\b(?:default\w*|devalu\w*|capital control\w*|bond\w*|yield\w*|currency|inflation|central bank|rate hike|recession|debt crisis|imf|bailout)\b',re.I)


def sha(raw):return hashlib.sha256(raw).hexdigest()
def encode(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')


def instant(value):
    if not isinstance(value,str) or not 1<=len(value)<=128:return None
    try:
        try:out=datetime.fromisoformat(value.strip().replace('Z','+00:00'))
        except ValueError:out=parsedate_to_datetime(value.strip())
        if out.tzinfo is None:return None
        return out.astimezone(timezone.utc)
    except (ValueError,TypeError,OverflowError):return None


def corpus(c):
    rows={}
    for group in GROUPS:
        for item in c.get(group,[]):
            url=item['url'];parts=urllib.parse.urlsplit(url)
            if parts.scheme not in ('https','http') or not parts.hostname or parts.username or parts.password or parts.fragment:
                raise ValueError('Complete public feed identity required')
            if url not in rows:
                rows[url]={'feed_id':sha(url.encode()),'name':item['name'],'url':url,'groups':[]}
            rows[url]['groups'].append(group)
    if not rows:raise ValueError('Configured feed population missing')
    return list(rows.values())


def parse(raw,feed,encoding=''):
    if not isinstance(raw,bytes) or not raw:raise ValueError('Empty feed body')
    original_sha=sha(raw)
    coding=(encoding or '').strip().lower()
    if coding in ('gzip','x-gzip'):
        with gzip.GzipFile(fileobj=BytesIO(raw)) as stream:raw=stream.read(8*1024*1024+1)
    elif coding=='deflate':
        decoder=zlib.decompressobj();raw=decoder.decompress(raw,8*1024*1024+1)
        if not decoder.eof or decoder.unused_data:raise ValueError('Incomplete or concatenated deflate response')
    elif coding not in ('','identity'):raise ValueError('Unsupported declared content encoding')
    if len(raw)>8*1024*1024:raise ValueError('Complete decoded feed exceeds safe bound')
    declaration_scan=raw.replace(b'\x00',b'').upper()
    if b'<!DOCTYPE' in declaration_scan or b'<!ENTITY' in declaration_scan:raise ValueError('XML declarations outside reviewed feed scope')
    root=ET.fromstring(raw)
    tag=lambda node:node.tag.rsplit('}',1)[-1].lower()
    if tag(root) not in ('rss','feed','rdf'):raise ValueError('RSS or Atom document required')
    entries=[]
    for item in root.iter():
        if tag(item) not in ('item','entry'):continue
        fields={};link=None
        for child in item:
            name=tag(child);text=''.join(child.itertext()).strip()
            if name in ('title','pubdate','published','updated','date','guid','id'):
                fields.setdefault(name,[]).append(text)
            if name=='link' and (child.attrib.get('rel','alternate')=='alternate'):
                link=child.attrib.get('href') or text or link
        title=(fields.get('title') or [''])[0]
        date_fields=[v for key in ('pubdate','published','date') for v in fields.get(key,[])]
        parsed=[instant(v) for v in date_fields]
        # Conflicting or invalid publication declarations cannot borrow an
        # acquisition/update clock. Atom updated is retained only as context.
        published=parsed[0] if parsed and all(p is not None and p==parsed[0] for p in parsed) else None
        if link:
            link=urllib.parse.urljoin(feed['url'],link)
            parts=urllib.parse.urlsplit(link)
            if parts.scheme not in ('https','http') or not parts.hostname or parts.username or parts.password:link=None
        index=len(entries)
        entries.append({'entry_id':sha(encode([feed['feed_id'],original_sha,index])),
                        'feed_id':feed['feed_id'],'entry_index':index,'title':title,'link':link,
                        'publication_declarations':date_fields,'publisher_updated_declarations':fields.get('updated',[]),
                        'published_at':published.isoformat() if published else None,
                        'publication_date_status':'dated' if published else 'missing_invalid_or_conflicting',
                        'normalized_title':unicodedata.normalize('NFKC',' '.join(title.split())).casefold(),
                        'feed_guid_declarations':fields.get('guid',[])+fields.get('id',[])})
    return entries


def build(feeds,captures,countries,at,sovereign,fetch_raw=None):
    now=instant(at)
    if now is None:raise ValueError('Frozen aware calculation clock required')
    expected={f['feed_id']:f for f in feeds}
    if len(expected)!=len(feeds) or len(captures)!=len(feeds) or {r['feed_id'] for r in captures}!=set(expected):
        raise ValueError('Every configured feed must have one acquisition result')
    rows=[];entries=[];statuses=Counter();valid_feeds=0;parsed_bytes=0
    for capture in captures:
        feed=expected[capture['feed_id']];row={**feed,'acquired_at':capture['acquired_at'],
            'http_status':capture.get('http_status'),'body_sha256':capture.get('body_sha256'),
            'body_bytes':capture.get('body_bytes'),'status':capture['status'],'parsed_entries':0}
        if capture['status']=='http_response' and capture.get('http_status')==200:
            raw=fetch_raw(capture) if fetch_raw else capture['raw']
            if len(raw)!=capture['body_bytes'] or sha(raw)!=capture['body_sha256']:raise ValueError('Whole feed bytes differ')
            try:parsed=parse(raw,feed,capture.get('content_encoding'))
            except (ValueError,ET.ParseError,UnicodeError,OSError,EOFError,zlib.error):row['status']='unparseable_feed'
            else:
                row['status']='parsed';valid_feeds+=1;row['parsed_entries']=len(parsed)
                for entry in parsed:
                    stamp=instant(entry['published_at'])
                    state=('missing_title' if not entry['title'] else 'undated_publication' if stamp is None else
                           'future_publication' if stamp>now else 'outside_48h' if stamp<now-timedelta(hours=48) else 'within_48h')
                    entry.update(window_status=state,within_24h=state=='within_48h' and stamp>=now-timedelta(hours=24))
                    parsed_bytes+=len(encode(entry))
                    if parsed_bytes>32*1024*1024:raise ValueError('Whole parsed population exceeds safe publication bound; nothing truncated')
                    statuses[state]+=1;entries.append(entry)
        rows.append(row)
    if not valid_feeds:raise ValueError('No parseable feeds; preserve previous publication')
    groups={}
    for e in entries:
        if e['window_status']=='within_48h':groups.setdefault(e['normalized_title'],[]).append(e)
    country_rows=[]
    for country,aliases in sorted(countries.items()):
        pattern=re.compile(r'(?<!\w)(?:'+'|'.join(re.escape(a) for a in aliases)+r')(?!\w)',re.I)
        matched=[g for title,g in groups.items() if pattern.search(title)]
        raw_entries=[e for g in matched for e in g]
        conflict=sum(bool(TERMS.search(g[0]['title'])) for g in matched)
        dates=[e['published_at'] for e in raw_entries]
        country_rows.append({'country':country,'matched_aliases':aliases,
            'mentions_24h':sum(any(e['within_24h'] for e in g) for g in matched),'mentions_48h':len(matched),
            'raw_mentions_24h':sum(e['within_24h'] for e in raw_entries),'raw_mentions_48h':len(raw_entries),
            'velocity_per_day':len(matched)/2,'crisis_hits':conflict,
            'market_hits':sum(bool(MARKET.search(g[0]['title'])) for g in matched),
            'crisis_share':conflict/len(matched) if matched else None,
            'first_observed_publication':min(dates,default=None),'last_observed_publication':max(dates,default=None),
            'entry_ids':[e['entry_id'] for e in raw_entries],
            'stress_score':None,'velocity_z':None,'crisis_z':None,'velocity_delta':None})
    failed=sum(r['status']!='parsed' for r in rows)
    uncertain_dates=statuses['undated_publication']+statuses['future_publication']
    return {'contract':CONTRACT,'version':'2.0.0','generated_at':at,'ok':True,**dict.fromkeys(FLAGS,False),
        'portfolio_action':'WAIT','meaning':'research abstention','global_temp':None,'top_country':None,'escalating':[],
        'rankings':country_rows,'entries':entries,'feeds':rows,
        'sources':{'feeds_in_corpus':len(feeds),'feeds_attempted':len(captures),'feeds_responding':valid_feeds,
                   'articles_scanned':len(entries),'dated_title_groups_48h':len(groups),'raw_dated_entries_48h':statuses['within_48h'],
                   'entry_window_counts':dict(statuses),'failed_or_unparseable_feeds':failed,
                   'corpus_attribution':'Curated public RSS URLs adapted from koala73/worldmonitor (AGPL-3.0); measurements are JustHodl-original.'},
        'quality':{'status':'partial' if failed or uncertain_dates else 'fresh','basis':'Acquisition coverage and explicit date exclusions, not economic-risk freshness.',
                   'observation_window_start':(now-timedelta(hours=48)).isoformat(),'observation_window_end':now.isoformat()},
        'gssi_cross':{'mapped_n':0,'rows':[],'news_leads':[],'priced_in':[],'source':'data/global-sovereign.json',
                      'source_generated_at':sovereign.get('generated_at') if isinstance(sovereign,dict) else None,
                      'status':'comparison_unqualified','note':'Different unvalidated news and sovereign scores cannot establish market pricing, confirmation or lead/lag.'},
        'methodology':{'mentions':'Distinct NFKC/casefold/whitespace-normalized title groups matching a configured country alias, within dated 24/48-hour windows. Raw feed-entry counts are separate.',
                       'publication_dates':'An explicit, aware and non-conflicting publisher publication date is required. Update, ingestion and generation clocks are never substituted. Future and undated entries remain inspectable but excluded.',
                       'title_groups':'Mechanical title grouping does not verify unique stories, independent sources or real-world events. Syndication and publisher selection bias remain.',
                       'lexicon':'Literal conflict and market vocabulary counts; ambiguous words such as strike or default are not event/severity determinations.',
                       'coverage':'All configured URLs are attempted. Failed/unparseable feeds remain visible; no unobserved source count is filled with zero. Valid empty feeds are distinct from failures.',
                       'history':'Old 20-day-labelled scores and 90-day archives are retained separately. New definitions are not compared with incompatible legacy observations.',
                       'limits':'No stress ranking, probability, escalation alert, priced-in conclusion, independent evidence vote or portfolio permission is established.'}}
