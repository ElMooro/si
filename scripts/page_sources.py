"""Shared public route and same-origin source graph. Exact path evidence, never basename matching."""
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import urlsplit, unquote
import re,json,subprocess
NON_CONSUMER_ASSETS={'private-artifacts.js','jh-data-inspector.js','auth.js','jh-wire.js'}
DENY={'aws','.github','scripts','ci','config','cloudflare','ops','docs','supabase','tools-src','chrome-extension','node_modules','vendor','_partials','_site','tests','.git'}
STRINGS=re.compile(r'//[^\n]*|/\*.*?\*/|(?P<q>[\'"`])(?P<s>(?:\\.|(?! (?P=q)).)*?)(?P=q)',re.S|re.X)
KEY=re.compile(r'(?<![A-Za-z0-9_./-])/?([a-zA-Z0-9_-]+(?:/[A-Za-z0-9_.-]+)+\.json(?:\.gz)?)(?![A-Za-z0-9_.-])')
IMPORT=re.compile(r'''(?:\b(?:import|export)\s+(?:[^;\n]*?\s+from\s*)?|\b(?:import|importScripts)\s*\()\s*["']([^"']+)["']''')
class HTML(HTMLParser):
    def __init__(self):super().__init__();self.scripts=[];self.inline=[];self.wires=[];self.primary_engines=[];self.inscript=False;self.redirect=None
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if tag=='meta' and a.get('http-equiv','').lower()=='refresh':self.redirect=a.get('content','')
        if tag=='meta' and a.get('name')=='jh-primary-engine':self.primary_engines.extend(x for x in re.split(r'[,;\s]+',a.get('content','')) if x)
        if tag=='script':
            self.inscript=a.get('type','').lower() in ('','module','text/javascript','application/javascript')
            if a.get('src'):self.scripts.append(a['src'])
            if a.get('data-feeds'):
                for part in a['data-feeds'].split(';'):
                    bits=[x.strip() for x in part.split('|')]
                    if bits[0]:self.wires.append({'feed':bits[0],'engine':bits[1] if len(bits)>1 else '', 'title':bits[2] if len(bits)>2 else bits[0], 'schema_version':a.get('data-schema-version','unspecified')})
    def handle_endtag(self,tag):
        if tag=='script':self.inscript=False
    def handle_data(self,data):
        if self.inscript:self.inline.append(data)

def pages(root):
    root=Path(root)
    return sorted(p for p in root.rglob('*.html') if not any(x in DENY or x.startswith('.') for x in p.relative_to(root).parts[:-1]))

def local_asset(root,parent,url):
    u=urlsplit(url)
    if u.scheme and (u.scheme!='https' or u.hostname not in ('justhodl.ai','www.justhodl.ai')):return None
    if u.netloc and u.hostname not in ('justhodl.ai','www.justhodl.ai'):return None
    path=unquote(u.path)
    candidate=(Path(root)/path.lstrip('/')) if path.startswith('/') else parent.parent/path
    candidate=candidate.resolve()
    try:candidate.relative_to(Path(root).resolve())
    except ValueError:return None
    return candidate if candidate.is_file() else None

_JS_CACHE={}

def prime_source_analysis(codes):
    missing=list(dict.fromkeys(code for code in codes if code not in _JS_CACHE))
    if not missing:return
    result=subprocess.run(['node',str(Path(__file__).with_name('js_source_refs.cjs'))],
        input=json.dumps(missing),text=True,capture_output=True,check=True)
    parsed=json.loads(result.stdout)
    if len(parsed)!=len(missing):raise ValueError('JavaScript ownership parser returned incomplete analysis')
    _JS_CACHE.update(zip(missing,parsed))

def source_analysis(code):
    prime_source_analysis([code]);return _JS_CACHE[code]

def literal_keys(text):
    return set(source_analysis(text)['keys'])

from functools import lru_cache
@lru_cache(maxsize=4096)
def asset_info(path):
    return source_analysis(Path(path).read_text(errors='replace'))

def page_graph(root,page):
    root=Path(root).resolve();page=Path(page).resolve();html=HTML();html.feed(page.read_text(errors='replace'))
    keys=set(r['feed'].lstrip('/') for r in html.wires);sources={};missing=[];direct_keys=set(keys);key_sources={str(page.relative_to(root)):set(keys)};parse_errors=[]
    pending=[(page,url) for url in html.scripts]
    for code in html.inline:
        analysis=source_analysis(code);inline_keys=set(analysis['keys']);keys.update(inline_keys);direct_keys.update(inline_keys);key_sources[str(page.relative_to(root))].update(inline_keys);pending.extend((page,url) for url in analysis['imports'])
        if analysis.get('error'):parse_errors.append({'source':str(page.relative_to(root)),'error':analysis['error']})
    seen=set()
    while pending:
        parent,url=pending.pop();p=local_asset(root,parent,url)
        if not p:
            if not urlsplit(url).netloc:missing.append({'from':str(parent.relative_to(root)),'script':url})
            continue
        if p in seen:continue
        seen.add(p);analysis=asset_info(str(p));asset_keys=set(analysis['keys']);imports=analysis['imports'];sources[str(p.relative_to(root))]=True
        if analysis.get('error'):parse_errors.append({'source':str(p.relative_to(root)),'error':analysis['error']})
        if str(p.relative_to(root)) not in NON_CONSUMER_ASSETS:
            keys.update(asset_keys);key_sources[str(p.relative_to(root))]=set(asset_keys)
        pending.extend((p,url) for url in imports)
    return {'keys':sorted(keys),'direct_keys':sorted(direct_keys),'key_sources':{source:sorted(values) for source,values in key_sources.items()},'primary_engines':html.primary_engines,'scripts':sorted(sources),'missing_scripts':missing,'script_parse_errors':parse_errors,'redirect':html.redirect,'wires':html.wires,'evidence':'static exact-path source reference; runtime consumption unverified'}

def scan_pages(root):
    root=Path(root);routes=pages(root);codes=[]
    for page in routes:
        html=HTML();html.feed(page.read_text(errors='replace'));codes.extend(html.inline)
    for path in root.rglob('*.js'):
        if not any(x in DENY or x.startswith('.') for x in path.relative_to(root).parts[:-1]):codes.append(path.read_text(errors='replace'))
    prime_source_analysis(codes)
    return {str(p.relative_to(root)):page_graph(root,p) for p in routes}
