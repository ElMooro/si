"""Shared public route and same-origin source graph. Exact path evidence, never basename matching."""
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import urlsplit, unquote
import re
DENY={'aws','.github','scripts','ci','config','cloudflare','ops','docs','supabase','tools-src','chrome-extension','node_modules','vendor','_partials','_site','tests','.git'}
STRINGS=re.compile(r'//[^\n]*|/\*.*?\*/|(?P<q>[\'"`])(?P<s>(?:\\.|(?! (?P=q)).)*?)(?P=q)',re.S|re.X)
KEY=re.compile(r'(?<![A-Za-z0-9_./-])/?([a-zA-Z0-9_-]+(?:/[A-Za-z0-9_.-]+)+\.json(?:\.gz)?)(?![A-Za-z0-9_.-])')
IMPORT=re.compile(r'''(?:\b(?:import|export)\s+(?:[^;\n]*?\s+from\s*)?|\b(?:import|importScripts)\s*\()\s*["']([^"']+)["']''')
class HTML(HTMLParser):
    def __init__(self):super().__init__();self.scripts=[];self.inline=[];self.wires=[];self.inscript=False
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if tag=='script':
            self.inscript=True
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

def literal_keys(text):
    out=set();i=0;n=len(text)
    while i<n:
        if text.startswith('//',i):
            end=text.find('\n',i);i=n if end<0 else end+1;continue
        if text.startswith('/*',i):
            end=text.find('*/',i+2);i=n if end<0 else end+2;continue
        if text[i] not in ('"', "'", '`'):i+=1;continue
        quote=text[i];i+=1;value=[]
        while i<n:
            char=text[i];i+=1
            if char==quote:break
            if char=='\\' and i<n:char=text[i];i+=1
            value.append(char)
        value=re.sub(r'https?://[^/\s]+/','/',''.join(value))
        out.update(KEY.findall(value))
    return out

from functools import lru_cache
@lru_cache(maxsize=4096)
def asset_info(path):
    code=Path(path).read_text(errors='replace')
    return literal_keys(code),tuple(m[1] for m in IMPORT.finditer(code))

def page_graph(root,page):
    root=Path(root).resolve();page=Path(page).resolve();html=HTML();html.feed(page.read_text(errors='replace'))
    keys=set(r['feed'].lstrip('/') for r in html.wires);sources={};missing=[]
    pending=[(page,url) for url in html.scripts]
    for code in html.inline:
        keys.update(literal_keys(code));pending.extend((page,m[1]) for m in IMPORT.finditer(code))
    seen=set()
    while pending:
        parent,url=pending.pop();p=local_asset(root,parent,url)
        if not p:
            if not urlsplit(url).netloc:missing.append({'from':str(parent.relative_to(root)),'script':url})
            continue
        if p in seen:continue
        seen.add(p);asset_keys,imports=asset_info(str(p));sources[str(p.relative_to(root))]=True
        keys.update(asset_keys);pending.extend((p,url) for url in imports)
    return {'keys':sorted(keys),'scripts':sorted(sources),'missing_scripts':missing,'wires':html.wires,'evidence':'static exact-path source reference; runtime consumption unverified'}

def scan_pages(root):return {str(p.relative_to(root)):page_graph(root,p) for p in pages(root)}
