from pathlib import Path
from http.server import BaseHTTPRequestHandler,HTTPServer
import json
R=Path(__file__).resolve().parents[3]
titles=['Rates & FX','Ω &lt;literal&gt;','<img src=x onerror="throw 1"> literal','Café ⚡ Research','Treasury & Settlement','Credit Review']
manifest={'generated_at':'2000-01-01','title_encoding':'unicode_text','n_pages':len(titles),'categories':[{'name':'Invented Research','count':len(titles),'pages':[{'href':'/invented-'+str(i)+'.html','title':s} for i,s in enumerate(titles)]}]}
page='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Invented navigation QA</title><style>body{margin:40px;background:#14130f;color:#eee;font:16px system-ui}a{color:#fcc65d}</style></head><body data-jh-no-chrome><h1>Invented navigation QA</h1><p>All six entries are synthetic. Use the navigation handle or Ctrl+B to browse.</p><script>
localStorage.setItem('jh_sw_gen','3372');localStorage.setItem('jh_favs','[]');localStorage.setItem('jh_tags','{}');sessionStorage.setItem('jh_diag_3276','1');
const realFetch=window.fetch.bind(window);window.fixtureRequests=[];window.fixtureErrors=[];
addEventListener('error',e=>window.fixtureErrors.push(e.message));
window.fetch=(url,options)=>{const u=new URL(url,location.href);if(u.origin!==location.origin||u.pathname!=='/nav-manifest.json'){throw new Error('Fixture disallows request');}window.fixtureRequests.push(u.pathname);return realFetch(url,options);};
</script><script src="/jh-nav-drawer.js"></script></body></html>'''
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  path=self.path.split('?',1)[0]
  assets={'/screener-fixture.html':('text/html; charset=utf-8',page.encode()),'/jh-nav-drawer.js':('application/javascript; charset=utf-8',(R/'jh-nav-drawer.js').read_bytes()),'/nav-manifest.json':('application/json',json.dumps(manifest,ensure_ascii=False).encode())}
  if path not in assets:self.send_response(404);self.end_headers();return
  kind,body=assets[path];self.send_response(200);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(body)));self.send_header('Cache-Control','no-store');self.send_header('Content-Security-Policy',"default-src 'none'; connect-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; base-uri 'none'; form-action 'none'");self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
server=HTTPServer(('127.0.0.1',0),Handler);address='http://127.0.0.1:'+str(server.server_port)+'/screener-fixture.html';print(address,flush=True);server.serve_forever()
