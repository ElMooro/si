import urllib.request, json
def get(url, t=30):
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=t) as r: return r.status, r.read().decode("utf-8","ignore")

print("=== (1) live page: valuations.html ===")
try:
    st, html = get("https://justhodl.ai/valuations.html?t=live")
    checks={
      "EDGAR Authority heading": "EDGAR Authority" in html,
      "edgar mount div": 'id="edgar"' in html,
      "renderEdgar() defined+called": html.count("renderEdgar")>=2,
      "fetches edgar feed": "data/edgar-authority.json" in html,
      "net-net table render": "NCAV NET-NET" in html,
      "crosscheck status render": "AUTHORITATIVE CROSS-CHECK" in html,
    }
    print(f"  http={st}")
    for k,v in checks.items(): print(f"    {'OK ' if v else 'MISS'} {k}")
except Exception as e: print("  page error:", str(e)[:90])

print("\n=== (2) live feed: data/edgar-authority.json ===")
try:
    st, body = get("https://justhodl.ai/data/edgar-authority.json?t=live")
    d=json.loads(body); cc=d.get("crosscheck",{})
    print(f"  http={st} gen={d.get('generated_at','')[:16]} elapsed={d.get('elapsed_s')}s")
    print(f"  net-nets credible={d.get('n_net_nets')} (raw={d.get('n_net_nets_raw')}) classic={d.get('n_classic_net_nets')} ncav_cov={d.get('ncav_coverage')}")
    print(f"  cross-check checked={cc.get('n_checked')} clean={cc.get('n_clean')} flagged={cc.get('n_flagged')} unverified={cc.get('n_unverified')}")
    print("  top net-nets:", [f"{x['ticker']}(-{x['discount_pct']}%)" for x in d.get("net_nets",[])[:6]])
    print("  RENDER-READY:", bool(d.get("net_nets") is not None and cc.get("n_checked")))
except Exception as e: print("  feed error:", str(e)[:90])
