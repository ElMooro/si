import urllib.request
def get(u):
    with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Cache-Control":"no-cache"}),timeout=20) as r: return r.read().decode("utf-8","replace")
html=get("https://justhodl.ai/bottleneck-boom.html")
print("rev accel as %:", "+v+'%';}" in html.replace(' ',''))
print("P/E row label:", ">P/E</span>" in html)
print("P/S row label:", ">P/S</span>" in html)
print("Once growth stage:", "Once growth ~2y" in html)
print("vs ind on stages:", "% vs ind)" in html)
print("industry_ps used:", "fv.industry_ps" in html and "cur_ps_vs_ind_pct" in html)
print("DONE 2294")
