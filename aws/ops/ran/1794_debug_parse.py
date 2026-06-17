import urllib.request, json, calendar
UA={"User-Agent":"JustHodl Research raafouis@gmail.com","Accept":"application/json, text/plain, */*"}
def get(url,t=45):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return None
FD="https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
MON={"Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06","Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}

print("=== TIC: first 18 non-empty lines (raw, show tabs as |) ===")
body=get("https://ticdata.treasury.gov/Publish/mfhhis01.txt")
cnt=0
for ln in body.split("\n"):
    if ln.strip():
        print("  ",repr(ln[:90]))
        cnt+=1
        if cnt>=18: break

print("\n=== TIC parser run: max date + Japan tail + grand-total name ===")
lines=body.split("\n"); series={}; i=0; blocks=0
while i<len(lines):
    cells=[c.strip() for c in lines[i].split("\t")]
    months=[c for c in cells if c in MON]
    if len(months)>=6 and i+1<len(lines):
        yrs=[c.strip() for c in lines[i+1].split("\t")]; years=[c for c in yrs if c.isdigit() and len(c)==4]
        dates=[f"{years[k]}-{MON[months[k]]}" for k in range(min(len(months),len(years)))]
        blocks+=1; i+=2
        while i<len(lines):
            row=lines[i]; rc=[c.strip() for c in row.split("\t")]
            if not row.strip() or len([c for c in rc if c in MON])>=6: break
            name=rc[0].strip().strip('"')
            if name and len(name)>1 and dates:
                for d,v in zip(dates,rc[1:1+len(dates)]):
                    try: series.setdefault(name,{})[d]=float(v.replace(",",""))
                    except: pass
            i+=1
    else: i+=1
print("  blocks parsed:",blocks,"| countries:",len(series))
jp=sorted(series.get("Japan",{}).items()); print("  Japan max date:",jp[-1] if jp else None," n=",len(jp))
print("  name candidates w/ 'Total' or 'China':",[k for k in series if 'Total' in k or 'China' in k][:6])

print("\n=== MTS: ALL rows for record 2026-05-31 with classification 'May' ===")
b=get(FD+"/v1/accounting/mts/mts_table_1?filter=record_date:eq:2026-05-31,classification_desc:eq:May&fields=record_date,classification_desc,parent_id,table_nbr,line_code_nbr,current_month_gross_rcpt_amt,current_month_gross_outly_amt,current_month_dfct_sur_amt&page[size]=50")
j=json.loads(b)
for r in j["data"]:
    print("  ",{k:r.get(k) for k in ('parent_id','table_nbr','line_code_nbr','current_month_dfct_sur_amt')})
