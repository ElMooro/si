import urllib.request, io, zipfile
url="https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx"
req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (compatible; JustHodlBot/1.0)"})
r=urllib.request.urlopen(req,timeout=45)
raw=r.read()
print("status:",r.status,"ctype:",r.headers.get("Content-Type"),"len:",len(raw))
print("first8 bytes:",raw[:8])
try:
    zf=zipfile.ZipFile(io.BytesIO(raw))
    sheets=[n for n in zf.namelist() if "worksheets/sheet" in n]
    print("sheets:",sheets)
    print("has sharedStrings:","xl/sharedStrings.xml" in zf.namelist())
    xml=zf.read(sorted(sheets)[0]).decode("utf-8","replace")
    print("sheet1 xml len:",len(xml))
    print("sheet1 head:",xml[:600])
    # find first occurrence of a <c ...><v> pattern
    i=xml.find("<c ")
    print("first <c> sample:",xml[i:i+200] if i>=0 else "NONE")
except Exception as e:
    print("zip err:",type(e).__name__,str(e)[:100])
    print("body head:",raw[:300])
print("DONE 2464")
