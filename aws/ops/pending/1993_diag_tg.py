import boto3, urllib.parse, urllib.request, json
ssm=boto3.client("ssm","us-east-1")
def gp(n,dec=False):
    try: return ssm.get_parameter(Name=n,WithDecryption=dec)["Parameter"]["Value"]
    except Exception as e: return f"__MISSING__ ({type(e).__name__})"
tok=gp("/justhodl/telegram/bot-token",True); cid=gp("/justhodl/telegram/chat_id")
print("SSM bot-token:", "present len="+str(len(tok)) if not tok.startswith("__MISSING") else tok)
print("SSM chat_id:", cid)
# test send
use_tok = tok if not tok.startswith("__MISSING") else "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
use_cid = cid if not cid.startswith("__MISSING") else "8678089260"
url=f"https://api.telegram.org/bot{use_tok}/sendMessage"
data=urllib.parse.urlencode({"chat_id":use_cid,"text":"✅ Boom Radar alert wiring is live. You'll now get catalyst-convergence alerts here.","parse_mode":"HTML"}).encode()
try:
    with urllib.request.urlopen(urllib.request.Request(url,data=data,method="POST"),timeout=12) as r:
        print("TEST SEND status:",r.status, r.read().decode()[:120])
except Exception as e:
    print("TEST SEND ERROR:",str(e)[:200])
print("DONE 1993")
