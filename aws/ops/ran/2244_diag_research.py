import urllib.request, json, time, boto3
LAMBDA_URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
CDN="https://justhodl-data-proxy.raafouis.workers.dev/equity-research"
def hit(u, method="GET", timeout=120, origin="https://justhodl.ai"):
    try:
        req=urllib.request.Request(u, method=method, headers={"User-Agent":"jh-verify","Origin":origin})
        t=time.time()
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body=r.read(400).decode("utf-8","replace")
            acao=r.headers.get("access-control-allow-origin")
            return f"status={r.status} time={time.time()-t:.1f}s ACAO={acao!r} body[:120]={body[:120]!r}"
    except urllib.error.HTTPError as e:
        return f"HTTPError {e.code} ACAO={e.headers.get('access-control-allow-origin')!r} body={e.read(200).decode('utf-8','replace')!r}"
    except Exception as e:
        return f"{type(e).__name__}: {str(e)[:90]}"
print("1) CDN cache for LDOS (expect miss):")
print("   ", hit(f"{CDN}/LDOS.json?v={int(time.time())}", timeout=20))
print("2) CDN cache for AAPL (expect hit — the working path):")
print("   ", hit(f"{CDN}/AAPL.json?v={int(time.time())}", timeout=20))
print("3) Lambda function URL GET for LDOS (the failing path) — CORS header + timing + result:")
print("   ", hit(f"{LAMBDA_URL}?ticker=LDOS", timeout=150))
print("4) Lambda function URL OPTIONS preflight (CORS):")
print("   ", hit(f"{LAMBDA_URL}?ticker=LDOS", method="OPTIONS", timeout=20))
# Lambda + function-url config
lam=boto3.client("lambda","us-east-1")
try:
    fn="justhodl-equity-research"
    c=lam.get_function_configuration(FunctionName=fn)
    print(f"5) Lambda {fn}: state={c.get('State')} timeout={c.get('Timeout')}s mem={c.get('MemorySize')} lastmod={c.get('LastModified')}")
    try:
        fu=lam.get_function_url_config(FunctionName=fn)
        print("   function URL:",fu.get("FunctionUrl"),"| authType=",fu.get("AuthType"),"| CORS=",json.dumps(fu.get("Cors")))
    except Exception as e: print("   function URL config:",str(e)[:80])
except Exception as e:
    print("5) Lambda lookup err:",str(e)[:100])
print("DONE 2244")
