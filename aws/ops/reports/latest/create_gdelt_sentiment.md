# Create/update justhodl-gdelt-sentiment Lambda + EB rule

**Status:** success  
**Duration:** 6.8s  
**Finished:** 2026-04-27T18:43:28+00:00  

## Log
- `18:43:21`   zip: 4309 bytes
## 1. Lambda

- `18:43:22`   Lambda missing — creating
- `18:43:26` ✅   ✓ created justhodl-gdelt-sentiment
- `18:43:26` ✅   ✓ reserved concurrency = 1
- `18:43:27` ✅   ✓ Function URL: https://sfots654xx2sprhteqdtdpdmqy0kapla.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:43:27` ✅   ✓ created rule justhodl-gdelt-sentiment-30min
- `18:43:27` ✅   ✓ target → justhodl-gdelt-sentiment
- `18:43:27` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:43:27`   invoking justhodl-gdelt-sentiment…
- `18:43:28` ✅   ✓ smoke test passed
- `18:43:28`     error                    GDELT fetch failed: <urlopen error [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'data.gdeltproject.org'. (_ssl.c:1010)>
