# ops 4699 — hiddenmetrix: raw content, not regex

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-08-15T15:48:14+00:00  

## Log
## 1. api.hiddenmetrix.com raw body (58 bytes seen before, never printed)

- `15:48:13`   status=200 ct=application/json body=b'{"app":"Hiddenmetrix","version":"2.0.0","docs":"disabled"}'
## 2. hiddenmetrix.com/data/BAMLH0A1HYBB raw body (29,938 bytes — regex found nothing, look directly)

- `15:48:14`   status=200 ct=text/html; charset=utf-8
- `15:48:14`   first 800 chars:
<!DOCTYPE html><html lang="de-DE" class="__variable_44151c __variable_47a102"><head><meta charSet="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"/><link rel="preload" href="/_next/static/media/0484562807a97172-s.p.woff2" as="font" crossorigin="" type="font/woff2"/><link rel="preload" href="/_next/static/media/4de1fea1a954a5b6-s.p.woff2" as="font" crossorigin="" type="font/woff2"/><link rel="preload" href="/_next/static/media/6d664cce900333ee-s.p.woff2" as="font" crossorigin="" type="font/woff2"/><link rel="preload" href="/_next/static/media/b957ea75a84b6ea7-s.p.woff2" as="font" crossorigin="" type="font/woff2"/><link rel="preload" href="/_next/static/media/eafabf029ad39a43-s.p.woff2" as="font" crossorigin="" type="font/woff2"/><link rel="pre
- `15:48:14`   ---
- `15:48:14`   last 800 chars:
ng\"}],[\"$\",\"link\",\"24\",{\"rel\":\"icon\",\"href\":\"/images/favicon-16x16.png\",\"sizes\":\"16x16\",\"type\":\"image/png\"}],[\"$\",\"link\",\"25\",{\"rel\":\"apple-touch-icon\",\"href\":\"/images/apple-touch-icon.png\"}],[\"$\",\"$L26\",\"26\",{}]],\"error\":null,\"digest\":\"$undefined\"}\n"])</script><script>self.__next_f.push([1,"25:\"$20:metadata\"\n1c:E{\"digest\":\"NEXT_HTTP_ERROR_FALLBACK;404\"}\n"])</script><script type="module" src="https://static.cloudflareinsights.com/beacon.min.js/v4513226cdae34746b4dedf0b4dfa099e1781791509496" integrity="sha512-ZE9pZaUXND66v380QUtch/5sE9tPFh2zg45pR2PB0CVkCtOREv2AJKkSidISWkysEuQ0EH8faUU5du78bx87UQ==" data-cf-beacon='{"version":"2024.11.0","token":"476ae1648fc644a4806eebddc3eb8aa2","r":1}' crossorigin="anonymous"></script>
</body></html>
- `15:48:14`   valid JSON: False | contains 'BAML': True | contains '7735' or '7,735': False | contains '1996': False
## 3. Homepage — hunt for the REAL API pattern (script refs, fetch/XHR hints, JSON blobs)

- `15:48:14`   script srcs found: ['/_next/static/chunks/4bd1b696-100b9d70ed4e49c1.js', '/_next/static/chunks/1255-8510363c7814390a.js', '/_next/static/chunks/main-app-f3336e172256d2ab.js', '/_next/static/chunks/9da6db1e-f5470b2ff655e955.js', '/_next/static/chunks/5662-e0e17d58e9a7cc95.js', '/_next/static/chunks/6360-e3027f4d9eea56f4.js', '/_next/static/chunks/2619-04bc32f026a0d946.js', '/_next/static/chunks/1647-de0113d1e1c14118.js', '/_next/static/chunks/9699-06183852f02b10b2.js', '/_next/static/chunks/1092-6e5d407e6e397354.js', '/_next/static/chunks/7273-3393c1ff7950c4b0.js', '/_next/static/chunks/4575-f9b92f34db282436.js', '/_next/static/chunks/8532-d00353d7eca414a5.js', '/_next/static/chunks/app/layout-fbd198ac5cfe5fb1.js', '/_next/static/chunks/app/error-93fc738a92ff39ff.js', '/_next/static/chunks/app/not-found-a92a4b7852e21845.js', '/_next/static/chunks/ca377847-aa3019222b0d840b.js', '/_next/static/chunks/5637-61a2fd76c6ea98fd.js', '/_next/static/chunks/2698-4e630e7ca36bd454.js', '/_next/static/chunks/6394-051667fe2c5f544b.js']
- `15:48:14`   '/...api...' path hints in HTML: ['/api']
- `15:48:14`   '/...batch...' path hints: []
- `15:48:14`   '...bb...' path hints (possible BB page route): []
## verdict

- `15:48:14` ✅ raw content inspected — see log above for whether real history is actually there
