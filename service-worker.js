// JustHodl.AI Service Worker — v1.1.0 (self-healing)
// Network-first for HTML so no client can wedge on a stale shell; old caches
// are purged and control is claimed immediately. /data/* is NEVER cached —
// live feeds must always be live.
const VERSION = "v1.1.0";
const CACHE_NAME = `justhodl-${VERSION}`;

self.addEventListener("install", (e) => { self.skipWaiting(); });

self.addEventListener("activate", (e) => {
  e.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)));
    await self.clients.claim();
  })());
});

self.addEventListener("fetch", (e) => {
  const url = new URL(e.request.url);
  if (e.request.method !== "GET" || url.origin !== self.location.origin) return;
  if (url.pathname.startsWith("/data/")) return; // always network, never cached

  const isHTML = e.request.mode === "navigate" ||
    (e.request.headers.get("accept") || "").includes("text/html");

  if (isHTML) {
    // network-first: fresh page whenever the network is up; cache only as offline fallback
    e.respondWith((async () => {
      try {
        const res = await fetch(e.request);
        const c = await caches.open(CACHE_NAME);
        c.put(e.request, res.clone());
        return res;
      } catch (_) {
        const hit = await caches.match(e.request);
        return hit || Response.error();
      }
    })());
    return;
  }

  // static assets: stale-while-revalidate
  e.respondWith((async () => {
    const c = await caches.open(CACHE_NAME);
    const hit = await c.match(e.request);
    const net = fetch(e.request).then(res => { c.put(e.request, res.clone()); return res; }).catch(() => null);
    return hit || (await net) || Response.error();
  })());
});
