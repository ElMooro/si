// JustHodl.AI Service Worker — v1.0
// Handles: offline cache shell, push notifications, click-to-open
const VERSION = "v1.0.19";
const CACHE_NAME = `justhodl-${VERSION}`;
const SHELL = [
  "/",
  "/index.html",
  "/manifest.json",
  "/portfolio.html",
  "/analogs.html",
  "/why.html",
  "/notifications.html",
];

// ─── Install: precache shell ───
self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) =>
      Promise.all(SHELL.map((url) =>
        cache.add(url).catch((e) => console.warn("[SW] precache miss", url, e))
      ))
    ).then(() => self.skipWaiting())
  );
});

// ─── Activate: drop old caches ───
self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((k) => k.startsWith("justhodl-") && k !== CACHE_NAME)
            .map((k) => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

// ─── Fetch: network-first with cache fallback ───
self.addEventListener("fetch", (event) => {
  const { request } = event;
  // Only handle GETs
  if (request.method !== "GET") return;
  const url = new URL(request.url);
  // Don't cache S3 data feeds (they need to be fresh)
  if (url.hostname.includes("s3.amazonaws.com") || url.hostname.includes("amazonaws.com")) {
    return;  // browser handles directly
  }
  // Don't cache external CDNs (jsdelivr, cloudflare)
  if (url.hostname !== self.location.hostname) return;

  event.respondWith(
    fetch(request)
      .then((response) => {
        // Cache successful HTML/asset responses
        if (response.ok && (request.destination === "document" || request.destination === "script" || request.destination === "style" || request.destination === "image")) {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(request, copy)).catch(() => {});
        }
        return response;
      })
      .catch(() => caches.match(request).then((r) => r || caches.match("/index.html")))
  );
});

// ─── Push notification handler ───
self.addEventListener("push", (event) => {
  let payload = { title: "JustHodl.AI", body: "New alert", url: "/" };
  if (event.data) {
    try {
      payload = { ...payload, ...event.data.json() };
    } catch (e) {
      payload.body = event.data.text() || payload.body;
    }
  }

  const options = {
    body: payload.body || "New alert from JustHodl",
    icon: payload.icon || "/manifest.json",  // SVG inline used as icon source
    badge: payload.badge || "/manifest.json",
    tag: payload.tag || "justhodl-alert",
    data: { url: payload.url || "/", ...payload.data },
    requireInteraction: payload.requireInteraction || false,
    actions: payload.actions || [
      { action: "open", title: "Open" },
      { action: "dismiss", title: "Dismiss" },
    ],
    timestamp: Date.now(),
    vibrate: [200, 100, 200],
  };

  event.waitUntil(self.registration.showNotification(payload.title, options));
});

// ─── Click: focus existing tab or open new ───
self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  if (event.action === "dismiss") return;
  const url = event.notification.data?.url || "/";
  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if (client.url.includes(url) && "focus" in client) return client.focus();
      }
      if (self.clients.openWindow) return self.clients.openWindow(url);
    })
  );
});

// ─── Background sync (placeholder for future "send queued message" use) ───
self.addEventListener("sync", (event) => {
  if (event.tag === "justhodl-sync") {
    console.log("[SW] background sync triggered");
  }
});

console.log("[SW] JustHodl.AI service worker loaded — version", VERSION);
