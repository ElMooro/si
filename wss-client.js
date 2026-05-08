/* JustHodl.AI — WebSocket live-data client.
 *
 * Drop-in vanilla JS. No deps. Adds itself as window.justhodlWss with:
 *   .subscribe(channel, callback)     — register a callback for a channel
 *   .unsubscribe(channel, callback)
 *   .state()                          — { connected, retries, channels }
 *   .send(obj)                        — send arbitrary message
 *
 * Channels (push only — pages still fetch full S3 JSONs as needed):
 *   report compound regime cross_asset options_flow eurodollar
 *   nobrainers narrative alerts system
 *
 * Connection strategy:
 *   1. Try the configured WSS endpoint immediately.
 *   2. On disconnect, exponential-backoff reconnect: 1s, 2s, 4s, 8s, 16s, 30s cap.
 *   3. After 5 consecutive failures, give up the WS attempt for this session
 *      (page polling continues to work). Reconnect again on user click /
 *      next page navigation.
 *   4. Server pings every 60s; client responds with {action:'ping'}.
 *
 * Page integration example:
 *   <script src="/wss-client.js"></script>
 *   <script>
 *     justhodlWss.subscribe("regime", (msg) => {
 *       if (msg.payload.regime !== currentRegime) location.reload();
 *     });
 *   </script>
 */
(function () {
  // Endpoint discovery: hard-coded for now; can be replaced with a fetch
  // to /wss-endpoint.json or similar later. Replaced by ops 365.
  const WSS_ENDPOINT = "wss://q7vco36knh.execute-api.us-east-1.amazonaws.com/prod";

  const RECONNECT_BACKOFF_MS = [1000, 2000, 4000, 8000, 16000, 30000];
  const MAX_FAILURES = 5;
  const PING_INTERVAL_MS = 60000;

  // Sentinel kept here for the deploy-time injector to detect "patched":
  // marker:__WS_API_ID__-replaced

  const subscribers = new Map();   // channel → Set<callback>
  let socket = null;
  let connected = false;
  let retries = 0;
  let pingTimer = null;
  let reconnectTimer = null;
  let givenUp = false;

  function log(...args) {
    if (window.JUSTHODL_WSS_DEBUG) console.log("[wss]", ...args);
  }

  function shouldConnect() {
    // Only run on real domain (not localhost while developing) — or override with flag
    if (window.JUSTHODL_WSS_FORCE) return true;
    const h = window.location.hostname;
    return h === "justhodl.ai" || h === "www.justhodl.ai";
  }

  function connect() {
    if (givenUp || socket) return;
    try {
      socket = new WebSocket(WSS_ENDPOINT);
    } catch (e) {
      log("WS construct failed:", e);
      scheduleReconnect();
      return;
    }
    socket.addEventListener("open", () => {
      connected = true;
      retries = 0;
      log("connected");
      // Resubscribe to all channels we have callbacks for
      const channels = [...subscribers.keys()].filter((c) => subscribers.get(c).size > 0);
      if (channels.length) {
        send({ action: "subscribe", channels });
      }
      // Start ping
      pingTimer = setInterval(() => send({ action: "ping" }), PING_INTERVAL_MS);
    });
    socket.addEventListener("message", (ev) => {
      let msg;
      try { msg = JSON.parse(ev.data); } catch (e) { return; }
      if (msg.action === "push" && msg.channel) {
        const cbs = subscribers.get(msg.channel);
        if (cbs) cbs.forEach((cb) => {
          try { cb(msg); } catch (e) { log("subscriber threw:", e); }
        });
      } else if (msg.action === "pong") {
        log("pong");
      } else if (msg.action === "subscribed" || msg.action === "unsubscribed") {
        log(msg.action, msg.channels);
      } else if (msg.action === "error") {
        log("server error:", msg.message);
      }
    });
    socket.addEventListener("close", () => {
      connected = false;
      socket = null;
      if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
      log("disconnected");
      scheduleReconnect();
    });
    socket.addEventListener("error", (e) => {
      log("ws error:", e);
    });
  }

  function scheduleReconnect() {
    if (givenUp) return;
    if (retries >= MAX_FAILURES) {
      log(`giving up after ${MAX_FAILURES} failures — page-poll fallback continues`);
      givenUp = true;
      return;
    }
    const delay = RECONNECT_BACKOFF_MS[Math.min(retries, RECONNECT_BACKOFF_MS.length - 1)];
    retries += 1;
    log(`reconnect attempt ${retries} in ${delay}ms`);
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connect, delay);
  }

  function send(obj) {
    if (!connected || !socket) return false;
    try { socket.send(JSON.stringify(obj)); return true; }
    catch (e) { log("send failed:", e); return false; }
  }

  function subscribe(channel, callback) {
    if (typeof callback !== "function") throw new Error("subscribe callback must be a function");
    if (!subscribers.has(channel)) subscribers.set(channel, new Set());
    subscribers.get(channel).add(callback);
    if (connected) send({ action: "subscribe", channels: [channel] });
  }

  function unsubscribe(channel, callback) {
    const set = subscribers.get(channel);
    if (!set) return;
    set.delete(callback);
    if (set.size === 0) {
      subscribers.delete(channel);
      if (connected) send({ action: "unsubscribe", channels: [channel] });
    }
  }

  function state() {
    return {
      connected, retries, given_up: givenUp,
      channels: [...subscribers.keys()],
    };
  }

  // Public API
  window.justhodlWss = { subscribe, unsubscribe, state, send, connect };

  // Auto-connect on load (if on the right host)
  if (shouldConnect()) {
    if (document.readyState === "complete") connect();
    else window.addEventListener("load", connect);
  }
})();
