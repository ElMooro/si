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

  // ────────────────────────────────────────────────────────────────────
  // Self-injecting LIVE status pill (top-right). Pages opt out by setting
  // window.JUSTHODL_WSS_NO_PILL = true *before* loading this script.
  // ────────────────────────────────────────────────────────────────────
  function injectPill() {
    if (window.JUSTHODL_WSS_NO_PILL) return;
    if (document.getElementById("wss-status")) return;  // already present (e.g. index.html)
    if (!document.body) return;

    // Inject CSS once
    if (!document.getElementById("wss-pill-style")) {
      const style = document.createElement("style");
      style.id = "wss-pill-style";
      style.textContent = `
        #wss-status {
          position: fixed; top: 10px; right: 12px; z-index: 999;
          display: inline-flex; align-items: center; gap: 6px;
          padding: 4px 10px; border-radius: 12px;
          font: 600 10px/1.2 -apple-system, system-ui, sans-serif;
          letter-spacing: 0.04em; text-transform: uppercase;
          background: rgba(20, 20, 20, 0.85); color: #888;
          border: 1px solid #2a2a2a; backdrop-filter: blur(6px);
          transition: all 0.2s; pointer-events: none; user-select: none;
        }
        #wss-status::before { content: "●"; font-size: 12px; }
        #wss-status.connected { color: #22c55e; border-color: rgba(34,197,94,0.3); }
        #wss-status.connecting { color: #f59e0b; border-color: rgba(245,158,11,0.3); }
        #wss-status.polling { color: #888; }
        #wss-status.connected::before { animation: wssPulse 2s ease-in-out infinite; }
        @keyframes wssPulse { 0%,100%{opacity:1;} 50%{opacity:0.4;} }
      `;
      document.head.appendChild(style);
    }

    const pill = document.createElement("div");
    pill.id = "wss-status";
    pill.className = "polling";
    pill.title = "Real-time data link status";
    pill.textContent = "POLLING";
    document.body.appendChild(pill);

    function updatePill() {
      const s = state();
      if (s.connected) {
        pill.textContent = "LIVE";
        pill.className = "connected";
        pill.title = `WS connected · ${s.channels.length} channels`;
      } else if (!s.given_up && s.retries > 0) {
        pill.textContent = "RECONNECTING";
        pill.className = "connecting";
        pill.title = `Attempt ${s.retries}`;
      } else {
        pill.textContent = "POLLING";
        pill.className = "polling";
        pill.title = "Falling back to S3 polling";
      }
    }
    setInterval(updatePill, 2000);
    updatePill();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", injectPill);
  } else {
    injectPill();
  }

  // Auto-connect on load (if on the right host)
  if (shouldConnect()) {
    if (document.readyState === "complete") connect();
    else window.addEventListener("load", connect);
  }
})();
