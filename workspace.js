(function () {
  "use strict";

  var PROXY = (window.JUSTHODL_AUTH_CONFIG && window.JUSTHODL_AUTH_CONFIG.syncBase)
    || "https://justhodl-data-proxy.raafouis.workers.dev";
  var STORAGE_PREFIX = "jh_engine_workspace_v1:";
  var DEVICE_KEY = "jh_engine_workspace_device_v1";
  var SCHEMA_VERSION = 1;
  var PAGE_SIZE = 100;
  var MAX_CARDS = 500;
  var ZONES = [
    { id: "conviction", title: "Conviction KPIs", hint: "The signals you read first" },
    { id: "synthesis", title: "Strategic / Tactical Synthesis", hint: "Posture and decision context" },
    { id: "analysis", title: "Analysis Panels", hint: "Evidence behind the call" },
    { id: "ranked", title: "Ranked & Detail Panels", hint: "Names, tables, and drill-downs" },
  ];
  var state = { schema: SCHEMA_VERSION, cards: [], updatedAt: null };
  var engines = [];
  var engineByName = new Map();
  var contracts = {};
  var feedCatalog = {};
  var feedCatalogMap = new Map();
  var feedCache = new Map();
  var visibleCards = new Set();
  var observer;
  var saveTimer;
  var authReady = false;
  var cloudHydratedFor = null;
  var currentScope = null;
  var authEpoch = 0;
  var editRevision = 0;
  var cloudRevision = 0;
  var saveChain = Promise.resolve();
  var pendingSave = null;
  var hydrationController = null;
  var draggedCardId = null;
  var pointerDrag = null;
  var libraryLimit = PAGE_SIZE;
  var libraryOpener = null;
  var theme = "dark";

  function el(id) { return document.getElementById(id); }
  function announce(message) { el("announcer").textContent = message; }
  function uid() { return "card-" + Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 8); }
  function clampText(value, max) {
    var text = value == null ? "" : String(value);
    return text.length > max ? text.slice(0, max - 1) + "…" : text;
  }
  function isObject(value) { return value && typeof value === "object" && !Array.isArray(value); }
  function isFeed(path) { return typeof path === "string" && (/\.json(?:l)?(?:$|\?)/i.test(path) || path.indexOf("data/") === 0); }
  function cleanFeed(path) {
    if (!path) return "";
    return String(path).replace(/^s3:\/\/[^/]+\//, "").replace(/^https?:\/\/justhodl-dashboard-live[^/]*\//, "").replace(/^\/+/, "");
  }
  function formatEngineName(name) {
    return String(name || "Engine").replace(/^justhodl-/, "").replace(/[-_]+/g, " ").replace(/\b\w/g, function (c) { return c.toUpperCase(); });
  }
  function searchText(value) {
    return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  }
  function deviceScope() {
    try {
      var id = localStorage.getItem(DEVICE_KEY);
      if (!id) {
        id = (self.crypto && crypto.randomUUID ? crypto.randomUUID() : uid()).replace(/[^a-zA-Z0-9_-]/g, "");
        localStorage.setItem(DEVICE_KEY, id);
      }
      return "anonymous:" + id;
    } catch (_) { return "anonymous:ephemeral"; }
  }
  function accountScope(user) { return user && user.id ? "account:" + String(user.id) : deviceScope(); }
  function storageKey(scope) { return STORAGE_PREFIX + scope; }
  function readLocal(scope) {
    try {
      var parsed = JSON.parse(localStorage.getItem(storageKey(scope || currentScope || deviceScope())) || "null");
      return validateWorkspace(parsed) ? parsed : null;
    } catch (_) { return null; }
  }
  function writeLocal(scope, snapshot) {
    try {
      localStorage.setItem(storageKey(scope || currentScope || deviceScope()), JSON.stringify(snapshot || state));
      return true;
    } catch (_) { return false; }
  }
  function validateWorkspace(candidate) {
    return !!(candidate && candidate.schema === SCHEMA_VERSION && Array.isArray(candidate.cards)
      && candidate.cards.length <= MAX_CARDS && candidate.cards.every(function (card) {
        return card && typeof card.id === "string" && typeof card.engine === "string"
          && ZONES.some(function (zone) { return zone.id === card.zone; });
      }));
  }
  function normalizeCard(card) {
    return {
      id: String(card.id || uid()).slice(0, 80),
      zone: ZONES.some(function (z) { return z.id === card.zone; }) ? card.zone : "analysis",
      engine: String(card.engine || "").slice(0, 180),
      feed: cleanFeed(card.feed || ""),
      renderer: ["auto", "kpi", "list", "table", "json"].indexOf(card.renderer) >= 0 ? card.renderer : "auto",
      size: ["small", "medium", "wide"].indexOf(card.size) >= 0 ? card.size : "medium",
      title: String(card.title || "").slice(0, 120),
      fields: Array.isArray(card.fields) ? card.fields.map(String).slice(0, 40) : [],
    };
  }
  function safeState(candidate) {
    return {
      schema: SCHEMA_VERSION,
      cards: candidate.cards.map(normalizeCard),
      updatedAt: candidate.updatedAt || null,
      revision: Number(candidate.revision) || 0,
    };
  }
  function feedsForEngine(engine) {
    if (!engine) return [];
    var raw = [].concat(engine.outs || [], engine.keys || [], engine.outputs || []);
    if (isObject(engine.outs)) raw = raw.concat(Object.values(engine.outs));
    return Array.from(new Set(raw.map(function (item) {
      return cleanFeed(isObject(item) ? (item.key || item.path || item.url || "") : item);
    }).filter(isFeed)));
  }
  function normalizeEngines(registry, manifest) {
    var joined = new Map();
    var manifestList = manifest && Array.isArray(manifest.engines) ? manifest.engines : [];
    manifestList.forEach(function (entry) {
      if (!entry || !entry.engine) return;
      joined.set(entry.engine, {
        name: entry.engine,
        description: entry.description || "",
        keys: Array.isArray(entry.keys) ? entry.keys : [],
        outs: [],
        source: "manifest",
      });
    });
    var records = registry && isObject(registry.engines) ? registry.engines : {};
    Object.keys(records).forEach(function (name) {
      var record = records[name] || {};
      var prior = joined.get(name) || { name: name, description: "", keys: [], outs: [], source: "registry" };
      joined.set(name, Object.assign({}, prior, record, {
        name: name,
        description: record.description || record.doc || prior.description || "",
        keys: [].concat(prior.keys || [], record.keys || []),
        outs: record.outs || record.outputs || prior.outs || [],
        source: "registry",
      }));
    });
    return Array.from(joined.values()).map(function (engine) {
      engine.feeds = feedsForEngine(engine);
      engine.search = searchText([engine.name, engine.description].concat(engine.feeds).join(" "));
      return engine;
    }).sort(function (a, b) { return a.name.localeCompare(b.name); });
  }
  async function fetchJsonCandidates(paths) {
    var error;
    for (var i = 0; i < paths.length; i++) {
      try {
        var response = await fetch(paths[i], { headers: { "Accept": "application/json" } });
        if (!response.ok) throw new Error("HTTP " + response.status);
        return await response.json();
      } catch (e) { error = e; }
    }
    throw error || new Error("Unavailable");
  }
  async function loadCatalogs() {
    var results = await Promise.allSettled([
      fetchJsonCandidates(["/data/engine-registry.json", PROXY + "/data/engine-registry.json"]),
      fetchJsonCandidates(["engine-manifest.json", "/engine-manifest.json", PROXY + "/engine-manifest.json"]),
      fetchJsonCandidates(["config/engine-contracts.json", "/config/engine-contracts.json", PROXY + "/config/engine-contracts.json"]),
      fetchJsonCandidates(["/data/feed-catalog.json", PROXY + "/data/feed-catalog.json"]),
    ]);
    var registry = results[0].status === "fulfilled" ? results[0].value : {};
    var manifest = results[1].status === "fulfilled" ? results[1].value : {};
    if (!registry.engines && !manifest.engines) throw new Error("Engine catalog unavailable");
    contracts = results[2].status === "fulfilled" ? (results[2].value.contracts || {}) : {};
    feedCatalog = results[3].status === "fulfilled" ? results[3].value : {};
    var catalogFeeds = Array.isArray(feedCatalog.feeds) ? feedCatalog.feeds : [];
    feedCatalogMap = new Map(catalogFeeds.filter(function (item) { return item && item.key; })
      .map(function (item) { return [cleanFeed(item.key), item]; }));
    engines = normalizeEngines(registry, manifest);
    engineByName = new Map(engines.map(function (engine) { return [engine.name, engine]; }));
    var feeds = engines.reduce(function (count, engine) { return count + engine.feeds.length; }, 0);
    el("catalogSummary").textContent = engines.length.toLocaleString() + " engines · " + feeds.toLocaleString() + " output links";
    var banner = el("systemBanner");
    banner.classList.add("ready");
    banner.querySelector("strong").textContent = engines.length.toLocaleString() + " engines joined";
    banner.querySelector("span:last-child").textContent = "Registry, manifest, catalog, and " + Object.keys(contracts).length.toLocaleString() + " data contracts.";
    renderLibrary();
  }
  function findEngineForFeed(pattern) {
    var regex = new RegExp(pattern, "i");
    return engines.find(function (engine) { return engine.feeds.some(function (feed) { return regex.test(feed); }); });
  }
  function createStarterState() {
    var recipes = [
      ["conviction", "signal-board|pm-decision", "Decision Posture", "kpi", "small", ["composite_posture","composite_signal","n_live","n_stale"]],
      ["conviction", "asset-compass", "Asset Compass", "kpi", "small", ["verdict_trio.highest_expected_10y","verdict_trio.best_risk_reward_10y","verdict_trio.most_attractive_vs_history"]],
      ["conviction", "risk-regime", "Risk Regime", "kpi", "small", ["risk_regime","risk_regime_score","posture.beta_tilt","systemic_stress.level"]],
      ["conviction", "liquidity-inflection", "Liquidity Pulse", "kpi", "small", ["composite.regime","composite.liquidity_score","trajectory.heading","usd.impulse_z"]],
      ["conviction", "master-allocation", "Allocation Signal", "kpi", "small", ["posture","confidence","active_risk_bps","best_asset.winner"]],
      ["synthesis", "pm-decision", "Strategic View", "list", "medium", ["posture","headline","macro_frame.regime","macro_frame.leading_markets_signal"]],
      ["synthesis", "signal-board", "Tactical Overlay", "list", "medium", ["composite_posture","composite_signal","n_live","n_stale"]],
      ["analysis", "asset-compass|cycle-clock", "Cross-Asset Analysis", "auto", "wide"],
      ["ranked", "master-ranker|best-setups", "Ranked Opportunities", "table", "wide"],
      ["ranked", "master-allocation", "Allocation Detail", "auto", "wide", ["allocations","posture","confidence","active_risk_bps","best_asset"]],
    ];
    var used = new Set();
    var cards = [];
    recipes.forEach(function (recipe) {
      var engine = findEngineForFeed(recipe[1]);
      if (!engine) engine = engines.find(function (item) { return item.feeds.length && !used.has(item.name); });
      if (!engine) return;
      var regex = new RegExp(recipe[1], "i");
      var feed = engine.feeds.filter(function (item) { return regex.test(item); })
        .sort(function (a, b) { return Number(/history|archive/i.test(a)) - Number(/history|archive/i.test(b)); })[0]
        || engine.feeds[0];
      used.add(engine.name);
      cards.push(normalizeCard({ id: uid(), zone: recipe[0], engine: engine.name, feed: feed, title: recipe[2], renderer: recipe[3], size: recipe[4], fields: recipe[5] || [] }));
    });
    if (!cards.length && engines.length) {
      engines.slice(0, 4).forEach(function (engine, index) {
        cards.push(normalizeCard({ id: uid(), zone: ZONES[index].id, engine: engine.name, feed: engine.feeds[0] || "", renderer: "auto", size: "medium" }));
      });
    }
    return { schema: SCHEMA_VERSION, cards: cards, updatedAt: Date.now(), revision: 0 };
  }
  function setSaveStatus(text, kind) {
    var node = el("saveState");
    node.textContent = text;
    node.className = "save-state " + (kind || "");
  }
  async function cloudRequest(method, payload, expectedUserId, signal) {
    var activeUser = window.JustHodlAuth && JustHodlAuth.getUser && JustHodlAuth.getUser();
    if (!activeUser || (expectedUserId && activeUser.id !== expectedUserId)) throw new Error("Account changed");
    var token = await JustHodlAuth.getAccessToken();
    if (!token) throw new Error("Session unavailable");
    activeUser = JustHodlAuth.getUser();
    if (!activeUser || (expectedUserId && activeUser.id !== expectedUserId)) throw new Error("Account changed");
    var response = await fetch(PROXY + "/workspace/home", {
      method: method,
      headers: Object.assign({ "Authorization": "Bearer " + token }, payload ? { "Content-Type": "application/json" } : {}),
      body: payload ? JSON.stringify(payload) : undefined,
      signal: signal,
    });
    if (!response.ok) {
      var detail = await response.json().catch(function () { return {}; });
      throw new Error(detail.error || "Sync failed");
    }
    return response.json();
  }
  async function hydrateCloud(user) {
    if (!user || cloudHydratedFor === user.id) return;
    cloudHydratedFor = user.id;
    var epoch = authEpoch;
    var startedAtEdit = editRevision;
    var scope = accountScope(user);
    if (hydrationController) hydrationController.abort();
    hydrationController = new AbortController();
    setSaveStatus("Loading cloud workspace…", "");
    try {
      var cloud = await cloudRequest("GET", null, user.id, hydrationController.signal);
      if (epoch !== authEpoch || currentScope !== scope || !JustHodlAuth.getUser() || JustHodlAuth.getUser().id !== user.id) return;
      if (validateWorkspace(cloud)) {
        cloudRevision = Number(cloud.revision) || 0;
        if (editRevision !== startedAtEdit) {
          setSaveStatus("Local changes pending sync", "");
          scheduleSave();
          return;
        }
        state = safeState(cloud);
        writeLocal(scope);
        renderWorkspace();
        setSaveStatus("Cloud workspace loaded", "");
      } else {
        scheduleSave(true);
      }
    } catch (e) {
      if (epoch === authEpoch && e.name !== "AbortError") {
        cloudHydratedFor = null;
        setSaveStatus("Cloud sync unavailable", "error");
      }
    }
  }
  function performCloudSave(job) {
    saveChain = saveChain.catch(function () {}).then(async function () {
      if (job.epoch !== authEpoch || currentScope !== job.scope) return;
      var user = JustHodlAuth.getUser();
      if (!user || user.id !== job.userId) return;
      var payload = Object.assign({}, job.snapshot, {
        revision: Math.max(cloudRevision + 1, Number(job.snapshot.revision) || 0),
        baseRevision: cloudRevision,
      });
      try {
        var result = await cloudRequest("PUT", payload, job.userId);
        if (job.epoch !== authEpoch || currentScope !== job.scope) return;
        cloudRevision = Number(result.revision) || payload.revision;
        if (pendingSave === job) setSaveStatus("Saved to cloud", "");
      } catch (e) {
        if (job.epoch !== authEpoch) return;
        if (/revision conflict/i.test(e.message)) {
          setSaveStatus("Cloud changed · retrying local changes", "error");
          try {
            var remote = await cloudRequest("GET", null, job.userId);
            if (job.epoch !== authEpoch || currentScope !== job.scope) return;
            cloudRevision = Number(remote.revision) || 0;
            performCloudSave(Object.assign({}, job, { retryCount: (job.retryCount || 0) + 1 }));
          } catch (reloadError) {
            setSaveStatus("Cloud conflict · local changes preserved", "error");
          }
        } else setSaveStatus("Cloud save failed", "error");
      }
    });
    return saveChain;
  }
  function scheduleSave(immediate) {
    editRevision += 1;
    state.updatedAt = Date.now();
    state.revision = Math.max(Number(state.revision) || 0, editRevision);
    writeLocal(currentScope);
    clearTimeout(saveTimer);
    var user = window.JustHodlAuth && JustHodlAuth.getUser && JustHodlAuth.getUser();
    if (!user || currentScope !== accountScope(user)) {
      setSaveStatus("Local only · sign in to sync", "local");
      return;
    }
    setSaveStatus("Saving…", "");
    var job = {
      epoch: authEpoch,
      scope: currentScope,
      userId: user.id,
      snapshot: JSON.parse(JSON.stringify(state)),
    };
    pendingSave = job;
    saveTimer = setTimeout(function () { performCloudSave(job); }, immediate ? 0 : 650);
  }
  function renderZonesShell() {
    var root = el("workspaceZones");
    root.replaceChildren();
    ZONES.forEach(function (zone) {
      var section = document.createElement("section");
      section.className = "zone";
      section.dataset.zone = zone.id;
      section.setAttribute("aria-labelledby", "zone-" + zone.id);
      var head = document.createElement("div");
      head.className = "zone-head";
      var heading = document.createElement("h2");
      heading.id = "zone-" + zone.id;
      heading.textContent = zone.title.toUpperCase();
      var count = document.createElement("span");
      count.className = "zone-count";
      count.textContent = "0";
      var add = document.createElement("button");
      add.className = "zone-add";
      add.type = "button";
      add.textContent = "+ Add";
      add.addEventListener("click", function () { openLibrary(zone.id); });
      head.append(heading, count, add);
      var cards = document.createElement("div");
      cards.className = "zone-cards";
      cards.dataset.zone = zone.id;
      cards.addEventListener("dragover", handleDragOver);
      cards.addEventListener("dragleave", function () { cards.classList.remove("drag-over"); });
      cards.addEventListener("drop", handleDrop);
      section.append(head, cards);
      root.append(section);
    });
  }
  function cardIndex(cardId) { return state.cards.findIndex(function (card) { return card.id === cardId; }); }
  function updateCard(cardId, patch, rerender) {
    var index = cardIndex(cardId);
    if (index < 0) return;
    state.cards[index] = normalizeCard(Object.assign({}, state.cards[index], patch));
    scheduleSave();
    if (rerender !== false) renderWorkspace();
  }
  function removeCard(cardId) {
    var index = cardIndex(cardId);
    var card = index >= 0 ? state.cards[index] : null;
    var siblings = card ? state.cards.filter(function (item) { return item.zone === card.zone; }) : [];
    var siblingIndex = siblings.findIndex(function (item) { return item.id === cardId; });
    var adjacent = siblings[siblingIndex + 1] || siblings[siblingIndex - 1];
    state.cards = state.cards.filter(function (card) { return card.id !== cardId; });
    scheduleSave();
    renderWorkspace(adjacent ? { cardId: adjacent.id, control: "drag-handle" } : null);
    if (!adjacent && card) {
      var zoneAdd = document.querySelector('.zone[data-zone="' + CSS.escape(card.zone) + '"] .zone-add');
      if (zoneAdd) zoneAdd.focus();
    }
    announce("Card removed");
  }
  function moveCard(cardId, direction, control) {
    var index = cardIndex(cardId);
    if (index < 0) return;
    var card = state.cards[index];
    var siblings = state.cards.filter(function (item) { return item.zone === card.zone; });
    var siblingIndex = siblings.findIndex(function (item) { return item.id === cardId; });
    var target = siblings[siblingIndex + direction];
    if (!target) return;
    var targetIndex = cardIndex(target.id);
    state.cards.splice(index, 1);
    state.cards.splice(targetIndex, 0, card);
    scheduleSave();
    renderWorkspace({ cardId: cardId, control: control || (direction < 0 ? "move-earlier" : "move-later") });
    announce("Moved " + (direction < 0 ? "up" : "down") + " in " + ZONES.find(function (z) { return z.id === card.zone; }).title);
  }
  function moveCardToZone(cardId, zoneDirection) {
    var index = cardIndex(cardId);
    if (index < 0) return false;
    var card = state.cards[index];
    var zoneIndex = ZONES.findIndex(function (zone) { return zone.id === card.zone; });
    var targetZone = ZONES[zoneIndex + zoneDirection];
    if (!targetZone) return false;
    state.cards.splice(index, 1);
    card.zone = targetZone.id;
    var lastTarget = -1;
    state.cards.forEach(function (item, itemIndex) { if (item.zone === targetZone.id) lastTarget = itemIndex; });
    state.cards.splice(lastTarget + 1, 0, card);
    scheduleSave();
    renderWorkspace({ cardId: cardId, control: "drag-handle" });
    announce("Moved card to " + targetZone.title);
    return true;
  }
  function reorderCard(cardId, targetZone, targetCardId) {
    if (!cardId || !targetZone || cardId === targetCardId) return false;
    var from = cardIndex(cardId);
    if (from < 0) return false;
    var oldZone = state.cards[from].zone;
    var targetOriginalIndex = targetCardId ? cardIndex(targetCardId) : -1;
    if (targetCardId && targetOriginalIndex < 0) return false;
    var card = state.cards.splice(from, 1)[0];
    card.zone = targetZone;
    var targetIndex = targetCardId ? cardIndex(targetCardId) : -1;
    if (targetIndex >= 0) state.cards.splice(targetIndex, 0, card);
    else {
      var lastInZone = -1;
      state.cards.forEach(function (item, index) { if (item.zone === targetZone) lastInZone = index; });
      state.cards.splice(lastInZone + 1, 0, card);
    }
    if (oldZone === targetZone && targetOriginalIndex === from) return false;
    scheduleSave();
    renderWorkspace({ cardId: cardId, control: "drag-handle" });
    announce("Moved card to " + ZONES.find(function (z) { return z.id === targetZone; }).title);
    return true;
  }
  function handleDragOver(event) {
    event.preventDefault();
    event.currentTarget.classList.add("drag-over");
    event.dataTransfer.dropEffect = "move";
  }
  function handleDrop(event) {
    event.preventDefault();
    var targetZone = event.currentTarget.dataset.zone;
    event.currentTarget.classList.remove("drag-over");
    var id = draggedCardId || event.dataTransfer.getData("text/plain");
    var targetCard = event.target.closest(".engine-card");
    draggedCardId = null;
    reorderCard(id, targetZone, targetCard && targetCard.dataset.cardId);
  }
  function focusSnapshot() {
    var active = document.activeElement;
    if (!active) return null;
    var card = active.closest && active.closest(".engine-card");
    return card ? { cardId: card.dataset.cardId, control: active.dataset.control || active.className } : null;
  }
  function renderWorkspace(restore) {
    restore = restore || focusSnapshot();
    var openSettings = new Set(Array.from(document.querySelectorAll(".engine-card")).filter(function (node) {
      return !node.querySelector(".card-settings").hidden;
    }).map(function (node) { return node.dataset.cardId; }));
    if (!el("workspaceZones").children.length) renderZonesShell();
    ZONES.forEach(function (zone) {
      var area = document.querySelector('.zone-cards[data-zone="' + zone.id + '"]');
      var zoneCards = state.cards.filter(function (card) { return card.zone === zone.id; });
      area.replaceChildren();
      area.parentElement.querySelector(".zone-count").textContent = zoneCards.length;
      if (!zoneCards.length) {
        var empty = document.createElement("div");
        empty.className = "zone-empty";
        empty.textContent = "Drop a card here or choose + Add.";
        area.append(empty);
      } else zoneCards.forEach(function (card) { area.append(renderCard(card)); });
    });
    observeCards();
    openSettings.forEach(function (id) {
      var node = document.querySelector('[data-card-id="' + CSS.escape(id) + '"]');
      if (node) {
        node.querySelector(".card-settings").hidden = false;
        node.querySelector(".configure-card").setAttribute("aria-expanded", "true");
      }
    });
    if (restore && restore.cardId) {
      var restoredCard = document.querySelector('[data-card-id="' + CSS.escape(restore.cardId) + '"]');
      var restored = restoredCard && restoredCard.querySelector('[data-control="' + CSS.escape(restore.control) + '"]');
      if (restored) restored.focus();
    }
  }
  function renderCard(card) {
    var node = el("cardTemplate").content.firstElementChild.cloneNode(true);
    node.dataset.cardId = card.id;
    node.dataset.size = card.size;
    var engine = engineByName.get(card.engine);
    node.querySelector(".card-kicker").textContent = card.engine || "Choose an engine";
    node.querySelector(".card-title").textContent = card.title || formatEngineName(card.engine);
    node.querySelector(".card-foot").textContent = card.feed || (engine && !engine.feeds.length ? "No published JSON output listed" : "Choose an output feed");
    var configure = node.querySelector(".configure-card");
    configure.dataset.control = "configure";
    configure.addEventListener("click", function () {
      var panel = node.querySelector(".card-settings");
      panel.hidden = !panel.hidden;
      configure.setAttribute("aria-expanded", String(!panel.hidden));
      if (!panel.hidden) panel.querySelector("input,select,button").focus();
    });
    node.addEventListener("dragstart", function (event) {
      draggedCardId = card.id;
      node.classList.add("dragging");
      event.dataTransfer.setData("text/plain", card.id);
      event.dataTransfer.effectAllowed = "move";
    });
    node.addEventListener("dragend", function () {
      draggedCardId = null;
      node.classList.remove("dragging");
      document.querySelectorAll(".drag-over").forEach(function (item) { item.classList.remove("drag-over"); });
    });
    var handle = node.querySelector(".drag-handle");
    handle.dataset.control = "drag-handle";
    handle.addEventListener("keydown", function (event) {
      if (event.key === "ArrowUp" || event.key === "ArrowDown") {
        event.preventDefault();
        moveCard(card.id, event.key === "ArrowUp" ? -1 : 1);
        renderWorkspace({ cardId: card.id, control: "drag-handle" });
      } else if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
        event.preventDefault();
        moveCardToZone(card.id, event.key === "ArrowLeft" ? -1 : 1);
      } else if (event.key === " " || event.key === "Enter") {
        event.preventDefault();
        var grabbed = handle.getAttribute("aria-pressed") === "true";
        handle.setAttribute("aria-pressed", String(!grabbed));
        announce(grabbed ? "Card dropped" : "Card grabbed. Use arrow keys to move.");
      }
    });
    handle.addEventListener("pointerdown", function (event) {
      if (event.pointerType === "mouse" && event.button !== 0) return;
      pointerDrag = { id: card.id, pointerId: event.pointerId };
      handle.setPointerCapture(event.pointerId);
      node.classList.add("dragging");
      event.preventDefault();
    });
    handle.addEventListener("pointermove", function (event) {
      if (!pointerDrag || pointerDrag.pointerId !== event.pointerId) return;
      document.querySelectorAll(".drag-over").forEach(function (item) { item.classList.remove("drag-over"); });
      var hit = document.elementFromPoint(event.clientX, event.clientY);
      var area = hit && hit.closest(".zone-cards");
      if (area) area.classList.add("drag-over");
    });
    function finishPointerDrag(event) {
      if (!pointerDrag || pointerDrag.pointerId !== event.pointerId) return;
      var hit = document.elementFromPoint(event.clientX, event.clientY);
      var area = hit && hit.closest(".zone-cards");
      var target = hit && hit.closest(".engine-card");
      node.classList.remove("dragging");
      document.querySelectorAll(".drag-over").forEach(function (item) { item.classList.remove("drag-over"); });
      var id = pointerDrag.id;
      pointerDrag = null;
      if (area) reorderCard(id, area.dataset.zone, target && target.dataset.cardId);
    }
    handle.addEventListener("pointerup", finishPointerDrag);
    handle.addEventListener("pointercancel", function () {
      pointerDrag = null;
      node.classList.remove("dragging");
      document.querySelectorAll(".drag-over").forEach(function (item) { item.classList.remove("drag-over"); });
    });
    buildSettings(node.querySelector(".card-settings"), card, engine);
    renderCardState(node, card);
    return node;
  }
  function option(value, label, selected) {
    var node = document.createElement("option");
    node.value = value; node.textContent = label; node.selected = value === selected;
    return node;
  }
  function labeledControl(labelText, control, className) {
    var label = document.createElement("label");
    if (className) label.className = className;
    label.append(document.createTextNode(labelText), control);
    return label;
  }
  function buildSettings(panel, card, engine) {
    panel.replaceChildren();
    var title = document.createElement("input");
    title.type = "text"; title.maxLength = 120; title.value = card.title; title.placeholder = "Default engine title"; title.setAttribute("aria-label", "Title"); title.dataset.control = "title";
    title.addEventListener("change", function () { updateCard(card.id, { title: title.value }); });
    var engineSelect = document.createElement("select");
    engineSelect.setAttribute("aria-label", "Engine");
    engineSelect.dataset.control = "engine";
    engines.forEach(function (item) { engineSelect.append(option(item.name, item.name, card.engine)); });
    engineSelect.addEventListener("change", function () {
      var next = engineByName.get(engineSelect.value);
      updateCard(card.id, { engine: engineSelect.value, feed: next && next.feeds[0] || "", fields: [] });
    });
    var feedSelect = document.createElement("select");
    feedSelect.setAttribute("aria-label", "Output feed");
    feedSelect.dataset.control = "feed";
    var feeds = engine ? engine.feeds : [];
    if (!feeds.length) feedSelect.append(option("", "No output feeds listed", ""));
    feeds.forEach(function (feed) { feedSelect.append(option(feed, feed, card.feed)); });
    feedSelect.addEventListener("change", function () { updateCard(card.id, { feed: feedSelect.value, fields: [] }); });
    var renderer = document.createElement("select");
    renderer.setAttribute("aria-label", "Renderer");
    renderer.dataset.control = "renderer";
    [["auto","Auto"],["kpi","KPI"],["list","List"],["table","Table"],["json","JSON"]].forEach(function (item) { renderer.append(option(item[0], item[1], card.renderer)); });
    renderer.addEventListener("change", function () { updateCard(card.id, { renderer: renderer.value }); });
    var size = document.createElement("select");
    size.setAttribute("aria-label", "Size");
    size.dataset.control = "size";
    [["small","Small"],["medium","Medium"],["wide","Wide"]].forEach(function (item) { size.append(option(item[0], item[1], card.size)); });
    size.addEventListener("change", function () { updateCard(card.id, { size: size.value }); });
    var zone = document.createElement("select");
    zone.setAttribute("aria-label", "Zone");
    zone.dataset.control = "zone";
    ZONES.forEach(function (item) { zone.append(option(item.id, item.title, card.zone)); });
    zone.addEventListener("change", function () { updateCard(card.id, { zone: zone.value }); announce("Card moved to " + zone.options[zone.selectedIndex].text); });
    panel.append(
      labeledControl("Title", title, "span-2"),
      labeledControl("Engine", engineSelect, "span-2"),
      labeledControl("Output feed", feedSelect, "span-2"),
      labeledControl("Renderer", renderer),
      labeledControl("Size", size),
      labeledControl("Zone", zone, "span-2")
    );
    var fields = document.createElement("fieldset");
    fields.className = "fields";
    var legend = document.createElement("legend");
    legend.textContent = "Visible fields";
    var options = document.createElement("div");
    options.className = "field-options";
    fields.append(legend, options);
    panel.append(fields);
    populateFieldControls(options, card);
    var actions = document.createElement("div");
    actions.className = "settings-actions";
    var move = document.createElement("div");
    move.className = "move-actions";
    var up = document.createElement("button"); up.type = "button"; up.className = "button secondary"; up.dataset.control = "move-earlier"; up.textContent = "↑ Earlier"; up.addEventListener("click", function () { moveCard(card.id, -1, "move-earlier"); });
    var down = document.createElement("button"); down.type = "button"; down.className = "button secondary"; down.dataset.control = "move-later"; down.textContent = "↓ Later"; down.addEventListener("click", function () { moveCard(card.id, 1, "move-later"); });
    var remove = document.createElement("button"); remove.type = "button"; remove.className = "button danger"; remove.dataset.control = "remove"; remove.textContent = "Remove card"; remove.addEventListener("click", function () { removeCard(card.id); });
    move.append(up, down); actions.append(move, remove); panel.append(actions);
  }
  function dataPaths(data, prefix, depth, result) {
    result = result || [];
    prefix = prefix || "";
    depth = depth || 0;
    if (result.length >= 80 || depth > 4) return result;
    if (Array.isArray(data)) {
      var sample = data.find(function (item) { return isObject(item) || Array.isArray(item); });
      if (sample) dataPaths(sample, prefix, depth + 1, result);
      else if (prefix) result.push(prefix);
      return result;
    }
    if (!isObject(data)) return result;
    Object.keys(data).slice(0, 60).forEach(function (key) {
      var path = prefix ? prefix + "." + key : key;
      var value = data[key];
      if ((isObject(value) || Array.isArray(value)) && depth < 4) dataPaths(value, path, depth + 1, result);
      else result.push(path);
    });
    return Array.from(new Set(result)).slice(0, 80);
  }
  function populateFieldControls(container, card) {
    var cached = feedCache.get(card.feed);
    var paths = cached && cached.data ? Array.from(new Set(card.fields.concat(dataPaths(cached.data)))) : [];
    if (!paths.length) {
      var help = document.createElement("span");
      help.className = "field-option";
      help.textContent = card.feed ? "Fields appear after this card loads." : "Choose an output feed first.";
      container.append(help);
      return;
    }
    paths.forEach(function (path) {
      var label = document.createElement("label");
      label.className = "field-option";
      var input = document.createElement("input");
      input.type = "checkbox"; input.checked = !card.fields.length || card.fields.indexOf(path) >= 0;
      input.addEventListener("change", function () {
        var selected = Array.from(container.querySelectorAll('input:checked')).map(function (item) { return item.value; });
        updateCard(card.id, { fields: selected }, false);
        var cardNode = document.querySelector('[data-card-id="' + CSS.escape(card.id) + '"]');
        if (cardNode) renderData(cardNode.querySelector(".card-body"), feedCache.get(card.feed).data, state.cards[cardIndex(card.id)]);
      });
      input.value = path;
      label.append(input, document.createTextNode(path));
      container.append(label);
    });
  }
  function renderCardState(node, card) {
    var body = node.querySelector(".card-body");
    var badge = node.querySelector(".freshness-badge");
    if (!card.feed) {
      renderMessage(body, "empty", "No output selected", engineByName.get(card.engine) && engineByName.get(card.engine).feeds.length ? "Choose a feed in card settings." : "This engine has no JSON output in the catalog.");
      badge.textContent = "UNWIRED";
      return;
    }
    var cached = feedCache.get(card.feed);
    if (!cached) {
      renderSkeleton(body);
      badge.textContent = "WAITING";
      return;
    }
    if (cached.status === "loading") {
      renderSkeleton(body); badge.textContent = "LOADING"; return;
    }
    if (cached.status === "error") {
      renderMessage(body, "error", "Feed unavailable", cached.error, "Retry", function () { feedCache.delete(card.feed); loadCard(card.id); });
      badge.textContent = "ERROR"; badge.className = "freshness-badge error"; return;
    }
    renderData(body, cached.data, card);
    setFreshness(badge, card.feed, cached);
  }
  function renderSkeleton(body) {
    body.replaceChildren();
    var box = document.createElement("div"); box.className = "state-panel";
    for (var i = 0; i < 3; i++) { var line = document.createElement("div"); line.className = "skeleton"; box.append(line); }
    body.append(box);
  }
  function renderMessage(body, kind, title, detail, action, handler) {
    body.replaceChildren();
    var box = document.createElement("div"); box.className = "state-panel " + kind;
    var strong = document.createElement("strong"); strong.textContent = title;
    var text = document.createElement("span"); text.textContent = clampText(detail, 180);
    box.append(strong, text);
    if (action) {
      var button = document.createElement("button"); button.className = "button secondary"; button.type = "button"; button.textContent = action; button.addEventListener("click", handler); box.append(button);
    }
    body.append(box);
  }
  function valueAt(data, path) {
    var parts = Array.isArray(path) ? path : String(path || "").split(".").filter(Boolean);
    if (!parts.length) return data;
    if (Array.isArray(data)) {
      return data.map(function (item) { return valueAt(item, parts); })
        .reduce(function (flat, item) { return flat.concat(Array.isArray(item) ? item : [item]); }, [])
        .filter(function (item) { return item !== undefined; });
    }
    if (data == null) return undefined;
    return valueAt(data[parts[0]], parts.slice(1));
  }
  function displayValue(value) {
    if (value == null) return "—";
    if (typeof value === "number") return new Intl.NumberFormat(undefined, { maximumFractionDigits: 3 }).format(value);
    if (typeof value === "boolean") return value ? "Yes" : "No";
    if (Array.isArray(value)) return value.map(function (item) { return isObject(item) ? JSON.stringify(item) : String(item); }).join(", ");
    if (isObject(value)) {
      var preferred = ["ticker", "asset", "symbol", "winner", "name", "label", "verdict", "value"];
      var key = preferred.find(function (item) { return value[item] != null && !isObject(value[item]); });
      return key ? displayValue(value[key]) : JSON.stringify(value);
    }
    return String(value);
  }
  function fieldScore(data, path, renderer) {
    var tail = path.split(".").pop().toLowerCase();
    var value = valueAt(data, path);
    var score = 0;
    if (/schema|version|generated|updated|timestamp|elapsed|duration|method|engine|^as_?of$|^asof$|status|source|note|warn/.test(tail)) score -= 100;
    if (/score|posture|regime|signal|confidence|risk|percentile|verdict|allocation|headline|stance|outlook|bias|level|winner|return|tilt|state|action/.test(tail)) score += 45;
    if (/composite|current|best|highest|lowest|expected|active|target|forward/.test(path.toLowerCase())) score += 18;
    if (typeof value === "number" || typeof value === "boolean") score += 12;
    if (typeof value === "string") score += value.length <= 100 ? 10 : -12;
    if (Array.isArray(value)) score += renderer === "table" ? 20 : -60;
    if (isObject(value)) score -= 80;
    return score;
  }
  function chooseFields(data, card) {
    var paths = dataPaths(data);
    var selected = card.fields.filter(function (path) { return valueAt(data, path) !== undefined; });
    if (selected.length) return selected;
    var renderer = card.renderer === "auto" ? autoRenderer(data) : card.renderer;
    return paths.sort(function (a, b) { return fieldScore(data, b, renderer) - fieldScore(data, a, renderer); })
      .filter(function (path) { return fieldScore(data, path, renderer) > -40; })
      .slice(0, renderer === "kpi" ? 6 : 10);
  }
  function autoRenderer(data) {
    if (Array.isArray(data)) return data.length && isObject(data[0]) ? "table" : "list";
    if (isObject(data)) {
      var arrayKey = Object.keys(data).find(function (key) { return Array.isArray(data[key]) && data[key].length && isObject(data[key][0]); });
      if (arrayKey) return "table";
      return "kpi";
    }
    return "list";
  }
  function findTable(data, fields, prefix) {
    prefix = prefix || "";
    if (Array.isArray(data) && data.some(isObject)) {
      return { rows: data.filter(isObject).slice(0, 30), basePath: prefix };
    }
    if (!isObject(data)) return { rows: [], basePath: "" };
    var preferred = fields.map(function (path) { return path.split(".").slice(0, -1).join("."); }).filter(Boolean);
    var keys = Object.keys(data);
    keys.sort(function (a, b) {
      var aPath = prefix ? prefix + "." + a : a;
      var bPath = prefix ? prefix + "." + b : b;
      return Number(preferred.indexOf(bPath) >= 0) - Number(preferred.indexOf(aPath) >= 0);
    });
    for (var i = 0; i < keys.length; i++) {
      var path = prefix ? prefix + "." + keys[i] : keys[i];
      var found = findTable(data[keys[i]], fields, path);
      if (found.rows.length) return found;
    }
    return { rows: [], basePath: "" };
  }
  function tableRows(data, fields) {
    return findTable(data, fields).rows;
  }
  function tableColumns(table, selectedFields) {
    var base = table.basePath ? table.basePath + "." : "";
    var selected = selectedFields.filter(function (path) { return !base || path.indexOf(base) === 0; })
      .map(function (path) { return base && path.indexOf(base) === 0 ? path.slice(base.length) : path; })
      .filter(function (path) { return path && table.rows.some(function (row) { return valueAt(row, path) !== undefined; }); });
    return selected.length ? selected : dataPaths(table.rows).slice(0, 8);
  }
  function renderData(body, data, card) {
    body.replaceChildren();
    if (data == null || (Array.isArray(data) && !data.length) || (isObject(data) && !Object.keys(data).length)) {
      renderMessage(body, "empty", "No rows in this output", "The feed loaded successfully but returned no displayable data.");
      return;
    }
    var renderer = card.renderer === "auto" ? autoRenderer(data) : card.renderer;
    var fields = chooseFields(data, card);
    if (renderer === "json") {
      var pre = document.createElement("pre"); pre.className = "json-view"; pre.textContent = JSON.stringify(data, null, 2); body.append(pre); return;
    }
    if (renderer === "table") {
      var tableData = findTable(data, fields);
      var rows = tableData.rows;
      if (!rows.length) { renderer = "list"; }
      else {
        var columns = tableColumns(tableData, card.fields);
        var table = document.createElement("table"); table.className = "data-table";
        var thead = document.createElement("thead"); var head = document.createElement("tr");
        columns.forEach(function (column) { var th = document.createElement("th"); th.scope = "col"; th.textContent = column; head.append(th); });
        thead.append(head); table.append(thead);
        var tbody = document.createElement("tbody");
        rows.forEach(function (row) {
          var tr = document.createElement("tr");
          columns.forEach(function (column) { var td = document.createElement("td"); td.textContent = clampText(displayValue(valueAt(row, column)), 80); tr.append(td); });
          tbody.append(tr);
        });
        table.append(tbody); body.append(table); return;
      }
    }
    if (renderer === "kpi") {
      var grid = document.createElement("div"); grid.className = "kpi-grid";
      fields.slice(0, 8).forEach(function (path) {
        var item = document.createElement("div"); item.className = "kpi";
        var label = document.createElement("div"); label.className = "kpi-label"; label.textContent = path.split(".").pop().replace(/_/g, " ");
        var value = document.createElement("div"); value.className = "kpi-value";
        var raw = valueAt(data, path); value.textContent = clampText(displayValue(raw), 44);
        if (typeof raw === "number" && raw !== 0) value.classList.add(raw > 0 ? "positive" : "negative");
        item.append(label, value); grid.append(item);
      });
      body.append(grid); return;
    }
    var list = document.createElement("ul"); list.className = "data-list";
    if (Array.isArray(data) && !isObject(data[0])) {
      data.slice(0, 12).forEach(function (raw, index) {
        var li = document.createElement("li"); var key = document.createElement("span"); key.textContent = String(index + 1);
        var value = document.createElement("span"); value.textContent = clampText(displayValue(raw), 120); li.append(key, value); list.append(li);
      });
    } else {
      fields.slice(0, 14).forEach(function (path) {
        var li = document.createElement("li"); var key = document.createElement("span"); key.textContent = path.split(".").pop().replace(/_/g, " ");
        var value = document.createElement("span"); value.textContent = clampText(displayValue(valueAt(data, path)), 120); li.append(key, value); list.append(li);
      });
    }
    body.append(list);
  }
  function feedMeta(feed) {
    var catalog = feedCatalog.feeds || feedCatalog.artifacts || feedCatalog;
    if (feedCatalogMap.has(feed)) return feedCatalogMap.get(feed);
    return isObject(catalog) ? (catalog[feed] || catalog["/" + feed] || {}) : {};
  }
  function setFreshness(badge, feed, cached) {
    var meta = feedMeta(feed);
    var contract = contracts[feed] || {};
    var generated = cached.data && (cached.data.generated_at || cached.data.as_of || cached.data.updated_at)
      || meta.generated_at || meta.last_modified || cached.loadedAt;
    var time = Date.parse(generated);
    var ageHours = Number.isFinite(time) ? (Date.now() - time) / 36e5 : null;
    var bound = Number(contract.max_age_hours || meta.max_age_hours || 24);
    var fresh = ageHours == null || ageHours <= bound;
    badge.textContent = ageHours == null ? "LIVE" : (ageHours < 1 ? Math.max(1, Math.round(ageHours * 60)) + "M" : Math.round(ageHours) + "H");
    badge.className = "freshness-badge " + (fresh ? "fresh" : "stale");
    badge.title = ageHours == null ? "Loaded now" : (fresh ? "Within" : "Beyond") + " " + bound + " hour freshness contract";
  }
  async function loadCard(cardId) {
    var index = cardIndex(cardId);
    if (index < 0) return;
    var card = state.cards[index];
    if (!card.feed) return;
    var cached = feedCache.get(card.feed);
    if (cached && (cached.status === "loading" || cached.status === "ready")) return;
    feedCache.set(card.feed, { status: "loading" });
    updateRenderedCards(card.feed);
    try {
      var data = await fetchJsonCandidates(["/" + card.feed, PROXY + "/" + card.feed]);
      feedCache.set(card.feed, { status: "ready", data: data, loadedAt: new Date().toISOString() });
    } catch (e) {
      feedCache.set(card.feed, { status: "error", error: "Could not load " + card.feed + ". " + e.message });
    }
    updateRenderedCards(card.feed);
  }
  function updateRenderedCards(feed) {
    state.cards.filter(function (card) { return card.feed === feed; }).forEach(function (card) {
      var node = document.querySelector('[data-card-id="' + CSS.escape(card.id) + '"]');
      if (!node) return;
      renderCardState(node, card);
      var fields = node.querySelector(".field-options");
      if (fields) { fields.replaceChildren(); populateFieldControls(fields, card); }
    });
  }
  function observeCards() {
    if (observer) observer.disconnect();
    visibleCards.clear();
    observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        var id = entry.target.dataset.cardId;
        if (entry.isIntersecting) { visibleCards.add(id); loadCard(id); }
        else visibleCards.delete(id);
      });
    }, { rootMargin: "160px 0px", threshold: .01 });
    document.querySelectorAll(".engine-card").forEach(function (card) { observer.observe(card); });
  }
  function openLibrary(zoneId, opener) {
    libraryOpener = opener || document.activeElement || el("openLibrary");
    if (zoneId) el("newCardZone").value = zoneId;
    el("libraryPanel").hidden = false;
    el("jh-auth-slot").hidden = true;
    el("openLibrary").setAttribute("aria-expanded", "true");
    libraryLimit = PAGE_SIZE;
    el("engineSearch").focus();
    renderLibrary();
  }
  function closeLibrary() {
    el("libraryPanel").hidden = true;
    el("jh-auth-slot").hidden = false;
    el("openLibrary").setAttribute("aria-expanded", "false");
    var opener = libraryOpener;
    libraryOpener = null;
    if (opener && opener.isConnected && !opener.disabled) opener.focus();
    else el("openLibrary").focus();
  }
  function renderLibrary() {
    var root = el("libraryResults");
    if (!root) return;
    var query = searchText(el("engineSearch").value);
    var matches = engines.filter(function (engine) { return !query || engine.search.indexOf(query) >= 0; });
    var showing = Math.min(matches.length, libraryLimit);
    el("libraryCount").textContent = matches.length.toLocaleString() + " matches · showing " + showing.toLocaleString();
    root.replaceChildren();
    matches.slice(0, libraryLimit).forEach(function (engine) {
      var item = document.createElement("article"); item.className = "library-item"; item.setAttribute("role", "listitem");
      var copy = document.createElement("div"); var heading = document.createElement("h3"); heading.textContent = engine.name;
      var detail = document.createElement("p"); detail.textContent = (engine.feeds.length ? engine.feeds.length + " feed" + (engine.feeds.length === 1 ? "" : "s") + " · " : "") + (engine.description || "No catalog description");
      copy.append(heading, detail);
      var add = document.createElement("button"); add.className = "button secondary"; add.type = "button"; add.textContent = "Add";
      add.addEventListener("click", function () {
        var zone = el("newCardZone").value;
        if (state.cards.length >= MAX_CARDS) {
          setSaveStatus("Card limit reached (" + MAX_CARDS + ")", "error");
          announce("Card limit reached. Remove a card before adding another.");
          return;
        }
        state.cards.push(normalizeCard({ id: uid(), zone: zone, engine: engine.name, feed: engine.feeds[0] || "", renderer: "auto", size: zone === "conviction" ? "small" : "medium" }));
        scheduleSave(); renderWorkspace(); closeLibrary(); announce(engine.name + " added to " + ZONES.find(function (z) { return z.id === zone; }).title);
      });
      item.append(copy, add); root.append(item);
    });
    if (showing < matches.length) {
      var more = document.createElement("button");
      more.type = "button";
      more.className = "button secondary library-more";
      more.textContent = "Load " + Math.min(PAGE_SIZE, matches.length - showing).toLocaleString() + " more";
      more.setAttribute("aria-label", "Load more engines");
      more.addEventListener("click", function () {
        libraryLimit += PAGE_SIZE;
        renderLibrary();
        var next = root.children[showing];
        if (next) {
          next.scrollIntoView({ block: "nearest" });
          var nextAdd = next.querySelector("button");
          if (nextAdd) nextAdd.focus();
        } else {
          var replacement = root.querySelector(".library-more");
          if (replacement) replacement.focus();
        }
      });
      root.append(more);
    }
  }
  function wireEvents() {
    ZONES.forEach(function (zone) { el("newCardZone").append(option(zone.id, zone.title, "analysis")); });
    el("openLibrary").addEventListener("click", function (event) { el("libraryPanel").hidden ? openLibrary(null, event.currentTarget) : closeLibrary(); });
    el("closeLibrary").addEventListener("click", closeLibrary);
    el("engineSearch").addEventListener("input", function () { libraryLimit = PAGE_SIZE; renderLibrary(); });
    el("resetWorkspace").addEventListener("click", function () {
      if (!window.confirm("Reset every card to the starter layout?")) return;
      state = createStarterState(); feedCache.clear(); scheduleSave(); renderWorkspace(); announce("Workspace reset");
    });
    el("themeToggle").addEventListener("click", function () {
      theme = theme === "dark" ? "light" : "dark";
      document.documentElement.setAttribute("data-theme", theme);
    });
    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && !el("libraryPanel").hidden) closeLibrary();
    });
  }
  function switchAuthScope(user) {
    authEpoch += 1;
    clearTimeout(saveTimer);
    pendingSave = null;
    if (hydrationController) hydrationController.abort();
    hydrationController = null;
    cloudHydratedFor = null;
    cloudRevision = 0;
    currentScope = accountScope(user);
    var scoped = readLocal(currentScope);
    state = scoped ? safeState(scoped) : createStarterState();
    editRevision += 1;
    renderWorkspace();
    if (user) hydrateCloud(user);
    else setSaveStatus("Local only · sign in to sync", "local");
  }
  async function boot() {
    document.documentElement.setAttribute("data-theme", theme);
    currentScope = deviceScope();
    wireEvents();
    renderZonesShell();
    try {
      await loadCatalogs();
      var local = readLocal(currentScope);
      state = local ? safeState(local) : createStarterState();
      renderWorkspace();
    } catch (e) {
      el("catalogSummary").textContent = "Catalog unavailable";
      var banner = el("systemBanner"); banner.querySelector("strong").textContent = "Catalog unavailable"; banner.querySelector("span:last-child").textContent = "Refresh to retry. Your saved layout remains local.";
      state = readLocal(currentScope) || { schema: SCHEMA_VERSION, cards: [], updatedAt: null, revision: 0 };
      renderWorkspace();
    }
    if (window.JustHodlAuth) {
      JustHodlAuth.onChange(function (user) {
        authReady = true;
        var nextScope = accountScope(user);
        if (currentScope !== nextScope) switchAuthScope(user);
        else if (user) hydrateCloud(user);
        else setSaveStatus("Local only · sign in to sync", "local");
      });
      await JustHodlAuth.init();
      authReady = true;
      var user = JustHodlAuth.getUser();
      if (currentScope !== accountScope(user)) switchAuthScope(user);
      else if (user) hydrateCloud(user);
      else setSaveStatus("Local only · sign in to sync", "local");
    } else {
      authReady = true;
      setSaveStatus("Local only · sign in to sync", "local");
    }
  }
  window.EngineWorkspace = {
    validateWorkspace: validateWorkspace,
    normalizeEngines: normalizeEngines,
    dataPaths: dataPaths,
    valueAt: valueAt,
    findTable: findTable,
    tableColumns: tableColumns,
    maxCards: MAX_CARDS,
  };
  window.addEventListener("DOMContentLoaded", boot);
})();
