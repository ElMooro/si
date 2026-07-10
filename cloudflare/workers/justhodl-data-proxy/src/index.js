/**
 * justhodl-data-proxy v2.0.0
 *
 * Edge-cached read-through proxy for all justhodl public S3 feeds.
 * Cloudflare's edge automatically applies Brotli/gzip compression for
 * text/JSON responses, so a 49 KB plain JSON typically reaches the
 * browser as ~10 KB. Invisible to the page (browser auto-decodes).
 *
 * Usage from pages — path passes through VERBATIM to the S3 bucket:
 *   GET https://justhodl-data-proxy.raafouis.workers.dev/crypto-intel.json
 *     -> s3://justhodl-dashboard-live/crypto-intel.json
 *
 *   GET https://justhodl-data-proxy.raafouis.workers.dev/data/pump-radar-summary.json
 *     -> s3://justhodl-dashboard-live/data/pump-radar-summary.json
 *
 * Backward-compat: legacy callers using paths without a slash that worked
 * pre-v2 (mapped to /data/<file>) still work — on 404, we retry under /data/.
 *
 * Browser sees Content-Encoding: gzip|br via Cloudflare edge. Worker
 * returns plain bytes; edge compresses transparently. Wire size
 * reduction: typically 60-80% on text/JSON.
 */

const BUCKET_BASE = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";

const CACHE_RULES = [
  { pattern: /(live|intraday|5min|streaming|tick|realtime)/i, ttl: 30 },
  { pattern: /(hourly|morning|sentiment|news|crypto-intel|options-flow|pump-positioning|pump-radar|catalyst|velocity|momentum)/i, ttl: 300 },
  { pattern: /(episode-reference|historical|crisis-knowledge|forward-returns|catalog|reference|themes)/i, ttl: 21600 },
  { pattern: /(daily|signal-board|portfolio|risk-recommendation)/i, ttl: 3600 },
];
const DEFAULT_TTL = 300;

function ttlFor(path) {
  for (const { pattern, ttl } of CACHE_RULES) {
    if (pattern.test(path)) return ttl;
  }
  return DEFAULT_TTL;
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "GET, HEAD, PUT, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, If-None-Match, If-Modified-Since, X-Brain-Pin, X-Requested-With",
    "Access-Control-Max-Age":       "86400",
    "Vary":                         "Accept-Encoding",
  };
}

function jsonResp(obj, status) {
  return new Response(JSON.stringify(obj), {
    status: status || 200,
    headers: { "Content-Type": "application/json", ...corsHeaders() },
  });
}

// Verify a Stripe webhook signature (HMAC-SHA256 over `${t}.${payload}`),
// using the Web Crypto API available in Workers. Tolerates 5-min clock skew.
async function verifyStripeSig(payload, sigHeader, secret) {
  try {
    const parts = {};
    sigHeader.split(",").forEach(kv => { const [k, v] = kv.split("="); parts[k] = v; });
    const t = parts["t"]; const v1 = parts["v1"];
    if (!t || !v1) return false;
    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey("raw", enc.encode(secret),
      { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    const mac = await crypto.subtle.sign("HMAC", key, enc.encode(`${t}.${payload}`));
    const hex = [...new Uint8Array(mac)].map(b => b.toString(16).padStart(2, "0")).join("");
    // constant-time-ish compare
    if (hex.length !== v1.length) return false;
    let diff = 0;
    for (let i = 0; i < hex.length; i++) diff |= hex.charCodeAt(i) ^ v1.charCodeAt(i);
    if (diff !== 0) return false;
    if (Math.abs(Date.now() / 1000 - Number(t)) > 300) return false;  // 5-min skew
    return true;
  } catch (e) { return false; }
}

async function fetchUpstream(upstreamUrl, ttl) {
  return fetch(upstreamUrl, {
    cf: { cacheTtl: ttl, cacheEverything: true },
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    // ─── PER-USER DATA SYNC (watchlists, flags, settings) ───
    // ── /journal — the Decision Journal: timestamped, locked decisions with the
    // reasoning + a price snapshot, graded later against what actually happened.
    // Same PIN as /brain. This is the user's personal track record of judgment. ──
    if (url.pathname === "/journal") {
      if (!env.USER_DATA) {
        return new Response(JSON.stringify({ error: "store unavailable" }),
          { status: 503, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const juid = (url.searchParams.get("uid") || "").replace(/[^a-zA-Z0-9_\-]/g, "").slice(0, 64);
      const jwho = juid && juid.length >= 8 ? juid : "khalid";
      const JKEY = "journal:" + jwho;
      const PIN_KEY = "brainpin:" + jwho;
      async function sha(s) {
        const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("jhsalt:" + s));
        return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, "0")).join("");
      }
      if (request.method === "GET") {
        const stored = await env.USER_DATA.get(JKEY);
        return new Response(stored || JSON.stringify({ entries: [] }),
          { headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeaders() } });
      }
      if (request.method === "PUT" || request.method === "POST") {
        const rawJ = await request.text();
        let pj = null; try { pj = JSON.parse(rawJ); } catch (e) {}
        let pin = request.headers.get("X-Brain-Pin") || "";
        if (!pin && pj && pj._pin) pin = String(pj._pin);
        const jAuthed = (jwho !== "khalid" && juid.length >= 20);  // logged-in UUID = auth
        if (!jAuthed) {
          const ph = await sha(pin);
          const existing = await env.USER_DATA.get(PIN_KEY);
          if (!existing || ph !== existing) {
            return new Response(JSON.stringify({ error: "wrong or unset pin (set it on the Brain first)" }),
              { status: 403, headers: { "Content-Type": "application/json", ...corsHeaders() } });
          }
        }
        try {
          if (!pj) throw new Error("invalid json");
          if (pj._pin) delete pj._pin;
          const bodyText = JSON.stringify(pj);
          if (bodyText.length > 10000000) {
            return new Response(JSON.stringify({ error: "too large" }), { status: 413, headers: { "Content-Type": "application/json", ...corsHeaders() } });
          }
          await env.USER_DATA.put(JKEY, bodyText);
          return new Response(JSON.stringify({ ok: true, saved_at: Date.now() }),
            { headers: { "Content-Type": "application/json", ...corsHeaders() } });
        } catch (e) {
          return new Response(JSON.stringify({ error: "invalid json" }), { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
        }
      }
      return new Response("method not allowed", { status: 405, headers: corsHeaders() });
    }

    // ── /brain — SHARDED storage: one KV key per note (bnote:<user>:<id>) + a
    // small index (bidx:<user>). Scales far past the 25MB single-value limit
    // (effectively unlimited / 500MB+), and each save writes only the ONE note
    // that changed — tiny, fast, no race, no size error.
    //   GET  /brain?uid=…                 → {notes:[…], scope}   (reads index + shards)
    //   PUT  /brain?uid=…  {note:{…}}      → upsert one note  (tiny write)
    //   PUT  /brain?uid=…  {delete:"<id>"} → delete one note
    //   PUT  /brain?uid=…  {notes:[…]}     → bulk replace (migration / import); shards it
    // ── /brain-debug — server-side truth: lists every brain identity in KV and
    // how many notes each holds. Lets us SEE where notes landed without the
    // browser. Read-only, no secrets exposed (ids are non-sensitive uuids). ──
    // ── /brain-purge — ADMIN maintenance: delete junk/test shards and prune the
    // index in bounded batches (the brain fragmented to 138k shards from test
    // loops, making reads exceed the subrequest limit). Gated by an admin token.
    // ?uid=<id>&token=<t>&max=<n>  → scans the index, deletes junk-pattern notes,
    // returns progress; call repeatedly until done. ──
    if (url.pathname === "/brain-purge") {
      if (!env.USER_DATA) return jsonResp({ error: "no kv" }, 503);
      const token = url.searchParams.get("token") || "";
      if (token !== "jhpurge_9f48_2026") return jsonResp({ error: "forbidden" }, 403);
      const uid = (url.searchParams.get("uid") || "").replace(/[^a-zA-Z0-9_\-]/g, "").slice(0, 64);
      if (url.searchParams.get("reset") === "1") {
        try {
          await env.USER_DATA.put("bidx:" + uid, "[]");
          await env.USER_DATA.put("bcache:" + uid, "[]");                 // clear served cache
          await env.USER_DATA.delete("brain:" + uid).catch(() => {});      // legacy blob
          await env.USER_DATA.delete("bcache:" + uid + ":wip").catch(() => {});
          await env.USER_DATA.delete("bcache:" + uid + ":keepids").catch(() => {});
          await env.USER_DATA.delete("bcache:" + uid + ":seen").catch(() => {});
          return jsonResp({ ok: true, mode: "reset", uid, cleared: ["index", "cache", "wip", "legacy"] });
        } catch (e) { return jsonResp({ error: String(e).slice(0, 120) }, 500); }
      }
      const max = Math.min(parseInt(url.searchParams.get("max") || "400", 10) || 400, 800);
      const IDX = "bidx:" + uid, NP = "bnote:" + uid + ":";
      var stripGarble = url.searchParams.get("garble") === "1";
      function isGarble(t){
        t=(t||"").trim();
        if(t.length<20) return true;                       // too short to be a real note
        var letters=(t.match(/[a-zA-Z]/g)||[]).length;
        var alnumWords=(t.match(/\b[a-zA-Z]{3,}\b/g)||[]).length;  // real words (3+ letters)
        var ratio=letters/t.length;
        // OCR garble = low letter ratio OR very few real words for its length
        if(ratio<0.55) return true;                        // lots of symbols/digits/pipes
        if(alnumWords < Math.max(4, t.length/40)) return true; // sparse real words
        if(/[|©£@]{2,}/.test(t) || /\b[a-z]\b(\s+\b[a-z]\b){4,}/i.test(t)) return true; // table/char fragments
        return false;
      }
      function isJunk(t) {
        t = t || "";
        if ((t.split("MACRO NOTE.").length > 4) || t.startsWith("XXXX") ||
          /^(test|cors check|kv reset check|save path test|round-trip persistence test)$/.test(t) ||
          /^(multi note |batch note |real note |device save test)/.test(t) ||
          t.indexOf("no-preflight") >= 0 || t.indexOf("PERMANENCE TEST") >= 0) return true;
        if (stripGarble && isGarble(t)) return true;
        return false;
      }
      try {
        let ids = JSON.parse(await env.USER_DATA.get(IDX) || "[]");
        const offset = Math.max(0, parseInt(url.searchParams.get("offset") || "0", 10) || 0);
        const window = ids.slice(offset, offset + max);   // walk by offset (was re-scanning front)
        let deleted = 0;
        const raws = await Promise.all(window.map(id => env.USER_DATA.get(NP + id).then(v => [id, v]).catch(() => [id, null])));
        const deadSet = new Set();
        for (const [id, raw] of raws) {
          let junk = false, txt = "";
          if (raw) { try { txt = (JSON.parse(raw).text) || ""; } catch (e) {} }
          if (!raw || isJunk(txt)) { await env.USER_DATA.delete(NP + id).catch(() => {}); deadSet.add(id); deleted++; }
        }
        if (deleted) {
          ids = ids.filter(x => !deadSet.has(x));
          await env.USER_DATA.put(IDX, JSON.stringify(ids));
          await env.USER_DATA.delete("bcache:" + uid).catch(() => {});  // invalidate fast-read cache
        }
        const nextOffset = offset + window.length - deleted;
        return jsonResp({ ok: true, uid, scanned: window.length, deleted, total_now: ids.length, next_offset: nextOffset, more: nextOffset < ids.length });
      } catch (e) {
        return jsonResp({ error: String(e).slice(0, 150) }, 500);
      }
    }

    if (url.pathname === "/brain-debug") {
      if (!env.USER_DATA) return jsonResp({ error: "no kv" }, 503);
      try {
        const out = { identities: [], device_ids: [], total_note_shards: 0 };
        let cursor = undefined;
        const indexes = [];
        do {
          const list = await env.USER_DATA.list({ prefix: "bidx:", cursor });
          for (const k of list.keys) indexes.push(k.name);
          cursor = list.list_complete ? undefined : list.cursor;
        } while (cursor);
        for (const idxKey of indexes) {
          const who = idxKey.slice("bidx:".length);
          let ids = [];
          try { ids = JSON.parse(await env.USER_DATA.get(idxKey) || "[]"); } catch (e) {}
          out.identities.push({ uid: who, kind: who.startsWith("dev-") ? "guest" : "account", notes: ids.length });
          out.total_note_shards += ids.length;
        }
        // also count raw note shards + legacy blobs
        let c2 = undefined, shardCount = 0, legacy = [];
        do {
          const list = await env.USER_DATA.list({ prefix: "bnote:", cursor: c2 });
          shardCount += list.keys.length;
          c2 = list.list_complete ? undefined : list.cursor;
        } while (c2);
        out.raw_note_shards = shardCount;
        let c3 = undefined;
        do {
          const list = await env.USER_DATA.list({ prefix: "brain:", cursor: c3 });
          for (const k of list.keys) legacy.push(k.name);
          c3 = list.list_complete ? undefined : list.cursor;
        } while (c3);
        out.legacy_blobs = legacy;
        return jsonResp(out);
      } catch (e) {
        return jsonResp({ error: String(e).slice(0, 150) }, 500);
      }
    }

    if (url.pathname === "/brain") {
      if (!env.USER_DATA) {
        return new Response(JSON.stringify({ error: "store unavailable" }),
          { status: 503, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const uidParam = (url.searchParams.get("uid") || "").replace(/[^a-zA-Z0-9_\-]/g, "").slice(0, 64);
      const who = uidParam && uidParam.length >= 8 ? uidParam : "khalid";
      const IDX_KEY = "bidx:" + who;            // JSON array of note ids (order)
      const NOTE_PREFIX = "bnote:" + who + ":";
      const CACHE_KEY = "bcache:" + who;        // SINGLE key holding the full notes array → instant reads
      const LEGACY_KEY = "brain:" + who;        // old single-blob (for migration)
      const PIN_KEY = "brainpin:" + who;
      const isAuthedUser = (who !== "khalid" && uidParam.length >= 20);
      async function sha(s) {
        const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("jhsalt:" + s));
        return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, "0")).join("");
      }
      async function readIndex() {
        try { return JSON.parse(await env.USER_DATA.get(IDX_KEY) || "[]"); } catch (e) { return []; }
      }
      async function readAllNotes() {
        let ids = await readIndex();
        // one-time migration: if no index yet but a legacy blob exists, shard it
        if ((!ids || !ids.length)) {
          const legacy = await env.USER_DATA.get(LEGACY_KEY);
          // Only migrate a reasonably-sized legacy blob. A huge one is almost
          // certainly junk/corrupt and would time out the read — skip + drop it.
          if (legacy && legacy.length < 2000000) {
            try {
              const obj = JSON.parse(legacy);
              const notes = Array.isArray(obj.notes) ? obj.notes : [];
              if (notes.length) {
                const newIds = [];
                for (const n of notes) {
                  if (n && n.id) { await env.USER_DATA.put(NOTE_PREFIX + n.id, JSON.stringify(n)); newIds.push(n.id); }
                }
                await env.USER_DATA.put(IDX_KEY, JSON.stringify(newIds));
                ids = newIds;
              }
              await env.USER_DATA.delete(LEGACY_KEY);  // migrated → remove the blob
            } catch (e) { await env.USER_DATA.delete(LEGACY_KEY); }
          } else if (legacy) {
            await env.USER_DATA.delete(LEGACY_KEY);  // oversized/junk → drop it, don't migrate
          }
        }
        // Fetch all note shards in PARALLEL (serial awaits were slow/timed out
        // with many notes → 'Loading…' hung). Batch to stay within subrequest
        // limits, preserve index order.
        const notes = [];
        const BATCH = 250;
        for (let i = 0; i < ids.length; i += BATCH) {
          const slice = ids.slice(i, i + BATCH);
          const raws = await Promise.all(slice.map(id => env.USER_DATA.get(NOTE_PREFIX + id).catch(() => null)));
          for (const raw of raws) {
            if (raw) { try { notes.push(JSON.parse(raw)); } catch (e) {} }
          }
        }
        return notes;
      }

      // CHUNKED CACHE BUILD: assemble bcache:<uid> in offset windows so we never
      // fan-out all 854 shards in one invocation (that 500s). Call repeatedly with
      // ?build=1&offset=N until done. Admin-token gated.
      // DEDUP: walk the index in offset windows, drop notes whose normalized text
      // was already seen (keeps first occurrence). Carries the seen-set across calls
      // via a temp key. Call ?dedup=1&offset=N&token=... until done. Updates index+cache.
      // ONE-SHOT DEDUP: read the cache, dedup by normalized text in memory, write
      // back the deduped index+cache in a single call (works for brains ≤~10k).
      if (url.searchParams.get("dedupnow") === "1") {
        if ((url.searchParams.get("token") || "") !== "jhpurge_9f48_2026") return jsonResp({ error: "forbidden" }, 403);
        try {
          function nz(t){ return String(t||"").toLowerCase().replace(/\s+/g," ").replace(/[^\w\s]/g,"").trim().slice(0,200); }
          let cache = [];
          try { cache = JSON.parse(await env.USER_DATA.get(CACHE_KEY) || "[]"); } catch(e){}
          const seen = new Set(); const keep = []; const keepIds = [];
          for (const n of cache) {
            if (!n || !n.id) continue;
            const k = nz(n.text);
            if (k.length >= 8 && seen.has(k)) continue;
            if (k.length >= 8) seen.add(k);
            keep.push(n); keepIds.push(n.id);
          }
          await env.USER_DATA.put(CACHE_KEY, JSON.stringify(keep));
          await env.USER_DATA.put(IDX_KEY, JSON.stringify(keepIds));
          return jsonResp({ ok: true, before: cache.length, after: keep.length, removed: cache.length - keep.length });
        } catch (e) { return jsonResp({ error: String(e).slice(0,150) }, 500); }
      }
      if (url.searchParams.get("dedup") === "1") {
        if ((url.searchParams.get("token") || "") !== "jhpurge_9f48_2026") return jsonResp({ error: "forbidden" }, 403);
        try {
          function norm(t){ return String(t||"").toLowerCase().replace(/\s+/g," ").replace(/[^\w\s]/g,"").trim().slice(0,300); }
          let ids = JSON.parse(await env.USER_DATA.get(IDX_KEY) || "[]");
          const offset = Math.max(0, parseInt(url.searchParams.get("offset") || "0", 10) || 0);
          const win = 200;
          const SEEN = CACHE_KEY + ":dedupseen", KEEP = CACHE_KEY + ":dedupkeep";
          if (offset === 0) { await env.USER_DATA.put(SEEN, "{}"); await env.USER_DATA.put(KEEP, "[]"); }
          let seen = JSON.parse(await env.USER_DATA.get(SEEN) || "{}");
          let keep = JSON.parse(await env.USER_DATA.get(KEEP) || "[]");
          const slice = ids.slice(offset, offset + win);
          const raws = await Promise.all(slice.map(id => env.USER_DATA.get(NOTE_PREFIX + id).then(v=>[id,v]).catch(()=>[id,null])));
          let dropped = 0;
          for (const [id, raw] of raws) {
            if (!raw) { dropped++; continue; }
            let txt = ""; try { txt = JSON.parse(raw).text || ""; } catch (e) {}
            const k = norm(txt);
            if (k.length < 8) { keep.push(id); continue; }   // too short to dedup safely → keep
            if (seen[k]) { await env.USER_DATA.delete(NOTE_PREFIX + id).catch(()=>{}); dropped++; }
            else { seen[k] = 1; keep.push(id); }
          }
          await env.USER_DATA.put(SEEN, JSON.stringify(seen));
          await env.USER_DATA.put(KEEP, JSON.stringify(keep));
          const next = offset + win;
          const done = next >= ids.length;
          if (done) {
            await env.USER_DATA.put(IDX_KEY, JSON.stringify(keep));
            // rebuild cache from the deduped keep set
            const cacheRaws = [];
            for (let i = 0; i < keep.length; i += 200) {
              const part = await Promise.all(keep.slice(i, i+200).map(id => env.USER_DATA.get(NOTE_PREFIX + id).catch(()=>null)));
              for (const r of part) { if (r) { try { cacheRaws.push(JSON.parse(r)); } catch(e){} } }
            }
            await env.USER_DATA.put(CACHE_KEY, JSON.stringify(cacheRaws));
          }
          return jsonResp({ ok: true, kept: keep.length, dropped_total: "see kept vs index", scanned_to: next, total_index: ids.length, done });
        } catch (e) { return jsonResp({ error: String(e).slice(0, 150) }, 500); }
      }
      if (url.searchParams.get("build") === "1") {
        if ((url.searchParams.get("token") || "") !== "jhpurge_9f48_2026") return jsonResp({ error: "forbidden" }, 403);
        try {
          // garble/junk filter so we keep ONLY real notes (index re-grew to 26k junk)
          function realNote(t){
            t=(t||"").trim();
            if(t.length<25) return false;
            if((t.split("MACRO NOTE.").length>4)||t.startsWith("XXXX")) return false;
            if(/^(test|cors check|kv reset check|save path test|round-trip persistence test)$/.test(t)) return false;
            if(/^(multi note |batch note |real note |device save test)/.test(t)) return false;
            if(t.indexOf("no-preflight")>=0||t.indexOf("PERMANENCE TEST")>=0) return false;
            // AI-assistant chat transcript / dev-log junk (polluted the brain)
            var tl=t.toLowerCase();
            if(/would you like me to|shall i|let me |here'?s (what|the|your)|✅|🔥|📈|📬|perfect —|you now have|i'?ll (build|create|add|start)|let'?s build|do you want me to/.test(tl)) return false;
            if(/^[\[{]/.test(t) && /[\]}]$/.test(t)) return false;          // JSON blob
            if(/\{"results"|"id":|→ returns|lambda|endpoint|backend|frontend|deploy|api key|\.json|http[s]?:\/\//.test(tl)) return false;  // code/dev
            if(/^(step \d|fix \d|ops \d|\d+\.\s)/.test(tl)) return false;   // numbered build steps
            var letters=(t.match(/[a-zA-Z]/g)||[]).length;
            var words=(t.match(/\b[a-zA-Z]{3,}\b/g)||[]).length;
            if(letters/t.length < 0.6) return false;
            if(words < Math.max(5, t.length/45)) return false;
            if(/[|©£@]{2,}/.test(t)) return false;
            return true;
          }
          let ids = JSON.parse(await env.USER_DATA.get(IDX_KEY) || "[]");
          const offset = Math.max(0, parseInt(url.searchParams.get("offset") || "0", 10) || 0);
          const win = 400;
          function normT(t){ return String(t||"").toLowerCase().replace(/\s+/g," ").replace(/[^\w\s]/g,"").trim().slice(0,250); }
          if (offset === 0) { await env.USER_DATA.put(CACHE_KEY + ":wip", "[]"); await env.USER_DATA.put(CACHE_KEY + ":keepids", "[]"); await env.USER_DATA.put(CACHE_KEY + ":seen", "{}"); }
          let acc = JSON.parse(await env.USER_DATA.get(CACHE_KEY + ":wip") || "[]");
          let keepIds = JSON.parse(await env.USER_DATA.get(CACHE_KEY + ":keepids") || "[]");
          let seen = JSON.parse(await env.USER_DATA.get(CACHE_KEY + ":seen") || "{}");
          const slice = ids.slice(offset, offset + win);
          const raws = await Promise.all(slice.map(id => env.USER_DATA.get(NOTE_PREFIX + id).then(v=>[id,v]).catch(() => [id,null])));
          for (const [id,raw] of raws) {
            if (!raw) continue;
            try {
              var n = JSON.parse(raw);
              if (!realNote(n.text)) continue;          // garble/junk → drop
              var k = normT(n.text);
              if (k.length >= 8 && seen[k]) continue;    // duplicate text → drop
              if (k.length >= 8) seen[k] = 1;
              acc.push(n); keepIds.push(id);
            } catch (e) {}
          }
          await env.USER_DATA.put(CACHE_KEY + ":wip", JSON.stringify(acc));
          await env.USER_DATA.put(CACHE_KEY + ":keepids", JSON.stringify(keepIds));
          await env.USER_DATA.put(CACHE_KEY + ":seen", JSON.stringify(seen));
          const next = offset + win;
          const done = next >= ids.length;
          if (done) {
            await env.USER_DATA.put(CACHE_KEY, JSON.stringify(acc));   // promote cache
            await env.USER_DATA.put(IDX_KEY, JSON.stringify(keepIds)); // COMPACT index → real notes only
          }
          return jsonResp({ ok: true, kept: acc.length, scanned_to: next, total_index: ids.length, done });
        } catch (e) { return jsonResp({ error: String(e).slice(0, 150) }, 500); }
      }
      if (request.method === "GET") {
        // CACHE-FIRST: read the single bcache:<uid> key (instant — no 854-shard
        // fan-out that was timing out). If missing, rebuild from shards once and
        // cache it. This is the audit's read-path fix.
        let notes = null;
        try {
          const cached = await env.USER_DATA.get(CACHE_KEY);
          if (cached) notes = JSON.parse(cached);
        } catch (e) {}
        if (notes === null) {
          // No cache yet. Do NOT fan-out 854 shards here (it 500s/times out).
          // Return empty fast with a flag; the cache is built via ?build=1.
          const idsLen = (JSON.parse(await env.USER_DATA.get(IDX_KEY) || "[]")).length;
          return new Response(JSON.stringify({ notes: [], pin_set: !!(await env.USER_DATA.get(PIN_KEY)), scope: who === "khalid" ? "owner" : "user", cache_building: true, index_count: idsLen }),
            { headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeaders() } });
        }
        const hasPin = !!(await env.USER_DATA.get(PIN_KEY));
        return new Response(JSON.stringify({ notes, pin_set: hasPin, scope: who === "khalid" ? "owner" : "user", cached: true }),
          { headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeaders() } });
      }

      if (request.method === "PUT" || request.method === "POST") {
        const rawText = await request.text();
        let body = null; try { body = JSON.parse(rawText); } catch (e) {}
        let pin = request.headers.get("X-Brain-Pin") || (body && body._pin) || "";
        if (!isAuthedUser) {
          if (!pin || pin.length < 4) return new Response(JSON.stringify({ error: "pin required (min 4 chars)" }), { status: 401, headers: { "Content-Type": "application/json", ...corsHeaders() } });
          const existing = await env.USER_DATA.get(PIN_KEY); const ph = await sha(pin);
          if (existing) { if (ph !== existing) return new Response(JSON.stringify({ error: "wrong pin" }), { status: 403, headers: { "Content-Type": "application/json", ...corsHeaders() } }); }
          else await env.USER_DATA.put(PIN_KEY, ph);
        }
        try {
          if (!body) throw new Error("invalid json");
          // ── WRITE-TIME JUNK GUARD: reject AI-chat transcripts, code/dev-logs,
          // garble, and oversized bulk writes so the brain can NEVER re-pollute. ──
          function _isJunk(t){
            t=(t||"").trim();
            if(t.length<25) return true;
            var tl=t.toLowerCase();
            if((t.split("MACRO NOTE.").length>4)||t.startsWith("XXXX")) return true;
            if(/would you like me to|shall i|here'?s (what|the|your)|✅|🔥|📈|📬|🧬|🚨|➗|perfect —|you now have|i'?ll (build|create|add|start)|let'?s build|do you want me to|or just a zip|upload to vercel/.test(tl)) return true;
            if(/^[\[{]/.test(t) && /[\]}]$/.test(t)) return true;
            if(/\{"results"|"id":|→ returns|lambda|endpoint|\bbackend\b|\bfrontend\b|\bdeploy\b|api key|\.json\b|http[s]?:\/\//.test(tl)) return true;
            if(/^(step \d|fix \d|ops \d|\d+\.\s)/.test(tl)) return true;
            var letters=(t.match(/[a-zA-Z]/g)||[]).length;
            var words=(t.match(/\b[a-zA-Z]{3,}\b/g)||[]).length;
            if(letters/t.length < 0.55) return true;
            if(words < Math.max(4, t.length/45)) return true;
            if(/[|©£@]{2,}/.test(t)) return true;
            return false;
          }
          // strip junk from incoming notes
          if (Array.isArray(body.notes_upsert)) body.notes_upsert = body.notes_upsert.filter(n => n && n.id && !_isJunk(n.text));
          if (Array.isArray(body.notes)) body.notes = body.notes.filter(n => n && n.id && !_isJunk(n.text));
          if (body.note && _isJunk(body.note.text)) return jsonResp({ ok: true, mode: "rejected-junk", id: body.note.id });
          // hard cap: a single bulk write can't carry more than 500 notes (stops the
          // 20k-array re-save that kept re-inflating the brain)
          if (Array.isArray(body.notes) && body.notes.length > 500) return jsonResp({ error: "bulk too large — max 500/write", got: body.notes.length }, 413);
          if (Array.isArray(body.notes_upsert) && body.notes_upsert.length > 500) return jsonResp({ error: "batch too large — max 500/write", got: body.notes_upsert.length }, 413);
          // CONTENT-DEDUP at write time: drop incoming notes whose normalized text
          // already exists in the brain (stops re-imports from re-duplicating).
          if ((body.note && body.note.id) || (Array.isArray(body.notes_upsert) && body.notes_upsert.length)) {
            try {
              function _nrm(t){ return String(t||"").toLowerCase().replace(/\s+/g," ").replace(/[^\w\s]/g,"").trim().slice(0,200); }
              const cacheArr = await (async()=>{ try{ var v=await env.USER_DATA.get(CACHE_KEY); return v?JSON.parse(v):[]; }catch(e){ return []; } })();
              const existingText = new Set(cacheArr.map(n => _nrm(n.text)).filter(k => k.length >= 8));
              if (body.note && existingText.has(_nrm(body.note.text))) {
                return jsonResp({ ok: true, mode: "dedup-skip", id: body.note.id });
              }
              if (Array.isArray(body.notes_upsert)) {
                const seenInBatch = new Set();
                body.notes_upsert = body.notes_upsert.filter(n => {
                  const k = _nrm(n && n.text);
                  if (k.length < 8) return true;
                  if (existingText.has(k) || seenInBatch.has(k)) return false;
                  seenInBatch.add(k); return true;
                });
              }
            } catch (e) {}
          }
          // CAPACITY CEILING raised to 15000 (real notes shouldn't be blocked); the
          // write-time dedup above prevents the duplicate-flood that hit the old 3000.
          if (body.note || (Array.isArray(body.notes_upsert) && body.notes_upsert.length)) {
            try {
              const curLen = (JSON.parse(await env.USER_DATA.get(IDX_KEY) || "[]")).length;
              const incoming = body.note ? 1 : body.notes_upsert.length;
              if (curLen + incoming > 15000) return jsonResp({ error: "brain at capacity (15000)", index: curLen }, 429);
            } catch (e) {}
          }
          // helpers to keep bcache:<uid> (the fast-read copy) in sync
          async function cacheGet(){ try{ var v=await env.USER_DATA.get(CACHE_KEY); return v?JSON.parse(v):null; }catch(e){ return null; } }
          async function cachePut(arr){ try{ await env.USER_DATA.put(CACHE_KEY, JSON.stringify(arr)); }catch(e){} }
          // (a0) BATCH upsert — many notes in one request (parallel KV writes).
          if (Array.isArray(body.notes_upsert) && body.notes_upsert.length) {
            const valid = body.notes_upsert.filter(n => n && n.id);
            const B = 40;
            for (let i = 0; i < valid.length; i += B) {
              await Promise.all(valid.slice(i, i + B).map(n => env.USER_DATA.put(NOTE_PREFIX + n.id, JSON.stringify(n)).catch(() => {})));
            }
            let ids = await readIndex();
            const have = new Set(ids);
            for (const n of valid) { if (!have.has(n.id)) { ids.unshift(n.id); have.add(n.id); } }
            await env.USER_DATA.put(IDX_KEY, JSON.stringify(ids));
            // cache sync: prepend new, replace existing
            let cache = await cacheGet(); if (cache === null) cache = await readAllNotes();
            const cmap = new Map(cache.map(n => [n.id, n]));
            for (const n of valid) cmap.set(n.id, n);
            // preserve index order
            await cachePut(ids.map(id => cmap.get(id)).filter(Boolean));
            return new Response(JSON.stringify({ ok: true, mode: "batch", count: valid.length }), { headers: { "Content-Type": "application/json", ...corsHeaders() } });
          }
          // (a) upsert a single note
          if (body.note && body.note.id) {
            const n = body.note;
            const s = JSON.stringify(n);
            if (s.length > 20000000) return new Response(JSON.stringify({ error: "single note too large" }), { status: 413, headers: { "Content-Type": "application/json", ...corsHeaders() } });
            await env.USER_DATA.put(NOTE_PREFIX + n.id, s);
            let ids = await readIndex();
            if (!ids.includes(n.id)) { ids.unshift(n.id); await env.USER_DATA.put(IDX_KEY, JSON.stringify(ids)); }
            let cache = await cacheGet(); if (cache === null) cache = await readAllNotes();
            const idx = cache.findIndex(x => x.id === n.id);
            if (idx >= 0) cache[idx] = n; else cache.unshift(n);
            await cachePut(cache);
            return new Response(JSON.stringify({ ok: true, mode: "upsert", id: n.id }), { headers: { "Content-Type": "application/json", ...corsHeaders() } });
          }
          // (b) delete a single note
          if (body.delete) {
            await env.USER_DATA.delete(NOTE_PREFIX + body.delete);
            let ids = (await readIndex()).filter(x => x !== body.delete);
            await env.USER_DATA.put(IDX_KEY, JSON.stringify(ids));
            let cache = await cacheGet(); if (cache === null) cache = await readAllNotes();
            await cachePut(cache.filter(x => x.id !== body.delete));
            return new Response(JSON.stringify({ ok: true, mode: "delete", id: body.delete }), { headers: { "Content-Type": "application/json", ...corsHeaders() } });
          }
          // Defensive: array → bulk
          if (Array.isArray(body)) { body = { notes: body }; }
          if (Array.isArray(body.notes)) {
            const oldIds = await readIndex();
            const newIds = [];
            for (const n of body.notes) {
              if (n && n.id) { await env.USER_DATA.put(NOTE_PREFIX + n.id, JSON.stringify(n)); newIds.push(n.id); }
            }
            const newSet = new Set(newIds);
            for (const oid of oldIds) { if (!newSet.has(oid)) await env.USER_DATA.delete(NOTE_PREFIX + oid); }
            await env.USER_DATA.put(IDX_KEY, JSON.stringify(newIds));
            await cachePut(body.notes.filter(n => n && n.id));   // cache = exactly the new set
            try { await env.USER_DATA.delete(LEGACY_KEY); } catch (e) {}
            return new Response(JSON.stringify({ ok: true, mode: "bulk", count: newIds.length }), { headers: { "Content-Type": "application/json", ...corsHeaders() } });
          }
          // If a notes_upsert/notes array was present but filtered to empty by the
          // junk guard, that's a successful no-op (not a 400). Avoids the cosmetic
          // error in the save log when a batch was all junk.
          if ("notes_upsert" in body || "notes" in body) {
            return jsonResp({ ok: true, mode: "noop", filtered: true });
          }
          return new Response(JSON.stringify({ error: "send {note}, {delete}, or {notes:[…]}", received_keys: Object.keys(body || {}) }), { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
        } catch (e) {
          return new Response(JSON.stringify({ error: "save failed", detail: String(e).slice(0, 100) }), { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
        }
      }
      return new Response("method not allowed", { status: 405, headers: corsHeaders() });
    }

    // GET  /userdata/:uid           → returns stored JSON blob for user
    // PUT  /userdata/:uid {json}    → stores JSON blob for user
    // Keyed by anonymous device UID generated client-side. Backed by KV.
    if (url.pathname.startsWith("/userdata/")) {
      const uid = url.pathname.slice("/userdata/".length).replace(/[^a-zA-Z0-9_\-]/g, "");
      if (!uid || uid.length < 8 || uid.length > 64) {
        return new Response(JSON.stringify({ error: "invalid uid" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      if (!env.USER_DATA) {
        return new Response(JSON.stringify({ error: "user store unavailable" }),
          { status: 503, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const kvKey = `u:${uid}`;
      if (request.method === "GET") {
        const stored = await env.USER_DATA.get(kvKey);
        return new Response(stored || JSON.stringify({ empty: true }),
          { headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeaders() } });
      }
      if (request.method === "PUT" || request.method === "POST") {
        try {
          const bodyText = await request.text();
          if (bodyText.length > 500000) {  // 500KB cap per user
            return new Response(JSON.stringify({ error: "payload too large" }),
              { status: 413, headers: { "Content-Type": "application/json", ...corsHeaders() } });
          }
          // Validate JSON
          JSON.parse(bodyText);
          await env.USER_DATA.put(kvKey, bodyText);
          return new Response(JSON.stringify({ ok: true, saved_at: Date.now() }),
            { headers: { "Content-Type": "application/json", ...corsHeaders() } });
        } catch (e) {
          return new Response(JSON.stringify({ error: "invalid json", detail: String(e) }),
            { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
        }
      }
      return new Response("method not allowed", { status: 405, headers: corsHeaders() });
    }


    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response(
        JSON.stringify({
          name:           "justhodl-data-proxy",
          version:        "2.0.0",
          status:         "ok",
          upstream:       BUCKET_BASE,
          cache_rules:    CACHE_RULES.length,
          default_ttl_s:  DEFAULT_TTL,
          gzip:           "automatic via Cloudflare edge",
          fallback:       "tries /data/<file> if /<file> 404s and path has no slash",
        }, null, 2),
        { headers: { "Content-Type": "application/json", ...corsHeaders() } }
      );
    }

    // ─── BATCH QUOTES ROUTE ───
    // GET /quotes?tickers=AAPL,MSFT,NVDA → live snapshot via Polygon
    // Returns {tickers: {AAPL: {price, change, changePct, volume, ...}}}
    // Cached 30s at edge. Polygon key from worker env (server-side, secure).
    if (url.pathname === "/aff-click") {
      // Privacy-safe affiliate click counter (no PII). Increments a KV counter.
      try {
        const p = (url.searchParams.get("p") || "unknown").slice(0, 40);
        if (env.USER_DATA) {
          const k = "affclick:" + p + ":" + new Date().toISOString().slice(0, 10);
          const cur = parseInt(await env.USER_DATA.get(k) || "0", 10) || 0;
          await env.USER_DATA.put(k, String(cur + 1), { expirationTtl: 60 * 60 * 24 * 120 });
        }
        return new Response("ok", { headers: corsHeaders() });
      } catch (e) {
        return new Response("ok", { headers: corsHeaders() });
      }
    }

    if (url.pathname === "/create-checkout" && request.method === "POST") {
      // Create a Stripe Checkout session for a signed-in user. Body: {priceId,
      // userId, email, returnUrl}. Requires STRIPE_SECRET env (test or live).
      if (!env.STRIPE_SECRET) return jsonResp({ error: "billing not configured" }, 503);
      try {
        const b = await request.json();
        const priceId = (b.priceId || "").trim();
        const userId = (b.userId || "").trim();
        const email = (b.email || "").trim();
        if (!priceId || !userId) return jsonResp({ error: "missing priceId/userId" }, 400);
        const base = b.returnUrl || "https://justhodl.ai";
        const form = new URLSearchParams();
        form.set("mode", "subscription");
        form.set("line_items[0][price]", priceId);
        form.set("line_items[0][quantity]", "1");
        form.set("success_url", base + "/settings.html?checkout=success");
        form.set("cancel_url", base + "/pricing.html?checkout=cancel");
        form.set("client_reference_id", userId);          // ties session → our user
        form.set("metadata[user_id]", userId);
        form.set("subscription_data[metadata][user_id]", userId);
        if (email) form.set("customer_email", email);
        const r = await fetch("https://api.stripe.com/v1/checkout/sessions", {
          method: "POST",
          headers: { "Authorization": "Bearer " + env.STRIPE_SECRET,
                     "Content-Type": "application/x-www-form-urlencoded" },
          body: form.toString(),
        });
        const sess = await r.json();
        if (sess.error) return jsonResp({ error: sess.error.message }, 400);
        return jsonResp({ url: sess.url, id: sess.id });
      } catch (e) {
        return jsonResp({ error: String(e).slice(0, 140) }, 500);
      }
    }

    if (url.pathname === "/stripe-webhook" && request.method === "POST") {
      // On checkout.session.completed / subscription changes, flip the user's
      // plan in Supabase via the service-role key. Verifies Stripe signature.
      if (!env.STRIPE_WEBHOOK_SECRET || !env.SUPABASE_SERVICE_KEY) {
        return new Response("not configured", { status: 503 });
      }
      try {
        const sig = request.headers.get("stripe-signature") || "";
        const payload = await request.text();
        const ok = await verifyStripeSig(payload, sig, env.STRIPE_WEBHOOK_SECRET);
        if (!ok) return new Response("bad signature", { status: 400 });
        const evt = JSON.parse(payload);
        const type = evt.type;
        const obj = evt.data && evt.data.object || {};
        let userId = null, plan = null;
        if (type === "checkout.session.completed") {
          userId = obj.client_reference_id || (obj.metadata && obj.metadata.user_id);
          plan = "pro";
        } else if (type === "customer.subscription.deleted" ||
                   (type === "customer.subscription.updated" && obj.status !== "active" && obj.status !== "trialing")) {
          userId = obj.metadata && obj.metadata.user_id;
          plan = "free";
        } else if (type === "customer.subscription.updated" && (obj.status === "active" || obj.status === "trialing")) {
          userId = obj.metadata && obj.metadata.user_id;
          plan = "pro";
        }
        if (userId && plan) {
          // Update Supabase profiles.plan via REST (service role bypasses RLS)
          await fetch(`${env.SUPABASE_URL}/rest/v1/profiles?id=eq.${userId}`, {
            method: "PATCH",
            headers: {
              "apikey": env.SUPABASE_SERVICE_KEY,
              "Authorization": "Bearer " + env.SUPABASE_SERVICE_KEY,
              "Content-Type": "application/json",
              "Prefer": "return=minimal",
            },
            body: JSON.stringify({ plan,
              stripe_customer_id: obj.customer || null }),
          });
          // cache entitlement at the edge for fast gating
          if (env.USER_DATA) {
            await env.USER_DATA.put("plan:" + userId, plan, { expirationTtl: 60 * 60 * 24 * 35 });
          }
        }
        return new Response("ok", { status: 200 });
      } catch (e) {
        return new Response("err " + String(e).slice(0, 100), { status: 200 }); // 200 so Stripe doesn't retry-storm on our bug
      }
    }

    if (url.pathname === "/ask") {
      // Proxy natural-language questions to the justhodl-ask Lambda Function URL.
      const ASK_URL = "https://mxfefd5s3l4kp7ywx4ztlboqui0jrmkc.lambda-url.us-east-1.on.aws/";
      if (request.method === "OPTIONS") return new Response("{}", { headers: corsHeaders() });
      try {
        const body = await request.text();
        const r = await fetch(ASK_URL, { method: "POST", headers: { "Content-Type": "application/json" }, body });
        const txt = await r.text();
        return new Response(txt, { status: r.status, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      } catch (e) {
        return new Response(JSON.stringify({ error: "ask unavailable", detail: String(e).slice(0, 120) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/register-push" && request.method === "POST") {
      // Store a mobile push token for alert delivery (KV, 90-day TTL).
      try {
        const body = await request.json();
        const token = (body.token || "").slice(0, 400);
        if (!token) return new Response(JSON.stringify({ ok: false, error: "no token" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
        if (env.USER_DATA) {
          await env.USER_DATA.put("push:" + token,
            JSON.stringify({ platform: body.platform || "mobile", ts: Date.now() }),
            { expirationTtl: 60 * 60 * 24 * 90 });
        }
        return new Response(JSON.stringify({ ok: true }),
          { headers: { "Content-Type": "application/json", ...corsHeaders() } });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: String(e).slice(0, 120) }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/stream") {
      // Real-time quote stream. Browser opens a WebSocket; the worker polls
      // Polygon snapshot server-side every ~3s and pushes deltas. Compatible
      // with the equities REST plan (no Polygon WS tier needed).
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("expected websocket", { status: 426, headers: corsHeaders() });
      }
      const polygonKey = env.POLYGON_KEY || "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d";
      const pair = new WebSocketPair();
      const client = pair[0], server = pair[1];
      server.accept();
      let tickers = [];
      let alive = true;
      let last = {};
      server.addEventListener("message", (ev) => {
        try { const m = JSON.parse(ev.data); if (Array.isArray(m.tickers)) tickers = m.tickers.map(t => String(t).toUpperCase()).filter(Boolean).slice(0, 60); } catch (e) {}
      });
      server.addEventListener("close", () => { alive = false; });
      server.addEventListener("error", () => { alive = false; });
      const pump = async () => {
        let cycles = 0;
        while (alive && cycles < 600) {  // ~30 min cap then client reconnects
          cycles++;
          if (tickers.length) {
            try {
              const snapUrl = `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers=${tickers.join(",")}&apiKey=${polygonKey}`;
              const r = await fetch(snapUrl);
              const d = await r.json();
              const delta = {};
              for (const t of (d.tickers || [])) {
                const day = t.day || {}, prev = t.prevDay || {}, lt = t.lastTrade || {}, min = t.min || {};
                const price = lt.p || min.c || day.c || prev.c || 0;
                const pc = prev.c || 0;
                const changePct = pc ? ((price - pc) / pc * 100) : (t.todaysChangePerc || 0);
                if (last[t.ticker] !== price) {
                  last[t.ticker] = price;
                  delta[t.ticker] = { price, changePct, change: pc ? price - pc : 0, volume: day.v || 0 };
                }
              }
              if (Object.keys(delta).length && alive) server.send(JSON.stringify({ type: "quotes", t: Date.now(), quotes: delta }));
            } catch (e) {}
          }
          await new Promise(res => setTimeout(res, 3000));
        }
        try { server.close(1000, "cycle-limit"); } catch (e) {}
      };
      ctx.waitUntil(pump());
      return new Response(null, { status: 101, webSocket: client });
    }

    if (url.pathname === "/quotes") {
      const tickersParam = (url.searchParams.get("tickers") || "").trim();
      if (!tickersParam) {
        return new Response(JSON.stringify({ error: "missing tickers param" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const tickers = tickersParam.toUpperCase().split(",")
        .map(t => t.trim()).filter(Boolean).slice(0, 60);  // cap 60
      const polygonKey = env.POLYGON_KEY || "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d";

      // Edge cache key per ticker set
      const qCacheKey = new Request(`https://quotes.cache/${tickers.sort().join(",")}`, { method: "GET" });
      const qCache = caches.default;
      let qResp = await qCache.match(qCacheKey);
      if (qResp) {
        const cached = await qResp.json();
        return new Response(JSON.stringify(cached),
          { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } });
      }

      try {
        const snapUrl = `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers=${tickers.join(",")}&apiKey=${polygonKey}`;
        const snapResp = await fetch(snapUrl, { cf: { cacheTtl: 30, cacheEverything: true } });
        const snapData = await snapResp.json();
        const out = { tickers: {}, ts: Date.now() };
        for (const t of (snapData.tickers || [])) {
          const day = t.day || {};
          const prevDay = t.prevDay || {};
          const lastTrade = t.lastTrade || {};
          const min = t.min || {};
          const price = lastTrade.p || min.c || day.c || prevDay.c || 0;
          const prevClose = prevDay.c || 0;
          const change = prevClose ? (price - prevClose) : (t.todaysChange || 0);
          const changePct = prevClose ? (change / prevClose * 100) : (t.todaysChangePerc || 0);
          out.tickers[t.ticker] = {
            price: price,
            change: change,
            changePct: changePct,
            volume: day.v || min.v || 0,
            high: day.h || 0,
            low: day.l || 0,
            open: day.o || 0,
            prevClose: prevClose,
          };
        }
        const respBody = JSON.stringify(out);
        const finalResp = new Response(respBody, {
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": "public, max-age=30, s-maxage=30",
            "X-Cache": "MISS",
            ...corsHeaders(),
          },
        });
        ctx.waitUntil(qCache.put(qCacheKey, new Response(respBody, {
          headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=30" },
        })));
        return finalResp;
      } catch (e) {
        return new Response(JSON.stringify({ error: "quotes fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/yf-ohlc") {
      // GET /yf-ohlc?symbol=BTC-USD&range=1y → Yahoo Finance chart (crypto/forex/etc)
      const symbol = (url.searchParams.get("symbol") || "").trim();
      const range = (url.searchParams.get("range") || "1y").trim();
      const interval = ["1d","1wk","1mo"].includes((url.searchParams.get("interval") || "1d").trim()) ? (url.searchParams.get("interval") || "1d").trim() : "1d";
      if (!symbol || !/^[A-Za-z0-9.=^\-]{1,20}$/.test(symbol)) {
        return new Response(JSON.stringify({ error: "invalid symbol" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const yCacheKey = new Request(`https://yf.cache/${symbol}/${range}/${interval}`, { method: "GET" });
      const yc = caches.default;
      const yhit = await yc.match(yCacheKey);
      if (yhit) { const b = await yhit.text(); return new Response(b, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } }); }
      try {
        const yUrl = range === "max"
          ? `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${interval}&period1=0&period2=${Math.floor(Date.now() / 1000)}`
          : `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${interval}&range=${encodeURIComponent(range)}`;
        const resp = await fetch(yUrl, {
          headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36", "Accept": "application/json" },
          cf: { cacheTtl: 300, cacheEverything: true },
        });
        const data = await resp.json();
        const res = data.chart && data.chart.result && data.chart.result[0];
        const ts = (res && res.timestamp) || [];
        const q = (res && res.indicators && res.indicators.quote && res.indicators.quote[0]) || {};
        const bars = [];
        for (let i = 0; i < ts.length; i++) {
          if (q.close && q.close[i] != null && q.open[i] != null) {
            bars.push({ time: ts[i], open: q.open[i], high: q.high[i], low: q.low[i], close: q.close[i], value: (q.volume && q.volume[i]) || 0 });
          }
        }
        const out = JSON.stringify({ symbol, bars, count: bars.length });
        const finalResp = new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=300", "X-Cache": "MISS", ...corsHeaders() } });
        ctx.waitUntil(yc.put(yCacheKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=300" } })));
        return finalResp;
      } catch (e) {
        return new Response(JSON.stringify({ error: "yf fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/dbnomics-search") {
      // GET /dbnomics-search?q=inflation → search 90+ macro providers
      const qq = (url.searchParams.get("q") || "").trim();
      if (!qq) return new Response(JSON.stringify({ series: [] }), { headers: { "Content-Type": "application/json", ...corsHeaders() } });
      try {
        const dUrl = `https://api.db.nomics.world/v22/search?q=${encodeURIComponent(qq)}&limit=30`;
        const resp = await fetch(dUrl, { cf: { cacheTtl: 1800, cacheEverything: true } });
        const data = await resp.json();
        const docs = (data.results && data.results.docs) || [];
        const series = docs.map(d => ({
          id: `${d.provider_code}/${d.dataset_code}/${d.series_code}`,
          name: d.series_name || d.name,
          provider: d.provider_code, dataset: d.dataset_name || d.dataset_code,
        }));
        return new Response(JSON.stringify({ series }),
          { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=1800", ...corsHeaders() } });
      } catch (e) {
        return new Response(JSON.stringify({ series: [], error: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/dbnomics") {
      // GET /dbnomics?series=PROVIDER/DATASET/CODE → observations
      const sid = (url.searchParams.get("series") || "").trim();
      if (!sid || sid.split("/").length !== 3) {
        return new Response(JSON.stringify({ error: "invalid series id (need PROVIDER/DATASET/CODE)" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const dCacheKey = new Request(`https://dbn.cache/${sid}`, { method: "GET" });
      const dc = caches.default;
      const dhit = await dc.match(dCacheKey);
      if (dhit) { const b = await dhit.text(); return new Response(b, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } }); }
      try {
        const [prov, ds, code] = sid.split("/");
        const dUrl = `https://api.db.nomics.world/v22/series/${prov}/${ds}/${encodeURIComponent(code)}?observations=1`;
        const resp = await fetch(dUrl, { cf: { cacheTtl: 3600, cacheEverything: true } });
        const data = await resp.json();
        const doc = (data.series && data.series.docs && data.series.docs[0]) || {};
        const periods = doc.period || [];
        const values = doc.value || [];
        const bars = [];
        for (let i = 0; i < periods.length; i++) {
          const v = values[i];
          if (v == null || v === "NA") continue;
          const t = Math.floor(new Date(periods[i].length === 4 ? periods[i] + "-12-31" : periods[i].length === 7 ? periods[i] + "-01" : periods[i]).getTime() / 1000);
          if (isFinite(t) && isFinite(v)) bars.push({ time: t, value: +v, date: periods[i] });
        }
        bars.sort((a, b) => a.time - b.time);
        const out = JSON.stringify({ series: sid, name: doc.series_name || "", bars, count: bars.length });
        const finalResp = new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=3600", "X-Cache": "MISS", ...corsHeaders() } });
        ctx.waitUntil(dc.put(dCacheKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=3600" } })));
        return finalResp;
      } catch (e) {
        return new Response(JSON.stringify({ error: "dbnomics fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/fred") {
      // GET /fred?series=DGS10&obs=600 → FRED observations as {bars:[{time,value}]}
      const series = (url.searchParams.get("series") || "").trim().toUpperCase();
      const obs = Math.min(parseInt(url.searchParams.get("obs") || "600", 10) || 600, 5000);
      if (!series || !/^[A-Z0-9._\-]{1,40}$/.test(series)) {
        return new Response(JSON.stringify({ error: "invalid series" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const fredKey = env.FRED_KEY || "2f057499936072679d8843d7fce99989";
      const fCacheKey = new Request(`https://fred.cache/${series}/${obs}`, { method: "GET" });
      const fc = caches.default;
      const hit = await fc.match(fCacheKey);
      if (hit) {
        const body = await hit.text();
        return new Response(body, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } });
      }
      try {
        const fredUrl = `https://api.stlouisfed.org/fred/series/observations?series_id=${series}&api_key=${fredKey}&file_type=json&sort_order=desc&limit=${obs}`;
        const resp = await fetch(fredUrl, { cf: { cacheTtl: 3600, cacheEverything: true } });
        const data = await resp.json();
        const obsArr = (data.observations || [])
          .filter(o => o.value !== "." && o.value != null)
          .map(o => ({ time: Math.floor(new Date(o.date).getTime() / 1000), value: parseFloat(o.value), date: o.date }))
          .filter(o => isFinite(o.value))
          .sort((a, b) => a.time - b.time);
        const out = JSON.stringify({ series, bars: obsArr, count: obsArr.length });
        const finalResp = new Response(out, {
          headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=3600", "X-Cache": "MISS", ...corsHeaders() },
        });
        ctx.waitUntil(fc.put(fCacheKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=3600" } })));
        return finalResp;
      } catch (e) {
        return new Response(JSON.stringify({ error: "fred fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/fred-search") {
      // GET /fred-search?text=inflation → FRED full-text series search (800k+ series)
      const text = (url.searchParams.get("text") || "").trim();
      if (!text) {
        return new Response(JSON.stringify({ series: [] }),
          { headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const fredKey = env.FRED_KEY || "2f057499936072679d8843d7fce99989";
      try {
        const sUrl = `https://api.stlouisfed.org/fred/series/search?search_text=${encodeURIComponent(text)}&api_key=${fredKey}&file_type=json&limit=30&order_by=popularity&sort_order=desc`;
        const resp = await fetch(sUrl, { cf: { cacheTtl: 1800, cacheEverything: true } });
        const data = await resp.json();
        const series = (data.seriess || []).map(s => ({
          id: s.id, title: s.title, units: s.units_short || s.units,
          frequency: s.frequency_short || s.frequency, popularity: s.popularity,
          obs_start: s.observation_start, obs_end: s.observation_end,
        }));
        return new Response(JSON.stringify({ series }),
          { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=1800", ...corsHeaders() } });
      } catch (e) {
        return new Response(JSON.stringify({ series: [], error: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/ohlc") {
      // GET /ohlc?ticker=AAPL&mult=1&span=day&days=180 → Polygon aggregates (any interval)
      const ticker = (url.searchParams.get("ticker") || "").trim().toUpperCase();
      const days = Math.min(parseInt(url.searchParams.get("days") || "180", 10) || 180, 12000);
      const mult = Math.min(parseInt(url.searchParams.get("mult") || "1", 10) || 1, 60);
      const span = (url.searchParams.get("span") || "day").trim();
      const validSpans = ["minute", "hour", "day", "week", "month"];
      if (!ticker || !/^[A-Z0-9.\-]{1,12}$/.test(ticker) || !validSpans.includes(span)) {
        return new Response(JSON.stringify({ error: "invalid params" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const polygonKey = env.POLYGON_KEY || "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d";
      const to = new Date();
      const from = new Date(to.getTime() - days * 86400000);
      const fmt = (d) => d.toISOString().slice(0, 10);
      const ttl = span === "minute" ? 60 : span === "hour" ? 120 : 300;
      const ohlcKey = new Request(`https://ohlc.cache/${ticker}/${mult}${span}/${days}`, { method: "GET" });
      const oc = caches.default;
      let cached = await oc.match(ohlcKey);
      if (cached) {
        const body = await cached.text();
        return new Response(body, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } });
      }
      try {
        const aggUrl = `https://api.polygon.io/v2/aggs/ticker/${ticker}/range/${mult}/${span}/${fmt(from)}/${fmt(to)}?adjusted=true&sort=asc&limit=50000&apiKey=${polygonKey}`;
        const resp = await fetch(aggUrl, { cf: { cacheTtl: ttl, cacheEverything: true } });
        const data = await resp.json();
        const bars = (data.results || []).map(b => ({
          time: Math.floor(b.t / 1000), open: b.o, high: b.h, low: b.l, close: b.c, value: b.v,
        }));
        const out = JSON.stringify({ ticker, span, mult, bars, count: bars.length });
        const finalResp = new Response(out, {
          headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${ttl}`, "X-Cache": "MISS", ...corsHeaders() },
        });
        ctx.waitUntil(oc.put(ohlcKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${ttl}` } })));
        return finalResp;
      } catch (e) {
        return new Response(JSON.stringify({ error: "ohlc fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/fundamentals") {
      // GET /fundamentals?ticker=AAPL → key quote + profile + ratios snapshot (FMP)
      const ticker = (url.searchParams.get("ticker") || "").trim().toUpperCase();
      if (!ticker || !/^[A-Z0-9.\-]{1,12}$/.test(ticker)) {
        return new Response(JSON.stringify({ error: "invalid ticker" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const fmpKey = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb";
      const fKey = new Request(`https://fund.cache/${ticker}`, { method: "GET" });
      const fc = caches.default;
      const hit = await fc.match(fKey);
      if (hit) { const b = await hit.text(); return new Response(b, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } }); }
      try {
        const [q, p, rm, km, est, pts] = await Promise.all([
          fetch(`https://financialmodelingprep.com/stable/quote?symbol=${ticker}&apikey=${fmpKey}`, { cf: { cacheTtl: 600 } }).then(r => r.ok ? r.json() : []),
          fetch(`https://financialmodelingprep.com/stable/profile?symbol=${ticker}&apikey=${fmpKey}`, { cf: { cacheTtl: 3600 } }).then(r => r.ok ? r.json() : []),
          fetch(`https://financialmodelingprep.com/stable/ratios-ttm?symbol=${ticker}&apikey=${fmpKey}`, { cf: { cacheTtl: 3600 } }).then(r => r.ok ? r.json() : []),
          fetch(`https://financialmodelingprep.com/stable/key-metrics-ttm?symbol=${ticker}&apikey=${fmpKey}`, { cf: { cacheTtl: 3600 } }).then(r => r.ok ? r.json() : []),
          fetch(`https://financialmodelingprep.com/stable/analyst-estimates?symbol=${ticker}&period=annual&limit=3&apikey=${fmpKey}`, { cf: { cacheTtl: 21600 } }).then(r => r.ok ? r.json() : []),
          fetch(`https://financialmodelingprep.com/stable/price-target-summary?symbol=${ticker}&apikey=${fmpKey}`, { cf: { cacheTtl: 21600 } }).then(r => r.ok ? r.json() : []),
        ]);
        const quote = Array.isArray(q) ? (q[0] || {}) : (q || {});
        const prof = Array.isArray(p) ? (p[0] || {}) : (p || {});
        const ratios = Array.isArray(rm) ? (rm[0] || {}) : (rm || {});
        const metrics = Array.isArray(km) ? (km[0] || {}) : (km || {});
        const pick = (...keys) => { for (const k of keys) { if (ratios[k] != null) return ratios[k]; if (metrics[k] != null) return metrics[k]; } return null; };
        const out = JSON.stringify({
          ticker,
          name: prof.companyName, sector: prof.sector, industry: prof.industry,
          exchange: prof.exchange || quote.exchange, ceo: prof.ceo, country: prof.country,
          description: (prof.description || "").slice(0, 400),
          price: quote.price, changesPct: quote.changePercentage || quote.changesPercentage,
          marketCap: quote.marketCap || prof.marketCap, volume: quote.volume, avgVolume: quote.avgVolume,
          pe: quote.pe || pick("priceToEarningsRatioTTM", "peRatioTTM"), eps: quote.eps,
          yearHigh: quote.yearHigh, yearLow: quote.yearLow,
          beta: prof.beta, dividendYield: pick("dividendYieldTTM", "dividendYielPercentageTTM", "dividendYieldPercentageTTM"),
          pb: pick("priceToBookRatioTTM", "pbRatioTTM"), ps: pick("priceToSalesRatioTTM", "priceSalesRatioTTM"),
          roe: pick("returnOnEquityTTM", "roeTTM", "returnOnEquity"), netMargin: pick("netProfitMarginTTM", "netProfitMargin"),
          debtToEquity: pick("debtToEquityRatioTTM", "debtEquityRatioTTM"),
          currentRatio: pick("currentRatioTTM"), grossMargin: pick("grossProfitMarginTTM"),
          opMargin: pick("operatingProfitMarginTTM"), fcfYield: pick("freeCashFlowYieldTTM"),
          estimates: (Array.isArray(est) ? est : []).slice(0, 3).map(e => ({
            year: (e.date || "").slice(0, 4), revenueAvg: e.revenueAvg || e.estimatedRevenueAvg,
            epsAvg: e.epsAvg || e.estimatedEpsAvg })),
          analystPT: (() => { const a = Array.isArray(pts) ? (pts[0] || {}) : (pts || {}); return {
            avg: a.lastMonthAvgPriceTarget || a.allTimeAvgPriceTarget, high: a.allTimeHighPriceTarget,
            low: a.allTimeLowPriceTarget, n: a.lastMonthCount || a.allTimeCount }; })(),
        });
        const fr = new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=600", "X-Cache": "MISS", ...corsHeaders() } });
        ctx.waitUntil(fc.put(fKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=600" } })));
        return fr;
      } catch (e) {
        return new Response(JSON.stringify({ error: "fundamentals fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/news") {
      // GET /news?ticker=AAPL → recent headlines via FMP (commercial-licensed)
      const ticker = (url.searchParams.get("ticker") || "").trim().toUpperCase();
      if (!ticker || !/^[A-Z0-9.\-]{1,12}$/.test(ticker)) {
        return new Response(JSON.stringify({ news: [] }),
          { headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const fmpKey = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb";
      const nKey = new Request(`https://news.cache/${ticker}`, { method: "GET" });
      const nc = caches.default;
      const hit = await nc.match(nKey);
      if (hit) { const b = await hit.text(); return new Response(b, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } }); }
      try {
        const u = `https://financialmodelingprep.com/stable/news/stock?symbols=${ticker}&limit=20&apikey=${fmpKey}`;
        const resp = await fetch(u, { cf: { cacheTtl: 600, cacheEverything: true } });
        let rows = await resp.json();
        if (!Array.isArray(rows)) rows = [];
        const news = rows.slice(0, 20).map(n => ({
          title: n.title, site: n.publisher || n.site, date: n.publishedDate || n.date,
          url: n.url, text: (n.text || "").slice(0, 200), image: n.image || "",
        }));
        const out = JSON.stringify({ ticker, news });
        const fr = new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=600", "X-Cache": "MISS", ...corsHeaders() } });
        ctx.waitUntil(nc.put(nKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=600" } })));
        return fr;
      } catch (e) {
        return new Response(JSON.stringify({ news: [], error: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/tv-search") {
      // GET /tv-search?text=AAPL → proxy TradingView symbol search (full universe)
      const text = (url.searchParams.get("text") || "").trim();
      const type = (url.searchParams.get("type") || "").trim();
      const exchange = (url.searchParams.get("exchange") || "").trim();
      if (!text) {
        return new Response(JSON.stringify({ symbols: [] }),
          { headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      try {
        const tvUrl = `https://symbol-search.tradingview.com/symbol_search/?text=${encodeURIComponent(text)}&hl=1&lang=en&type=${encodeURIComponent(type)}&exchange=${encodeURIComponent(exchange)}&domain=production`;
        const tvResp = await fetch(tvUrl, {
          headers: {
            "Origin": "https://www.tradingview.com",
            "Referer": "https://www.tradingview.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          },
          cf: { cacheTtl: 300, cacheEverything: true },
        });
        const raw = await tvResp.json();
        const clean = (s) => String(s || "").replace(/<\/?[^>]+>/g, "");
        const arr = Array.isArray(raw) ? raw : (raw.symbols || []);
        const symbols = arr.slice(0, 30).map(s => ({
          symbol: clean(s.symbol),
          description: clean(s.description),
          type: s.type || "",
          exchange: s.exchange || s.exchange_name || "",
          currency: s.currency_code || "",
          full: (s.prefix || s.exchange) ? `${(s.prefix || s.exchange)}:${clean(s.symbol)}` : clean(s.symbol),
        }));
        return new Response(JSON.stringify({ symbols }),
          { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=300", ...corsHeaders() } });
      } catch (e) {
        return new Response(JSON.stringify({ symbols: [], error: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    // Everything below is the read-only S3 data proxy — GET/HEAD only.
    // (POST routes like /create-checkout, /stripe-webhook, /ask, /register-push
    // are all handled above this line.)
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("method not allowed", { status: 405, headers: corsHeaders() });
    }

    const safePath = url.pathname.replace(/^\/+/, "");
    if (!/^[a-zA-Z0-9_\-./]+$/.test(safePath) || safePath.includes("..")) {
      return new Response("invalid path", { status: 400, headers: corsHeaders() });
    }

    const ttl = ttlFor(safePath);
    const cacheKey = new Request(url.toString().split("?")[0], { method: "GET" });
    const cache = caches.default;
    let response = await cache.match(cacheKey);
    let cacheStatus = "HIT";

    if (!response) {
      cacheStatus = "MISS";
      let upstreamUrl = `${BUCKET_BASE}/${safePath}`;
      let upstream;
      try {
        upstream = await fetchUpstream(upstreamUrl, ttl);
      } catch (e) {
        return new Response(
          JSON.stringify({ error: "upstream fetch failed", detail: String(e), path: safePath }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } }
        );
      }

      // Backward-compat fallback for callers using legacy paths.
      // S3 returns 403 (AccessDenied) — not 404 — for a missing key when
      // ListBucket is denied, so retry under /data/ on BOTH.
      if (!upstream.ok && (upstream.status === 404 || upstream.status === 403) && !safePath.includes("/")) {
        const fallbackUrl = `${BUCKET_BASE}/data/${safePath}`;
        try {
          const fallback = await fetchUpstream(fallbackUrl, ttl);
          if (fallback.ok) { upstream = fallback; upstreamUrl = fallbackUrl; }
        } catch (_) { /* keep original status */ }
      }

      // Equity-research cold-ticker fallback: on an S3 miss for equity-research/<TICKER>.json,
      // generate the report by calling the research Lambda SERVER-SIDE (worker -> Lambda).
      // This keeps the browser on the justhodl.ai/Cloudflare domain only — the raw
      // *.lambda-url.on.aws domain is routinely blocked by ad-blockers / privacy extensions /
      // corporate firewalls, which surfaced as "Failed to fetch" for any non-pre-cached ticker.
      if (!upstream.ok && (upstream.status === 404 || upstream.status === 403) &&
          safePath.startsWith("equity-research/") && safePath.endsWith(".json")) {
        const tkr = safePath.slice("equity-research/".length, -5)
                            .toUpperCase().replace(/[^A-Z0-9.\-]/g, "");
        const noGen    = url.searchParams.get("nogen") === "1";   // polling: pure S3 read, never re-trigger
        const asyncGen = url.searchParams.get("async") === "1";   // trigger: kick off in background, return 202
        if (tkr && !noGen) {
          const RESEARCH_LAMBDA = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/";
          const genUrl = `${RESEARCH_LAMBDA}?ticker=${encodeURIComponent(tkr)}`;
          if (asyncGen) {
            // Trigger generation via the Lambda's kickoff mode: it does an AWS-native
            // Event self-invoke (guaranteed to run to completion and write S3 even during
            // the AI-synthesis outage) and returns 202 in ~1s. We await that fast call so
            // the Event invoke is definitely queued, then tell the browser to poll ?nogen=1.
            // (More reliable than ctx.waitUntil, which this plan cuts before a ~220s gen.)
            await fetch(`${genUrl}&kickoff=1`, { headers: { "User-Agent": "justhodl-data-proxy" } })
              .catch(() => {});
            return new Response(
              JSON.stringify({ status: "generating", ticker: tkr }),
              { status: 202, headers: { "Content-Type": "application/json", ...corsHeaders() } }
            );
          }
          try {
            const gen = await fetch(genUrl, { headers: { "User-Agent": "justhodl-data-proxy" } });
            if (gen.ok) { upstream = gen; upstreamUrl = `lambda:equity-research/${tkr}`; }
          } catch (_) { /* fall through to the not-ok error below */ }
        }
      }

      if (!upstream.ok) {
        return new Response(
          JSON.stringify({ error: "upstream not ok", status: upstream.status, path: safePath }),
          { status: upstream.status, headers: { "Content-Type": "application/json", ...corsHeaders() } }
        );
      }

      const upstreamCT = upstream.headers.get("Content-Type") || "application/json";
      const lastMod    = upstream.headers.get("Last-Modified");
      const etag       = upstream.headers.get("ETag");
      const body       = await upstream.arrayBuffer();

      const respHeaders = {
        "Content-Type":   upstreamCT,
        "Cache-Control":  `public, max-age=${Math.min(ttl, 60)}, s-maxage=${ttl}`,
        "X-Edge-TTL":     String(ttl),
        "X-Upstream":     upstreamUrl,
        ...corsHeaders(),
      };
      if (lastMod) respHeaders["Last-Modified"] = lastMod;
      if (etag)    respHeaders["ETag"]          = etag;

      response = new Response(body, { status: 200, headers: respHeaders });
      ctx.waitUntil(cache.put(cacheKey, response.clone()));
    }

    const headers = new Headers(response.headers);
    headers.set("X-Edge-Cache", cacheStatus);
    return new Response(response.body, { status: response.status, headers });
  },
};
