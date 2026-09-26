/* brain-note-cap.js — 100k write cap overlay. No access to brain.html let-bindings. */
(function () {
  if (window.__JH_BRAIN_NOTE_CAP) return;
  window.__JH_BRAIN_NOTE_CAP = 1;
  var NOTE_MAX = 100000, PREVIEW = 2000, ORIG_SPLIT = 16000;
  window.NOTE_MAX = NOTE_MAX;
  function clip(s) { s = String(s == null ? "" : s); return s.length > NOTE_MAX ? s.slice(0, NOTE_MAX) : s; }
  function fmt(n) { return Number(n || 0).toLocaleString(); }
  function catOf() {
    var el = document.querySelector("#cat-row .cat-chip.active");
    return (el && el.getAttribute("data-cat")) || "lesson";
  }
  function hint() {
    var ta = document.getElementById("note-input");
    var h = document.getElementById("composer-hint");
    if (!ta || !h) return;
    var n = (ta.value || "").length;
    h.textContent = fmt(n) + " / " + fmt(NOTE_MAX) + " · Cmd/Ctrl+Enter to add";
    h.style.color = n > NOTE_MAX ? "#ff5577" : (n > NOTE_MAX * 0.9 ? "#fbbf24" : "");
  }
  window.clipNote = clip;
  window.updateComposerCount = hint;

  function wrapSave() {
    if (typeof window.saveNote !== "function" || window.saveNote.__jhClip) return;
    var orig = window.saveNote;
    window.saveNote = function (n) {
      if (n && n.text) n.text = clip(n.text);
      return orig.apply(this, arguments);
    };
    window.saveNote.__jhClip = 1;
  }

  function patchAdd() {
    if (typeof window.addNote !== "function") return false;
    if (window.addNote.__jhCap) return true;
    var orig = window.addNote;
    window.addNote = function () {
      var ta = document.getElementById("note-input");
      var v = ta ? (ta.value || "").trim() : "";
      if (!v || v.length <= ORIG_SPLIT) return orig.apply(this, arguments);
      if (typeof window.saveNote !== "function" || typeof window.uid !== "function") return orig.apply(this, arguments);
      var text = clip(v);
      if (v.length > NOTE_MAX && window.badge) badge("capped at " + fmt(NOTE_MAX) + " chars", "err");
      saveNote({ id: uid(), cat: catOf(), text: text, created: Date.now(), pinned: false });
      ta.value = "";
      hint();
      try { if (window._flushBatch) _flushBatch(); } catch (e) {}
      if (window.badge) badge("saving long note…", "saving");
      setTimeout(function () { try { if (window.load) load(); } catch (e2) {} }, 700);
    };
    window.addNote.__jhCap = 1;
    var btn = document.getElementById("add-btn");
    if (btn) btn.onclick = window.addNote;
    return true;
  }

  function patchRender() {
    if (typeof window.renderNotes !== "function") return false;
    if (window.renderNotes.__jhCap) return true;
    var orig = window.renderNotes;
    window.renderNotes = function () {
      orig.apply(this, arguments);
      var box = document.getElementById("notes");
      if (!box) return;
      box.querySelectorAll(".note .nbody").forEach(function (body) {
        if (body._jhPrev) return;
        var full = body.textContent || "";
        if (full.length <= PREVIEW) return;
        body._jhPrev = 1;
        body._full = full;
        body.textContent = full.slice(0, PREVIEW) + "…";
        var more = document.createElement("div");
        more.className = "nmore";
        more.textContent = "show all · " + fmt(full.length) + " chars";
        more.style.cssText = "font-family:ui-monospace,Menlo,monospace;font-size:11px;color:#22d3ee;cursor:pointer;margin-top:8px";
        more.onclick = function () { body.textContent = body._full || full; more.remove(); };
        body.after(more);
      });
    };
    window.renderNotes.__jhCap = 1;
    return true;
  }

  function bindHint() {
    var ta = document.getElementById("note-input");
    if (!ta) return false;
    if (!ta.__jhCapBound) {
      ta.__jhCapBound = 1;
      ta.addEventListener("input", hint);
    }
    hint();
    return true;
  }

  function install() {
    wrapSave();
    var ok = patchAdd() && bindHint();
    patchRender();
    if (ok && window.renderNotes) renderNotes();
    try {
      var el = document.getElementById("brain-debug");
      if (el) el.textContent = new Date().toLocaleTimeString() + " NOTE_MAX=" + NOTE_MAX + " overlay\n" + el.textContent;
    } catch (e) {}
    return ok;
  }

  if (install()) return;
  var n = 0, t = setInterval(function () { n++; if (install() || n > 40) clearInterval(t); }, 250);
})();
