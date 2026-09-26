/* brain-note-cap.js — 100,000-char Brain write cap. Ask/engines stay at 8k. */
(function () {
  if (window.__JH_NOTE_MAX_PATCH) return;
  if (!/brain\.html/i.test(location.pathname || "")) return;
  var NOTE_MAX = 100000;
  var NOTE_PREVIEW = 2000;
  window.NOTE_MAX = NOTE_MAX;
  function clipNote(s) {
    s = String(s == null ? "" : s);
    return s.length > NOTE_MAX ? s.slice(0, NOTE_MAX) : s;
  }
  function fmtCount(n) { return Number(n || 0).toLocaleString(); }
  function activeChip() {
    var el = document.querySelector("#cat-row .cat-chip.active");
    return (el && el.getAttribute("data-cat")) || "lesson";
  }
  function updateComposerCount() {
    var ta = document.getElementById("note-input");
    var h = document.getElementById("composer-hint");
    if (!ta || !h) return;
    var n = (ta.value || "").length;
    h.textContent = fmtCount(n) + " / " + fmtCount(NOTE_MAX) + " · Cmd/Ctrl+Enter to add";
    h.style.color = n > NOTE_MAX ? "var(--red)" : (n > NOTE_MAX * 0.9 ? "var(--amber)" : "");
  }
  function previewCards() {
    var box = document.getElementById("notes");
    if (!box) return;
    box.querySelectorAll(".note .nbody").forEach(function (body) {
      if (body.dataset.full === "1") return;
      var full = body._full || body.textContent || "";
      if (full.length <= NOTE_PREVIEW) return;
      body._full = full;
      body.textContent = full.slice(0, NOTE_PREVIEW);
      var note = body.parentElement;
      if (!note || note.querySelector("[data-act='more']")) return;
      var more = document.createElement("div");
      more.className = "nmore";
      more.setAttribute("data-act", "more");
      more.style.cssText = "font-family:ui-monospace,Menlo,monospace;font-size:11px;color:var(--cyan);cursor:pointer;margin-top:8px";
      more.textContent = "show all · " + fmtCount(full.length) + " chars";
      more.onclick = function () {
        body.textContent = body._full || full;
        body.dataset.full = "1";
        more.remove();
      };
      body.insertAdjacentElement("afterend", more);
    });
  }
  function patch() {
    if (window.__JH_NOTE_MAX_PATCH) return true;
    if (typeof addNote !== "function" || typeof saveNote !== "function" || typeof uid !== "function") return false;
    window.__JH_NOTE_MAX_PATCH = 1;
    var origAdd = addNote;
    var origRender = typeof renderNotes === "function" ? renderNotes : null;
    var origImport = typeof importNotes === "function" ? importNotes : null;
    window.addNote = function () {
      if (typeof brainUid === "function" && !brainUid()) {
        if (typeof badge === "function") badge("Sign in before adding notes", "err");
        return;
      }
      var ta = document.getElementById("note-input");
      var v = ta ? String(ta.value || "").trim() : "";
      if (!v) return;
      if (v.length <= 16000) {
        origAdd();
        updateComposerCount();
        setTimeout(previewCards, 0);
        return;
      }
      var n = { id: uid(), cat: activeChip(), text: clipNote(v), created: Date.now(), pinned: false };
      saveNote(n);
      ta.value = "";
      updateComposerCount();
      if (typeof badge === "function") badge("saving…", "saving");
      if (typeof load === "function") {
        Promise.resolve(load()).then(function () { setTimeout(previewCards, 50); });
      }
    };
    if (origRender) {
      window.renderNotes = function () {
        origRender.apply(this, arguments);
        previewCards();
      };
    }
    if (origImport) {
      window.importNotes = async function (file) {
        var raw = await file.text();
        var incoming = null;
        try {
          var d = JSON.parse(raw);
          incoming = Array.isArray(d.notes) ? d.notes : (Array.isArray(d) ? d : null);
        } catch (e) {}
        var blob = new File([raw], file.name || "brain.json", { type: "application/json" });
        await origImport(blob);
        if (!incoming) return;
        incoming.forEach(function (n) {
          if (n && n.text && String(n.text).length > 8000) {
            saveNote(Object.assign({}, n, { id: n.id || uid(), text: clipNote(n.text) }));
          }
        });
        if (typeof load === "function") {
          Promise.resolve(load()).then(function () { setTimeout(previewCards, 50); });
        }
      };
    }
    var addBtn = document.getElementById("add-btn");
    if (addBtn) addBtn.onclick = addNote;
    var ta = document.getElementById("note-input");
    if (ta) {
      ta.addEventListener("input", updateComposerCount);
      ta.onkeydown = function (e) {
        if ((e.metaKey || e.ctrlKey) && e.key === "Enter") addNote();
      };
    }
    updateComposerCount();
    previewCards();
    return true;
  }
  function boot(n) {
    if (patch()) return;
    if ((n || 0) > 80) return;
    setTimeout(function () { boot((n || 0) + 1); }, 50);
  }
  boot(0);
})();
