# Chart help accessibility — draft only

## Cause and scope

The focus defect predates PR37. The retained predecessor includes the shipped
Timing button, but the older generic help lifecycle already used a fixed,
full-viewport dimmed backdrop (`#indhelp`, z-index 80), click-outside dismissal,
close button and Escape. It created a `div`, never focused help, never restored
the opener and supplied no dialog semantics or background inertness. PR37 added
a new route into that same lifecycle. Parent live QA confirmed warning visibility
and content, Enter/open, Escape/close and ×/close work, but Tab reached background
Zoom out and focus did not return to Timing.

This is a modal interaction by its existing pointer/backdrop design. The repair
uses native `dialog.showModal()` rather than declaring a nonmodal div modal with
ARIA. Native dialog supplies the dialog role, top-layer behavior, background
inertness and keyboard confinement. `aria-labelledby` points to the existing
help title. The close button has the accessible name "Close help" and receives
initial focus. Text, thresholds, warning visibility, classifications and marker
positions are unchanged. Only `jh-chart-indux.js` changes production behavior;
engine/SymDir and classifiers are untouched.

## Lifecycle

- Owned Timing, legend-help and indicator-list triggers pass the actual control.
  Existing callers remain compatible: without a second argument, use the active
  element. No engine changes or hidden navigation.
- Internal help-topic links rebuild content and focus Close while preserving
  the original opener, rather than retaining a soon-detached topic link.
- Close, backdrop click, native cancel, Escape and external native close share
  restoration. Keyboard events inside help do not bubble into chart shortcuts;
  Tab/Enter/Space default behavior remains native.
- Escape closes only the top help, preserving an underlying settings/indicator
  panel. On return, that panel's initiating control receives focus.
- If an opener was removed/disabled/hidden/inert, try a usable parent-panel
  button, replacement Timing button, then chart legend information button.
  Never deliberately focus a disconnected/hidden control or navigate elsewhere.
- A delayed native close event cannot hide a help dialog reopened before that
  event arrives. Repeated opens use one dialog and one lifecycle handler set.
- Original overlay/box dimensions are retained, with native-dialog margin,
  border and padding defaults explicitly reset. No false `aria-modal` assertion
  or custom tab-order rewrite.

## Evidence and acceptance

Base: `5ce0d146bdc823bd0d103ae763b72e8f34c94ee6`.
Retained complete help predecessor:
`tests/fixtures/chart-help-indux-predecessor.js.txt`.

`node --test tests/chart-help-accessibility.test.js tests/chart-annotation-honesty.test.js`
passes 16 tests. The added lifecycle tests execute the whole script using the
existing DOM contract stub, extended with focus/native-dialog contracts. They
cover repeated opening/closing, different explicit/implicit triggers, detail
navigation, Escape, cancel, backdrop, external close, removed opener and a queued
close event. The prior annotation suite still verifies warning lifecycle/content,
missing dates, source-output parity and no extra scan calls. A copy comparison
proves the help dictionaries and warning qualification text are unchanged.

The DOM stub does NOT prove native focus trapping, accessibility-tree exposure,
layout, touch or mobile behavior. The runnable
`tests/chart-help-accessibility-browser.cjs` covers 1440/390 widths, Enter,
Tab/Shift+Tab, background focus prevention, close/restoration, detail navigation,
removed/replaced opener, pointer/touch and backdrop. It uses only synthetic local
UI and aborts network requests; normal sandbox is mandatory. Launch failed in
this environment (Chromium crashpad/SUID-sandbox startup), before any browser
assertion ran. No sandbox or TLS bypass was attempted.

Before merge: independent source review plus normal managed-browser acceptance
of the draft at desktop/mobile widths, including accessibility role/name and
keyboard behavior. Parent production observations concern the predecessor, not
acceptance of this draft. No merge/deployment requested by this PR.

Full frontend/worker gate: **2,437 passed**. Public page syntax: **599 graphs**,
zero errors. Wiring: **36 pages / 143 entries**, no missing/stale entries.
Page regressions **6**, sovereign assets **3**, offline-page tests **6** pass.
