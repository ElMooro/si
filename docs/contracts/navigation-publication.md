# Complete navigation titles and atomic publication

The navigation builder reads every root HTML file as UTF-8. It parses the complete
title, including attributes and titles after long head sections, ignores script
text, preserves title RCDATA and decodes entities once. Existing whitespace and
JustHodl suffix normalization remains. Missing/empty/redirect titles remain
excluded. Unreadable, invalid UTF-8, duplicate or incomplete titles stop the build
before replacement; they cannot silently erase a destination.

`title_encoding: unicode_text` declares that page titles are plain Unicode text.
The drawer escapes these titles without decoding them again. Older manifests
retain their existing entity interpretation. The homepage already escapes plain
text. Titles containing literal markup never become active elements.

Output is serialized into a unique UTF-8/LF temporary file in the destination
directory, then closed and replaced atomically. Failed serialization or replacement
preserves the entire prior manifest and removes the temporary file. This covers
application write failure, not disk durability after power loss.

Rendering a favorite or tag update reapplies the existing search and tag
intersection. Search listeners are installed once; repeated refreshes cannot
multiply handlers. Original routes, category assignments, favorites and account
sync contracts remain. No engine measurement or investment permission changes.

The complete predecessor generator, drawer and manifest are retained under
`tests/fixtures/navigation`. Python regressions cover complete parsing, UTF-8 mode
disabled, invalid input and interrupted replacement. JavaScript tests execute the
actual renderer and retained predecessor. `preview_server.py` serves six invented
entries, blocks external traffic, disables native diagnostics and needs no login.
Browser acceptance covers titles, search, favorite refresh and mobile/desktop
rendering. The navigation disclosure now names its region and search field, exposes the
handle's expanded/controls relationship and removes closed controls from focus
and the accessibility tree using inert, visibility and aria-hidden. Opening focuses
search synchronously. Closing restores a connected, enabled opener (otherwise
the handle), without moving focus if the user already returned to the page.
Repeated or rapid open/close cannot leave a delayed focus callback. This is a
non-modal navigation disclosure, not a modal dialog or whole-site accessibility
certification. Category and favorite/tag keyboard controls remain separate work.
