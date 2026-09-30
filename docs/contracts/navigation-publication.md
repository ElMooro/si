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
rendering. A separate observed accessibility gap remains: Escape closes the drawer
visually but focus stays in its offscreen search field. That is not accepted as
complete keyboard accessibility and needs the next repair.
