# Census discovery bridge

The published warehouse catalogue lists `census-us`, while the reviewed chart adapter and exact series use `census`. Previously All providers offered only the warehouse identity, leaving the reviewed economic series inaccessible through that path.

Keep every original provider entry and stored-file row. Add an explicitly labelled chart-adapter entry for Census economic histories when the published catalogue lacks it. The existing Census warehouse view also links to those histories. Stored files for the chart adapter use the original `census-us` catalogue and validate that exact identity; no nonexistent `census.json` request is made.

Original downloaded metadata and its receipt remain unchanged. A local adapter entry does not acquire a warehouse catalogue timestamp, history coverage, licence, fresh observations or trading authority. Directory failure remains visible; it never becomes a synthetic success. Existing provider and unknown-instrument handling remain intact.

Acceptance requires All providers -> Census -> dataset -> exact series -> plotted public observations at desktop and mobile widths, the warehouse-to-chart bridge, full stored-file identity/pagination checks, no data-provider fallbacks, and exact served source verification.
