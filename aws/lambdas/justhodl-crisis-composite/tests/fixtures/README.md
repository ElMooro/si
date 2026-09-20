The 46 JSON files in originals preserve exact FRED definition/observation bytes
retained by the public report warehouse, acquired on 2026-09-20 or earlier.
Their filenames are SHA-256 values verified against the original evidence
receipts. Credentials are absent from their data and source references.

macro.json is an offline 23-series slice reconstructed from these original
responses using report_observations.py at the warehouse's recorded compilation
time. It is a test fixture, not a claim that this sliced packet was published.
manifest.json records its selected original inputs and output binding. Tests
never call an external provider or read private accounts.

The retained ECB CSV and ciss.json/ciss-manifest.json bind the original euro-area
CISS headline, exact row metadata, acquisition clock and reviewed compiler.
No mapped stress score or historical rank is used by the Crisis compiler.
