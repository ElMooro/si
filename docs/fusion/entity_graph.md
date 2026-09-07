# Entity master + exposure graph (Release 3 design; Release 1 groundwork)

## Release 1
- Canonical ids `type:SYMBOL` and symbol canonicalisation in `jhsignal.py`.
- `config/jh-fusion-universe.json` seeds aliases (GOOG -> GOOGL, SPX -> etf:SPY, X:BTCUSD -> crypto:BTC) and first
  `affects` edges (NVDA -> industry:SEMICONDUCTORS 0.8, etf:SMH 0.5; TSM -> country:TW 0.7; ASML supplier 0.6).
  Every signal carries `affects[]`; nothing propagates yet.

## Release 3 plan (relational, no graph database)
Table `entity_relationships` (DynamoDB single-table or S3-backed `data/entities/relationships.json.gz`):
source_entity, target_entity, relationship_type (sector, industry, country, etf_constituent, supplier, customer,
commodity_exposure, currency_exposure, geopolitical_exposure, theme, index, major_shareholder, trade_partner),
relationship_strength, confidence, effective_from/to, source (finviz-universe, fundamental-census, symdir, 13F,
etf stock-exposure-lookup, impact_mapper graph), updated_at.
Sources already in the fleet: finviz sector/industry/country per ticker, `etf-flows/stock-exposure-lookup.json`,
fortress `IND_ETF`, `aws/shared/impact_mapper.py` (impact-map/1.0 graph + betas), 13F holders, symdir aliases/ISINs.

Propagation rule (Release 3): a signal reaches a related entity with strength x relationship_strength x
horizon/type relevance, depth <= 2, provenance kept (`propagated_from`, `path`, `attenuation`). Never silent.
