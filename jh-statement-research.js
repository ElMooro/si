(function (root) {
  'use strict';
  const PREFIX = 'data/statement-research/', CURRENT = 'data/forensic-screen.json';
  const CONTRACT = 'financial-statement-original-research.v2', HASH = /^[a-f0-9]{64}$/;
  const FLAGS = ['calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified'];
  const COMPILERS = {
    statement_research_source: 'fe019e9a09192db264bdec8de7e6b9b18f553a0867ef6ba471bb3c8e3c720433',
    statement_measurements: '6529d6bbe2236615572ea97a0e5e08d340673c6d86facbe4ebd951736f866b7c',
    statement_research_model: 'd2aae2535f2e8a04a54d66cb965bad5be73f01c7fc7742ab182b7d69cc05c3a1',
    statement_research_store: '64e7d656d51ee709edd6d20291362d842ace90db98a426084314a50bffec4f4c',
    statement_research_identity: 'f9c56e31a43e82b913d7aaae77af84c9c2464802a2cf95a86f96efe16d20daab',
    statement_research_v2: '23e37211d7224b543e980092593c9c4eb40f92b479a3364f51ce616aea237e54',
    statement_research_store_v2: '8c527042f833f9d3cc2abb6bdfbf66f0f56a2887493eb7322297e8d28f4e70e1'
  };
  const I = 'income-statement', B = 'balance-sheet-statement', C = 'cash-flow-statement';
  const FORMULAS = {
    gross_margin_pct: [[[I,'grossProfit',1]], [I,'revenue'],100],
    operating_margin_pct: [[[I,'operatingIncome',1]], [I,'revenue'],100],
    net_margin_pct: [[[I,'netIncome',1]], [I,'revenue'],100],
    operating_cash_margin_pct: [[[C,'operatingCashFlow',1]], [I,'revenue'],100],
    cash_conversion_multiple: [[[C,'operatingCashFlow',1]], [I,'netIncome'],1],
    current_ratio: [[[B,'totalCurrentAssets',1]], [B,'totalCurrentLiabilities'],1],
    debt_to_assets_pct: [[[B,'totalDebt',1]], [B,'totalAssets'],100],
    goodwill_to_assets_pct: [[[B,'goodwill',1]], [B,'totalAssets'],100],
    net_receivables_to_revenue_pct: [[[B,'netReceivables',1]], [I,'revenue'],100],
    sga_to_revenue_pct: [[[I,'sellingGeneralAndAdministrativeExpenses',1]], [I,'revenue'],100],
    reported_fcf_margin_pct: [[[C,'freeCashFlow',1]], [I,'revenue'],100],
    earnings_cash_gap_to_assets_pct: [[[I,'netIncome',1],[C,'operatingCashFlow',-1]], [B,'totalAssets'],100],
    net_debt_derived: [[[B,'totalDebt',1],[B,'cashAndCashEquivalents',-1]],null,1],
    earnings_cash_gap: [[[I,'netIncome',1],[C,'operatingCashFlow',-1]],null,1],
    balance_identity_residual: [[[B,'totalAssets',1],[B,'totalLiabilities',-1],[B,'totalEquity',-1]],null,1],
    gross_profit_residual: [[[I,'grossProfit',1],[I,'revenue',-1],[I,'costOfRevenue',1]],null,1],
    operating_cash_alias_residual: [[[C,'operatingCashFlow',1],[C,'netCashProvidedByOperatingActivities',-1]],null,1]
  };
  function canonical(value) {
    let result;
    if (Array.isArray(value)) result = '[' + value.map(canonical).join(',') + ']';
    else if (value && typeof value === 'object') result = '{' + Object.keys(value).sort().map(k => canonical(k) + ':' + canonical(value[k])).join(',') + '}';
    else result = JSON.stringify(value);
    return result.replace(/[\u007f-\uffff]/g, c => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0'));
  }
  async function hash(bytes) { return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256', bytes)), n => n.toString(16).padStart(2,'0')).join(''); }
  async function get(fetcher, key, ref) {
    if (key !== CURRENT && !new RegExp('^' + PREFIX + '(?:runs|inputs|outputs|records)/[a-f0-9]{64}\\.json$').test(key)) throw Error('Invalid accounting artifact path');
    const response = await fetcher('/' + key, {cache:'no-store',credentials:'same-origin'});
    if (!response.ok) throw Error('Recorded accounting research is unavailable (HTTP ' + response.status + ')');
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.length > 32 * 1024 * 1024) throw Error('Accounting artifact exceeds its size limit');
    const digest = await hash(bytes);
    if (ref && (ref.key !== key || ref.bytes !== bytes.length || ref.sha256 !== digest)) throw Error('Accounting artifact verification failed');
    if (key !== CURRENT && key.split('/').pop().slice(0,64) !== digest) throw Error('Accounting artifact identity differs');
    return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
  }
  function descriptive(value) { return value && FLAGS.every(k => value[k] === false) && value.independent_investment_votes === 0; }
  function valid(p) {
    return descriptive(p) && p.contract === CONTRACT && ['call','score','grade','m_score'].every(k => p[k] === null)
      && Array.isArray(p.issuers) && p.issuers.length === p.reported_names && p.provider_responses === p.reported_names * 6
      && new Set(p.issuers.map(v => v.symbol)).size === p.reported_names && p.quality?.original_source_bytes_replayed === true
      && p.quality.original_sec_filings_replayed === false && p.quality.accounting_audit === false
      && p.quality.historical_first_availability_verified === false && p.quality.current_sec_index_replayed === true
      && p.quality.historical_security_continuity_verified === false && p.quality.issuer_conflicts_withheld === true;
  }
  async function load(fetcher, pinned) {
    let reference, current = null;
    if (pinned !== null && pinned !== undefined) {
      if (!HASH.test(pinned)) throw Error('Invalid recorded accounting run');
      reference = PREFIX + 'runs/' + pinned + '.json';
    } else {
      current = await get(fetcher,CURRENT);
      if (!valid(current) || !current.replay) throw Error('Source-qualified native accounting research is not available yet');
      reference = current.replay.manifest_key;
    }
    const run = await get(fetcher,reference);
    if (run.contract !== 'financial-statement-original-replay.v2' || !HASH.test(run.output_sha256)
        || canonical(Object.keys(run.compilers || {}).sort()) !== canonical(Object.keys(COMPILERS).sort())) throw Error('Unsupported accounting calculation');
    for (const [name,digest] of Object.entries(COMPILERS)) {
      if (run.compilers[name].sha256 !== digest || run.compilers[name].key !== PREFIX + 'compilers/' + digest + '.py') throw Error('Unqualified accounting compiler');
    }
    const packet = await get(fetcher,run.output.key,run.output);
    if (!valid(packet) || run.output.sha256 !== run.output_sha256 || packet.generated_at !== run.generated_at) throw Error('Recorded accounting publication differs');
    if (current) {
      const body = {...current}; delete body.replay;
      if (current.replay.output_sha256 !== run.output_sha256 || await hash(new TextEncoder().encode(canonical(body))) !== run.output_sha256) throw Error('Current accounting head differs from its recorded publication');
    }
    const index = await get(fetcher,packet.identity_index.original.key,packet.identity_index.original);
    const pairs = new Map();
    if (packet.identity_index.url !== 'https://www.sec.gov/files/company_tickers.json' || packet.identity_index.historical_security_continuity_verified !== false
        || !index || Array.isArray(index) || Object.keys(index).length < 1000 || Object.keys(index).length > 50000) throw Error('Complete SEC current identity source required');
    for (const [key,row] of Object.entries(index)) {
      if (!/^\d+$/.test(key) || !row || !Number.isInteger(row.cik_str) || row.cik_str <= 0 || row.cik_str >= 1e10
          || typeof row.ticker !== 'string' || !row.ticker || typeof row.title !== 'string' || !row.title) throw Error('Invalid SEC current identity source');
      if (!pairs.has(row.ticker)) pairs.set(row.ticker,new Set());
      pairs.get(row.ticker).add(String(row.cik_str).padStart(10,'0'));
    }
    return {packet,run,reference,pairs,runId:reference.split('/').pop().slice(0,64)};
  }
  function fraction(value) {
    if (typeof value !== 'string' || !/^-?\d{1,230}(?:\.\d{1,230})?$/.test(value)) throw Error('Invalid exact recorded decimal');
    const [whole,decimals = ''] = value.replace(/^-/, '').split('.');
    return [(value[0] === '-' ? -1n : 1n) * BigInt(whole + decimals),10n ** BigInt(decimals.length)];
  }
  function add(a,b) { return [a[0]*b[1]+b[0]*a[1],a[1]*b[1]]; }
  function rounded(value) {
    const negative = value[0] < 0n, top = (negative ? -value[0] : value[0]) * 10n**12n;
    let result = top / value[1]; const remainder = top % value[1];
    if (2n*remainder > value[1] || (2n*remainder === value[1] && result % 2n)) result += 1n;
    return (negative && result !== 0n ? '-' : '') + (result / 10n**12n) + '.' + (result % 10n**12n).toString().padStart(12,'0');
  }
  function verifyMetric(name, metric) {
    const definition = FORMULAS[name];
    if (!definition || metric.supports_investment_action !== false || metric.period_annualized !== false) throw Error('Unsupported accounting measurement');
    const [terms,denominator,scale] = definition, refs = terms.map(([e,f]) => [e,f]).concat(denominator ? [denominator] : []);
    if (!Array.isArray(metric.inputs) || metric.inputs.length !== refs.length) throw Error('Accounting inputs differ');
    const values = metric.inputs.map((field,i) => {
      if (field.endpoint !== refs[i][0] || field.field !== refs[i][1]) throw Error('Accounting field definition differs');
      return field.reported_value === null ? null : fraction(field.reported_value);
    });
    const available = values.every(v => v !== null) && (!denominator || values.at(-1)[0] > 0n);
    if (!available) {
      if (metric.value !== null || !['required_statement_record_missing','required_provider_field_missing','nonpositive_denominator','reviewed_statement_records_required'].includes(metric.status)) throw Error('Unavailable input used in accounting calculation');
      return {available:false,reason:metric.status};
    }
    if (metric.status !== 'descriptive_calculation') throw Error('Available accounting calculation was withheld without a valid reason');
    let value = terms.reduce((sum,term,i) => add(sum,[values[i][0]*BigInt(term[2]),values[i][1]]),[0n,1n]);
    if (denominator) { const d = values.at(-1); value = [value[0]*d[1]*BigInt(scale),value[1]*d[0]]; }
    if (metric.value !== rounded(value)) throw Error('Recorded accounting arithmetic differs');
    return {available:true,exact_value:metric.value};
  }
  async function record(fetcher,state,symbol) {
    const summary = state.packet.issuers.find(row => row.symbol === symbol);
    if (!summary || !descriptive(summary)) throw Error('Choose an exact name from this recorded universe');
    const shard = await get(fetcher,summary.record.key,summary.record);
    if (!descriptive(shard) || shard.contract !== 'financial-statement-issuer-records.v2' || shard.requested_symbol !== symbol
        || !Array.isArray(shard.records) || shard.records.length !== summary.records || shard.source_row_count !== summary.original_rows) throw Error('Recorded issuer history differs');
    let rows = 0; const seen = new Set();
    if (canonical(shard.identity_index) !== canonical(state.packet.identity_index)) throw Error('Issuer identity source differs');
    for (const row of shard.records) {
      if (!descriptive(row) || !HASH.test(row.record_id) || !Array.isArray(row.source_rows)) throw Error('Unqualified issuer record');
      for (const coord of row.source_rows) {
        const origin = state.packet.sources[coord.capture_id];
        if (!origin || origin.original_sha256 !== coord.source_id || !Number.isInteger(coord.source_row) || coord.source_row < 0
            || coord.source_row >= origin.rows || origin.request.symbol !== symbol || origin.request.endpoint !== coord.endpoint
            || origin.request.period !== coord.request_period || coord.request_period !== row.request_period) throw Error('Recorded accounting source locator differs');
        const id=coord.capture_id+':'+coord.source_row;if(seen.has(id))throw Error('Repeated source coordinate');seen.add(id);rows++;
      }
      verifyIdentity(row,symbol,state.pairs);
      if (!row.measurements) continue;
      if (!descriptive(row.measurements) || canonical(Object.keys(row.measurements.metrics).sort()) !== canonical(Object.keys(FORMULAS).sort())) throw Error('Unqualified accounting measurements');
      for (const [name,metric] of Object.entries(row.measurements.metrics)) {
        verifyMetric(name,metric);
        const unit=['current_ratio','cash_conversion_multiple'].includes(name)?'multiple':FORMULAS[name][1]?'%':row.identity.reportedCurrency;
        if(metric.unit!==unit)throw Error('Accounting measurement unit differs');
        for (const input of metric.inputs) {
          if (input.source_id !== null && !row.source_rows.some(c => c.source_id === input.source_id && c.source_row === input.source_row && c.endpoint === input.endpoint)) throw Error('Measurement source row differs');
        }
      }
    }
    if (rows !== summary.original_rows) throw Error('Issuer source row conservation differs');
    return {summary,shard,artifact:summary.record};
  }

  function verifyIdentity(row,symbol,pairs) {
    const fields=['symbol','cik','reportedCurrency','date','fiscalYear','period','filingDate','acceptedDate'];
    if (!Array.isArray(row.identity_evidence) || row.identity_evidence.length !== row.source_rows.length || row.historical_security_continuity_verified !== false) throw Error('Original identity evidence required');
    let matches=true;const seen=new Set(),ciks=[...(pairs.get(symbol)||[])].sort();
    for(const item of row.identity_evidence){
      const key=item.capture_id+':'+item.source_row;
      if(seen.has(key)||!row.source_rows.some(c=>c.capture_id===item.capture_id&&c.source_row===item.source_row&&c.source_id===item.source_id&&c.endpoint===item.endpoint&&c.request_period===item.request_period))throw Error('Identity source coordinate differs');
      seen.add(key);
      if(canonical(Object.keys(item.reported_identity||{}).sort())!==canonical([...fields].sort())||canonical(Object.keys(item.reported_identity_types||{}).sort())!==canonical([...fields].sort())||!Array.isArray(item.missing_fields))throw Error('Original typed identity metadata required');
      const reported=item.reported_identity.cik,type=item.reported_identity_types.cik;
      const cikValid=((type==='string'&&typeof reported==='string')||(type==='number'&&Number.isInteger(reported)))&&/^\d{1,10}$/.test(String(reported))&&Number(reported)>0;
      const status=!ciks.length?'not_in_current_sec_ticker_index':ciks.length!==1?'ambiguous_current_sec_ticker_index':!cikValid?'invalid_provider_cik':String(reported).padStart(10,'0')!==ciks[0]?'provider_cik_differs_from_current_sec_index':'current_ticker_cik_pair_corroborated';
      if(item.current_identity_status!==status||canonical(item.current_sec_ciks)!==canonical(ciks)||item.historical_security_continuity_verified!==false)throw Error('Current issuer corroboration differs');
      matches=matches&&status==='current_ticker_cik_pair_corroborated';
      if(row.measurements){
        if(fields.some(field=>String(item.reported_identity[field])!==row.identity?.[field])||item.missing_fields.length||item.clock_issues.length)throw Error('Calculated statement identity differs');
        const ident=row.identity;
        if(ident.symbol!==symbol||!/^\d{4}-\d{2}-\d{2}$/.test(ident.date)||ident.filingDate<ident.date||ident.acceptedDate.slice(0,10)<ident.date)throw Error('Invalid identity used in calculation');
      }
    }
    if(row.current_ticker_cik_corroborated!==matches||(!matches&&row.measurements))throw Error('Uncorroborated issuer used in calculation');
    return matches;
  }
  function scenario(a) {
    function fixed(v) {
      if (typeof v !== 'string' || !/^-?\d{1,18}(?:\.\d{1,8})?$/.test(v)) throw Error('Use decimal assumptions with at most eight decimal places');
      const [n,d] = fraction(v); return n * 10n**8n / d;
    }
    const q=fixed(a.shares),p=fixed(a.price),shock=fixed(a.shock),cost=fixed(a.cost);
    if (!/^[A-Z]{3}$/.test(a.currency) || p<=0n || cost<0n || shock < -100n*10n**8n || shock > 1000n*10n**8n) throw Error('Use a currency code, positive price, nonnegative costs and a price change from -100% to 1,000%');
    function exact(n,scale) {
      const sign=n<0n?'-':''; let s=(n<0n?-n:n).toString().padStart(scale+1,'0');
      return sign+(s.slice(0,-scale)+'.'+s.slice(-scale)).replace(/(\.\d*?)0+$/,'$1').replace(/\.$/,'');
    }
    return {assumptions:{...a},signed_notional:exact(q*p,16),pnl:exact(q*p*shock-cost*10n**18n,26),currency:a.currency,forecast:false};
  }
  const api={PREFIX,CURRENT,CONTRACT,COMPILERS,FORMULAS,load,record,verifyMetric,verifyIdentity,scenario,canonical,hash};
  if (typeof module !== 'undefined' && module.exports) module.exports=api; else root.JHStatementResearch=api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
