const test = require("node:test");
const assert = require("node:assert/strict");

const homeValidation = require("../home.js");
const riskPageValidation = require("../khalidrisk.js");

const NOW = Date.parse("2026-09-04T22:00:00.000Z");
const HOUR = 60 * 60 * 1000;
const CRITICAL = [
  ["risk_gate", 30],
  ["crisis", 8],
  ["bond_warroom", 84],
  ["eurodollar_stress", 30],
  ["credit_composite", 30],
];

function copy(value) {
  return structuredClone(value);
}

function riskFixture() {
  const generatedAt = NOW - (30 * 60 * 1000);
  const sourceAsOf = generatedAt - HOUR;
  const sourceHealth = CRITICAL.map(([name, maxAge]) => ({
    name,
    critical: true,
    status: "FRESH",
    as_of: new Date(sourceAsOf).toISOString(),
    age_h: 1,
    max_age_h: maxAge,
  }));
  return {
    engine: "justhodl-khalid-risk",
    schema_version: "1.0.0",
    version: "1.0.0",
    generated_at: new Date(generatedAt).toISOString(),
    as_of: new Date(sourceAsOf).toISOString(),
    status: "OK",
    policy: {
      mode: "SELECTIVE_RISK_ON",
      allows_new_entries: true,
      exposure_cap_pct: 80,
    },
    capital_decision: "INVEST SELECTIVELY",
    exposure_cap_pct: 80,
    risk_score: 20,
    plain_english: "Invest selectively, subject to the authoritative risk controls.",
    coverage: {
      total: 5,
      fresh: 5,
      stale: 0,
      missing: 0,
      invalid: 0,
      unknown: 0,
      ratio: 1,
      status: "OK",
    },
    freshness: { status: "FRESH", oldest_age_h: 1 },
    treasury_fails: { status: "MISSING" },
    domains: [],
    hard_vetoes: [],
    tighteners: [],
    conflicts: [],
    source_health: sourceHealth,
    reasons: [],
    methodology: { authority: "Tighten only." },
    risk_board: {
      mode: "SELECTIVE_RISK_ON",
      allows_new_entries: true,
      capital_decision: "INVEST SELECTIVELY",
      exposure_cap_pct: 80,
    },
  };
}

function fusionFixture() {
  const generatedAt = NOW - (30 * 60 * 1000);
  const sourceAsOf = generatedAt - HOUR;
  return {
    engine: "justhodl-engine-fusion",
    schema_version: "1.0.0",
    version: "1.0.0",
    generated_at: new Date(generatedAt).toISOString(),
    status: "OK",
    authoritative_verdict: null,
    packets: [{
      evidence_id: "ev1:test",
      source_id: "credit",
      subject: "GLOBAL",
      domain: "risk",
      direction: "NEUTRAL",
      score: 50,
      confidence: 0.8,
      freshness: "FRESH",
      as_of: new Date(sourceAsOf).toISOString(),
      active: true,
    }],
    inactive_packets: [],
    coverage: {
      allowlisted_sources: 1,
      fresh_active_before_dedupe: 1,
      active_after_dedupe: 1,
      ratio: 1,
      independent_root_evidence: 1,
      synthesized_views: 0,
      freshness: { fresh: 1, stale: 0, missing: 0, invalid: 0, unknown: 0 },
    },
    dedupe: { dropped_count: 0, dropped: [] },
    disagreements: [],
    vetoes: [],
    subscriptions: {},
    trace: [{
      source_id: "credit",
      freshness: "FRESH",
      as_of: new Date(sourceAsOf).toISOString(),
      age_h: 1,
      max_age_h: 24,
      active: true,
    }],
    methodology: { decision: "No aggregate verdict." },
  };
}

const riskValidators = [
  ["homepage", homeValidation.validateRiskArtifact],
  ["Khalid Risk page", riskPageValidation.validateRiskArtifact],
];

test("both frontends directly accept a fresh, internally consistent risk artifact", () => {
  for (const [name, validate] of riskValidators) {
    assert.equal(validate(riskFixture(), NOW).ok, true, name);
  }
});

test("both frontends reject schema drift, stale generation, and excessive future skew", () => {
  for (const [name, validate] of riskValidators) {
    const wrongVersion = riskFixture();
    wrongVersion.version = "1.0.1";
    assert.equal(validate(wrongVersion, NOW).ok, false, `${name}: version`);

    const stale = riskFixture();
    stale.generated_at = new Date(NOW - homeValidation.maxArtifactAgeMs - 1).toISOString();
    assert.equal(validate(stale, NOW).ok, false, `${name}: stale`);

    const future = riskFixture();
    future.generated_at = new Date(NOW + homeValidation.futureToleranceMs + 1).toISOString();
    assert.equal(validate(future, NOW).ok, false, `${name}: future`);

    const toleratedSkew = riskFixture();
    toleratedSkew.generated_at = new Date(NOW + homeValidation.futureToleranceMs).toISOString();
    for (const row of toleratedSkew.source_health) {
      row.as_of = toleratedSkew.generated_at;
      row.age_h = 0;
    }
    assert.equal(validate(toleratedSkew, NOW).ok, true, `${name}: tolerance boundary`);
  }
});

test("both frontends reject decision, entry permission, cap, and status contradictions", () => {
  const mutations = [
    (data) => { data.status = "DATA_HOLD"; data.coverage.status = "DATA_HOLD"; data.freshness.status = "DATA_HOLD"; },
    (data) => { data.risk_board.capital_decision = "STAY IN CASH / SHORT-TERM TREASURIES"; },
    (data) => { data.policy.allows_new_entries = false; data.risk_board.allows_new_entries = false; },
    (data) => { data.policy.exposure_cap_pct = 79; },
    (data) => { data.exposure_cap_pct = 0; data.policy.exposure_cap_pct = 0; data.risk_board.exposure_cap_pct = 0; },
  ];
  for (const [name, validate] of riskValidators) {
    for (const mutate of mutations) {
      const data = riskFixture();
      mutate(data);
      assert.equal(validate(data, NOW).ok, false, name);
    }
  }
});

test("both frontends derive OK versus DEGRADED from source health", () => {
  for (const [name, validate] of riskValidators) {
    const falseOk = riskFixture();
    falseOk.source_health.push({
      name: "optional_lens",
      critical: false,
      status: "MISSING",
      as_of: null,
      age_h: null,
      max_age_h: 30,
    });
    falseOk.coverage.total = 6;
    falseOk.coverage.missing = 1;
    falseOk.coverage.ratio = 0.8333;
    assert.equal(validate(falseOk, NOW).ok, false, name);

    falseOk.status = "DEGRADED";
    falseOk.coverage.status = "DEGRADED";
    falseOk.freshness.status = "DEGRADED";
    assert.equal(validate(falseOk, NOW).ok, true, name);
  }
});

test("critical source-health claims are recomputed and cannot leave risk-on enabled", () => {
  for (const [name, validate] of riskValidators) {
    const missingCritical = riskFixture();
    missingCritical.source_health.pop();
    missingCritical.coverage.total = 4;
    missingCritical.coverage.fresh = 4;
    assert.equal(validate(missingCritical, NOW).ok, false, `${name}: missing critical source`);

    const staleClaimedFresh = riskFixture();
    const source = staleClaimedFresh.source_health[0];
    source.as_of = new Date(NOW - (31 * HOUR)).toISOString();
    source.age_h = 30.5;
    assert.equal(validate(staleClaimedFresh, NOW).ok, false, `${name}: false FRESH claim`);

    const unhealthyRiskOn = riskFixture();
    unhealthyRiskOn.source_health[0].status = "STALE";
    unhealthyRiskOn.coverage.fresh = 4;
    unhealthyRiskOn.coverage.stale = 1;
    unhealthyRiskOn.coverage.ratio = 0.8;
    assert.equal(validate(unhealthyRiskOn, NOW).ok, false, `${name}: critical stale risk-on`);
  }
});

test("a genuinely fail-closed DATA_HOLD artifact remains displayable as restrictive", () => {
  const data = riskFixture();
  data.status = "DATA_HOLD";
  data.policy = { mode: "DATA_HOLD", allows_new_entries: false, exposure_cap_pct: 0 };
  data.capital_decision = "STAY IN CASH / SHORT-TERM TREASURIES";
  data.exposure_cap_pct = 0;
  data.risk_board = {
    mode: "DATA_HOLD",
    allows_new_entries: false,
    capital_decision: "STAY IN CASH / SHORT-TERM TREASURIES",
    exposure_cap_pct: 0,
  };
  data.source_health[0].status = "STALE";
  data.coverage = {
    total: 5,
    fresh: 4,
    stale: 1,
    missing: 0,
    invalid: 0,
    unknown: 0,
    ratio: 0.8,
    status: "DATA_HOLD",
  };
  data.freshness.status = "DATA_HOLD";
  for (const [name, validate] of riskValidators) {
    assert.equal(validate(data, NOW).ok, true, name);
  }
});

test("homepage directly validates fusion generation and evidence freshness", () => {
  assert.equal(homeValidation.validateFusionArtifact(fusionFixture(), NOW).ok, true);

  const staleArtifact = fusionFixture();
  staleArtifact.generated_at = new Date(NOW - homeValidation.maxArtifactAgeMs - 1).toISOString();
  assert.equal(homeValidation.validateFusionArtifact(staleArtifact, NOW).ok, false);

  const futureArtifact = fusionFixture();
  futureArtifact.generated_at = new Date(NOW + homeValidation.futureToleranceMs + 1).toISOString();
  assert.equal(homeValidation.validateFusionArtifact(futureArtifact, NOW).ok, false);

  const expiredEvidence = fusionFixture();
  expiredEvidence.trace[0].as_of = new Date(NOW - (25 * HOUR)).toISOString();
  expiredEvidence.trace[0].age_h = 24.5;
  assert.equal(homeValidation.validateFusionArtifact(expiredEvidence, NOW).ok, false);

  const falseCounts = fusionFixture();
  falseCounts.coverage.freshness.fresh = 0;
  assert.equal(homeValidation.validateFusionArtifact(falseCounts, NOW).ok, false);

  const falseActiveCount = fusionFixture();
  falseActiveCount.coverage.fresh_active_before_dedupe = 0;
  assert.equal(homeValidation.validateFusionArtifact(falseActiveCount, NOW).ok, false);
});

test("homepage rejects fusion that can loosen risk or carries an opaque verdict", () => {
  const unsafeVeto = fusionFixture();
  unsafeVeto.vetoes.push({
    active: true,
    effect: "LOOSEN",
    may_loosen_risk: true,
  });
  assert.equal(homeValidation.validateFusionArtifact(unsafeVeto, NOW).ok, false);

  const opaque = fusionFixture();
  opaque.authoritative_verdict = "RISK_ON";
  assert.equal(homeValidation.validateFusionArtifact(opaque, NOW).ok, false);
});
