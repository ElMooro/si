# Phase Z7 — Extend bucket policy for HTML pages

  invoking policy patcher...
  status: 200

## Original policy statements:
  - PublicReadDataDir: arn:aws:s3:::justhodl-dashboard-live/data/*
  - PublicReadScreener: arn:aws:s3:::justhodl-dashboard-live/screener/*
  - PublicReadSentiment: arn:aws:s3:::justhodl-dashboard-live/sentiment/*
  - PublicReadRootDashboardFiles: ['arn:aws:s3:::justhodl-dashboard-live/flow-data.json', 'arn:aws:s3:::justhodl-dashboard-live/crypto-intel.json', 'arn:a
  - PublicReadRegime: arn:aws:s3:::justhodl-dashboard-live/regime/*
  - PublicReadDivergence: arn:aws:s3:::justhodl-dashboard-live/divergence/*
  - PublicReadCOT: arn:aws:s3:::justhodl-dashboard-live/cot/*
  - PublicReadRisk: arn:aws:s3:::justhodl-dashboard-live/risk/*
  - PublicReadOpportunities: arn:aws:s3:::justhodl-dashboard-live/opportunities/*
  - PublicReadPortfolio: arn:aws:s3:::justhodl-dashboard-live/portfolio/*
  - PublicReadInvestorDebate: arn:aws:s3:::justhodl-dashboard-live/investor-debate/*
  - PublicReadReports: arn:aws:s3:::justhodl-dashboard-live/reports/*
  - PublicReadArchive: arn:aws:s3:::justhodl-dashboard-live/archive/*
  - PublicReadLearning: arn:aws:s3:::justhodl-dashboard-live/learning/*
  - PublicReadAllRootJSON: arn:aws:s3:::justhodl-dashboard-live/*.json
  - PublicReadHtmlPages: ['arn:aws:s3:::justhodl-dashboard-live/*.html', 'arn:aws:s3:::justhodl-dashboard-live/intel/*', 'arn:aws:s3:::justhodl-d

## Steps applied:
  ✓ Added PublicReadHtmlPages statement
  ✓ Applied new policy

## Fetch tests after policy update:
  ✅ https://justhodl-dashboard-live.s3.amazonaws.com/intel/index.html — 200 (25382b)
  ✅ https://justhodl-dashboard-live.s3.amazonaws.com/intel.html — 200 (25382b)
  ✅ https://justhodl-dashboard-live.s3.amazonaws.com/index.html — 200 (56228b)