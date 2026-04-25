# Extend bucket policy for all phase data paths

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-04-25T22:21:54+00:00  

## Data

| statements_added | total_statements |
|---|---|
| 10 | 14 |

## Log
## A. Read current bucket policy

- `22:21:54`   Current statements: 4
- `22:21:54`     - PublicReadDataDir
- `22:21:54`     - PublicReadScreener
- `22:21:54`     - PublicReadSentiment
- `22:21:54`     - PublicReadRootDashboardFiles
## B. Extending policy with phase paths

- `22:21:54`   + PublicReadRegime: arn:aws:s3:::justhodl-dashboard-live/regime/*
- `22:21:54`   + PublicReadDivergence: arn:aws:s3:::justhodl-dashboard-live/divergence/*
- `22:21:54`   + PublicReadCOT: arn:aws:s3:::justhodl-dashboard-live/cot/*
- `22:21:54`   + PublicReadRisk: arn:aws:s3:::justhodl-dashboard-live/risk/*
- `22:21:54`   + PublicReadOpportunities: arn:aws:s3:::justhodl-dashboard-live/opportunities/*
- `22:21:54`   + PublicReadPortfolio: arn:aws:s3:::justhodl-dashboard-live/portfolio/*
- `22:21:54`   + PublicReadInvestorDebate: arn:aws:s3:::justhodl-dashboard-live/investor-debate/*
- `22:21:54`   + PublicReadReports: arn:aws:s3:::justhodl-dashboard-live/reports/*
- `22:21:54`   + PublicReadArchive: arn:aws:s3:::justhodl-dashboard-live/archive/*
- `22:21:54`   + PublicReadLearning: arn:aws:s3:::justhodl-dashboard-live/learning/*
- `22:21:54`   + extending PublicReadRootDashboardFiles with intelligence-report.json
- `22:21:54`   + extending PublicReadRootDashboardFiles with edge-data.json
- `22:21:54`   + extending PublicReadRootDashboardFiles with repo-data.json
- `22:21:54`   + extending PublicReadRootDashboardFiles with ai-prediction.json
- `22:21:54`   + extending PublicReadRootDashboardFiles with options-flow.json
- `22:21:54`   + extending PublicReadRootDashboardFiles with valuations.json
- `22:21:54`   + extending PublicReadRootDashboardFiles with morning-brief.json
- `22:21:54` 
  Total statements added: 10
- `22:21:54`   New policy statement count: 14
## C. Apply updated policy

- `22:21:54` ✅   ✅ Policy updated
## D. Verify (re-read policy)

- `22:21:54`   Statements after update: 14
- `22:21:54`     PublicReadDataDir              → data/*
- `22:21:54`     PublicReadScreener             → screener/*
- `22:21:54`     PublicReadSentiment            → sentiment/*
- `22:21:54`     PublicReadRootDashboardFiles   → 11 resources
- `22:21:54`     PublicReadRegime               → regime/*
- `22:21:54`     PublicReadDivergence           → divergence/*
- `22:21:54`     PublicReadCOT                  → cot/*
- `22:21:54`     PublicReadRisk                 → risk/*
- `22:21:54`     PublicReadOpportunities        → opportunities/*
- `22:21:54`     PublicReadPortfolio            → portfolio/*
- `22:21:54`     PublicReadInvestorDebate       → investor-debate/*
- `22:21:54`     PublicReadReports              → reports/*
- `22:21:54`     PublicReadArchive              → archive/*
- `22:21:54`     PublicReadLearning             → learning/*
- `22:21:54` Done — refresh desk-v2.html to verify (may need to clear browser CORS cache)
