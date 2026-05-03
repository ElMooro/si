# End-to-end 13F page verification

**Status:** success  
**Duration:** 1.1s  
**Finished:** 2026-05-03T23:12:14+00:00  

## Log
## 1. data/13f-positions.json freshness + completeness

- `23:12:13`   size: 14,608,769 bytes
- `23:12:13`   last_modified: 2026-05-03T22:51:19+00:00
- `23:12:13`   age: 0.3h
- `23:12:14`     ✓ by_fund: 17 entries
- `23:12:14`     ✓ most_bought: 25 entries
- `23:12:14`     ✓ most_sold: 25 entries
- `23:12:14`     ✓ consensus_holds: 30 entries
- `23:12:14`     ✓ rare_picks: 20 entries
## 2. S3 CORS — browser must be able to fetch from S3

- `23:12:14`   found 1 CORS rules
- `23:12:14`     rule: methods=['GET', 'HEAD'] origins=['https://justhodl.ai', 'https://elmooro.github.io', 'http://localhost:*', '*']
- `23:12:14` ✅   ✓ CORS allows browser GET
## 3. Public-read on data/* objects

- `23:12:14`   bucket has policy (length 2815 chars)
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/data/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/screener/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/sentiment/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=['arn:aws:s3:::justhodl-dashboard-live/flow-data.json', 'arn:aws:s3:::justhodl-d
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/regime/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/divergence/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/cot/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/risk/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/opportunities/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/portfolio/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/investor-debate/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/reports/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/archive/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/learning/*
- `23:12:14`     stmt: action=s3:GetObject principal=* resource=arn:aws:s3:::justhodl-dashboard-live/*.json
## 4. Sanity check: actual sample data

- `23:12:14`   Top 3 most-bought:
- `23:12:14`     AAPL     Apple Inc                      +10 buying / -2 selling
- `23:12:14`     AMZN     Amazon.com Inc                 +10 buying / -3 selling
- `23:12:14`     GOOGL    Alphabet Inc Class A           +9 buying / -4 selling
