# Diagnose 5 RED items from system-health dashboard

**Status:** success  
**Duration:** 6.4s  
**Finished:** 2026-05-02T21:27:53+00:00  

## Log
## 1. repo-data.json (20h stale)

- `21:27:48`   last_modified: 2026-05-02T12:01:38+00:00
- `21:27:48`   age_hours: 9.4
- `21:27:48`   size: 43379
- `21:27:48`   top_keys: ['timestamp', 'generated_at', 'stress', 'intelligence', 'data', 'summary']
- `21:27:48`   internal_gen_at: 2026-05-02T12:01:37.460250+00:00
- `21:27:49` 
  justhodl-repo-monitor: 24h=6 inv / 0 err
- `21:27:49`   justhodl-repo-monitor: 72h=52 inv / 0 err
- `21:27:49`   last log event: {'stream': '2026/05/02/[$LATEST]2dcca91164cb49ec845f3ca2ba794dc0', 'timestamp': '2026-05-02T12:01:37.612000+00:00', 'age_hours': 9.4}
- `21:27:49`   EB rules: [{'name': 'justhodl-repo-30min', 'schedule': 'cron(0/30 13-23 ? * MON-FRI *)', 'state': 'ENABLED', 'description': 'Repo monitor 30min market hours'}, {'name': 'justhodl-repo-daily', 'schedule': 'cron(0 12 * * ? *)', 'state': 'ENABLED', 'description': 'Repo daily 7AM ET'}]
## 2+3. justhodl-intelligence + intelligence-report.json

- `21:27:50`   last_modified: 2026-05-02T12:10:50+00:00
- `21:27:50`   age_hours: 9.3
- `21:27:50`   size: 5080
- `21:27:50`   top_keys: ['timestamp', 'generated_at', 'version', 'data_sources', 'headline', 'headline_detail', 'phase', 'phase_color', 'action_required', 'forecast']
- `21:27:51` 
  justhodl-intelligence: 24h=3 inv / 0 err
- `21:27:51`   justhodl-intelligence: 72h=29 inv / 0 err
- `21:27:51`   last log event: {'stream': '2026/05/02/[$LATEST]b9906c9007904b3aa69282f66712e8f5', 'timestamp': '2026-05-02T12:10:49.152000+00:00', 'age_hours': 9.3}
- `21:27:51`   EB rules: [{'name': 'justhodl-intel-daily', 'schedule': 'cron(10 12 * * ? *)', 'state': 'ENABLED', 'description': 'Intelligence daily 7:10AM ET'}, {'name': 'justhodl-intel-hourly', 'schedule': 'cron(5 12-23 ? * MON-FRI *)', 'state': 'ENABLED', 'description': 'Intelligence hourly market hours'}]
## 4. justhodl-nyfed-dealer-survey (0 inv / 24h)

- `21:27:52`   24h: 0 inv / 0 err
- `21:27:52`   72h: 0 inv / 0 err
- `21:27:52`   168h (7d): 2 inv / 0 err
- `21:27:52`   last log event: {'stream': '2026/04/27/[$LATEST]c0e16b9bba714d76bb779e15f25067ba', 'timestamp': '2026-04-27T18:43:45.111000+00:00', 'age_hours': 122.7}
- `21:27:52`   EB rules: [{'name': 'justhodl-nyfed-dealer-survey-weekly', 'schedule': 'rate(7 days)', 'state': 'ENABLED', 'description': 'Trigger justhodl-nyfed-dealer-survey'}]
## 5. justhodl-oecd-cli (0 inv / 24h)

- `21:27:53`   24h: 0 inv / 0 err
- `21:27:53`   72h: 0 inv / 0 err
- `21:27:53`   168h (7d): 3 inv / 0 err
- `21:27:53`   last log event: {'stream': '2026/04/27/[$LATEST]3e1a186213b84b099a132d155fff8435', 'timestamp': '2026-04-27T18:49:04.322000+00:00', 'age_hours': 122.6}
- `21:27:53`   EB rules: [{'name': 'justhodl-oecd-cli-weekly', 'schedule': 'rate(7 days)', 'state': 'ENABLED', 'description': 'Trigger justhodl-oecd-cli'}]
