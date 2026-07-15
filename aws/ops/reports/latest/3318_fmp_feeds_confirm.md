## GRADES-NEWS (limit only)

**Status:** failure  
**Duration:** 1.9s  
**Finished:** 2026-07-15T01:50:33+00:00  

## Error

```
SystemExit: 1
```

## Data

| RESULT | grades-latest-news | grades_news | key_suffix | price-target-latest-news | price-target-news |
|---|---|---|---|---|---|
|  |  |  | S8xb |  |  |
|  |  | {'http': 400, 'err': 'Query Error: Invalid or missing query parameter - symbol'} |  |  |  |
|  |  |  |  |  | {'http': 400, 'err': 'Query Error: Invalid or missing query parameter - symbol'} |
|  |  |  |  | {'http': 200, 'n': 100, 'fields': ['symbol', 'publishedDate', 'newsURL', 'newsTitle', 'analystName', 'priceTarget', 'adjPriceTarget', 'priceWhenPosted', 'newsPublisher', 'newsBaseURL', 'analystCompany'], 'sample': {'symbol': 'BA', 'publishedDate': '2026-07-14T23:03:14.000Z', 'newsURL': 'https://thefly.com/ajax/news_get.php?id=4384951', 'newsTitle': 'Boeing initiated with a Neutral at BTG Pactual', 'analystName': '', 'priceTarget': 260, 'adjPriceTarget': 260, 'priceWhenPosted': 217.11, 'newsPublisher': 'TheFly', 'newsBaseURL': 'thefly.com', 'analystCompany': 'BTG Pactual'}} |  |
|  | {'http': 200, 'n': 100, 'fields': ['symbol', 'publishedDate', 'newsURL', 'newsTitle', 'newsBaseURL', 'newsPublisher', 'newGrade', 'previousGrade', 'gradingCompany', 'action', 'priceWhenPosted'], 'sample': {'symbol': 'ALL', 'publishedDate': '2026-07-15T00:30:27.000Z', 'newsURL': 'https://thefly.com/ajax/news_get.php?id=4384959', 'newsTitle': 'Allstate downgraded to Neutral from Buy at UBS', 'newsBaseURL': 'thefly.com', 'newsPublisher': 'TheFly', 'newGrade': 'Neutral', 'previousGrade': 'Buy', 'gradingCompany': 'UBS', 'action': 'downgrade', 'priceWhenPosted': 250.35}} |  |  |  |  |
| FAIL |  |  |  |  |  |

## Log
## PT-NEWS VARIANTS

## VERDICT

- `01:50:33` ✗ grades-news still empty
