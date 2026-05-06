
# 1) SEC EDGAR full-text search for SC 13D filings (last 7 days)

- `09:20:30`       ✓ 13D filings last 7d status=200 ct=application/json
- `09:20:30`         keys=['took', 'timed_out', '_shards', 'hits', 'aggregations', 'query']
- `09:20:30`         sample: {"took": 1471, "timed_out": false, "_shards": {"total": 50, "successful": 50, "skipped": 0, "failed": 0}, "hits": {"total": {"value": 0, "relation": "eq"}, "max_score": null, "hits": []}, "aggregations": {"entity_filter": {"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0, "buckets": []}, "

# 2) EDGAR full-text search for SC 13G

- `09:20:30`       ✓ 13G filings last 7d status=200 ct=application/json
- `09:20:30`         keys=['took', 'timed_out', '_shards', 'hits', 'aggregations', 'query']
- `09:20:30`         sample: {"took": 462, "timed_out": false, "_shards": {"total": 50, "successful": 50, "skipped": 0, "failed": 0}, "hits": {"total": {"value": 0, "relation": "eq"}, "max_score": null, "hits": []}, "aggregations": {"entity_filter": {"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0, "buckets": []}, "s

# 3) EDGAR daily index files (alternative — full daily list)

- `09:20:31`       ✓ 13D current RSS feed status=200 ct=application/atom+xml
- `09:20:31`         preview: <?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
<title>Latest Filings - Wed, 06 May 2026 05:20:30 EDT</title>
<link rel="alternate" href="/cgi-bin/browse-edgar?action=getcurrent"/>
<link rel="self" href="/cgi-bin/browse-edgar?action=getcurrent"/>
<id>https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent</id>
<author><name>Webmaster</name><email>webmaster@sec.gov</email></author>
<updated>2026-05-06T05:20:30-04:00</updated>
<entry>
<title>SC 13D/A - GE

# 4) EDGAR Atom feed for 13D/A (amendments)

- `09:20:31`       ✓ 13D/A amendments RSS status=200 ct=application/atom+xml
- `09:20:31`         preview: <?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
<title>Latest Filings - Wed, 06 May 2026 05:20:31 EDT</title>
<link rel="alternate" href="/cgi-bin/browse-edgar?action=getcurrent"/>
<link rel="self" href="/cgi-bin/browse-edgar?action=getcurrent"/>
<id>https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent</id>
<author><name>Webmaster</name><email>webmaster@sec.gov</email></author>
<updated>2026-05-06T05:20:31-04:00</updated>
<entry>
<title>SC 13D/A - GE

# 5) EDGAR Atom feed 13G

- `09:20:31`       ✓ 13G current RSS status=200 ct=application/atom+xml
- `09:20:31`         preview: <?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
<title>Latest Filings - Wed, 06 May 2026 05:20:31 EDT - No recent filings</title>
<link rel="alternate" href="/cgi-bin/browse-edgar?action=getcurrent"/>
<link rel="self" href="/cgi-bin/browse-edgar?action=getcurrent"/>
<id>https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent</id>
<author><name>Webmaster</name><email>webmaster@sec.gov</email></author>
<updated>2026-05-06T05:20:31-04:00</updated>
</feed>


# 6) EDGAR Atom feed Form 4 (insider buys we already track)

- `09:20:31`       ✓ Form 4 baseline status=200 ct=application/atom+xml
- `09:20:31`         preview: <?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
<title>Latest Filings - Wed, 06 May 2026 05:20:31 EDT</title>
<link rel="alternate" href="/cgi-bin/browse-edgar?action=getcurrent"/>
<link rel="self" href="/cgi-bin/browse-edgar?action=getcurrent"/>
<id>https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent</id>
<author><name>Webmaster</name><email>webmaster@sec.gov</email></author>
<updated>2026-05-06T05:20:31-04:00</updated>
<entry>
<title>4 - Next Lion

# 7) EDGAR daily-index directory (raw filings list)

- `09:20:31`       ✓ current quarter index status=200 ct=text/html
- `09:20:31`         preview: <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-type" content="text/html;charset=UTF-8">
<title>Directory listing of full-index/2026/QTR2/</title>
<link rel="stylesheet" type="text/css" href="/css/third-party/reset.min.css">
<link rel="stylesheet" type="text/css" href="/css/third-party/960.min.css">
<link rel="stylesheet" type="text/css" href="/css/basic-1.css">
<!-- [if IE 7] -->
<style type="text/css