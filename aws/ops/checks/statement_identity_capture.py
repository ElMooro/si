"""One durably claimed SEC current-ticker request per accounting refresh."""
import urllib.request, urllib.error
import financial_statement_campaign as campaign
import statement_research_identity as identity
import statement_research_source as source
import statement_research_store_v2 as store
import statement_producer as producer


def capture(client, request_id, clock, fetch=None):
    key = campaign.request_key(request_id, 'sec-identity')
    try:
        prior = source.strict(store.bounded(client.get_object(Bucket=campaign.BUCKET, Key=key)['Body']))
    except Exception as exc:
        if not producer.missing(exc): raise
        prior = None
    if prior:
        if prior.get('status') != 'complete':
            raise ValueError('SEC identity request already attempted; inspect retained state before recovery')
        identity.capture(prior['capture'], store.reader(client, campaign.BUCKET))
        return prior['capture']
    progress = {'request_id': request_id, 'url': identity.URL, 'requested_at': clock(), 'status': 'claimed'}
    campaign.journal(client, key, progress, True)
    try:
        request = urllib.request.Request(identity.URL, headers={'User-Agent': 'JustHodl Research raafouis@gmail.com',
            'Accept': 'application/json', 'Accept-Encoding': 'identity'})
        try:
            response = (fetch or (lambda r: urllib.request.build_opener(campaign.NoRedirect()).open(r, timeout=30)))(request)
        except urllib.error.HTTPError as exc:
            response = exc
        status = response.status
        headers = {k.lower(): v for k, v in response.headers.items()
            if k.lower() in ('date', 'content-type', 'content-length', 'last-modified', 'etag')}
        body = store.bounded(response)
        progress.update(status='response_retained', received_at=clock(), http_status=status, headers=headers,
            original=campaign.retain(client, body))
        campaign.journal(client, key, progress)
        ref = campaign.retain(client, source.encoded(progress))
        identity.capture(ref, store.reader(client, campaign.BUCKET))
        campaign.journal(client, key, {**progress, 'status': 'complete', 'capture': ref})
        return ref
    except Exception as exc:
        campaign.journal(client, key, {**progress, 'status': 'failed', 'error_type': type(exc).__name__})
        raise
