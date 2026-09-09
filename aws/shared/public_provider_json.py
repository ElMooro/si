"""Bounded first-party provider JSON reads with fixed, credential-safe errors."""
import json
import urllib.error
import urllib.parse
import urllib.request

HOSTS = {'www.alphavantage.co', 'data.nasdaq.com'}


class ProviderError(RuntimeError):
    def __init__(self, reason, http_status=None):
        super().__init__(reason)
        self.reason, self.http_status = reason, http_status


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):return None


def get_json(url, *, timeout=10, maximum=4_000_000):
    try:
        parsed=urllib.parse.urlsplit(url)
        if parsed.scheme!='https' or parsed.hostname not in HOSTS or parsed.username or parsed.password or parsed.port not in (None,443):
            raise ProviderError('PROVIDER_HOST_REJECTED')
        request=urllib.request.Request(url,headers={'User-Agent':'JustHodl-PublicData/20260909'})
        with urllib.request.build_opener(NoRedirect).open(request,timeout=timeout) as response:
            raw=response.read(maximum+1)
        if len(raw)>maximum:raise ProviderError('PROVIDER_RESPONSE_LIMIT')
        def reject(value):raise ValueError('non_finite')
        data=json.loads(raw,parse_constant=reject)
        json.dumps(data,allow_nan=False)
        if not isinstance(data,dict):raise ProviderError('PROVIDER_SCHEMA_INVALID')
        return data
    except ProviderError:raise
    except urllib.error.HTTPError as exc:
        status=exc.code;exc.close()
        raise ProviderError('PROVIDER_HTTP_ERROR',status) from None
    except Exception:
        raise ProviderError('PROVIDER_REQUEST_OR_JSON_FAILED') from None


def error_metadata(error):
    return {'error':error.reason,**({'http_status':error.http_status} if error.http_status is not None else {})}
