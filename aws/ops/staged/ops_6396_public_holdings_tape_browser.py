"""One read-only public Chromium check; no cloud clients or application writes."""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import os, shutil, subprocess, sys, tempfile
from urllib.parse import urlsplit
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops'))
from ops_report import report

EXPECTED = '2026-09-30T22:48:28.445590+00:00'
PAGES = ('etf-holdings.html', 'flow-lookthrough.html')


def need(value, label):
    if not value:
        raise RuntimeError(label)


def record_observation(r, observation, **changes):
    r.kv(**{**observation, **changes})


def ready(page, selector, prefix):
    page.wait_for_function("([s,p])=>{const e=document.querySelector(s);return e&&(e.textContent.startsWith(p)||e.textContent.includes('unavailable'));}", arg=[selector,prefix], timeout=45000)
    need(page.locator(selector).inner_text().startswith(prefix), 'public_contract_or_access_unavailable')


def common(page, width):
    need(page.evaluate('innerWidth') == width, 'actual_viewport_differs')
    return page.evaluate('({viewport:innerWidth,document_width:document.documentElement.scrollWidth,overflow:document.documentElement.scrollWidth>innerWidth+1})')


def run_browser(r, env):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        executable = next((shutil.which(n) for n in ('google-chrome','google-chrome-stable','chromium','chromium-browser') if shutil.which(n)), None)
        if not executable:
            proc = subprocess.run([sys.executable,'-m','playwright','install','chromium'], env=env, capture_output=True, timeout=240)
            need(proc.returncode == 0, 'official_browser_install_failed')
        browser = pw.chromium.launch(executable_path=executable, headless=True, chromium_sandbox=True, env=env)
        r.kv(browser_version=browser.version, certificate_validation='normal', sandbox=True, stored_credentials=False)
        try:
            for width in (1440,390):
                for name in (*PAGES,'index.html'):
                    ctx = browser.new_context(viewport={'width':width,'height':900}, ignore_https_errors=False)
                    denied = []; errors = Counter(); requests = Counter(); responses = {}; blocked_writes = []
                    def route_request(route):
                        if route.request.method not in ('GET','HEAD','OPTIONS'):
                            blocked_writes.append(route.request.method);route.abort();return
                        route.continue_()
                    ctx.route('**/*', route_request)
                    page = ctx.new_page();page.set_default_timeout(45000)
                    def response_seen(response):
                        path = urlsplit(response.url).path
                        if path.startswith('/data/') and urlsplit(response.url).hostname == 'justhodl.ai':
                            responses[path] = response
                            if response.status in (401,403):denied.append(response.status)
                    def console_seen(msg):
                        if msg.type=='error':
                            text=msg.text.lower()
                            errors['csp' if 'content security policy' in text or 'content-security-policy' in text else 'other_console_error'] += 1
                    page.on('response',response_seen)
                    page.on('request',lambda req: requests.update([urlsplit(req.url).path]))
                    page.on('console',console_seen)
                    page.on('pageerror',lambda exc: errors.update(['page_exception']))
                    observed = {'page':name,'width':width,'observed_at':datetime.now(timezone.utc).isoformat()}
                    try:
                        response=page.goto('https://justhodl.ai/'+name,wait_until='domcontentloaded',timeout=45000)
                        need(response is not None and response.status==200,'page_http_access_failed')
                        if name in PAGES:
                            page.locator('[data-hd-table] table').wait_for()
                            need(not denied,'public_data_access_denied')
                            root_key='/data/etf-holdings-research.json' if name==PAGES[0] else '/data/flow-lookthrough.json'
                            packet=responses[root_key].json();source=packet.get('source_generated_at',packet['generated_at'])
                            need(source==EXPECTED,'canonical_generation_changed')
                            need(packet['ownership_summary']['status']=='complete','summary_not_complete')
                            summary_key='/'+packet['ownership_summary']['manifest']['key']
                            need(requests[summary_key]==0,'summary_not_lazy')
                            page.locator('[data-own-open]').click();ready(page,'[data-own-status]','Metadata verified')
                            need(page.locator('[data-own-cohort]').input_value()=='','cohort_was_implicitly_selected')
                            manifest=responses[summary_key].json()
                            cohort=next(g for g in manifest['cohorts'] if g['kind']=='current_membership' and g['effective_dates']==['2026-09-29'])
                            need(cohort['eligible_fund_count']==11,'dated_denominator_changed')
                            page.locator('[data-own-cohort]').select_option(cohort['cohort_id'])
                            coverage=page.locator('[data-own-coverage]').inner_text()
                            need('11 eligible / 300 configured' in coverage and '281' in coverage and 'incomplete_returned_snapshot' in coverage,'coverage_labels_missing')
                            need('Fresh lower bounds separately expire' in coverage and 'Eligible ranking expires' in coverage,'independent_deadlines_missing')
                            page.locator('[data-own-coverage] summary').click()
                            need('Source acquired' in page.locator('[data-own-coverage]').inner_text(),'source_clock_labels_missing')
                            page.locator('[data-own-next]').click();ready(page,'[data-own-status]','Verified 200 /')
                            first_key='/'+cohort['parts'][0]['key'];count=requests[first_key]
                            need(count==1,'one_page_request_expected')
                            page.locator('[data-own-view]').select_option('lower');page.locator('[data-own-next]').click();ready(page,'[data-own-status]','Verified 200 /')
                            need(requests[first_key]==count,'verified_page_cache_not_reused')
                            page.locator('[data-own-cancel]').click();need(page.locator('[data-own-result]').inner_text()=='','cancel_did_not_clear')
                            page.locator('[data-own-open]').click();ready(page,'[data-own-status]','Metadata verified')
                            need(page.locator('[data-own-cohort]').input_value()=='','retry_did_not_reset_selection')
                            page.locator('[data-own-cohort]').select_option(cohort['cohort_id']);page.locator('[data-own-view]').select_option('qualified')
                            page.locator('[data-own-next]').click();ready(page,'[data-own-status]','Verified 200 /')
                            need(requests[first_key]==count,'retry_cache_not_reused')
                            need(page.locator('[data-hd-fund]').input_value()=='SPY' and page.locator('[data-hd-table] table').count()==1,'selected_fund_panel_missing')
                            controls=[]
                            for selector in ('[data-own-open]','[data-own-cohort]','[data-own-view]','[data-own-next]','[data-hd-fund]'):
                                el=page.locator(selector);el.scroll_into_view_if_needed();box=el.bounding_box();font=el.evaluate('e=>parseFloat(getComputedStyle(e).fontSize)')
                                need(box and box['width']>0 and box['x']>=-1 and box['x']+box['width']<=width+1 and font>=11,'control_not_readable_in_viewport')
                                controls.append({'selector':selector,'font_px':font,'width_px':round(box['width'],1)})
                            summary_pages={x['key'] for g in manifest['cohorts'] for x in g['parts']}
                            loaded=sum(requests['/'+k]>0 for k in summary_pages);need(loaded==1,'unexpected_summary_page_sweep')
                            observed.update(generated_at=packet['generated_at'],source_generated_at=source,summary_records=manifest['record_count'],summary_pages=manifest['page_count'],economic_date='2026-09-29',eligible=11,configured=300,unique_summary_pages_loaded=loaded,cache_retry_cancel='passed',source_clock_exclusion_labels='passed',selected_fund='SPY',controls=controls)
                        else:
                            page.locator('#jhc-tape [data-sym="SPX"]').wait_for(state='attached')
                            need(not denied,'market_public_data_access_denied')
                            packet=responses['/data/market-tape.json'].json();chips=[]
                            need(packet.get('sizing_eligible') is False,'market_authority_flag_changed')
                            for label in ('SPX','COMP','BTC','GOLD'):
                                chip=page.locator('#jhc-tape [data-sym="'+label+'"]');need(chip.count()==1,'quote_chip_missing')
                                item=next(i for i in packet['items'] if i['label']==label)
                                need(item['quality']['status'] in ('fresh','delayed') and item['observation_date'],'quote_source_clock_invalid')
                                need('Observed:' in chip.get_attribute('title') and chip.locator('.jhc-asof').inner_text(),'quote_clock_not_rendered')
                                chips.append({'label':label,'visible':chip.is_visible(),'rendered':True,'observed_at':item.get('observed_at'),'quality':item['quality']['status']})
                            gap=page.locator('#jhc-tape .jhc-chip').filter(has_text=str(len(packet['gaps']))+' unavailable')
                            need(gap.count()==1 and len(packet['gaps'])>=1,'missing_state_not_rendered')
                            stamp=datetime.fromisoformat(packet['generated_at'].replace('Z','+00:00'));age=(datetime.now(timezone.utc)-stamp).total_seconds();need(-300<=age<=900,'market_packet_stale')
                            observed.update(generated_at=packet['generated_at'],quote_chips=chips,unavailable_count=len(packet['gaps']),gap_labels=[g['label'] for g in packet['gaps']],unavailable_badge_visible=gap.is_visible(),mobile_css_scope='Existing CSS shows only SPX/BTC at <=640px; no presentation change')
                        need(not denied,'public_data_access_denied')
                        layout=common(page,width);need(not layout['overflow'],'document_horizontal_overflow')
                        observed.update(layout=layout,error_categories=dict(errors),blocked_non_read_methods=len(blocked_writes),status='passed')
                        record_observation(r,observed)
                    except Exception as exc:
                        message=str(exc)
                        category='tls_validation_failed' if 'ERR_CERT' in message or 'certificate' in message.lower() else 'public_access_denied' if denied else 'browser_or_assertion_failure'
                        # Never retain browser exception messages/URLs or console bodies.
                        reason=message if isinstance(exc,RuntimeError) and message.replace('_','').isalnum() else type(exc).__name__
                        record_observation(r,observed,status='failed',failure_category=category,reason=reason,error_categories=dict(errors),denied_statuses=denied)
                        raise RuntimeError(category) from None
                    finally:ctx.close()
        finally:browser.close()


def main():
    need(os.environ.get('GITHUB_ACTIONS')=='true','runner_only')
    name=Path(__file__).stem
    with report(name) as r, tempfile.TemporaryDirectory(prefix='jh-public-browser-') as temp:
        env={k:os.environ[k] for k in ('PATH','HOME','LANG','LD_LIBRARY_PATH') if k in os.environ}
        env['PLAYWRIGHT_BROWSERS_PATH']=str(Path(temp)/'browsers')
        deps=Path(temp)/'dependencies';env['PYTHONPATH']=str(deps)
        install=subprocess.run([sys.executable,'-m','pip','--isolated','install','--disable-pip-version-check','--no-input','--index-url','https://pypi.org/simple','--target',str(deps),'playwright==1.51.0'],env=env,capture_output=True,timeout=180)
        need(install.returncode==0,'official_playwright_install_failed');sys.path.insert(0,str(deps))
        r.kv(scope='Public browser read-only; no AWS clients/provider acquisition/app writes',viewports=[1440,390],pages=[*PAGES,'index.html'],consumer_proof='index.html -> jh-nav-drawer.js -> jh-market-tape.js -> #jhc-tape',new_schedules=0)
        try:run_browser(r,env)
        except Exception as exc:raise RuntimeError("browser_probe_stopped_"+type(exc).__name__) from None

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
