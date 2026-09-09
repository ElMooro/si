"""Capital-constrained daily marked replay of an immutable *research* ledger.

This adapter executes supplied historical events; it does not manufacture fills,
marks, corporate actions, fees, or a strategy from completed signal outcomes.
USD cash equities/ETFs, flat starting book, raw marks, explicit session calendar.
Unsupported assets/actions/currencies fail closed. No owner broker data may enter
this public research output. Independent model validation remains a publication gate.
"""
import hashlib
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from donor_contract import parse_timestamp

SCHEMA='research-capital-ledger-1.0'
REQUIRED=('ledger_id','book_id','account_id','generated_at','initial','instruments','calendar','sessions',
          'policies','decisions','fills','cash_events','actions','financing','reconciliations','coverage')


class LedgerBlocked(ValueError):
    pass


def require(condition, message):
    if not condition:raise LedgerBlocked(message)


def number(value, label, minimum=None, positive=False):
    require(not isinstance(value,bool), label+': boolean is not a number')
    try:v=Decimal(str(value))
    except (InvalidOperation,ValueError,TypeError):raise LedgerBlocked(label+': finite number required')
    require(v.is_finite(),label+': finite number required')
    if minimum is not None:require(v>=Decimal(str(minimum)),label+': below minimum')
    if positive:require(v>0,label+': must be positive')
    return v


def stamp(value,label):
    t=parse_timestamp(value)
    require(t is not None and isinstance(value,str) and len(value)>10,label+': explicit UTC timestamp required')
    return t


def digest(data):
    return hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def verified_payload(record,label,before=None):
    require(isinstance(record,dict),label+': payload descriptor missing')
    require(record.get('source_record_id'),label+': source record identity missing')
    require(isinstance(record.get('payload'),dict),label+': immutable payload missing')
    require(record.get('sha256')==digest(record['payload']),label+': payload checksum mismatch')
    available=stamp(record.get('available_at'),label+' available_at')
    if before is not None:require(available<=before,label+': unavailable at decision')
    observed=stamp(record.get('observed_at'),label+' observed_at')
    require(observed<=available,label+': observation after availability')
    return record['payload']


def order_snapshot(record,point,before,instruments):
    data=verified_payload(record,'complete open order snapshot',before)
    require(stamp(record['observed_at'],'order snapshot observed')==point,'order snapshot must match decision/closing instant')
    require(isinstance(data.get('orders'),list),'complete open order array required')
    orders={}
    for row in data['orders']:
        oid=row.get('order_id');require(oid and oid not in orders,'order id missing/duplicate')
        require(row.get('symbol') in instruments and row.get('side') in ('BUY','SELL'),'order instrument/side invalid')
        item=dict(row);item['remaining_quantity']=number(row.get('remaining_quantity'),'remaining order quantity',minimum=0)
        item['limit_price']=number(row.get('limit_price'),'order limit price',positive=True)
        require(isinstance(row.get('reduce_only',False),bool),'reduce_only must be boolean')
        if item['remaining_quantity']>0:orders[oid]=item
    return orders


def order_reserves(orders,holdings):
    reserved={};cash_required=Decimal(0);reduce_totals={}
    for order in orders.values():
        symbol=order['symbol'];qty=order['remaining_quantity'];exposure=qty*order['limit_price']
        if order.get('reduce_only'):
            position=holdings.get(symbol,Decimal(0))
            require((position>0 and order['side']=='SELL') or (position<0 and order['side']=='BUY'),'reduce-only order would increase/reverse position')
            reduce_totals[symbol]=reduce_totals.get(symbol,Decimal(0))+qty
            require(reduce_totals[symbol]<=abs(position),'aggregate reduce-only orders exceed position quantity')
        else:reserved[symbol]=reserved.get(symbol,Decimal(0))+exposure
        if order['side']=='BUY':cash_required+=exposure
    return reserved,cash_required


def enforce_capital(values,cash,nav,orders,holdings,limits):
    require(nav>0,'nonpositive marked equity')
    reserved,buy_cash=order_reserves(orders,holdings);reserve_total=sum(reserved.values(),Decimal(0))
    gross=sum((abs(v) for v in values.values()),Decimal(0));net=sum(values.values(),Decimal(0))
    require((gross+reserve_total)/nav<=number(limits['gross_limit'],'gross limit'),'overlapping positions/orders breach gross capital limit')
    require((abs(net)+reserve_total)/nav<=number(limits['absolute_net_limit'],'net limit'),'net capital limit exceeded')
    require(all((abs(values.get(n,Decimal(0)))+reserved.get(n,Decimal(0)))/nav<=number(limits['name_limit'],'name limit') for n in set(values)|set(reserved)),'name concentration limit exceeded')
    require(cash-buy_cash>=number(limits['minimum_cash'],'minimum cash'),'cash funding constraint including open buy orders exceeded')


def blocked(reason):
    return {'schema_version':SCHEMA,'adapter_implemented':True,'status':'BLOCKED','reason':reason,
            'curve_semantics':'daily_marked_capital_ledger','publication_eligible':False,
            'nav_curve':[],'daily_returns':[], 'supported_scope':'USD cash equity/ETF research book starting flat',
            'required_inputs':list(REQUIRED)}


def replay_research_ledger(doc,now=None):
    """All-or-nothing: any incomplete session or capital violation invalidates replay."""
    try:
        result=_replay(doc,now or datetime.now(timezone.utc))
        json.dumps(result,allow_nan=False)
        return result
    except (LedgerBlocked,KeyError,TypeError,ValueError,AttributeError,IndexError,ArithmeticError) as exc:
        return blocked(str(exc)[:300])


def _replay(doc,now):
    require(isinstance(doc,dict),'immutable research ledger unavailable')
    require(doc.get('schema_version')==SCHEMA,'ledger schema version mismatch')
    require(doc.get('book_type')=='SIMULATED_RESEARCH_BOOK','public backtest only accepts simulated research books; owner broker ledger rejected')
    require(doc.get('currency')=='USD','FX conversion adapter unavailable; USD book required')
    require(doc.get('accounting_basis')=='TRADE_DATE_CASH' and doc.get('account_type')=='RESEARCH_MARGIN_ACCOUNT','trade-date margin research account required; settlement cash account adapter unavailable')
    for key in REQUIRED:require(key in doc,key+': required ledger component missing')
    require(all(isinstance(doc.get(k),str) and doc[k] for k in ('ledger_id','book_id','account_id')),'ledger/book/account identity required')
    require(stamp(doc['generated_at'],'generated_at')<=now,'future ledger publication')
    for key in ('fills','cash_events','actions','financing','decisions','sessions','policies','reconciliations'):
        require(isinstance(doc[key],list),key+': array required')
    coverage=doc['coverage']
    require(isinstance(coverage,dict) and all(coverage.get(k) is True for k in ('fills','cash','corporate_actions','marks','financing','reconciliations','orders')),
            'all event families require complete source coverage, including explicit zero-event coverage')
    instruments=doc['instruments'];require(isinstance(instruments,dict),'instrument master unavailable')
    for symbol,ins in instruments.items():
        require(isinstance(ins,dict) and ins.get('asset_class') in ('EQUITY','ETF') and ins.get('currency')=='USD',symbol+': unsupported instrument/currency')
        require(number(ins.get('multiplier'),symbol+' multiplier')==1,symbol+': derivative multipliers unsupported')
    sessions=doc['sessions'];require(bool(sessions),'session calendar empty')
    calendar=verified_payload(doc['calendar'],'calendar',stamp(sessions[0]['open_at'],'first open'))
    require(calendar.get('exchange')=='XNYS','explicit XNYS research session calendar required')
    require(calendar.get('session_dates')==[s.get('date') for s in sessions],'supplied sessions must exactly match the immutable exchange calendar')
    previous=None; session_by_date={}
    for s in sessions:
        opened=stamp(s['open_at'],'session open');closed=stamp(s['close_at'],'session close');valuation=stamp(s['valuation_at'],'session valuation')
        require(opened<closed<=valuation<=now,'session ordering or future valuation invalid')
        require(s['date']==opened.date().isoformat()==closed.date().isoformat(),'session date mismatch')
        require(opened.weekday()<5 and (previous is None or previous<opened),'session calendar unsorted/duplicate/weekend')
        require(s['date'] not in session_by_date,'duplicate session')
        session_by_date[s['date']]=s;previous=valuation
    initial=doc['initial'];cash=number(initial['cash'],'initial cash',positive=True);liabilities=number(initial['liabilities'],'initial liabilities',minimum=0)
    require(initial.get('positions')=={} and number(initial.get('receivables'),'initial receivables')==0,'adapter requires a reconciled flat starting book')
    require(stamp(initial['as_of'],'initial as_of')<=stamp(sessions[0]['open_at'],'first open'),'initial book after first session')
    require(initial.get('source_snapshot_id'),'initial reconciliation source snapshot missing')
    require(stamp(initial['available_at'],'initial availability')<=stamp(sessions[0]['open_at'],'first open'),'initial capital not available before replay')
    initial_nav=cash-liabilities;require(initial_nav>0,'initial NAV must be positive')
    require(abs(number(initial['equity_nav'],'initial equity')-initial_nav)<=Decimal('.01'),'initial equity does not reconcile')
    policies={}
    for policy in doc['policies']:
        pid=policy.get('policy_id');require(pid and pid not in policies,'policy id missing/duplicate')
        config=verified_payload(policy,'policy '+pid)
        start=stamp(policy['evaluation_start_at'],'evaluation start');end=stamp(policy['evaluation_end_at'],'evaluation end')
        trained=stamp(policy['training_ended_at'],'training end');labels=stamp(policy['training_label_cutoff_at'],'training label cutoff')
        embargo=number(policy.get('embargo_hours'),'embargo hours',minimum=0)
        require(labels<=trained and trained+timedelta(hours=float(embargo))<=start<=end,'training labels/embargo overlap evaluation')
        require(stamp(policy['available_at'],'policy availability')<=start,'policy became available after evaluation started')
        require(policy.get('fold_id') and config.get('strategy_id'),'fold and immutable strategy configuration required')
        limits=config.get('constraints') or {}
        for field in ('gross_limit','absolute_net_limit','name_limit','participation_limit'):
            number(limits.get(field),'constraint '+field,positive=True)
        require(number(limits['participation_limit'],'participation')<=1,'participation exceeds 100%')
        number(limits.get('minimum_cash'),'minimum_cash')
        number(limits.get('max_risk_mark_age_seconds'),'risk mark age',positive=True)
        number(limits.get('max_liquidity_age_hours'),'liquidity age',positive=True)
        policies[pid]=(policy,config,start,end)
    require(bool(policies),'predeclared out-of-sample policies missing')
    for s in sessions:
        require(any(start<=stamp(s['open_at'],'open') and stamp(s['close_at'],'close')<=end for _,_,start,end in policies.values()),'session outside all declared evaluation folds')
    decisions={}
    for decision in doc['decisions']:
        did=decision.get('decision_id');require(did and did not in decisions,'decision id missing/duplicate')
        at=stamp(decision['decided_at'],'decision timestamp');pid=decision.get('policy_id')
        require(pid in policies,'decision policy unavailable')
        policy,config,start,end=policies[pid]
        require(start<=at<=end and stamp(policy['available_at'],'policy availability')<=at,'decision outside policy availability/evaluation')
        require(bool(decision.get('inputs')),'decision feature provenance required')
        for feature in decision['inputs']:verified_payload(feature,'decision feature',at)
        require(isinstance(decision.get('risk_marks'),dict),'decision risk marks required')
        order_snapshot(decision.get('open_orders'),at,at,instruments)
        decisions[did]=decision
    finance={};recons={}
    for name,dest in (('financing',finance),('reconciliations',recons)):
        for row in doc[name]:
            day=row.get('session');require(day in session_by_date and day not in dest,name+': duplicate/unknown session')
            require(row.get('source_record_id'),name+': source record missing')
            require(stamp(row['available_at'],name+' availability')<=now,name+': future publication')
            dest[day]=row
        require(set(dest)==set(session_by_date),name+': every calendar session must be covered')
    events=[];ids=set();ordering=set()
    for family,timefield,idfield in (('fills','executed_at','fill_id'),('cash_events','at','event_id'),('actions','effective_at','action_id')):
        for event in doc[family]:
            eid=event.get(idfield);require(eid and eid not in ids,'event id missing/duplicate');ids.add(eid)
            require(event.get('source_record_id'),family+': source record id missing')
            at=stamp(event[timefield],family+' event time');sequence=event.get('sequence')
            require(isinstance(sequence,int) and not isinstance(sequence,bool),'explicit event sequence required')
            require((at,sequence) not in ordering,'ambiguous same-instant event ordering');ordering.add((at,sequence))
            session=next((s for s in sessions if stamp(s['open_at'],'open')<=at<=stamp(s['close_at'],'close')),None)
            require(session is not None,'event outside supplied exchange sessions')
            published=stamp(event['available_at'],'event availability')
            require(published<=now,'event record not yet available')
            if family!='actions':require(published>=at,'execution/cash confirmation cannot predate the event')
            events.append((at,sequence,family,event,session['date']))
    for decision in decisions.values():
        at=stamp(decision['decided_at'],'decision time');sequence=decision.get('sequence')
        require(isinstance(sequence,int) and not isinstance(sequence,bool),'decision event sequence required')
        require((at,sequence) not in ordering,'ambiguous decision/event ordering');ordering.add((at,sequence))
        session=next((s for s in sessions if stamp(s['open_at'],'open')<=at<=stamp(s['close_at'],'close')),None)
        require(session is not None,'decision outside supplied exchange sessions')
        events.append((at,sequence,'decisions',decision,session['date']))
    events.sort(key=lambda e:(e[0],e[1]))
    holdings={};open_orders={};entitlements=[];curve=[];returns=[];previous_nav=initial_nav;wealth=Decimal(1);peak=Decimal(1);maxdd=Decimal(0);fills_processed=0
    for session in sessions:
        day=session['date'];opened=stamp(session['open_at'],'open');closed=stamp(session['close_at'],'close');valued=stamp(session['valuation_at'],'valuation')
        flows=Decimal(0);subperiod_factor=Decimal(1);subperiod_start=previous_nav;short_seen={sym for sym,qty in holdings.items() if qty<0}
        def settle_dividends(at):
            nonlocal cash
            for entitlement in entitlements:
                if entitlement['pay_at']<=at and not entitlement['paid']:
                    cash+=entitlement['amount'];entitlement['paid']=True
        def receivables():return sum((e['amount'] for e in entitlements if not e['paid']),Decimal(0))
        def decision_prices(decision,names,at):
            _,config,_,_=policies[decision['policy_id']];limits=config['constraints'];prices={}
            decided=stamp(decision['decided_at'],'decision timestamp')
            for name in names:
                mark=decision['risk_marks'].get(name)
                data=verified_payload(mark,'risk mark '+name,decided)
                require(data.get('basis')=='UNADJUSTED','risk marks must be raw to avoid double-counting actions')
                require((at-stamp(mark['observed_at'],'risk mark observed')).total_seconds()<=float(number(limits['max_risk_mark_age_seconds'],'risk mark age')),'stale decision risk mark')
                prices[name]=number(data['price'],'risk mark price',positive=True)
            return prices
        settle_dividends(opened)
        for at,sequence,family,event,event_day in events:
            if event_day!=day:continue
            settle_dividends(at)
            if family=='decisions':
                open_orders=order_snapshot(event['open_orders'],at,at,instruments)
                prices=decision_prices(event,{n for n,q in holdings.items() if q},at)
                values={n:q*prices[n] for n,q in holdings.items() if q}
                nav=cash-liabilities+receivables()+sum(values.values(),Decimal(0))
                enforce_capital(values,cash,nav,open_orders,holdings,policies[event['policy_id']][1]['constraints'])
            elif family=='cash_events':
                kind=event.get('kind');amount=number(event['amount'],'cash amount')
                require(kind in ('EXTERNAL_FLOW','FEE'),'unsupported cash event kind')
                if kind=='EXTERNAL_FLOW':
                    require(at==opened,'external flows supported only at session open')
                    require(all(e[1]<sequence for e in events if e[2]=='actions' and e[4]==day),'external flows must follow opening corporate actions')
                    opening=session.get('opening_marks');require(isinstance(opening,dict),'external flows require actual opening valuation marks')
                    opening_values={}
                    for name,position in holdings.items():
                        if not position:continue
                        mark=opening.get(name);data=verified_payload(mark,'flow-time opening mark '+name,valued)
                        require(stamp(mark['observed_at'],'opening mark observed')==opened and data.get('basis')=='UNADJUSTED','flow-time valuation must use actual raw opening marks')
                        opening_values[name]=position*number(data['price'],'opening mark price',positive=True)
                    before_flow=cash-liabilities+receivables()+sum(opening_values.values(),Decimal(0))
                    require(before_flow>0 and subperiod_start>0 and before_flow+amount>0,'external flow exhausts capital')
                    subperiod_factor*=before_flow/subperiod_start
                    subperiod_start=before_flow+amount
                    flows+=amount
                else:require(amount<=0,'fee cash event must be a debit')
                cash+=amount
            elif family=='actions':
                sym=event.get('symbol');require(sym in instruments,'corporate action instrument missing')
                require(at==opened,'corporate actions must be effective at session open')
                require(stamp(event['available_at'],'action availability')<=at,'corporate action not known by effective session')
                kind=event.get('kind')
                if kind=='SPLIT':holdings[sym]=holdings.get(sym,Decimal(0))*number(event['ratio'],'split ratio',positive=True)
                elif kind=='DIVIDEND':
                    pay=stamp(event['pay_at'],'dividend payment')
                    require(pay>=at,'dividend pays before ex-date')
                    entitlement={'pay_at':pay,'amount':holdings.get(sym,Decimal(0))*number(event['cash_per_share'],'dividend per share',minimum=0),'paid':False}
                    if pay<=opened:cash+=entitlement['amount'];entitlement['paid']=True
                    entitlements.append(entitlement)
                else:raise LedgerBlocked('unsupported corporate action requires explicit adapter: '+str(kind))
            else:
                sym=event.get('symbol');require(sym in instruments,'fill instrument missing')
                decision=decisions.get(event.get('decision_id'));require(decision is not None,'fill decision missing')
                decided=stamp(decision['decided_at'],'decided_at');require(opened<=decided<=at,'fill before decision or decision from another session')
                policy,config,_,_=policies[decision['policy_id']];limits=config['constraints']
                qty=number(event['quantity'],'fill quantity');require(qty!=0,'zero fill quantity')
                price=number(event['price'],'fill price',positive=True);fees=number(event['fees'],'fill fees',minimum=0)
                risk_prices=decision_prices(decision,{n for n,q in holdings.items() if q}|{sym},at)
                oid=event.get('order_id');require(oid in open_orders,'fill missing live order or order already fully consumed')
                order=open_orders[oid]
                require(order['symbol']==sym and order['side']==('BUY' if qty>0 else 'SELL'),'fill order symbol/side mismatch')
                require(abs(qty)<=order['remaining_quantity'],'cumulative fills exceed remaining order quantity')
                require((qty>0 and price<=order['limit_price']) or (qty<0 and price>=order['limit_price']),'fill worse than accepted limit price')
                if order.get('reduce_only'):
                    prior_qty=holdings.get(sym,Decimal(0))
                    require(prior_qty*qty<0 and abs(qty)<=abs(prior_qty),'reduce-only fill increases/reverses position')
                order['remaining_quantity']-=abs(qty)
                if order['remaining_quantity']==0:del open_orders[oid]
                volume=verified_payload(event.get('liquidity'),'execution liquidity',decided)
                require(volume.get('basis')=='TRAILING_DOLLAR_ADV','execution capacity needs trailing dollar ADV')
                adv=number(volume['value'],'dollar ADV',positive=True)
                require((decided-stamp(event['liquidity']['observed_at'],'ADV observation')).total_seconds()<=float(number(limits['max_liquidity_age_hours'],'liquidity age'))*3600,'stale execution liquidity estimate')
                # Participation is cumulative across all desk fills in the same name/session.
                traded=sum((abs(number(e[3]['quantity'],'quantity'))*number(e[3]['price'],'price') for e in events if e[2]=='fills' and e[4]==day and e[3].get('symbol')==sym and (e[0],e[1])<=(at,sequence)),Decimal(0))
                require(traded<=adv*number(limits['participation_limit'],'participation'),'cumulative name participation cap exceeded')
                newqty=holdings.get(sym,Decimal(0))+qty
                if newqty<0:
                    locate=verified_payload(event.get('locate'),'short locate',decided)
                    require(number(locate['shares'],'locate shares',minimum=0)>=abs(newqty) and stamp(locate['expires_at'],'locate expiration')>=at,'short exceeds available locate')
                    short_seen.add(sym)
                cash-=qty*price+fees;holdings[sym]=newqty;risk_prices[sym]=price
                values={n:q*risk_prices[n] for n,q in holdings.items() if q}
                nav=cash-liabilities+receivables()+sum(values.values(),Decimal(0))
                enforce_capital(values,cash,nav,open_orders,holdings,limits)
                fills_processed+=1
        settle_dividends(closed)
        costs=finance[day];borrow=costs.get('borrow_fees');require(isinstance(borrow,dict),'daily explicit borrow fees required')
        require(short_seen.issubset(borrow),'missing borrowing charge coverage for short exposure')
        borrow_total=sum((number(v,'borrow fee',minimum=0) for v in borrow.values()),Decimal(0))
        cash+=number(costs['cash_interest'],'cash interest')-number(costs['margin_interest'],'margin interest',minimum=0)-borrow_total
        marks=session.get('marks');require(isinstance(marks,dict),'session marks unavailable')
        values={}
        for sym,qty in holdings.items():
            if not qty:continue
            mark=marks.get(sym);payload=verified_payload(mark,'daily mark '+sym,valued)
            require(stamp(mark['observed_at'],'daily mark observed')==closed,'mark is not from aligned actual closing session')
            require(payload.get('basis')=='UNADJUSTED','daily marks must be raw to avoid double-counting actions')
            values[sym]=qty*number(payload['price'],'daily price',positive=True)
        nav=cash-liabilities+receivables()+sum(values.values(),Decimal(0));require(nav>0,'nonpositive closing equity')
        # Independent closing order provenance covers cancellations/replacements,
        # including sessions with no fills. A missing snapshot is never an empty book.
        open_orders=order_snapshot(session.get('open_orders'),closed,valued,instruments)
        active=[p for p in policies.values() if p[2]<=opened and closed<=p[3]]
        for _,cfg,_,_ in active:
            enforce_capital(values,cash,nav,open_orders,holdings,cfg['constraints'])
        recon=recons[day]
        for field,value in (('cash',cash),('liabilities',liabilities),('receivables',receivables()),('equity_nav',nav)):
            require(abs(number(recon[field],'reconciliation '+field)-value)<=Decimal('.01'),'independent reconciliation mismatch: '+day+' '+field)
        rq=recon.get('positions');require(isinstance(rq,dict),'independent quantity reconciliation missing')
        require({k:number(v,'reconciled quantity') for k,v in rq.items() if number(v,'quantity')!=0}=={k:v for k,v in holdings.items() if v!=0},'independent positions reconciliation mismatch')
        require(subperiod_start>0,'invalid subperiod capital')
        ret=subperiod_factor*(nav/subperiod_start)-1;wealth*=1+ret;peak=max(peak,wealth);maxdd=min(maxdd,wealth/peak-1)
        curve.append({'date':day,'equity_nav':float(nav),'cash':float(cash),'liabilities':float(liabilities),'receivables':float(receivables()),
                      'gross_exposure':float(sum((abs(v) for v in values.values()),Decimal(0))),'net_exposure':float(sum(values.values(),Decimal(0))),
                      'external_flow':float(flows),'reconciled':True,'wealth_index':float(wealth)})
        returns.append({'date':day,'return':float(ret),'basis':'FLOW_TIME_SUBPERIOD_TWR'})
        previous_nav=nav
    return {'schema_version':SCHEMA,'adapter_implemented':True,'status':'READY','book_type':'SIMULATED_RESEARCH_BOOK',
            'curve_semantics':'daily_marked_capital_ledger','publication_eligible':False,
            'publication_reason':'Independent model review, provenance certification and sufficiently long out-of-sample sample required',
            'nav_curve':curve,'daily_returns':returns,'n_sessions':len(curve),'n_fills':fills_processed,
            'initial_nav':float(initial_nav),'final_nav':float(previous_nav),'time_weighted_return_pct':float((wealth-1)*100),
            'max_drawdown_pct':float(maxdd*100),'annualized_return_pct':float((wealth**(Decimal(252)/len(curve))-1)*100) if len(curve)>=252 else None,
            'out_of_sample_folds':sorted({p[0]['fold_id'] for p in policies.values()}),
            'scope':'USD equity/ETF simulated margin research book, trade-date cash accounting; supplied immutable fills, open-order reservations and raw daily marks; settlement cash/liquidity and owner broker accounts are unsupported',
            'reconciliation':'Every exchange session reconciled to independent cash, liabilities, receivables, quantity and equity snapshots'}
