import copy,json,unittest
from test_originals import retained,AT,rows
import foreign_original as n,foreign_supplement as s

def fiscal_doc():
    data=[]
    for day,bills in [('2026-06-30','100'),('2026-07-31','125')]:
        for kind in sorted(s.LAYOUTS[-1]):data.append({'record_date':day,'security_type_desc':'Marketable','security_class_desc':kind,'debt_held_public_mil_amt':bills if kind=='Bills' else '0'})
    return {'data':data,'meta':{'count':len(data),'total-count':len(data),'total-pages':1,'dataFormats':{'debt_held_public_mil_amt':'$1,000,000'}},'links':{'next':None}}

def fiscal(doc):
    ref,read=retained(n.encoded(doc),n.MSPD_URL)
    return s.fiscal(ref,read,AT,rows([('2026-07-01','10')]),'2026-07-01')

def auction_row(**changes):
    return {'auctionDate':'2026-09-10T00:00:00','cusip':'912810UX4','securityType':'Note','securityTerm':'10-Year','tips':'No','floatingRate':'No',
        'indirectBidderAccepted':'60','directBidderAccepted':'25','primaryDealerAccepted':'15','competitiveAccepted':'100',
        'totalAccepted':'140','noncompetitiveAccepted':'5','somaAccepted':'35','bidToCoverRatio':'2.1','highYield':'4.3',**changes}

def auctions(data):
    refs={};bodies={}
    for key,url in n.AUCTION_URLS.items():
        ref,read=retained(n.encoded(data if key=='auction_note' else []),url);refs[key]=ref;bodies[ref['evidence']['key']]=read(ref['evidence']['key'])
    return s.auctions(refs,bodies.__getitem__,AT)

class Tests(unittest.TestCase):
    def test_stock_change_is_not_cash_issuance_or_an_absorption_share(self):
        result=fiscal(fiscal_doc());row=result['rows'][-1]
        self.assertEqual(row['stock_change_usd_million_decimal'],'25');self.assertEqual(row['scale_comparison_pct'],40)
        self.assertIsNone(row['absorption_pct']);self.assertIsNone(result['agg_12m']['pct'])
        self.assertIsNone(result['agg_12m']['stock_change_bn'])
        self.assertEqual(len(result['raw_dimensional_rows']),12)

    def test_incomplete_dimensional_layout_is_not_assumed_zero(self):
        doc=fiscal_doc();doc['data']=[r for r in doc['data'] if not(r['record_date']=='2026-07-31' and r['security_class_desc']=='Federal Financing Bank')]
        doc['meta'].update(count=11,**{'total-count':11});result=fiscal(doc)
        self.assertIsNone(result['rows'][-1]['stock_usd_million_decimal']);self.assertEqual(result['incomplete_layout_periods'],1)

    def test_source_dimensions_duplicates_pagination_and_units_fail_closed(self):
        for kind in ('pagination','next','unit','duplicate','security','date','negative'):
            doc=fiscal_doc()
            if kind=='pagination':doc['meta']['total-count']+=1
            elif kind=='next':doc['links']['next']='page2'
            elif kind=='unit':doc['meta']['dataFormats']['debt_held_public_mil_amt']='$1'
            elif kind=='duplicate':doc['data'][-1]=copy.deepcopy(doc['data'][-2])
            elif kind=='security':doc['data'][-1]['security_type_desc']='Nonmarketable'
            elif kind=='date':doc['data'][-1]['record_date']='2026-07-30'
            else:doc['data'][-1]['debt_held_public_mil_amt']='-1'
            with self.subTest(kind=kind),self.assertRaises(ValueError):fiscal(doc)

    def test_auction_share_uses_competitive_not_total_and_remains_a_sample(self):
        result=auctions([auction_row()]);self.assertEqual(result['weighted_indirect_share_pct'],60)
        self.assertEqual(result['recent'][0]['indirect_pct'],60);self.assertFalse(result['complete_auction_universe_verified'])
        self.assertEqual(result['recent'][0]['accepted_usd_decimal']['totalAccepted'],'140')

    def test_auction_inconsistent_amounts_cannot_be_scored(self):
        result=auctions([auction_row(competitiveAccepted='80')]);self.assertIsNone(result['weighted_indirect_share_pct'])
        self.assertEqual(result['eligible_returned_auctions'],0);self.assertIsNone(result['recent'][0]['indirect_pct'])

    def test_auction_request_identity_dates_duplicates_and_flags(self):
        for changes in ({'securityType':'Bill'},{'auctionDate':'2025-01-01'},{'tips':None},{'cusip':'bad'},{'indirectBidderAccepted':'-1'}):
            with self.subTest(changes=changes),self.assertRaises(ValueError):auctions([auction_row(**changes)])
        with self.assertRaises(ValueError):auctions([auction_row(),auction_row()])
        result=auctions([auction_row(tips='Yes')]);self.assertEqual(result['recent'][0]['yield_basis'],'real_percent')

if __name__=='__main__':unittest.main(verbosity=2)
