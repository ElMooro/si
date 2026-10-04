#!/usr/bin/env python3
"""Route dark economics that are the published transform, or the same math on stored inputs."""
import hashlib, json, pathlib, re, sys

ADD = [
    'ECONOMICS:AEBOT|worldbank:NE.RSB.GNFS.CD:AE',
    'ECONOMICS:AFBOT|worldbank:NE.RSB.GNFS.CD:AF',
    'ECONOMICS:AFGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:AF',
    'ECONOMICS:ALBOT|calc:minus:worldbank:NE.EXP.GNFS.CD:AL~worldbank:NE.IMP.GNFS.CD:AL',
    'ECONOMICS:AMBOT|calc:minus:worldbank:NE.EXP.GNFS.CD:AM~worldbank:NE.IMP.GNFS.CD:AM',
    'ECONOMICS:AMGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:AM',
    'ECONOMICS:AOBOT|worldbank:NE.RSB.GNFS.CD:AO',
    'ECONOMICS:ARBOT|worldbank:NE.RSB.GNFS.CD:AR',
    'ECONOMICS:ARGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:AR',
    'ECONOMICS:AWBOT|worldbank:NE.RSB.GNFS.CD:AW',
    'ECONOMICS:AZBOT|worldbank:NE.RSB.GNFS.CD:AZ',
    'ECONOMICS:AZGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:AZ',
    'ECONOMICS:BABOT|calc:minus:worldbank:NE.EXP.GNFS.CD:BA~worldbank:NE.IMP.GNFS.CD:BA',
    'ECONOMICS:BBGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BB',
    'ECONOMICS:BDBOT|worldbank:NE.RSB.GNFS.CD:BD',
    'ECONOMICS:BDGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BD',
    'ECONOMICS:BFBOT|worldbank:NE.RSB.GNFS.CD:BF',
    'ECONOMICS:BGBOT|worldbank:NE.RSB.GNFS.CD:BG',
    'ECONOMICS:BHBOT|worldbank:NE.RSB.GNFS.CD:BH',
    'ECONOMICS:BHGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BH',
    'ECONOMICS:BIGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BI',
    'ECONOMICS:BJBOT|worldbank:NE.RSB.GNFS.CD:BJ',
    'ECONOMICS:BJGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BJ',
    'ECONOMICS:BMBOT|worldbank:NE.RSB.GNFS.CD:BM',
    'ECONOMICS:BMGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BM',
    'ECONOMICS:BNBOT|worldbank:NE.RSB.GNFS.CD:BN',
    'ECONOMICS:BNGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BN',
    'ECONOMICS:BOBOT|worldbank:NE.RSB.GNFS.CD:BO',
    'ECONOMICS:BOGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BO',
    'ECONOMICS:BRBOT|worldbank:NE.RSB.GNFS.CD:BR',
    'ECONOMICS:BRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BR',
    'ECONOMICS:BSBOT|worldbank:NE.RSB.GNFS.CD:BS',
    'ECONOMICS:BSGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BS',
    'ECONOMICS:BTBOT|worldbank:NE.RSB.GNFS.CD:BT',
    'ECONOMICS:BTGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BT',
    'ECONOMICS:BWBOT|worldbank:NE.RSB.GNFS.CD:BW',
    'ECONOMICS:BWGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BW',
    'ECONOMICS:BYBOT|worldbank:NE.RSB.GNFS.CD:BY',
    'ECONOMICS:BYGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BY',
    'ECONOMICS:BZBOT|worldbank:NE.RSB.GNFS.CD:BZ',
    'ECONOMICS:BZGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:BZ',
    'ECONOMICS:CDBOT|worldbank:NE.RSB.GNFS.CD:CD',
    'ECONOMICS:CDGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CD',
    'ECONOMICS:CFBOT|worldbank:NE.RSB.GNFS.CD:CF',
    'ECONOMICS:CGBOT|worldbank:NE.RSB.GNFS.CD:CG',
    'ECONOMICS:CIBOT|worldbank:NE.RSB.GNFS.CD:CL',
    'ECONOMICS:CIGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CL',
    'ECONOMICS:CMBOT|worldbank:NE.RSB.GNFS.CD:CM',
    'ECONOMICS:CMGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CM',
    'ECONOMICS:CNBOT|worldbank:NE.RSB.GNFS.CD:CN',
    'ECONOMICS:CNGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CN',
    'ECONOMICS:CNIRYY|calc:yoy:fred:CHNCPIALLMINMEI',
    'ECONOMICS:CUBOT|worldbank:NE.RSB.GNFS.CD:CU',
    'ECONOMICS:CUGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CU',
    'ECONOMICS:CVBOT|calc:minus:worldbank:NE.EXP.GNFS.CD:CV~worldbank:NE.IMP.GNFS.CD:CV',
    'ECONOMICS:CVGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CV',
    'ECONOMICS:CYBOT|worldbank:NE.RSB.GNFS.CD:CY',
    'ECONOMICS:DJBOT|worldbank:NE.RSB.GNFS.CD:DJ',
    'ECONOMICS:DJGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:DJ',
    'ECONOMICS:DOBOT|worldbank:NE.RSB.GNFS.CD:DO',
    'ECONOMICS:DZBOT|worldbank:NE.RSB.GNFS.CD:DZ',
    'ECONOMICS:DZGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:DZ',
    'ECONOMICS:ECBOT|worldbank:NE.RSB.GNFS.CD:EC',
    'ECONOMICS:ECGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:EC',
    'ECONOMICS:EGBOT|calc:minus:worldbank:NE.EXP.GNFS.CD:EG~worldbank:NE.IMP.GNFS.CD:EG',
    'ECONOMICS:EGGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:EG',
    'ECONOMICS:ERBOT|worldbank:NE.RSB.GNFS.CD:ER',
    'ECONOMICS:ERGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:ER',
    'ECONOMICS:ETBOT|worldbank:NE.RSB.GNFS.CD:ET',
    'ECONOMICS:ETGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:ET',
    'ECONOMICS:EUBOT|calc:minus:worldbank:NE.EXP.GNFS.CD:EU~worldbank:NE.IMP.GNFS.CD:EU',
    'ECONOMICS:FJBOT|worldbank:NE.RSB.GNFS.CD:FJ',
    'ECONOMICS:FJGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:FJ',
    'ECONOMICS:FOBOT|worldbank:NE.RSB.GNFS.CD:FO',
    'ECONOMICS:GABOT|worldbank:NE.RSB.GNFS.CD:GA',
    'ECONOMICS:GAGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:GA',
    'ECONOMICS:GEBOT|worldbank:NE.RSB.GNFS.CD:DE',
    'ECONOMICS:GEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:DE',
    'ECONOMICS:GHBOT|worldbank:NE.RSB.GNFS.CD:GH',
    'ECONOMICS:GHGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:GH',
    'ECONOMICS:GMBOT|worldbank:NE.RSB.GNFS.CD:GM',
    'ECONOMICS:GMGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:GM',
    'ECONOMICS:GNBOT|worldbank:NE.RSB.GNFS.CD:GN',
    'ECONOMICS:GNGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:GN',
    'ECONOMICS:GQBOT|worldbank:NE.RSB.GNFS.CD:GQ',
    'ECONOMICS:GTBOT|calc:minus:worldbank:NE.EXP.GNFS.CD:GT~worldbank:NE.IMP.GNFS.CD:GT',
    'ECONOMICS:GTGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:GT',
    'ECONOMICS:GWBOT|worldbank:NE.RSB.GNFS.CD:GW',
    'ECONOMICS:GYBOT|worldbank:NE.RSB.GNFS.CD:GY',
    'ECONOMICS:GYGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:GY',
    'ECONOMICS:HKBOT|worldbank:NE.RSB.GNFS.CD:HK',
    'ECONOMICS:HKGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:HK',
    'ECONOMICS:HNBOT|worldbank:NE.RSB.GNFS.CD:HN',
    'ECONOMICS:HNGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:HN',
    'ECONOMICS:HRBOT|worldbank:NE.RSB.GNFS.CD:HR',
    'ECONOMICS:HTBOT|worldbank:NE.RSB.GNFS.CD:HT',
    'ECONOMICS:HTGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:HT',
    'ECONOMICS:IDBOT|worldbank:NE.RSB.GNFS.CD:ID',
    'ECONOMICS:IDGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:ID',
    'ECONOMICS:INBOT|worldbank:NE.RSB.GNFS.CD:IN',
    'ECONOMICS:INGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:IN',
    'ECONOMICS:IQBOT|worldbank:NE.RSB.GNFS.CD:IQ',
    'ECONOMICS:IQGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:IQ',
    'ECONOMICS:IRBOT|worldbank:NE.RSB.GNFS.CD:IR',
    'ECONOMICS:IRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:IR',
    'ECONOMICS:JMGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:JM',
    'ECONOMICS:JOBOT|worldbank:NE.RSB.GNFS.CD:JO',
    'ECONOMICS:JOGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:JO',
    'ECONOMICS:KEBOT|worldbank:NE.RSB.GNFS.CD:KE',
    'ECONOMICS:KEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:KE',
    'ECONOMICS:KGBOT|worldbank:NE.RSB.GNFS.CD:KG',
    'ECONOMICS:KGGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:KG',
    'ECONOMICS:KHBOT|worldbank:NE.RSB.GNFS.CD:KH',
    'ECONOMICS:KHGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:KH',
    'ECONOMICS:KMBOT|worldbank:NE.RSB.GNFS.CD:KM',
    'ECONOMICS:KMGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:KM',
    'ECONOMICS:KWBOT|worldbank:NE.RSB.GNFS.CD:KW',
    'ECONOMICS:KWGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:KW',
    'ECONOMICS:KYGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:KY',
    'ECONOMICS:KZBOT|worldbank:NE.RSB.GNFS.CD:KZ',
    'ECONOMICS:KZGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:KZ',
    'ECONOMICS:LABOT|worldbank:NE.RSB.GNFS.CD:LA',
    'ECONOMICS:LAGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:LA',
    'ECONOMICS:LBBOT|worldbank:NE.RSB.GNFS.CD:LB',
    'ECONOMICS:LBGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:LB',
    'ECONOMICS:LKBOT|worldbank:NE.RSB.GNFS.CD:LK',
    'ECONOMICS:LKGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:LK',
    'ECONOMICS:LRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:LR',
    'ECONOMICS:LSBOT|worldbank:NE.RSB.GNFS.CD:LS',
    'ECONOMICS:LSGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:LS',
    'ECONOMICS:LYBOT|worldbank:NE.RSB.GNFS.CD:LY',
    'ECONOMICS:LYGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:LY',
    'ECONOMICS:MABOT|worldbank:NE.RSB.GNFS.CD:MA',
    'ECONOMICS:MAGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MA',
    'ECONOMICS:MAIRYY|worldbank:FP.CPI.TOTL.ZG:MA',
    'ECONOMICS:MCGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MC',
    'ECONOMICS:MDBOT|worldbank:NE.RSB.GNFS.CD:MD',
    'ECONOMICS:MDGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MD',
    'ECONOMICS:MEBOT|worldbank:NE.RSB.GNFS.CD:ME',
    'ECONOMICS:MGBOT|worldbank:NE.RSB.GNFS.CD:MG',
    'ECONOMICS:MGGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MG',
    'ECONOMICS:MKBOT|worldbank:NE.RSB.GNFS.CD:MK',
    'ECONOMICS:MLBOT|worldbank:NE.RSB.GNFS.CD:ML',
    'ECONOMICS:MLGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:ML',
    'ECONOMICS:MMGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MM',
    'ECONOMICS:MNBOT|worldbank:NE.RSB.GNFS.CD:MN',
    'ECONOMICS:MNGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MN',
    'ECONOMICS:MOGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MO',
    'ECONOMICS:MRBOT|worldbank:NE.RSB.GNFS.CD:MR',
    'ECONOMICS:MRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MR',
    'ECONOMICS:MTBOT|worldbank:NE.RSB.GNFS.CD:MT',
    'ECONOMICS:MUBOT|worldbank:NE.RSB.GNFS.CD:MU',
    'ECONOMICS:MUGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MU',
    'ECONOMICS:MVBOT|worldbank:NE.RSB.GNFS.CD:MV',
    'ECONOMICS:MVGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MV',
    'ECONOMICS:MWBOT|worldbank:NE.RSB.GNFS.CD:MW',
    'ECONOMICS:MWGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MW',
    'ECONOMICS:MYBOT|worldbank:NE.RSB.GNFS.CD:MY',
    'ECONOMICS:MYGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MY',
    'ECONOMICS:MZBOT|worldbank:NE.RSB.GNFS.CD:MZ',
    'ECONOMICS:MZGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:MZ',
    'ECONOMICS:NABOT|worldbank:NE.RSB.GNFS.CD:NA',
    'ECONOMICS:NAGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:NA',
    'ECONOMICS:NCBOT|worldbank:NE.RSB.GNFS.CD:NC',
    'ECONOMICS:NEBOT|worldbank:NE.RSB.GNFS.CD:NE',
    'ECONOMICS:NEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:NE',
    'ECONOMICS:NGGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:NG',
    'ECONOMICS:NIBOT|worldbank:NE.RSB.GNFS.CD:NI',
    'ECONOMICS:NIGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:NI',
    'ECONOMICS:NPBOT|worldbank:NE.RSB.GNFS.CD:NP',
    'ECONOMICS:NPGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:NP',
    'ECONOMICS:OMBOT|worldbank:NE.RSB.GNFS.CD:OM',
    'ECONOMICS:OMGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:OM',
    'ECONOMICS:PABOT|calc:minus:worldbank:NE.EXP.GNFS.CD:PA~worldbank:NE.IMP.GNFS.CD:PA',
    'ECONOMICS:PAGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:PA',
    'ECONOMICS:PEBOT|worldbank:NE.RSB.GNFS.CD:PE',
    'ECONOMICS:PEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:PE',
    'ECONOMICS:PGBOT|worldbank:NE.RSB.GNFS.CD:PG',
    'ECONOMICS:PHBOT|worldbank:NE.RSB.GNFS.CD:PH',
    'ECONOMICS:PHGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:PH',
    'ECONOMICS:PKBOT|worldbank:NE.RSB.GNFS.CD:PK',
    'ECONOMICS:PKGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:PK',
    'ECONOMICS:PRBOT|worldbank:NE.RSB.GNFS.CD:PR',
    'ECONOMICS:PRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:PR',
    'ECONOMICS:PSBOT|worldbank:NE.RSB.GNFS.CD:PS',
    'ECONOMICS:PSGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:PS',
    'ECONOMICS:PYBOT|worldbank:NE.RSB.GNFS.CD:PY',
    'ECONOMICS:PYGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:PY',
    'ECONOMICS:QABOT|calc:minus:worldbank:NE.EXP.GNFS.CD:QA~worldbank:NE.IMP.GNFS.CD:QA',
    'ECONOMICS:QAGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:QA',
    'ECONOMICS:ROBOT|worldbank:NE.RSB.GNFS.CD:RO',
    'ECONOMICS:RSBOT|worldbank:NE.RSB.GNFS.CD:RS',
    'ECONOMICS:RUBOT|worldbank:NE.RSB.GNFS.CD:RU',
    'ECONOMICS:RUGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:RU',
    'ECONOMICS:RWBOT|worldbank:NE.RSB.GNFS.CD:RW',
    'ECONOMICS:RWGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:RW',
    'ECONOMICS:SABOT|worldbank:NE.RSB.GNFS.CD:SA',
    'ECONOMICS:SCBOT|worldbank:NE.RSB.GNFS.CD:SC',
    'ECONOMICS:SCGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SC',
    'ECONOMICS:SDBOT|worldbank:NE.RSB.GNFS.CD:SD',
    'ECONOMICS:SDGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SD',
    'ECONOMICS:SGGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SG',
    'ECONOMICS:SLBOT|worldbank:NE.RSB.GNFS.CD:SL',
    'ECONOMICS:SNBOT|worldbank:NE.RSB.GNFS.CD:SN',
    'ECONOMICS:SNGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SN',
    'ECONOMICS:SOBOT|worldbank:NE.RSB.GNFS.CD:SO',
    'ECONOMICS:SOGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SO',
    'ECONOMICS:SRGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SR',
    'ECONOMICS:SSBOT|worldbank:NE.RSB.GNFS.CD:SS',
    'ECONOMICS:STBOT|worldbank:NE.RSB.GNFS.CD:ST',
    'ECONOMICS:SVBOT|worldbank:NE.RSB.GNFS.CD:SV',
    'ECONOMICS:SVGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SV',
    'ECONOMICS:SYBOT|worldbank:NE.RSB.GNFS.CD:SY',
    'ECONOMICS:SYGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:SY',
    'ECONOMICS:SZBOT|worldbank:NE.RSB.GNFS.CD:CH',
    'ECONOMICS:SZGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:CH',
    'ECONOMICS:TDBOT|worldbank:NE.RSB.GNFS.CD:TD',
    'ECONOMICS:TDGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:TD',
    'ECONOMICS:TGBOT|worldbank:NE.RSB.GNFS.CD:TG',
    'ECONOMICS:TGGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:TG',
    'ECONOMICS:THBOT|worldbank:NE.RSB.GNFS.CD:TH',
    'ECONOMICS:THGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:TH',
    'ECONOMICS:TJBOT|worldbank:NE.RSB.GNFS.CD:TJ',
    'ECONOMICS:TJGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:TJ',
    'ECONOMICS:TLBOT|worldbank:NE.RSB.GNFS.CD:TL',
    'ECONOMICS:TLGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:TL',
    'ECONOMICS:TMBOT|worldbank:NE.RSB.GNFS.CD:TM',
    'ECONOMICS:TNBOT|worldbank:NE.RSB.GNFS.CD:TN',
    'ECONOMICS:TNGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:TN',
    'ECONOMICS:TZBOT|worldbank:NE.RSB.GNFS.CD:TZ',
    'ECONOMICS:TZGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:TZ',
    'ECONOMICS:UABOT|worldbank:NE.RSB.GNFS.CD:UA',
    'ECONOMICS:UAGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:UA',
    'ECONOMICS:UGBOT|worldbank:NE.RSB.GNFS.CD:UG',
    'ECONOMICS:UGGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:UG',
    'ECONOMICS:USBOT|worldbank:NE.RSB.GNFS.CD:US',
    'ECONOMICS:USIPMM|calc:mom:fred:INDPRO',
    'ECONOMICS:USPPIYY|calc:yoy:fred:PPIACO',
    'ECONOMICS:UYBOT|worldbank:NE.RSB.GNFS.CD:UY',
    'ECONOMICS:UYGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:UY',
    'ECONOMICS:UZBOT|worldbank:NE.RSB.GNFS.CD:UZ',
    'ECONOMICS:UZGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:UZ',
    'ECONOMICS:VEBOT|worldbank:NE.RSB.GNFS.CD:VE',
    'ECONOMICS:VEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:VE',
    'ECONOMICS:VNBOT|worldbank:NE.RSB.GNFS.CD:VN',
    'ECONOMICS:VNGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:VN',
    'ECONOMICS:XKBOT|calc:minus:worldbank:NE.EXP.GNFS.CD:XK~worldbank:NE.IMP.GNFS.CD:XK',
    'ECONOMICS:XKGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:XK',
    'ECONOMICS:YEBOT|worldbank:NE.RSB.GNFS.CD:YE',
    'ECONOMICS:YEGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:YE',
    'ECONOMICS:ZABOT|worldbank:NE.RSB.GNFS.CD:ZA',
    'ECONOMICS:ZAGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:ZA',
    'ECONOMICS:ZMBOT|worldbank:NE.RSB.GNFS.CD:ZM',
    'ECONOMICS:ZMGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:ZM',
    'ECONOMICS:ZWBOT|worldbank:NE.RSB.GNFS.CD:ZW',
    'ECONOMICS:ZWGDPYY|worldbank:NY.GDP.MKTP.KD.ZG:ZW'
]
MARKER = 'ECONOMICS:AEBOT|worldbank:NE.RSB.GNFS.CD:AE'
COUNT_OLD = "assert.equal(providerEntries.length,420);"
COUNT_NEW = 676
WB = re.compile(r"^worldbank:[A-Z0-9.]+:[A-Z0-9]{2,3}$")
CALC = re.compile(r"^calc:(?:yoy|mom):fred:[A-Z0-9]+$|^calc:minus:worldbank:[A-Z0-9.]+:[A-Z0-9]{2,3}~worldbank:[A-Z0-9.]+:[A-Z0-9]{2,3}$")
CAT_FN = '\n  function calcShift(iso, years, months) {\n    var y = +String(iso).slice(0, 4), m = +String(iso).slice(5, 7);\n    var d = String(iso).length >= 10 ? String(iso).slice(8, 10) : "01";\n    if (!y || !m) return "";\n    y -= years; m -= months;\n    while (m < 1) { m += 12; y -= 1; }\n    return y + "-" + (m < 10 ? "0" : "") + m + "-" + d;\n  }\n  function calcMap(obs) {\n    var m = Object.create(null), i, p, v;\n    if (!Array.isArray(obs)) return m;\n    for (i = 0; i < obs.length; i++) {\n      p = obs[i];\n      if (!Array.isArray(p) || typeof p[0] !== "string") continue;\n      v = p[1];\n      if (typeof v !== "number" || !isFinite(v)) continue;\n      m[p[0]] = v;\n    }\n    return m;\n  }\n  async function calcSeries(id) {\n    var raw = String(id || ""), body = raw.slice(5), cut = body.indexOf(":"), kind = cut > 0 ? body.slice(0, cut) : "", rest = cut > 0 ? body.slice(cut + 1) : "";\n    var obs = [], name = "", unit = "Percent", freq = null;\n    if (!global.JHObservationSeries || typeof global.JHObservationSeries.warehouse !== "function" || !global.JHObservationCache) return { d: [], src: "Observation history unavailable: required module not loaded" };\n    if (kind === "minus") {\n      var parts = rest.split("~");\n      if (parts.length !== 2 || !parts[0] || !parts[1]) return { d: [], src: "calc identity rejected" };\n      var ea = await loadWarehouse(parts[0]), eb = await loadWarehouse(parts[1]);\n      var pa = ea && ea.packet, pb = eb && eb.packet;\n      if (!pa || !pb || String(pa.id || "").toLowerCase() !== parts[0].toLowerCase() || String(pb.id || "").toLowerCase() !== parts[1].toLowerCase()) return { d: [], src: "calc inputs unavailable" };\n      if (!/Exports of goods and services/i.test(pa.name || "") || !/Imports of goods and services/i.test(pb.name || "")) return { d: [], src: "calc inputs are not exports and imports" };\n      var mb = calcMap(pb.obs), ma = calcMap(pa.obs);\n      Object.keys(ma).sort().forEach(function (dt) {\n        if (Object.prototype.hasOwnProperty.call(mb, dt)) obs.push([dt, ma[dt] - mb[dt]]);\n      });\n      name = "Exports minus imports of goods and services";\n      unit = "Current US$";\n      freq = pa.freq || null;\n    } else if (kind === "yoy" || kind === "mom") {\n      var got = await loadWarehouse(rest), pkt = got && got.packet;\n      if (!pkt || String(pkt.id || "").toLowerCase() !== rest.toLowerCase() || !Array.isArray(pkt.obs)) return { d: [], src: "calc input unavailable" };\n      if (!/index/i.test(pkt.name || "")) return { d: [], src: "calc input is not an index" };\n      var base = calcMap(pkt.obs);\n      Object.keys(base).sort().forEach(function (dt) {\n        var prev = kind === "mom" ? calcShift(dt, 0, 1) : calcShift(dt, 1, 0);\n        var b = base[prev];\n        if (b === undefined || b === 0) return;\n        obs.push([dt, 100 * (base[dt] / b - 1)]);\n      });\n      name = (kind === "mom" ? "Month-over-month percent of " : "Year-over-year percent of ") + (pkt.name || rest);\n      unit = "Percent";\n      freq = pkt.freq || null;\n    } else return { d: [], src: "calc identity rejected" };\n    if (obs.length < 8) return { d: [], src: "calc produced fewer than 8 observations" };\n    var doc = { id: raw, provider: "calc", provider_name: "Calculated", name: name, unit: unit, freq: freq, source: "calc", obs: obs };\n    var parsed = global.JHObservationSeries.warehouse(doc, raw, PROXY + "/series?id=" + encodeURIComponent(raw));\n    parsed.src += " · calculated from stored observations of the same instrument";\n    return parsed;\n  }\n'

ROOT = pathlib.Path(__file__).resolve()
while ROOT != ROOT.parent and not (ROOT / "jh-chart-tvwatch.js").exists():
    ROOT = ROOT.parent
TV = ROOT / "jh-chart-tvwatch.js"
CAT = ROOT / "jh-chart-catalog.js"
FIX = ROOT / "tests/fixtures/watchlist-correctness/source-transition.json"
BEFORE = ROOT / "tests/fixtures/watchlist-correctness/jh-chart-tvwatch.js.txt"
TEST = ROOT / "tests/watchlist-identity.test.js"
ECON = ROOT / "tests/fixtures/watchlist-correctness/economic-qualifications.json"

def sha(b):
    return hashlib.sha256(b).hexdigest()

def reverse(text, edits):
    for e in reversed(edits):
        got = text[e["start"]:e["end"]]
        if got != e["after"]:
            sys.exit("reverse mismatch at %s" % e["start"])
        text = text[:e["start"]] + e["before"] + text[e["end"]:]
    return text

def ok_target(t):
    return WB.fullmatch(t) is not None or CALC.fullmatch(t) is not None

def sync_routes():
    econ = json.loads(ECON.read_text())
    routes = econ["routes"]
    changed = False
    for line in ADD:
        sym, _, rest = line.partition("|")
        if sym in routes and routes[sym] != rest:
            sys.exit("econ conflict " + sym)
        if routes.get(sym) != rest:
            routes[sym] = rest
            changed = True
    if changed:
        if len(econ.get("records") or []) != 36:
            sys.exit("econ records moved")
        ECON.write_text(json.dumps(econ, indent=2) + "\n")
    return changed

def patch_catalog(text):
    if "function calcSeries(" in text and "calc:1" in text:
        return text
    old = "var SERIES_PROV = { fred:1, nyfed:1,"
    new = "var SERIES_PROV = { fred:1, calc:1, nyfed:1,"
    if text.count(old) != 1:
        sys.exit("catalog provider anchor")
    text = text.replace(old, new, 1)
    anchor = "  async function klines(sym) {\n    var s = String(sym || \"\");\n"
    repl = CAT_FN + "  async function klines(sym) {\n    var s = String(sym || \"\");\n    if (/^calc:/i.test(s)) return calcSeries(s);\n"
    if text.count(anchor) != 1:
        sys.exit("catalog klines anchor")
    text = text.replace(anchor, repl, 1)
    if text.count("function calcSeries(") != 1:
        sys.exit("catalog splice")
    return text

text = TV.read_text()
cat = CAT.read_text()
if MARKER in text:
    cat2 = patch_catalog(cat)
    if cat2 != cat:
        CAT.write_text(cat2)
    sync_routes()
    print("already applied")
    sys.exit(0)
fn = text.find("function providerRest(u)")
if fn < 0:
    sys.exit("no providerRest")
key = 'var raw = "'
at = text.find(key, fn)
end = text.find('", lines = raw.split("\\n"), i, p, a, b;', at)
if at < 0 or end < 0:
    sys.exit("raw bounds")
start = at + len(key)
raw = text[start:end]
lines = raw.split("\\n")
if len(lines) != 420:
    sys.exit("line count %s" % len(lines))
if not lines[-1].startswith("ECONOMICS:SKGDG|"):
    sys.exit("tail moved")
seen = set()
for line in lines:
    sym, sep, rest = line.partition("|")
    if sep != "|" or not rest or sym in seen:
        sys.exit("bad existing " + line[:80])
    seen.add(sym)
new_lines = list(lines)
for line in ADD:
    sym, sep, rest = line.partition("|")
    if sep != "|" or sym in seen or not ok_target(rest):
        sys.exit("bad add " + line)
    seen.add(sym)
    new_lines.append(line)
if len(new_lines) != COUNT_NEW:
    sys.exit("new count %s" % len(new_lines))
new_raw = "\\n".join(new_lines)
if "\\n\\n" in new_raw or new_raw.startswith("\\n") or new_raw.endswith("\\n"):
    sys.exit("blank")
new = text[:start] + new_raw + text[end:]
if new.count(MARKER) != 1 or new.count("function providerRest(") != 1:
    sys.exit("splice")
for name in ("function extraChart(", "function fredId(", "function chartSymbol(", "function economicQualification("):
    if text.count(name) != 1 or new.count(name) != 1:
        sys.exit("helper " + name)
cat2 = patch_catalog(cat)
fix = json.loads(FIX.read_text())
entry = fix["jh-chart-tvwatch.js"]
if sha(text.encode()) != entry["after_sha256"]:
    sys.exit("working tree hash != fixture")
edit = {"start": start, "end": start + len(new_raw), "before": raw, "after": new_raw}
edits = list(entry["edits"]) + [edit]
restored = reverse(new, edits)
base = BEFORE.read_bytes()
if restored.encode() != base:
    rb, bb = restored.encode(), base
    n = min(len(rb), len(bb))
    i = next((k for k in range(n) if rb[k] != bb[k]), n)
    sys.exit("preservation reverse failed at %s" % i)
prev = new
scopes = [e.get("scope") for e in edits]
last = max(i for i, s in enumerate(scopes) if s == "watchlist-handoff-coverage-2")
for e in reversed(edits[last + 1:]):
    if prev[e["start"]:e["end"]] != e["after"]:
        sys.exit("post-coverage mismatch at %s" % e["start"])
    prev = prev[:e["start"]] + e["before"] + prev[e["end"]:]
for e in reversed(edits[:last + 1]):
    if e.get("scope") != "watchlist-handoff-coverage-2":
        continue
    if prev[e["start"]:e["end"]] != e["after"]:
        sys.exit("coverage mismatch at %s" % e["start"])
    prev = prev[:e["start"]] + e["before"] + prev[e["end"]:]
test = TEST.read_text()
if test.count(COUNT_OLD) != 1:
    sys.exit("identity count anchor")
test = test.replace(COUNT_OLD, "assert.equal(providerEntries.length,%d);" % COUNT_NEW, 1)
entry["edits"] = edits
entry["after_sha256"] = sha(new.encode())
if entry.get("before_sha256") != "41b1c217e58942d501eda70d07792522f45115cb1a6cad048372683676917e33":
    sys.exit("before hash moved")
TV.write_text(new)
CAT.write_text(cat2)
FIX.write_text(json.dumps(fix, indent=2) + "\n")
TEST.write_text(test)
sync_routes()
print("ok bytes", len(new.encode()), "lines", len(new_lines), "catalog", len(cat2.encode()))
