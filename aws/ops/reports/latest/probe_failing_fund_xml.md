# Probe XML structure of failing funds

**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-05-03T17:01:10+00:00  

## Log
## ── AQR ──

- `17:01:08`   fetching infotable.xml (8666571 bytes)…
- `17:01:08`   first 800 chars:
- `17:01:08`     <?xml version="1.0" ?> <informationTable xsi:schemaLocation="http://www.sec.gov/edgar/document/thirteenf/informationtable eis_13FDocument.xsd" xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable" xmlns:n1="http://www.sec.gov/edgar/document/thirteenf/informationtable" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">   <infoTable>     <nameOfIssuer>10X GENOMICS INC</nameOfIssuer>     <titleOfClass>CL A COM</titleOfClass>     <cusip>88025U109</cusip>     <figi>BBG007WX14Y9</figi>     <value>552031</value>     <shrsOrPrnAmt>       <sshPrnamt>34013</sshPrnamt>       <sshPrnamtType>SH</sshPrnamtType>     </shrsOrPrnAmt>     <investmentDiscretion>OTR</investmentDiscretion>     <otherManager>7</otherManager>     <votingAuthority>       <Sole>34013</Sole>  
- `17:01:08` 
  marker check:
- `17:01:08`     has '<infoTable':         16934
- `17:01:08`     has '<ns1:infoTable':     0
- `17:01:08`     has '<ns2:infoTable':     0
- `17:01:08`     has '<n:infoTable':       0
- `17:01:08`     has '<nameOfIssuer':      16934
- `17:01:08`     has 'xmlns=':             1
- `17:01:08`     has 'cusip':              16934
- `17:01:09` 
  after cleaning: '<infoTable' count = 16934
- `17:01:09`   after cleaning first 400:
- `17:01:09`     <?xml version="1.0" ?> <informationTable xsi:schemaLocation="http://www.sec.gov/edgar/document/thirteenf/informationtable eis_13FDocument.xsd">   <infoTable>     <nameOfIssuer>10X GENOMICS INC</nameOfIssuer>     <titleOfClass>CL A COM</titleOfClass>     <cusip>88025U109</cusip>     <figi>BBG007WX14Y9</figi>     <value>552031</value>     <shrsOrPrnAmt>       <sshPrnamt>34013</sshPrnamt>       <sshP
- `17:01:09`   ET parse error: unbound prefix: line 2, column 0
## ── PERSHING ──

- `17:01:09`   fetching infotable.xml (5536 bytes)…
- `17:01:09`   first 800 chars:
- `17:01:09`     <?xml version="1.0" ?> <informationTable xsi:schemaLocation="http://www.sec.gov/edgar/document/thirteenf/informationtable eis_13FDocument.xsd" xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable" xmlns:n1="http://www.sec.gov/edgar/document/thirteenf/informationtable" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">   <infoTable>     <nameOfIssuer>ALPHABET INC</nameOfIssuer>     <titleOfClass>CAP STK CL A</titleOfClass>     <cusip>02079K305</cusip>     <value>212306961</value>     <shrsOrPrnAmt>       <sshPrnamt>678297</sshPrnamt>       <sshPrnamtType>SH</sshPrnamtType>     </shrsOrPrnAmt>     <investmentDiscretion>SOLE</investmentDiscretion>     <votingAuthority>       <Sole>678297</Sole>       <Shared>0</Shared>       <None>0</None>     </votingAu
- `17:01:09` 
  marker check:
- `17:01:09`     has '<infoTable':         11
- `17:01:09`     has '<ns1:infoTable':     0
- `17:01:09`     has '<ns2:infoTable':     0
- `17:01:09`     has '<n:infoTable':       0
- `17:01:09`     has '<nameOfIssuer':      11
- `17:01:09`     has 'xmlns=':             1
- `17:01:09`     has 'cusip':              11
- `17:01:09` 
  after cleaning: '<infoTable' count = 11
- `17:01:09`   after cleaning first 400:
- `17:01:09`     <?xml version="1.0" ?> <informationTable xsi:schemaLocation="http://www.sec.gov/edgar/document/thirteenf/informationtable eis_13FDocument.xsd">   <infoTable>     <nameOfIssuer>ALPHABET INC</nameOfIssuer>     <titleOfClass>CAP STK CL A</titleOfClass>     <cusip>02079K305</cusip>     <value>212306961</value>     <shrsOrPrnAmt>       <sshPrnamt>678297</sshPrnamt>       <sshPrnamtType>SH</sshPrnamtTyp
- `17:01:09`   ET parse error: unbound prefix: line 2, column 0
## ── CITADEL ──

- `17:01:09`   fetching infotable.xml (7738934 bytes)…
- `17:01:09`   first 800 chars:
- `17:01:09`     <?xml version="1.0" ?> <informationTable xsi:schemaLocation="http://www.sec.gov/edgar/document/thirteenf/informationtable eis_13FDocument.xsd" xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable" xmlns:n1="http://www.sec.gov/edgar/document/thirteenf/informationtable" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">   <infoTable>     <nameOfIssuer>1 800 FLOWERS COM INC</nameOfIssuer>     <titleOfClass>CL A</titleOfClass>     <cusip>68243Q106</cusip>     <value>412257</value>     <shrsOrPrnAmt>       <sshPrnamt>104900</sshPrnamt>       <sshPrnamtType>SH</sshPrnamtType>     </shrsOrPrnAmt>     <putCall>Call</putCall>     <investmentDiscretion>DFND</investmentDiscretion>     <otherManager>1</otherManager>     <votingAuthority>       <Sole>104900</Sole>
- `17:01:09` 
  marker check:
- `17:01:09`     has '<infoTable':         15403
- `17:01:09`     has '<ns1:infoTable':     0
- `17:01:09`     has '<ns2:infoTable':     0
- `17:01:09`     has '<n:infoTable':       0
- `17:01:09`     has '<nameOfIssuer':      15403
- `17:01:09`     has 'xmlns=':             1
- `17:01:09`     has 'cusip':              15403
- `17:01:10` 
  after cleaning: '<infoTable' count = 15403
- `17:01:10`   after cleaning first 400:
- `17:01:10`     <?xml version="1.0" ?> <informationTable xsi:schemaLocation="http://www.sec.gov/edgar/document/thirteenf/informationtable eis_13FDocument.xsd">   <infoTable>     <nameOfIssuer>1 800 FLOWERS COM INC</nameOfIssuer>     <titleOfClass>CL A</titleOfClass>     <cusip>68243Q106</cusip>     <value>412257</value>     <shrsOrPrnAmt>       <sshPrnamt>104900</sshPrnamt>       <sshPrnamtType>SH</sshPrnamtType>
- `17:01:10`   ET parse error: unbound prefix: line 2, column 0
## ── SCION ──

- `17:01:10`   fetching infotable.xml (4438 bytes)…
- `17:01:10`   first 800 chars:
- `17:01:10`     <?xml version="1.0" ?> <informationTable xsi:schemaLocation="http://www.sec.gov/edgar/document/thirteenf/informationtable eis_13FDocument.xsd" xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable" xmlns:n1="http://www.sec.gov/edgar/document/thirteenf/informationtable" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">   <infoTable>     <nameOfIssuer>BRUKER CORP</nameOfIssuer>     <titleOfClass>6.375 PREF SER A</titleOfClass>     <cusip>116794207</cusip>     <value>13137181</value>     <shrsOrPrnAmt>       <sshPrnamt>48334</sshPrnamt>       <sshPrnamtType>SH</sshPrnamtType>     </shrsOrPrnAmt>     <investmentDiscretion>DFND</investmentDiscretion>     <otherManager>1,2</otherManager>     <votingAuthority>       <Sole>48334</Sole>       <Shared>0</Shared
- `17:01:10` 
  marker check:
- `17:01:10`     has '<infoTable':         8
- `17:01:10`     has '<ns1:infoTable':     0
- `17:01:10`     has '<ns2:infoTable':     0
- `17:01:10`     has '<n:infoTable':       0
- `17:01:10`     has '<nameOfIssuer':      8
- `17:01:10`     has 'xmlns=':             1
- `17:01:10`     has 'cusip':              8
- `17:01:10` 
  after cleaning: '<infoTable' count = 8
- `17:01:10`   after cleaning first 400:
- `17:01:10`     <?xml version="1.0" ?> <informationTable xsi:schemaLocation="http://www.sec.gov/edgar/document/thirteenf/informationtable eis_13FDocument.xsd">   <infoTable>     <nameOfIssuer>BRUKER CORP</nameOfIssuer>     <titleOfClass>6.375 PREF SER A</titleOfClass>     <cusip>116794207</cusip>     <value>13137181</value>     <shrsOrPrnAmt>       <sshPrnamt>48334</sshPrnamt>       <sshPrnamtType>SH</sshPrnamtTy
- `17:01:10`   ET parse error: unbound prefix: line 2, column 0
