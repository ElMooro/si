# Why does parse_infotable return 0 positions for BERKSHIRE?

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-05-03T16:57:06+00:00  

## Log
## 1. Fetch raw XML

- `16:57:06`   size: 55376 chars
- `16:57:06` 
  first 600 chars:
- `16:57:06`     <informationTable xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable"
- `16:57:06`       <infoTable>
- `16:57:06`         <nameOfIssuer>ALLY FINL INC</nameOfIssuer>
- `16:57:06`         <titleOfClass>COM</titleOfClass>
- `16:57:06`         <cusip>02005N100</cusip>
- `16:57:06`         <value>576074081</value>
- `16:57:06`         <shrsOrPrnAmt>
- `16:57:06`           <sshPrnamt>12719675</sshPrnamt>
- `16:57:06`           <sshPrnamtType>SH</sshPrnamtType>
- `16:57:06`         </shrsOrPrnAmt>
- `16:57:06`         <investmentDiscretion>DFND</investmentDiscretion>
- `16:57:06`         <otherManager>4</otherManager>
- `16:57:06`         <votingAuthority>
- `16:57:06`           <Sole>12719675</Sole>
- `16:57:06`           <Shared>0</Shared>
## 2. Strip namespaces (current logic)

- `16:57:06`   cleaned size: 55253 chars
- `16:57:06` 
  first 600 chars after cleaning:
- `16:57:06`     <informationTable>
- `16:57:06`       <infoTable>
- `16:57:06`         <nameOfIssuer>ALLY FINL INC</nameOfIssuer>
- `16:57:06`         <titleOfClass>COM</titleOfClass>
- `16:57:06`         <cusip>02005N100</cusip>
- `16:57:06`         <value>576074081</value>
- `16:57:06`         <shrsOrPrnAmt>
- `16:57:06`           <sshPrnamt>12719675</sshPrnamt>
- `16:57:06`           <sshPrnamtType>SH</sshPrnamtType>
- `16:57:06`         </shrsOrPrnAmt>
- `16:57:06`         <investmentDiscretion>DFND</investmentDiscretion>
- `16:57:06`         <otherManager>4</otherManager>
- `16:57:06`         <votingAuthority>
- `16:57:06`           <Sole>12719675</Sole>
- `16:57:06`           <Shared>0</Shared>
## 3. ElementTree.fromstring + count infoTable elements

- `16:57:06`   root tag: informationTable
- `16:57:06`   root attribs: {}
- `16:57:06`   num direct children: 110
- `16:57:06`   child tags (first 10): ['infoTable', 'infoTable', 'infoTable', 'infoTable', 'infoTable', 'infoTable', 'infoTable', 'infoTable', 'infoTable', 'infoTable']
- `16:57:06` 
  root.iter('infoTable'): 110 matches
- `16:57:06`   tags with 'info' or 'table' substring: ['infoTable', 'informationTable']
- `16:57:06`   all unique tags (first 20): ['None', 'Shared', 'Sole', 'cusip', 'infoTable', 'informationTable', 'investmentDiscretion', 'nameOfIssuer', 'otherManager', 'shrsOrPrnAmt', 'sshPrnamt', 'sshPrnamtType', 'titleOfClass', 'value', 'votingAuthority']
## 4. Alternative — find ALL elements containing <nameOfIssuer>

- `16:57:06`   found 110 <nameOfIssuer> elements
- `16:57:06`   first nameOfIssuer text: 'ALLY FINL INC'
- `16:57:06`   parent of first nameOfIssuer: infoTable
## 5. Try minimal regex extraction as fallback

- `16:57:06`   regex matched 110 names; first 5: ['ALLY FINL INC', 'ALLY FINL INC', 'ALLY FINL INC', 'ALLY FINL INC', 'ALLY FINL INC']
