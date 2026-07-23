# ops 3753 — STB rail CSV shape verification (canary #9)

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-07-23T00:38:29+00:00  

## Data

| buildable | commodity_capable |
|---|---|
| True | none |

## Log
## A — STB-1145 per-railroad weekly files

- `00:38:29` ✅   BNSF HTTP 200 rows=51
- `00:38:29`     header row idx=0 cols=24
- `00:38:29`     cols: ['wide_format_line_number', 'agency_name_abbreviation', 'omb_control_number', 'report_office_abbreviation', 'report_name_full', 'report_name_abbreviation', 'report_collection_frequenc', 'measure_confidentiality', 'railroad_aar_reporting_mar', 'railroad_name', 'report_period_start_date', 'report_period_end_date', 'system_percentage_manifest', 'system_percentage_successf']
- `00:38:29`     row: ['1', 'STB', '2140-0044', 'OE', 'WEEKLY REPORT OF PERFO', 'wkly_report_reciprocal', 'weekly', 'Public', 'BNSF', 'BNSF RAILWAY']
- `00:38:29`     row: ['2', 'STB', '2140-0044', 'OE', 'WEEKLY REPORT OF PERFO', 'wkly_report_reciprocal', 'weekly', 'Public', 'BNSF', 'BNSF RAILWAY']
- `00:38:29`     commodity words in HEADER: []
- `00:38:29`     commodity words in BODY:   []
- `00:38:29` ✅   CPKC HTTP 200 rows=51
- `00:38:29`     header row idx=0 cols=21
- `00:38:29`     cols: ['wide_format_line_number', 'agency_name_abbreviation', 'omb_control_number', 'report_office_abbreviation', 'report_name_full', 'report_name_abbreviation', 'report_collection_frequenc', 'measure_confidentiality', 'railroad_aar_reporting_mar', 'railroad_name', 'report_period_start_date', 'report_period_end_date', 'system_percentage_manifest', 'system_percentage_successf']
- `00:38:29`     row: ['1', 'STB', '2140-0044', 'OE', 'WEEKLY REPORT OF PERFO', 'wkly_report_reciprocal', 'weekly', 'Public', 'CPKC', 'CPKC']
- `00:38:29`     row: ['2', 'STB', '2140-0044', 'OE', 'WEEKLY REPORT OF PERFO', 'wkly_report_reciprocal', 'weekly', 'Public', 'CPKC', 'CPKC']
- `00:38:29`     commodity words in HEADER: []
- `00:38:29`     commodity words in BODY:   []
- `00:38:29` ✅   CSXT HTTP 200 rows=51
- `00:38:29`     header row idx=0 cols=23
- `00:38:29`     cols: ['wide_format_line_number', 'agency_name_abbreviation', 'omb_control_number', 'report_office_abbreviation', 'report_name_full', 'report_name_abbreviation', 'report_collection_frequenc', 'measure_confidentiality', 'railroad_aar_reporting_mar', 'railroad_name', 'report_period_start_date', 'report_period_end_date', 'system_percentage_manifest', 'system_percentage_successf']
- `00:38:29`     row: ['1', 'STB', '2140-0044', 'OE', 'WEEKLY REPORT OF PERFO', 'wkly_report_reciprocal', 'weekly', 'Public', 'CSXT', 'CSX']
- `00:38:29`     row: ['2', 'STB', '2140-0044', 'OE', 'WEEKLY REPORT OF PERFO', 'wkly_report_reciprocal', 'weekly', 'Public', 'CSXT', 'CSX']
- `00:38:29`     commodity words in HEADER: []
- `00:38:29`     commodity words in BODY:   []
## B — STB 49 CFR 1247 cars loaded/terminated

- `00:38:29`   candidate 1247 links: ['https://www.stb.gov/wp-content/uploads/STB_49_CFR_1247_CARS_LOAD_TERM_RRRR_YEAR_YYYYMMDDHHMM.csv', 'https://www.stb.gov/wp-content/uploads/STB_49_CFR_1247_CARS_LOAD_TERM_RRRR_YEAR_YYYYMMDDHHMM.csv']
- `00:38:29` ✅     TB_49_CFR_1247_CARS_LOAD_TERM_RRRR_YEAR_YYYYMMDDHHMM.csv HTTP 200 rows=89
- `00:38:29`       cols: ['report_line_number', 'agency_name_abbreviation', 'omb_control_number', 'report_office_abbreviation', 'report_name_full', 'report_name_abbreviation', 'report_collection_frequenc', 'measure_heading', 'measure_name', 'sub_measure', 'measure_name_analytics', 'measure_confidentiality', 'measure_scale', 'measure_units']
- `00:38:29`       row: ['1', 'STB', '2140-0011', 'OE', 'Annual Report of Cars ', 'stb_54', 'annual', 'FREIGHT CARS LOADED', 'RAILROAD CARS LOADED, ', 'PLAIN 40FT BOX']
- `00:38:29`       row: ['2', 'STB', '2140-0011', 'OE', 'Annual Report of Cars ', 'stb_54', 'annual', 'FREIGHT CARS LOADED', 'RAILROAD CARS LOADED, ', 'PLAIN 50 TO 59FT LESS ']
- `00:38:29` ✅     TB_49_CFR_1247_CARS_LOAD_TERM_RRRR_YEAR_YYYYMMDDHHMM.csv HTTP 200 rows=89
- `00:38:29`       cols: ['report_line_number', 'agency_name_abbreviation', 'omb_control_number', 'report_office_abbreviation', 'report_name_full', 'report_name_abbreviation', 'report_collection_frequenc', 'measure_heading', 'measure_name', 'sub_measure', 'measure_name_analytics', 'measure_confidentiality', 'measure_scale', 'measure_units']
- `00:38:29`       row: ['1', 'STB', '2140-0011', 'OE', 'Annual Report of Cars ', 'stb_54', 'annual', 'FREIGHT CARS LOADED', 'RAILROAD CARS LOADED, ', 'PLAIN 40FT BOX']
- `00:38:29`       row: ['2', 'STB', '2140-0011', 'OE', 'Annual Report of Cars ', 'stb_54', 'annual', 'FREIGHT CARS LOADED', 'RAILROAD CARS LOADED, ', 'PLAIN 50 TO 59FT LESS ']
## VERDICT

- `00:38:29`   railroads with a commodity dimension: []
- `00:38:29` ✅ SHAPE VERIFY COMPLETE
