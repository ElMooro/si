"""Lossless, page-bounded option records with shared field definitions.

The public record block avoids repeating each unit/source path hundreds of times.
Its expanded digest binds the exact previously qualified parser output.
"""
from copy import deepcopy
import hashlib,json,re

CONTRACT='option-research-record-block.v1'
MAX_ROWS=250
MAX_BYTES=4*1024*1024
VALUE_KEYS={'state','value','reported_value'}


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(value):return hashlib.sha256(encoded(value)).hexdigest()


def pack(records):
    if not isinstance(records,list) or not 1<=len(records)<=MAX_ROWS:raise ValueError('One nonempty bounded source page required')
    source={k:deepcopy(records[0]['evidence'][k]) for k in ('original','page')}
    if type(source['page']) is not int or source['page']<1:raise ValueError('Original page required')
    common={k:deepcopy(v) for k,v in records[0].items() if k not in ('evidence','metrics')
        and all(k in r and r[k]==v for r in records)}
    definitions={};packed=[];positions=set()
    for record in records:
        if {k:record['evidence'][k] for k in ('original','page')}!=source:raise ValueError('Source pages cannot be mixed')
        index=record['evidence']['row_index']
        if type(index) is not int or not 0<=index<250 or index in positions:raise ValueError('Unique original row index required')
        positions.add(index)
        if record['evidence']!={**source,'row_index':index,'row_pointer':'/results/'+str(index)}:raise ValueError('Original row pointer differs')
        metadata={k:deepcopy(v) for k,v in record.items() if k not in common and k not in ('evidence','metrics')}
        cells={}
        for name,cell in record['metrics'].items():
            template={k:deepcopy(v) for k,v in cell.items() if k not in VALUE_KEYS}
            if name in definitions and definitions[name]!=template:raise ValueError('A field definition changed within one page')
            definitions[name]=template
            if not {'state','value'}<=set(cell):raise ValueError('Explicit metric state and value required')
            values={k:deepcopy(v) for k,v in cell.items() if k in VALUE_KEYS}
            # Default source value equals the valid reported value; encode other cases explicitly.
            if values['state'] in ('reported','reported_zero') and values.get('reported_value')==values['value']:
                del values['reported_value']
            cells[name]=values
        packed.append({'source_row':index,'record':metadata,'cells':cells})
    result={'contract':CONTRACT,'source':source,'common_record':common,'field_definitions':definitions,
        'row_count':len(records),'expanded_sha256':sha(records),'rows':packed}
    if len(encoded(result))>MAX_BYTES:raise ValueError('Public record block exceeds byte bound')
    if unpack(result)!=records:raise ValueError('Record codec did not preserve the qualified records')
    return result


def unpack(block):
    if (not isinstance(block,dict) or block.get('contract')!=CONTRACT
            or set(block)!={'contract','source','common_record','field_definitions','row_count','expanded_sha256','rows'}):
        raise ValueError('Typed record block required')
    rows=block['rows'];common=block['common_record'];definitions=block['field_definitions'];source=block['source']
    if (not isinstance(rows,list) or not 1<=len(rows)<=MAX_ROWS or type(block['row_count']) is not int or block['row_count']!=len(rows)
            or not isinstance(common,dict) or not isinstance(definitions,dict) or not isinstance(source,dict)
            or set(source)!={'original','page'} or type(source['page']) is not int or source['page']<1
            or set(common)&{'evidence','metrics'}):raise ValueError('Record block schema differs')
    if not re.fullmatch('[a-f0-9]{64}',block.get('expanded_sha256','')):raise ValueError('Expanded record digest required')
    if len(encoded(block))>MAX_BYTES:raise ValueError('Public record block exceeds byte bound')
    restored=[];positions=set()
    for row in rows:
        if not isinstance(row,dict) or set(row)!={'source_row','record','cells'}:raise ValueError('Record row schema differs')
        index=row['source_row'];metadata=row['record'];cells=row['cells']
        if type(index) is not int or not 0<=index<250 or index in positions:raise ValueError('Unique original row index required')
        positions.add(index)
        if not isinstance(metadata,dict) or set(metadata)&(set(common)|{'evidence','metrics'}) or not isinstance(cells,dict):
            raise ValueError('Record cannot override its shared definitions')
        metrics={}
        for name,values in cells.items():
            if (name not in definitions or not isinstance(definitions[name],dict) or set(definitions[name])&VALUE_KEYS
                    or not isinstance(values,dict) or not {'state','value'}<=set(values) or set(values)-VALUE_KEYS):
                raise ValueError('Metric cannot override its field definition')
            cell={**deepcopy(definitions[name]),**deepcopy(values)}
            if cell['state'] in ('reported','reported_zero'):
                if 'reported_value' in cell and cell['reported_value']!=cell['value']:raise ValueError('Reported numeric value differs')
                cell['reported_value']=cell['value']
            metrics[name]=cell
        restored.append({**deepcopy(common),**deepcopy(metadata),'metrics':metrics,
            'evidence':{**deepcopy(source),'row_index':index,'row_pointer':'/results/'+str(index)}})
    if sha(restored)!=block['expanded_sha256']:raise ValueError('Expanded records differ from qualified output')
    return restored
