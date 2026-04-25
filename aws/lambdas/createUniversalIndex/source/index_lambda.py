import json
import boto3
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print('Starting comprehensive index creation v3...')
    
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
    }
    
    try:
        # Get the latest all_data file
        bucket = 'macro-data-lake'
        
        # First, find the latest file
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix='comprehensive/2025/05/30/',
            MaxKeys=100
        )
        
        if 'Contents' not in response:
            return {
                'statusCode': 404,
                'headers': headers,
                'body': json.dumps({'error': 'No files found'})
            }
        
        # Find the all_data file with highest timestamp
        all_data_files = [obj for obj in response['Contents'] if 'all_data_' in obj['Key']]
        if not all_data_files:
            return {
                'statusCode': 404,
                'headers': headers,
                'body': json.dumps({'error': 'No all_data file found'})
            }
        
        # Get the latest one
        all_data_file = sorted(all_data_files, key=lambda x: x['Key'], reverse=True)[0]['Key']
        print(f'Using file: {all_data_file}')
        
        # Get the data
        obj = s3.get_object(Bucket=bucket, Key=all_data_file)
        all_data = json.loads(obj['Body'].read())
        
        print(f"Raw indicators_collected: {all_data.get('indicators_collected')}")
        print(f"Sources found: {list(all_data.get('sources', {}).keys())}")
        
        # Create comprehensive index
        indicators = []
        search_terms = set()
        source_counts = {}
        
        # Process ALL sources
        for source, source_data in all_data.get('sources', {}).items():
            print(f"\nProcessing source: {source}")
            source_counts[source] = 0
            
            if not isinstance(source_data, dict):
                print(f"  Skipping {source} - not a dict")
                continue
            
            # Special handling for different structures
            if source == 'yahoo':
                # Yahoo has nested categories
                for category, items in source_data.items():
                    if isinstance(items, dict):
                        print(f"  Yahoo category {category}: {len(items)} items")
                        for symbol, data in items.items():
                            key = f'yahoo.{symbol}'
                            indicators.append({
                                'key': key,
                                'source': 'yahoo',
                                'indicator': symbol,
                                'category': category
                            })
                            # Add multiple search terms
                            search_terms.add(symbol)
                            search_terms.add(symbol.lower())
                            search_terms.add(key)
                            search_terms.add(symbol.replace('^', '').replace('=', '').replace('-', ''))
                            source_counts[source] += 1
            
            elif source == 'nyfed' and 'primary_dealer_excel_links' in source_data:
                # Handle NY Fed special case
                excel_links = source_data.get('primary_dealer_excel_links', [])
                if excel_links:
                    indicators.append({
                        'key': 'nyfed.primary_dealer_data',
                        'source': 'nyfed',
                        'indicator': 'primary_dealer_data',
                        'type': 'excel_links'
                    })
                    source_counts[source] = 1
                # Process other NY Fed data
                for indicator, data in source_data.items():
                    if indicator != 'primary_dealer_excel_links' and data:
                        key = f'{source}.{indicator}'
                        indicators.append({
                            'key': key,
                            'source': source,
                            'indicator': indicator
                        })
                        search_terms.add(indicator)
                        source_counts[source] += 1
            
            else:
                # Regular structure for other sources
                item_count = 0
                for indicator, data in source_data.items():
                    if indicator == 'primary_dealer_excel_links':
                        continue
                    
                    item_count += 1
                    key = f'{source}.{indicator}'
                    indicators.append({
                        'key': key,
                        'source': source,
                        'indicator': indicator
                    })
                    
                    # Add search terms
                    search_terms.add(indicator)
                    search_terms.add(indicator.lower())
                    search_terms.add(key)
                    
                    # Split underscored names
                    if '_' in indicator:
                        parts = indicator.split('_')
                        for part in parts:
                            if len(part) > 2:
                                search_terms.add(part.lower())
                    
                    source_counts[source] += 1
                
                print(f"  Processed {item_count} indicators from {source}")
        
        # Build comprehensive index
        index = {
            'created': datetime.utcnow().isoformat(),
            'dataFile': all_data_file,
            'totalIndicators': len(indicators),
            'reportedIndicators': all_data.get('indicators_collected', 0),
            'totalDataPoints': all_data.get('total_data_points', 0),
            'indicators': indicators,
            'searchTerms': sorted(list(search_terms)),
            'sources': {k: {'count': v} for k, v in source_counts.items()},
            'metadata': {
                'lastUpdated': datetime.utcnow().isoformat(),
                'version': '3.0',
                'comprehensive': True
            }
        }
        
        # Save main index
        s3.put_object(
            Bucket=bucket,
            Key='index/universal_search_index_enhanced.json',
            Body=json.dumps(index, indent=2),
            ContentType='application/json'
        )
        
        # Save compact version
        compact = {
            'indicators': [ind['key'] for ind in indicators],
            'count': len(indicators),
            'sources': source_counts,
            'created': datetime.utcnow().isoformat()
        }
        
        s3.put_object(
            Bucket=bucket,
            Key='index/search_index_compact.json',
            Body=json.dumps(compact),
            ContentType='application/json'
        )
        
        print(f'\nIndex saved with {len(indicators)} indicators')
        print('\nFinal source breakdown:')
        total = 0
        for source, count in source_counts.items():
            print(f'  {source}: {count}')
            total += count
        print(f'  TOTAL: {total}')
        
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'message': 'Comprehensive index created successfully',
                'indicators': len(indicators),
                'searchTerms': len(search_terms),
                'sources': source_counts,
                'totalFromAllSources': total,
                'files': [
                    'index/universal_search_index_enhanced.json',
                    'index/search_index_compact.json'
                ]
            })
        }
        
    except Exception as e:
        print(f'Error: {str(e)}')
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({
                'error': str(e),
                'type': type(e).__name__
            })
        }
