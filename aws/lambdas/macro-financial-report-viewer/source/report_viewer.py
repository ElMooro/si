import json
import boto3
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Simple report viewer that returns the latest report
    """
    try:
        # Get latest report from S3
        response = s3.get_object(
            Bucket='macro-financial-intelligence',
            Key='daily_briefs/latest_ultimate_brief.json'
        )
        
        report_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Format for easy reading
        formatted_report = f"""
# ULTIMATE MACRO FINANCIAL REPORT
Generated: {report_data.get('timestamp', 'N/A')}
Date: {report_data.get('date', 'N/A')}

## EXECUTIVE SUMMARY
- Liquidity Stress Score: {report_data.get('liquidity_stress_score', 'N/A')}/100
- ML Model Accuracy: {report_data.get('ml_accuracy', 0):.1%}
- Auctions Analyzed: {report_data.get('auction_forensics', {}).get('total_analyzed', 0)}

## PRIMARY DEALER FAILS
Status: {report_data.get('pd_fails_watch', {}).get('status', 'unknown')}

## TREASURY AUCTIONS (Recent)
"""
        
        # Add auction details
        auctions = report_data.get('auction_forensics', {}).get('recent_auctions', [])[:10]
        for auction in auctions:
            formatted_report += f"""
### {auction.get('security_type')} - {auction.get('term')} ({auction.get('date')})
- Bid-to-Cover: {auction.get('bid_to_cover', 0):.2f} (avg: {auction.get('avg_btc', 0):.2f})
- High Yield: {auction.get('high_yield', 0):.3f}%
- Stress Score: {auction.get('stress_score', 0):.1f}/100
- Alert Level: {auction.get('alert_level', 'unknown').upper()}
"""
        
        # Add AI synthesis
        formatted_report += f"""
## AI SYNTHESIS
{report_data.get('ai_synthesis', 'Not available')}

## RAW DATA
Full report available at: s3://macro-financial-intelligence/daily_briefs/{report_data.get('date')}_ultimate_brief.json
"""
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/plain',
                'Access-Control-Allow-Origin': '*'
            },
            'body': formatted_report
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': str(e),
                'message': 'Failed to fetch report'
            })
        }
