import boto3
import json
from datetime import datetime, timedelta
import logging
import re

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Simple sequential Lambda function to refresh 13 QuickSight datasets
    - Processes datasets one by one
    - Modifies SQL to single day, triggers refresh, reverts SQL
    - No waiting, no concurrency, no EventBridge
    """
    
    # Get AWS Account ID
    AWS_ACCOUNT_ID = get_account_id(context)
    
    # Initialize QuickSight client
    quicksight = boto3.client('quicksight', region_name='us-east-1')
    
    # Your 13 datasets with their rolling windows
    DATASETS = [
        {'dataset_id': 'dataset-01-spread-analysis', 'name': 'spread_analysis', 'rolling_window_days': 30},
        {'dataset_id': 'dataset-02-nbbo-positions', 'name': 'nbbo_positions', 'rolling_window_days': 60},
        {'dataset_id': 'dataset-03-quote-quality', 'name': 'quote_quality', 'rolling_window_days': 90},
        {'dataset_id': 'dataset-04-trades-analysis', 'name': 'trades_analysis', 'rolling_window_days': 45},
        {'dataset_id': 'dataset-05-nbbo-volume', 'name': 'nbbo_volume', 'rolling_window_days': 120},
        {'dataset_id': 'dataset-06-venue-broker', 'name': 'venue_broker_analysis', 'rolling_window_days': 180},
        {'dataset_id': 'dataset-07-market-share', 'name': 'market_share', 'rolling_window_days': 365},
        {'dataset_id': 'dataset-08-liquidity-metrics', 'name': 'liquidity_metrics', 'rolling_window_days': 30},
        {'dataset_id': 'dataset-09-time-series', 'name': 'time_series_analysis', 'rolling_window_days': 252},
        {'dataset_id': 'dataset-10-cross-venue', 'name': 'cross_venue_analysis', 'rolling_window_days': 90},
        {'dataset_id': 'dataset-11-volatility', 'name': 'volatility_analysis', 'rolling_window_days': 60},
        {'dataset_id': 'dataset-12-performance', 'name': 'performance_metrics', 'rolling_window_days': 150},
        {'dataset_id': 'dataset-13-summary', 'name': 'daily_summary', 'rolling_window_days': 30}
    ]
    
    try:
        # Calculate yesterday's date (any day, including weekends)
        target_date = get_previous_day()
        yyyymmdd = target_date.strftime('%Y%m%d')
        
        logger.info(f"Starting sequential refresh for {len(DATASETS)} datasets")
        logger.info(f"Target market date: {yyyymmdd}")
        
        # Process each dataset sequentially
        results = []
        successful = 0
        failed = 0
        
        for i, dataset_info in enumerate(DATASETS, 1):
            dataset_id = dataset_info['dataset_id']
            dataset_name = dataset_info['name']
            window_days = dataset_info['rolling_window_days']
            
            logger.info(f"[{i}/{len(DATASETS)}] Processing {dataset_name} ({window_days}-day window)")
            
            try:
                # Step 1: Modify SQL to single day
                logger.info(f"  → Modifying SQL to single day: {yyyymmdd}")
                success = modify_dataset_sql_to_single_day(quicksight, AWS_ACCOUNT_ID, dataset_id, yyyymmdd)
                if not success:
                    raise Exception("Failed to modify SQL")
                
                # Step 2: Trigger incremental refresh
                logger.info(f"  → Triggering incremental refresh")
                ingestion_result = trigger_incremental_refresh(quicksight, AWS_ACCOUNT_ID, dataset_id, yyyymmdd)
                ingestion_id = ingestion_result['IngestionId']
                
                # Step 3: Revert SQL back to rolling window
                logger.info(f"  → Reverting SQL to {window_days}-day window")
                revert_success = revert_dataset_sql_to_window(quicksight, AWS_ACCOUNT_ID, dataset_id, window_days)
                
                # Record success
                results.append({
                    'dataset_id': dataset_id,
                    'dataset_name': dataset_name,
                    'target_date': yyyymmdd,
                    'rolling_window_days': window_days,
                    'ingestion_id': ingestion_id,
                    'status': 'success',
                    'order': i
                })
                
                successful += 1
                logger.info(f"  ✅ Successfully processed {dataset_name}")
                
            except Exception as e:
                logger.error(f"  ❌ Failed to process {dataset_name}: {str(e)}")
                results.append({
                    'dataset_id': dataset_id,
                    'dataset_name': dataset_name,
                    'status': 'failed',
                    'error': str(e),
                    'order': i
                })
                failed += 1
        
        # Final summary
        logger.info(f"Sequential processing complete: {successful}/{len(DATASETS)} successful")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Sequential incremental refresh completed',
                'target_date': yyyymmdd,
                'total_datasets': len(DATASETS),
                'successful': successful,
                'failed': failed,
                'success_rate': f"{(successful/len(DATASETS)*100):.1f}%",
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_account_id(context=None):
    """Get AWS Account ID - works in Lambda and locally"""
    try:
        if context and hasattr(context, 'invoked_function_arn'):
            return context.invoked_function_arn.split(':')[4]
        else:
            sts = boto3.client('sts')
            return sts.get_caller_identity()['Account']
    except Exception as e:
        logger.error(f"Failed to get account ID: {str(e)}")
        return None

def get_previous_day():
    """Get previous day - ANY day (not just business days)"""
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    return yesterday

def modify_dataset_sql_to_single_day(quicksight_client, account_id, dataset_id, yyyymmdd):
    """Modify dataset SQL to query only specific day"""
    try:
        # Get dataset
        response = quicksight_client.describe_data_set(
            AwsAccountId=account_id,
            DataSetId=dataset_id
        )
        
        dataset = response['DataSet']
        physical_table_map = dataset['PhysicalTableMap']
        
        # Find custom SQL table and modify it
        for table_id, table_def in physical_table_map.items():
            if 'CustomSql' in table_def:
                original_sql = table_def['CustomSql']['SqlQuery']
                modified_sql = replace_rolling_window_with_single_day(original_sql, yyyymmdd)
                
                # Update dataset
                updated_table_map = {
                    table_id: {
                        'CustomSql': {
                            'DataSourceArn': table_def['CustomSql']['DataSourceArn'],
                            'Name': table_def['CustomSql']['Name'],
                            'SqlQuery': modified_sql,
                            'Columns': table_def['CustomSql']['Columns']
                        }
                    }
                }
                
                quicksight_client.update_data_set(
                    AwsAccountId=account_id,
                    DataSetId=dataset_id,
                    Name=dataset['Name'],
                    PhysicalTableMap=updated_table_map,
                    ImportMode=dataset['ImportMode']
                )
                
                return True
                
        return False
        
    except Exception as e:
        logger.error(f"Failed to modify SQL: {str(e)}")
        return False

def revert_dataset_sql_to_window(quicksight_client, account_id, dataset_id, window_days):
    """Revert dataset SQL back to rolling window"""
    try:
        # Get dataset
        response = quicksight_client.describe_data_set(
            AwsAccountId=account_id,
            DataSetId=dataset_id
        )
        
        dataset = response['DataSet']
        physical_table_map = dataset['PhysicalTableMap']
        
        # Find custom SQL table and revert it
        for table_id, table_def in physical_table_map.items():
            if 'CustomSql' in table_def:
                current_sql = table_def['CustomSql']['SqlQuery']
                reverted_sql = replace_single_day_with_rolling_window(current_sql, window_days)
                
                # Update dataset
                updated_table_map = {
                    table_id: {
                        'CustomSql': {
                            'DataSourceArn': table_def['CustomSql']['DataSourceArn'],
                            'Name': table_def['CustomSql']['Name'],
                            'SqlQuery': reverted_sql,
                            'Columns': table_def['CustomSql']['Columns']
                        }
                    }
                }
                
                quicksight_client.update_data_set(
                    AwsAccountId=account_id,
                    DataSetId=dataset_id,
                    Name=dataset['Name'],
                    PhysicalTableMap=updated_table_map,
                    ImportMode=dataset['ImportMode']
                )
                
                return True
                
        return False
        
    except Exception as e:
        logger.error(f"Failed to revert SQL: {str(e)}")
        return False

def replace_rolling_window_with_single_day(sql, yyyymmdd):
    """
    Replace ALL bv.yyyymmdd rolling window filters with single day
    SIMPLIFIED: Assumes fixed alias 'bv.yyyymmdd' for all date filters
    
    Example SQL:
    WHERE bv.yyyymmdd BETWEEN CAST(format_datetime(current_date - interval '60' day, 'yyyyMMdd') AS INTEGER) 
                          AND CAST(format_datetime(current_date - interval '1' day, 'yyyyMMdd') AS INTEGER)
      AND bv.yyyymmdd BETWEEN CAST(format_datetime(current_date - interval '30' day, 'yyyyMMdd') AS INTEGER) 
                          AND CAST(format_datetime(current_date - interval '1' day, 'yyyyMMdd') AS INTEGER)
    
    Becomes:
    WHERE bv.yyyymmdd = 20250108
      AND bv.yyyymmdd = 20250108
    """
    
    # SIMPLE pattern - only look for bv.yyyymmdd BETWEEN
    between_pattern = r"""
        bv\.yyyymmdd\s+BETWEEN\s+
        CAST\(format_datetime\(current_date\s*-\s*interval\s*'\d+'\s*day,\s*'yyyyMMdd'\)\s*AS\s*INTEGER\)\s+
        AND\s+
        CAST\(format_datetime\(current_date\s*-\s*interval\s*'\d+'\s*day,\s*'yyyyMMdd'\)\s*AS\s*INTEGER\)
    """
    
    # Backup pattern for >= clauses
    ge_pattern = r"bv\.yyyymmdd\s*>=\s*CAST\(format_datetime\(current_date\s*-\s*interval\s*'\d+'\s*day,\s*'yyyyMMdd'\)\s*AS\s*INTEGER\)"
    
    modified_sql = sql
    replacement_count = 0
    
    # Replace BETWEEN patterns - MUCH simpler replacement
    def replace_between(match):
        nonlocal replacement_count
        replacement_count += 1
        logger.info(f"Replacing bv.yyyymmdd BETWEEN filter #{replacement_count}")
        return f"bv.yyyymmdd = {yyyymmdd}"
    
    # Apply BETWEEN pattern replacement
    modified_sql = re.sub(between_pattern, replace_between, modified_sql, flags=re.IGNORECASE | re.VERBOSE)
    
    # Replace any >= patterns
    def replace_ge(match):
        nonlocal replacement_count
        replacement_count += 1
        logger.info(f"Replacing bv.yyyymmdd >= filter #{replacement_count}")
        return f"bv.yyyymmdd = {yyyymmdd}"
    
    modified_sql = re.sub(ge_pattern, replace_ge, modified_sql, flags=re.IGNORECASE)
    
    logger.info(f"Total bv.yyyymmdd filters replaced: {replacement_count}")
    
    return modified_sql

def replace_single_day_with_rolling_window(sql, window_days):
    """
    Revert ALL bv.yyyymmdd single day filters back to BETWEEN rolling windows
    SIMPLIFIED: Only looks for bv.yyyymmdd = pattern
    
    Example:
    WHERE bv.yyyymmdd = 20250108
      AND bv.yyyymmdd = 20250108
    
    Becomes:
    WHERE bv.yyyymmdd BETWEEN CAST(format_datetime(current_date - interval '60' day, 'yyyyMMdd') AS INTEGER) 
                          AND CAST(format_datetime(current_date - interval '1' day, 'yyyyMMdd') AS INTEGER)
      AND bv.yyyymmdd BETWEEN CAST(format_datetime(current_date - interval '60' day, 'yyyyMMdd') AS INTEGER) 
                          AND CAST(format_datetime(current_date - interval '1' day, 'yyyyMMdd') AS INTEGER)
    """
    
    # Create BETWEEN clause for reversion
    between_clause = f"""bv.yyyymmdd BETWEEN CAST(format_datetime(current_date - interval '{window_days}' day, 'yyyyMMdd') AS INTEGER) AND CAST(format_datetime(current_date - interval '1' day, 'yyyyMMdd') AS INTEGER)"""
    
    # SIMPLE pattern - only look for bv.yyyymmdd =
    single_day_pattern = r'bv\.yyyymmdd\s*=\s*\d{8}'
    
    revert_count = 0
    
    def revert_func(match):
        nonlocal revert_count
        revert_count += 1
        logger.info(f"Reverting bv.yyyymmdd filter #{revert_count} to {window_days}-day window")
        return between_clause
    
    # Apply reversion to ALL instances
    reverted_sql = re.sub(single_day_pattern, revert_func, sql, flags=re.IGNORECASE)
    
    logger.info(f"Total bv.yyyymmdd filters reverted: {revert_count}")
    
    return reverted_sql

def trigger_incremental_refresh(quicksight_client, account_id, dataset_id, yyyymmdd):
    """Trigger incremental refresh"""
    ingestion_id = f"daily-{yyyymmdd}-{datetime.now().strftime('%H%M%S')}"
    
    response = quicksight_client.create_ingestion(
        DataSetId=dataset_id,
        IngestionId=ingestion_id,
        AwsAccountId=account_id,
        IngestionType='INCREMENTAL_REFRESH'
    )
    
    return response

# For local testing
if __name__ == "__main__":
    # Test locally
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2))
