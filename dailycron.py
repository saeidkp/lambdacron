import boto3
import json
from datetime import datetime, timedelta
import logging
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Daily Lambda function to process 13 QuickSight datasets
    - Modifies each dataset SQL to query only yesterday's data
    - Triggers incremental refresh (fast & cheap)
    - Reverts SQL back to 60-day rolling window
    """
    
    # Initialize AWS clients
    quicksight = boto3.client('quicksight', region_name='us-east-1')
    AWS_ACCOUNT_ID = context.invoked_function_arn.split(':')[4]
    
    # Your 13 datasets - each with its own rolling window
    DATASETS = [
        {
            'dataset_id': 'dataset-01-spread-analysis',
            'name': 'spread_analysis',
            'description': 'Broker spread ranking analysis',
            'rolling_window_days': 30  # 30-day window
        },
        {
            'dataset_id': 'dataset-02-nbbo-positions', 
            'name': 'nbbo_positions',
            'description': 'NBBO position analysis',
            'rolling_window_days': 60  # 60-day window
        },
        {
            'dataset_id': 'dataset-03-quote-quality',
            'name': 'quote_quality', 
            'description': 'Quote quality metrics',
            'rolling_window_days': 90  # 90-day window
        },
        {
            'dataset_id': 'dataset-04-trades-analysis',
            'name': 'trades_analysis',
            'description': 'ETF trades with effective spreads',
            'rolling_window_days': 45  # 45-day window
        },
        {
            'dataset_id': 'dataset-05-nbbo-volume',
            'name': 'nbbo_volume',
            'description': 'NBBO volume by venue',
            'rolling_window_days': 120 # 120-day window
        },
        {
            'dataset_id': 'dataset-06-venue-broker',
            'name': 'venue_broker_analysis',
            'description': 'Venue broker contribution analysis',
            'rolling_window_days': 180 # 180-day window
        },
        {
            'dataset_id': 'dataset-07-market-share',
            'name': 'market_share',
            'description': 'Broker market share analysis',
            'rolling_window_days': 365 # 365-day window (1 year)
        },
        {
            'dataset_id': 'dataset-08-liquidity-metrics',
            'name': 'liquidity_metrics',
            'description': 'ETF liquidity measurements',
            'rolling_window_days': 30  # 30-day window
        },
        {
            'dataset_id': 'dataset-09-time-series',
            'name': 'time_series_analysis', 
            'description': 'Time series market data',
            'rolling_window_days': 252 # 252-day window (trading year)
        },
        {
            'dataset_id': 'dataset-10-cross-venue',
            'name': 'cross_venue_analysis',
            'description': 'Cross venue comparison',
            'rolling_window_days': 90  # 90-day window
        },
        {
            'dataset_id': 'dataset-11-volatility',
            'name': 'volatility_analysis',
            'description': 'Market volatility metrics',
            'rolling_window_days': 60  # 60-day window
        },
        {
            'dataset_id': 'dataset-12-performance',
            'name': 'performance_metrics',
            'description': 'Trading performance analysis',
            'rolling_window_days': 150 # 150-day window
        },
        {
            'dataset_id': 'dataset-13-summary',
            'name': 'daily_summary',
            'description': 'Daily market summary',
            'rolling_window_days': 30  # 30-day window
        }
    ]
    
    try:
        # Calculate yesterday's market date
        target_date = get_previous_business_day()
        yyyymmdd = target_date.strftime('%Y%m%d')
        
        logger.info(f"Starting daily refresh for {len(DATASETS)} datasets")
        logger.info(f"Target market date: {yyyymmdd}")
        logger.info(f"Each dataset has its own rolling window configuration")
        
        # Track results for all datasets
        refresh_results = []
        successful_refreshes = 0
        failed_refreshes = 0
        
        # Process each dataset
        for idx, dataset_info in enumerate(DATASETS, 1):
            dataset_id = dataset_info['dataset_id']
            dataset_name = dataset_info['name']
            description = dataset_info['description']
            rolling_window_days = dataset_info['rolling_window_days']
            
            logger.info(f"Processing dataset {idx}/{len(DATASETS)}: {dataset_name} ({rolling_window_days}-day window)")
            
            try:
                # Step 1: Modify SQL to query only yesterday's data
                logger.info(f"  → Modifying SQL for single day: {yyyymmdd}")
                original_sql = modify_dataset_sql_for_single_day(
                    quicksight, AWS_ACCOUNT_ID, dataset_id, yyyymmdd
                )
                
                if not original_sql:
                    raise Exception("Failed to modify dataset SQL")
                
                # Step 2: Trigger incremental refresh
                logger.info(f"  → Triggering incremental refresh")
                refresh_result = trigger_incremental_refresh(
                    quicksight, AWS_ACCOUNT_ID, dataset_id, yyyymmdd
                )
                
                # Step 3: Wait a moment for the ingestion to start
                time.sleep(2)
                
                # Step 4: Revert SQL back to its specific rolling window
                logger.info(f"  → Reverting SQL to {rolling_window_days}-day window")
                revert_success = revert_dataset_sql_to_rolling_window(
                    quicksight, AWS_ACCOUNT_ID, dataset_id, rolling_window_days
                )
                
                # Record success
                refresh_results.append({
                    'dataset_id': dataset_id,
                    'dataset_name': dataset_name,
                    'description': description,
                    'target_date': yyyymmdd,
                    'rolling_window_days': rolling_window_days,
                    'ingestion_id': refresh_result['IngestionId'],
                    'sql_modified': True,
                    'sql_reverted': revert_success,
                    'status': 'success',
                    'processing_order': idx
                })
                
                successful_refreshes += 1
                logger.info(f"  ✅ Successfully processed {dataset_name}")
                
            except Exception as e:
                logger.error(f"  ❌ Failed to process {dataset_name}: {str(e)}")
                refresh_results.append({
                    'dataset_id': dataset_id,
                    'dataset_name': dataset_name,
                    'description': description,
                    'rolling_window_days': rolling_window_days,
                    'status': 'failed',
                    'error': str(e),
                    'processing_order': idx
                })
                failed_refreshes += 1
        
        # Generate summary
        summary = {
            'total_datasets': len(DATASETS),
            'successful_refreshes': successful_refreshes,
            'failed_refreshes': failed_refreshes,
            'success_rate': f"{(successful_refreshes/len(DATASETS)*100):.1f}%"
        }
        
        logger.info(f"Refresh Summary: {successful_refreshes}/{len(DATASETS)} successful ({summary['success_rate']})")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Daily incremental refresh completed for {yyyymmdd}',
                'target_date': yyyymmdd,
                'datasets_processed': len(DATASETS),
                'summary': summary,
                'detailed_results': refresh_results
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'target_date': yyyymmdd if 'yyyymmdd' in locals() else 'unknown'
            })
        }

def get_previous_business_day():
    """
    Calculate the previous business day
    Accounts for weekends - if today is Monday, returns Friday
    """
    today = datetime.now()
    
    if today.weekday() == 0:  # Monday
        return today - timedelta(days=3)  # Previous Friday
    elif today.weekday() == 6:  # Sunday  
        return today - timedelta(days=2)  # Previous Friday
    else:
        return today - timedelta(days=1)  # Previous day

def modify_dataset_sql_for_single_day(quicksight_client, account_id, dataset_id, yyyymmdd):
    """
    Modify dataset SQL to query only a specific day
    Returns the original SQL for reference
    """
    try:
        # Get current dataset definition
        response = quicksight_client.describe_data_set(
            AwsAccountId=account_id,
            DataSetId=dataset_id
        )
        
        dataset = response['DataSet']
        physical_table_map = dataset['PhysicalTableMap']
        
        # Find and modify the custom SQL table
        for table_id, table_def in physical_table_map.items():
            if 'CustomSql' in table_def:
                original_sql = table_def['CustomSql']['SqlQuery']
                
                # Replace rolling window filter with single day filter
                modified_sql = replace_rolling_window_with_single_day(original_sql, yyyymmdd)
                
                # Update the dataset
                updated_physical_table_map = {
                    table_id: {
                        'CustomSql': {
                            'DataSourceArn': table_def['CustomSql']['DataSourceArn'],
                            'Name': table_def['CustomSql']['Name'],
                            'SqlQuery': modified_sql,
                            'Columns': table_def['CustomSql']['Columns']
                        }
                    }
                }
                
                # Apply the update
                update_response = quicksight_client.update_data_set(
                    AwsAccountId=account_id,
                    DataSetId=dataset_id,
                    Name=dataset['Name'],
                    PhysicalTableMap=updated_physical_table_map,
                    ImportMode=dataset['ImportMode']
                )
                
                return original_sql
                
        return None
        
    except Exception as e:
        logger.error(f"Failed to modify dataset SQL: {str(e)}")
        return None

def replace_rolling_window_with_single_day(sql, yyyymmdd):
    """
    Replace 60-day rolling window with single day filter
    
    From: yyyymmdd >= CAST(format_datetime(current_date - interval '60' day, 'yyyyMMdd') AS INTEGER)
    To:   yyyymmdd = 20250108
    """
    import re
    
    # Pattern to match 60-day rolling window (and other intervals)
    rolling_patterns = [
        # Standard format_datetime pattern
        r"yyyymmdd\s*>=\s*CAST\(format_datetime\(current_date\s*-\s*interval\s*'\d+'\s*day,\s*'yyyyMMdd'\)\s*AS\s*INTEGER\)",
        # Alternative patterns
        r"yyyymmdd\s*>=\s*\d{8}",
        # Date range patterns
        r"yyyymmdd\s*>=\s*CAST\([^)]+\)",
    ]
    
    single_day_filter = f"yyyymmdd = {yyyymmdd}"
    
    modified_sql = sql
    for pattern in rolling_patterns:
        modified_sql = re.sub(pattern, single_day_filter, modified_sql, flags=re.IGNORECASE)
    
    # If no existing filter found, add one
    if f"yyyymmdd = {yyyymmdd}" not in modified_sql and "yyyymmdd >=" not in modified_sql:
        # Add filter before GROUP BY if it exists
        if 'GROUP BY' in modified_sql.upper():
            modified_sql = re.sub(
                r'(\s+GROUP\s+BY)',
                f'\nWHERE {single_day_filter}\\1',
                modified_sql,
                flags=re.IGNORECASE
            )
        else:
            # Add at the end
            modified_sql = modified_sql.rstrip() + f'\nWHERE {single_day_filter}'
    
    return modified_sql

def revert_dataset_sql_to_rolling_window(quicksight_client, account_id, dataset_id, window_days):
    """
    Revert dataset SQL back to 60-day rolling window
    """
    try:
        # Get current dataset definition
        response = quicksight_client.describe_data_set(
            AwsAccountId=account_id,
            DataSetId=dataset_id
        )
        
        dataset = response['DataSet']
        physical_table_map = dataset['PhysicalTableMap']
        
        # Find and revert the custom SQL table
        for table_id, table_def in physical_table_map.items():
            if 'CustomSql' in table_def:
                current_sql = table_def['CustomSql']['SqlQuery']
                
                # Replace single day filter with rolling window
                reverted_sql = replace_single_day_with_rolling_window(current_sql, window_days)
                
                # Update the dataset
                updated_physical_table_map = {
                    table_id: {
                        'CustomSql': {
                            'DataSourceArn': table_def['CustomSql']['DataSourceArn'],
                            'Name': table_def['CustomSql']['Name'],
                            'SqlQuery': reverted_sql,
                            'Columns': table_def['CustomSql']['Columns']
                        }
                    }
                }
                
                # Apply the update
                update_response = quicksight_client.update_data_set(
                    AwsAccountId=account_id,
                    DataSetId=dataset_id,
                    Name=dataset['Name'],
                    PhysicalTableMap=updated_physical_table_map,
                    ImportMode=dataset['ImportMode']
                )
                
                return True
                
        return False
        
    except Exception as e:
        logger.error(f"Failed to revert dataset SQL: {str(e)}")
        return False

def replace_single_day_with_rolling_window(sql, window_days):
    """
    Replace single day filter with rolling window
    
    From: yyyymmdd = 20250108
    To:   yyyymmdd >= CAST(format_datetime(current_date - interval '60' day, 'yyyyMMdd') AS INTEGER)
    """
    import re
    
    rolling_window_filter = f"yyyymmdd >= CAST(format_datetime(current_date - interval '{window_days}' day, 'yyyyMMdd') AS INTEGER)"
    
    # Replace single day patterns with rolling window
    single_day_patterns = [
        r'yyyymmdd\s*=\s*\d{8}',
        r'yyyymmdd\s*=\s*\'\d{8}\'',
    ]
    
    reverted_sql = sql
    for pattern in single_day_patterns:
        reverted_sql = re.sub(pattern, rolling_window_filter, reverted_sql, flags=re.IGNORECASE)
    
    return reverted_sql

def trigger_incremental_refresh(quicksight_client, account_id, dataset_id, yyyymmdd):
    """
    Trigger incremental refresh for the dataset
    This will be fast because SQL now only queries one day
    """
    ingestion_id = f"daily-{yyyymmdd}-{datetime.now().strftime('%H%M%S')}"
    
    response = quicksight_client.create_ingestion(
        DataSetId=dataset_id,
        IngestionId=ingestion_id,
        AwsAccountId=account_id,
        IngestionType='INCREMENTAL_REFRESH'
    )
    
    return response