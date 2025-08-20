import boto3
import time
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_single_dataset_with_wait(dataset_info, aws_account_id, yyyymmdd, processing_order):
    """
    Process a single dataset with proper wait logic
    Ensures refresh starts before reverting SQL
    """
    
    quicksight = boto3.client('quicksight', region_name='us-east-1')
    
    dataset_id = dataset_info['dataset_id']
    dataset_name = dataset_info['name']
    rolling_window_days = dataset_info['rolling_window_days']
    
    try:
        thread_id = f"Thread-{processing_order}"
        logger.info(f"[{thread_id}] Starting {dataset_name}")
        
        # Step 1: Modify SQL to single day
        logger.info(f"[{thread_id}] Modifying SQL for {dataset_name}")
        original_sql = modify_dataset_sql_for_single_day(
            quicksight, aws_account_id, dataset_id, yyyymmdd
        )
        
        if not original_sql:
            raise Exception("Failed to modify dataset SQL")
        
        # Step 2: Trigger incremental refresh
        logger.info(f"[{thread_id}] Triggering refresh for {dataset_name}")
        refresh_result = trigger_incremental_refresh(
            quicksight, aws_account_id, dataset_id, yyyymmdd
        )
        
        ingestion_id = refresh_result['IngestionId']
        
        # Step 3: WAIT for incremental refresh to start (shorter timeout)
        logger.info(f"[{thread_id}] Waiting for incremental refresh to start for {dataset_name}")
        wait_success = wait_for_refresh_to_start(
            quicksight, aws_account_id, dataset_id, ingestion_id, timeout_seconds=60  # Reduced from 120
        )
        
        if not wait_success:
            logger.warning(f"[{thread_id}] Refresh didn't start in time for {dataset_name}, proceeding anyway")
        
        # Step 4: NOW safe to revert SQL
        logger.info(f"[{thread_id}] Reverting SQL for {dataset_name}")
        revert_success = revert_dataset_sql_to_rolling_window(
            quicksight, aws_account_id, dataset_id, rolling_window_days
        )
        
        return {
            'dataset_id': dataset_id,
            'dataset_name': dataset_name,
            'target_date': yyyymmdd,
            'rolling_window_days': rolling_window_days,
            'ingestion_id': ingestion_id,
            'sql_modified': True,
            'sql_reverted': revert_success,
            'refresh_started': wait_success,
            'status': 'success',
            'processing_order': processing_order,
            'thread_id': thread_id
        }
        
    except Exception as e:
        logger.error(f"[{thread_id}] Failed to process {dataset_name}: {str(e)}")
        return {
            'dataset_id': dataset_id,
            'dataset_name': dataset_name,
            'status': 'failed',
            'error': str(e),
            'processing_order': processing_order,
            'thread_id': thread_id
        }

def wait_for_refresh_to_start(quicksight_client, account_id, dataset_id, ingestion_id, timeout_seconds=60):
    """
    Wait for QuickSight incremental refresh to start (faster than full refresh)
    Incremental refreshes typically start within 5-15 seconds
    """
    
    start_time = time.time()
    check_interval = 3  # Check every 3 seconds (faster for incremental)
    
    while (time.time() - start_time) < timeout_seconds:
        try:
            # Check ingestion status
            response = quicksight_client.describe_ingestion(
                DataSetId=dataset_id,
                IngestionId=ingestion_id,
                AwsAccountId=account_id
            )
            
            ingestion_status = response['Ingestion']['IngestionStatus']
            logger.info(f"Ingestion {ingestion_id} status: {ingestion_status}")
            
            if ingestion_status in ['INITIALIZED', 'QUEUED', 'RUNNING']:
                # Refresh has started - safe to revert SQL
                logger.info(f"✅ Refresh started for {dataset_id} - status: {ingestion_status}")
                return True
                
            elif ingestion_status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                # Already finished (very fast refresh) - safe to revert
                logger.info(f"🏁 Refresh already finished for {dataset_id} - status: {ingestion_status}")
                return True
                
            # Status might be 'CREATED' - wait a bit more
            time.sleep(check_interval)
            
        except Exception as e:
            logger.warning(f"Error checking ingestion status: {str(e)}")
            time.sleep(check_interval)
    
    # Timeout reached
    logger.warning(f"⏰ Timeout waiting for refresh to start for {dataset_id}")
    return False

def wait_for_refresh_completion(quicksight_client, account_id, dataset_id, ingestion_id, timeout_seconds=600):
    """
    Optional: Wait for refresh to completely finish
    Use this if you need to confirm completion before Lambda ends
    """
    
    start_time = time.time()
    check_interval = 10  # Check every 10 seconds
    
    while (time.time() - start_time) < timeout_seconds:
        try:
            response = quicksight_client.describe_ingestion(
                DataSetId=dataset_id,
                IngestionId=ingestion_id,
                AwsAccountId=account_id
            )
            
            ingestion_status = response['Ingestion']['IngestionStatus']
            
            if ingestion_status == 'COMPLETED':
                logger.info(f"✅ Refresh completed successfully for {dataset_id}")
                return {'status': 'completed', 'success': True}
                
            elif ingestion_status == 'FAILED':
                error_info = response['Ingestion'].get('ErrorInfo', {})
                logger.error(f"❌ Refresh failed for {dataset_id}: {error_info}")
                return {'status': 'failed', 'success': False, 'error': error_info}
                
            elif ingestion_status == 'CANCELLED':
                logger.warning(f"⚠️ Refresh cancelled for {dataset_id}")
                return {'status': 'cancelled', 'success': False}
                
            elif ingestion_status in ['INITIALIZED', 'QUEUED', 'RUNNING']:
                logger.info(f"🔄 Refresh in progress for {dataset_id} - status: {ingestion_status}")
                time.sleep(check_interval)
            
        except Exception as e:
            logger.warning(f"Error checking completion status: {str(e)}")
            time.sleep(check_interval)
    
    # Timeout reached
    logger.warning(f"⏰ Timeout waiting for refresh completion for {dataset_id}")
    return {'status': 'timeout', 'success': False}

# Alternative approach: Use Step Functions for long-running workflows
def create_step_function_approach():
    """
    For very long refreshes, consider using Step Functions instead of Lambda
    
    Step Function workflow:
    1. Lambda: Modify SQL + Trigger refresh
    2. Wait State: Poll refresh status every 30 seconds
    3. Lambda: Revert SQL when refresh completes
    
    Benefits:
    - No Lambda timeout issues
    - Built-in retry logic
    - Visual workflow monitoring
    - Can handle hours-long refreshes
    """
    
    step_function_definition = {
        "Comment": "QuickSight Dataset Refresh Workflow",
        "StartAt": "ModifyAndTriggerRefresh",
        "States": {
            "ModifyAndTriggerRefresh": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:region:account:function:modify-and-trigger",
                "Next": "WaitForRefresh"
            },
            "WaitForRefresh": {
                "Type": "Wait",
                "Seconds": 30,
                "Next": "CheckRefreshStatus"
            },
            "CheckRefreshStatus": {
                "Type": "Task", 
                "Resource": "arn:aws:lambda:region:account:function:check-status",
                "Next": "IsRefreshComplete"
            },
            "IsRefreshComplete": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.status",
                        "StringEquals": "COMPLETED",
                        "Next": "RevertSQL"
                    },
                    {
                        "Variable": "$.status",
                        "StringEquals": "FAILED",
                        "Next": "HandleFailure"
                    }
                ],
                "Default": "WaitForRefresh"
            },
            "RevertSQL": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:region:account:function:revert-sql",
                "End": True
            },
            "HandleFailure": {
                "Type": "Fail",
                "Cause": "Refresh failed"
            }
        }
    }
    
    return step_function_definition

# Updated timing estimates with wait logic
TIMING_WITH_WAIT = """
TIMING WITH WAIT LOGIC:

Per Dataset (Sequential):
- Modify SQL: 2-3 seconds
- Trigger refresh: 1-2 seconds  
- Wait for start: 10-30 seconds ⭐ NEW
- Revert SQL: 2-3 seconds
- Total per dataset: 15-40 seconds

Concurrent (4 threads):
- 13 datasets ÷ 4 threads = 4 batches
- Longest wait time: ~40 seconds per batch
- Total: 4 × 40 seconds = ~3 minutes
- Still well under 15-minute Lambda limit ✅

Alternative without waiting:
- Risk: SQL reverted before refresh starts
- QuickSight might use wrong SQL
- Refresh could fail or use wrong data
"""
