import re

def replace_all_yyyymmdd_filters(sql, yyyymmdd):
    """
    Handle SQL with multiple WHERE clauses containing yyyymmdd filters
    Replaces ALL instances of yyyymmdd rolling window filters
    """
    
    # Enhanced pattern to catch all yyyymmdd filters with table aliases
    yyyymmdd_patterns = [
        # Pattern 1: With table alias (q.yyyymmdd, s.yyyymmdd, etc.)
        r"(\w+\.)yyyymmdd\s*>=\s*CAST\(format_datetime\(current_date\s*-\s*interval\s*'\d+'\s*day,\s*'yyyyMMdd'\)\s*AS\s*INTEGER\)",
        
        # Pattern 2: Without table alias  
        r"\byyyymmdd\s*>=\s*CAST\(format_datetime\(current_date\s*-\s*interval\s*'\d+'\s*day,\s*'yyyyMMdd'\)\s*AS\s*INTEGER\)",
        
        # Pattern 3: Simple date formats
        r"(\w+\.)?yyyymmdd\s*>=\s*\d{8}",
    ]
    
    modified_sql = sql
    replacement_count = 0
    
    for pattern in yyyymmdd_patterns:
        def replacement_func(match):
            nonlocal replacement_count
            replacement_count += 1
            
            # Preserve table alias if it exists
            table_alias = ""
            if match.lastindex and match.group(1):
                table_alias = match.group(1)
            
            return f"{table_alias}yyyymmdd = {yyyymmdd}"
        
        modified_sql = re.sub(pattern, replacement_func, modified_sql, flags=re.IGNORECASE)
    
    print(f"Replaced {replacement_count} yyyymmdd filters")
    return modified_sql

def revert_all_yyyymmdd_filters(sql, window_days):
    """
    Revert ALL single-day yyyymmdd filters back to rolling windows
    Handles multiple WHERE clauses
    """
    
    rolling_filter = f"yyyymmdd >= CAST(format_datetime(current_date - interval '{window_days}' day, 'yyyyMMdd') AS INTEGER)"
    
    # Patterns to find single day filters
    single_day_patterns = [
        # With table alias
        r"(\w+\.)yyyymmdd\s*=\s*\d{8}",
        # Without table alias
        r"\byyyymmdd\s*=\s*\d{8}",
    ]
    
    reverted_sql = sql
    revert_count = 0
    
    for pattern in single_day_patterns:
        def revert_func(match):
            nonlocal revert_count
            revert_count += 1
            
            # Preserve table alias if it exists
            table_alias = ""
            if match.lastindex and match.group(1):
                table_alias = match.group(1)
            
            return f"{table_alias}{rolling_filter}"
        
        reverted_sql = re.sub(pattern, revert_func, reverted_sql, flags=re.IGNORECASE)
    
    print(f"Reverted {revert_count} yyyymmdd filters")
    return reverted_sql

# Test with your multi-WHERE SQL
def test_multiple_where_clauses():
    """
    Test the function with SQL containing multiple WHERE clauses
    """
    
    sample_sql = """
    WITH DateFilteredQuotes AS (
        SELECT * FROM "public"."bv_quote_quality" 
        WHERE yyyymmdd >= CAST(format_datetime(current_date - interval '60' day, 'yyyyMMdd') AS INTEGER)
    ),
    AggregatedSpreads AS (
        SELECT q.yyyymmdd, q.ticker, s.provider, q.brokerid
        FROM DateFilteredQuotes q
        INNER JOIN "public"."3m_etf_view" s ON q.ticker = s.ticker
        WHERE q.brokerid IN ('2','7','9','79','80','85','39','14','22') 
            AND q.brokerbidpx <> 0 
            AND s.yyyymmdd >= CAST(format_datetime(current_date - interval '30' day, 'yyyyMMdd') AS INTEGER)
            AND q.brokeraskpx > q.brokerbidpx
        GROUP BY q.yyyymmdd, q.ticker, s.provider, q.brokerid
    )
    SELECT * FROM AggregatedSpreads
    """
    
    print("=== ORIGINAL SQL ===")
    print(sample_sql)
    
    print("\n=== STEP 1: MODIFY TO SINGLE DAY ===")
    modified = replace_all_yyyymmdd_filters(sample_sql, "20250108")
    print(modified)
    
    print("\n=== STEP 2: REVERT TO 60-DAY WINDOW ===")
    reverted = revert_all_yyyymmdd_filters(modified, 60)
    print(reverted)
    
    return modified, reverted

# Integration with your main function
def modify_dataset_sql_multiple_where(quicksight_client, account_id, dataset_id, yyyymmdd):
    """
    Modified version that handles multiple WHERE clauses properly
    """
    try:
        response = quicksight_client.describe_data_set(
            AwsAccountId=account_id,
            DataSetId=dataset_id
        )
        
        dataset = response['DataSet']
        physical_table_map = dataset['PhysicalTableMap']
        
        for table_id, table_def in physical_table_map.items():
            if 'CustomSql' in table_def:
                original_sql = table_def['CustomSql']['SqlQuery']
                
                print(f"Processing dataset {dataset_id}")
                print(f"Original SQL preview: {original_sql[:200]}...")
                
                # Use the enhanced multiple WHERE handler
                modified_sql = replace_all_yyyymmdd_filters(original_sql, yyyymmdd)
                
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
        print(f"Failed to modify dataset SQL: {str(e)}")
        return None

def revert_dataset_sql_multiple_where(quicksight_client, account_id, dataset_id, window_days):
    """
    Revert version that handles multiple WHERE clauses
    """
    try:
        response = quicksight_client.describe_data_set(
            AwsAccountId=account_id,
            DataSetId=dataset_id
        )
        
        dataset = response['DataSet']
        physical_table_map = dataset['PhysicalTableMap']
        
        for table_id, table_def in physical_table_map.items():
            if 'CustomSql' in table_def:
                current_sql = table_def['CustomSql']['SqlQuery']
                
                print(f"Reverting dataset {dataset_id} to {window_days}-day window")
                
                # Use the enhanced revert function
                reverted_sql = revert_all_yyyymmdd_filters(current_sql, window_days)
                
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
        print(f"Failed to revert dataset SQL: {str(e)}")
        return False
