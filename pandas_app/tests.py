import pandas as pd
import os
import numpy as np



import logging
logger = logging.getLogger(__name__)

def validate_raw_costs(raw_costs_path):
   # Read CSV file with headers
    df = pd.read_csv(raw_costs_path)

    # Convert column names to lowercase
    df.columns = [col.lower() for col in df.columns]
    assert 'customer_id' in df.columns
    assert 'segments_date' in df.columns
    assert df['customer_id'].notnull().all()
    assert pd.to_datetime(df['segments_date'], errors='coerce').notnull().all()
    logger.info("Validation passed for raw costs file: customer_id present and segments_date can be converted to dates and there is no null customer_ids.")

def validate_exchange_rates(exchange_rates_path):
    df = pd.read_csv(exchange_rates_path)
    df.columns = [col.lower() for col in df.columns]
    assert 'currency' in df.columns
    assert 'base' in df.columns
    assert 'rate' in df.columns
    assert df['rate'].apply(lambda x: isinstance(x, (int, float))).all
    logger.info("Validation passed for exchange rates: currency and base and rate are present and rate is numeric.")

def validate_account_info(account_info_path):
    df = pd.read_csv(account_info_path)
    df.columns = [col.lower() for col in df.columns]
    assert 'account_id' in df.columns
    assert 'currency' in df.columns
    assert df['account_id'].notnull().all()
    logger.info("Validation passed for accounts file: account_id and currency are present and there are no null account_ids.")

def validate_customer_account_realtionship(raw_costs_path, account_info_path):
    raw_df = pd.read_csv(raw_costs_path)
    accounts_df = pd.read_csv(account_info_path)
    raw_df.columns = [col.lower() for col in raw_df.columns]
    accounts_df.columns = [col.lower() for col in accounts_df.columns]
    raw_ids = set(raw_df['customer_id'].unique())
    account_ids = set(accounts_df['account_id'].unique())
    # Check if all customer_ids in raw_costs have corresponding account_ids in account_info
    assert raw_ids.issubset(account_ids), f"Missing ACCOUNT_IDs for CUSTOMER_IDs: {raw_ids - account_ids}"
    logger.info("Validation passed for customer-account relationship: all CUSTOMER_IDs in raw_costs have corresponding ACCOUNT_IDs in accounts_info.")
   
def validate_duplicates_in_raw_costs(raw_costs_path):
    raw_df = pd.read_csv(raw_costs_path)
    # Convert column names to lowercase
    raw_df.columns = [col.lower() for col in raw_df.columns]
    duplicates = raw_df.duplicated(subset=['campaign_id', 'segments_date', 'customer_id', 'last_extracted_at'], keep=False)
    assert duplicates.any(), "Didn't Find duplicates in raw_costs.csv with the same campaign_id, segments_date, customer_id and last_extracted_at"
    logger.info("Validation passed for duplicates in raw costs: found duplicates based on campaign_id, segments_date, customer_id and last_extracted_at.")

def validate_cost_eur_calculation(costs_df):
    calculated_cost = costs_df['total_metrics_cost'] / costs_df['rate']
    assert np.isclose(costs_df['total_spend_eur'], calculated_cost, rtol=1e-5).all(), "Mismatch in cost_eur calculation"
    logger.info("Validation passed for cost_eur calculation: total_spend_eur matches calculated value.")    

def validate_rate_for_eur(costs_df):
    eur_df = costs_df[costs_df['currency'] == 'EUR']
    assert (eur_df['rate'] == 1.0).all(), "Found EUR rows where rate is not 1.0"
    logger.info("Validation passed for EUR rate: all EUR rows have rate = 1.0.")
    
if __name__ == "__main__":
    try:
        validate_raw_costs()
        print(" raw_costs.csv validation passed.")
        validate_exchange_rates()
        print(" exchange_rates.csv validation passed.")
        validate_account_info()
        print(" account_info.csv validation passed.")
        validate_customer_account_realtionship()
        print(" Customer-Account relationship validation passed.") 
        validate_duplicates_in_raw_costs()
        print(" duplicates found in raw_costs.csv, based on campaign_id, segments_date, customer_id and last_extracted_at.")       
    except AssertionError as e:
        print(f" Validation failed: {e}")    

