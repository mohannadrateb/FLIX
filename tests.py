import pandas as pd
import os
import numpy as np
DATA_PATH = os.path.join(os.path.dirname(__file__), 'DATA_FILES')

def validate_raw_costs():
    df = pd.read_csv(os.path.join(DATA_PATH, 'raw_costs.csv'))
    assert 'CUSTOMER_ID' in df.columns
    assert 'SEGMENTS_DATE' in df.columns
    assert df['CUSTOMER_ID'].notnull().all()
    assert pd.to_datetime(df['SEGMENTS_DATE'], errors='coerce').notnull().all()

def validate_exchange_rates():
    df = pd.read_csv(os.path.join(DATA_PATH, 'exchange_rates.csv'))
    assert 'CURRENCY' in df.columns
    assert 'BASE' in df.columns
    assert 'RATE' in df.columns
    assert df['RATE'].apply(lambda x: isinstance(x, (int, float))).all()

def validate_account_info():
    df = pd.read_csv(os.path.join(DATA_PATH, 'accounts_info.csv'))
    assert 'ACCOUNT_ID' in df.columns
    assert 'CURRENCY' in df.columns
    assert df['ACCOUNT_ID'].notnull().all()

def validate_customer_account_realtionship():
    raw_df = pd.read_csv(os.path.join(DATA_PATH, 'raw_costs.csv'))
    accounts_df = pd.read_csv(os.path.join(DATA_PATH, 'accounts_info.csv'))
    missing_accounts = set(raw_df['CUSTOMER_ID']) - set(accounts_df['ACCOUNT_ID'])
    assert not missing_accounts, f"Missing ACCOUNT_IDs for CUSTOMER_IDs: {missing_accounts}"
   
def validate_duplicates_in_raw_costs():
    raw_df = pd.read_csv(os.path.join(DATA_PATH, 'raw_costs.csv'))
    raw_df.columns = raw_df.columns.str.lower()
    duplicates = raw_df[raw_df.duplicated(subset=['campaign_id','segments_date', 'customer_id', 'last_extracted_at'])]
    assert not duplicates.empty, f"Didn't Find duplicates in raw_costs.csv with the same campaign_id, segments_date,customer_id and last_extracted: {len(duplicates)}"

def validate_cost_eur_calculation(costs_df):
    calculated_cost = costs_df['total_metrics_cost'] / costs_df['rate']
    assert np.isclose(costs_df['total_spend_eur'], calculated_cost, rtol=1e-5).all(), "Mismatch in cost_eur calculation"

def validate_rate_for_eur(costs_df):
    eur_df = costs_df[costs_df['currency'] == 'EUR']
    assert (eur_df['rate'] == 1.0).all(), "Found EUR rows where rate is not 1.0"
    
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

