import pandas as pd
import os

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
    except AssertionError as e:
        print(f" Validation failed: {e}")    

