import pandas as pd
import numpy as np
from tests import (
    validate_raw_costs,
    validate_exchange_rates,
    validate_account_info,
    validate_customer_account_realtionship,
    validate_duplicates_in_raw_costs,
    validate_cost_eur_calculation,
    validate_rate_for_eur
)


def run_validations_input_files():
    validate_raw_costs()
    validate_exchange_rates()
    validate_account_info()
    validate_customer_account_realtionship()
    validate_duplicates_in_raw_costs()
    print("All validations passed!")


def load_data(raw_costs_path, account_info_path, exchange_rates_path):
    raw_costs_df = pd.read_csv(raw_costs_path)
    account_info_df = pd.read_csv(account_info_path)
    exchange_rates_df = pd.read_csv(exchange_rates_path)

    return raw_costs_df, account_info_df, exchange_rates_df


def preprocess_data(raw_costs_df, account_info_df, exchange_rates_df):
    dataframes = [raw_costs_df, account_info_df, exchange_rates_df]
    for df in dataframes:
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
        )
    # Convert date columns to timestamps
    raw_costs_df['segments_date'] = pd.to_datetime(raw_costs_df['segments_date'],utc=True)
    raw_costs_df['last_extracted_at'] = pd.to_datetime(raw_costs_df['last_extracted_at'], utc=True)
    exchange_rates_df['valid_from'] = pd.to_datetime(exchange_rates_df['valid_from'], utc=True)
    exchange_rates_df['valid_to'] = pd.to_datetime(exchange_rates_df['valid_to'], utc=True)


    # Ensure RATE is float
    exchange_rates_df['rate'] = pd.to_numeric(exchange_rates_df['rate'], errors='coerce')
    return raw_costs_df, account_info_df, exchange_rates_df

def drop_duplicates_keep_last(raw_costs_df):
    """
    Drops duplicate rows in raw_costs_df, keeping the last occurrence.

    """
    raw_costs_sorted_df = raw_costs_df.reset_index().sort_values(
        by=["last_extracted_at", "index"],
        ascending=[False, False]
    ).drop(columns="index")


    raw_costs_last_extracted = raw_costs_sorted_df.drop_duplicates(
        subset=["campaign_id", "segments_date", "customer_id"],
        keep="first"
    )
    #print(f"Original count: {raw_costs_df.shape[0]}")
    #print(f"After drop duplicates: {raw_costs_last_extracted.shape[0]}")
    return raw_costs_last_extracted

def join_costs_with_accounts(raw_costs_df, account_info_df):
    """
    Merge raw_costs with account_info on CUSTOMER_ID and ACCOUNT_ID.
    """
    merged_df = raw_costs_df.merge(
        account_info_df,
        left_on='customer_id',
        right_on='account_id',
        how='left'
    )
    return merged_df

def get_exchange_rate(row, exchange_rates_df):
    """Returns the EUR exchange rate for a given row."""
    if row['currency'] == 'EUR':
        return 1.0

 
    # Filter matching exchange rate based on date range and currency
    matching = exchange_rates_df[
        (exchange_rates_df['currency'] == row['currency']) &
        (exchange_rates_df['base'] == 'EUR') &
        (row['segments_date'] >= exchange_rates_df['valid_from']) &
        (row['segments_date'] <= exchange_rates_df['valid_to'])
    ]

    if not matching.empty:
        return matching.iloc[0]['rate']
    
    return None
    
def add_eur_cost_column(costs_df, exchange_rates_df):
    """Adds rate and cost_eur columns to the input dataframe."""

    # Apply exchange rate lookup
    costs_df['rate'] = costs_df.apply(
        lambda row: get_exchange_rate(row, exchange_rates_df), axis=1
    )

    # Compute cost in EUR
    costs_df['cost_eur'] = costs_df['metrics_cost_micros'] / costs_df['rate']
    
    return costs_df

def aggregate_daily_spend(costs_df):
    """
    Groups the cost data by segments_date, domain, brand, and channel_type,
    and calculates the total spend in EUR per group.

    Parameters:
        costs_df (pd.DataFrame): DataFrame containing a 'cost_eur' column and the required grouping columns.

    Returns:
        pd.DataFrame: Aggregated daily spend with column 'total_spend_eur'.
    """
    
    daily_spend_validate = (
        costs_df.groupby(['segments_date', 'domain', 'brand', 'channel_type'], as_index=False)
        .agg({
            'cost_eur': 'sum',
            'metrics_cost_micros': 'sum',
            'rate': 'mean' ,
            'currency': 'first'
        })
        .rename(columns={
            'cost_eur': 'total_spend_eur',
            'metrics_cost_micros': 'total_metrics_cost',
            
        })
    )
    daily_spend = daily_spend_validate.drop(columns=['total_metrics_cost', 'rate'])

    
    return (daily_spend,daily_spend_validate)


def run_validations_output(daily_spend_validate):
    """
    Validates the output DataFrame for expected conditions.
    """
    assert 'total_spend_eur' in daily_spend_validate.columns, "Output DataFrame must contain 'total_spend_eur' column"
    assert daily_spend_validate['total_spend_eur'].apply(lambda x: isinstance(x, (int, float))).all(), "All values in 'total_spend_eur' must be numeric"
    
    # Validate cost_eur calculation
    validate_cost_eur_calculation(daily_spend_validate)
    validate_rate_for_eur(daily_spend_validate)
    
    print("Output validations passed!")


def process_pipeline(raw_costs_path, account_info_path, exchange_rates_path):
    """
    Main function to process the data pipeline.
    Loads, preprocesses, and aggregates the cost data.
    """

    run_validations_input_files()

    print("Starting the main pipeline...")


    raw_costs_df, account_info_df, exchange_rates_df = load_data(
        raw_costs_path, account_info_path, exchange_rates_path
    )
    
    raw_costs_df, account_info_df, exchange_rates_df = preprocess_data(
        raw_costs_df, account_info_df, exchange_rates_df
    )
    raw_costs_df = drop_duplicates_keep_last(raw_costs_df)
    
    costs_with_accounts = join_costs_with_accounts(raw_costs_df, account_info_df)
    
    costs_with_eur = add_eur_cost_column(costs_with_accounts, exchange_rates_df)
    


    """
    filtered_costs = costs_with_eur[
    (costs_with_eur['domain'] == 'de') &
    (costs_with_eur['segments_date'].dt.date == pd.to_datetime('2025-01-01').date())&
    (costs_with_eur['brand'] == 'FlixTrain') &
    (costs_with_eur['channel_type'] == 'mixed')
    ]
    
    np.set_printoptions(suppress=True)  # Disable scientific notation for numpy arrays
    cost_eur_values = filtered_costs["cost_eur"].values
    print("cost_eur values:", cost_eur_values)
    # Number of rows

    
    print(f"Number of rows: {row_count}")
    with open('output/filtered_costs.txt', 'w') as f:
        f.write(filtered_costs.to_string(index=False))
    
    #print(filtered_costs)
    """
    daily_spend,daily_spend_validate = aggregate_daily_spend(costs_with_eur)
    # Format total_spend_eur to show full numbers without scientific notation
    run_validations_output(daily_spend_validate)

    daily_spend['total_spend_eur'] = daily_spend['total_spend_eur'].map('{:.0f}'.format)
    return daily_spend, daily_spend_validate










if __name__ == "__main__":  
    import sys
    if len(sys.argv) != 4:
        print("PLEASE provide the paths to the raw costs, account info, and exchange rates CSV files as command line arguments.")
        sys.exit(1)
        
    raw_costs_path = sys.argv[1]
    account_info_path = sys.argv[2]
    exchange_rates_path = sys.argv[3]

    daily_spend_df, daily_spend_validate = process_pipeline(raw_costs_path, account_info_path, exchange_rates_path)

    print(daily_spend_df.head())
    print("Data processing completed successfully.")


    with open('output/daily_spend.txt', 'w') as f:
        f.write(daily_spend_df.to_string(index=False))

    with open('output/daily_spend_validate.txt', 'w') as f:
        f.write(daily_spend_validate.to_string(index=False))    


    





