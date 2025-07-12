import pandas as pd

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
            .str.replace(' ', '_')
        )
    # Convert SEGMENTS_DATE to datetime
    raw_costs_df['segments_date'] = pd.to_datetime(raw_costs_df['segments_date'],utc=True)
    exchange_rates_df['valid_from'] = pd.to_datetime(exchange_rates_df['valid_from'], utc=True)
    exchange_rates_df['valid_to'] = pd.to_datetime(exchange_rates_df['valid_to'], utc=True)

    # Ensure RATE is float
    exchange_rates_df['rate'] = pd.to_numeric(exchange_rates_df['rate'], errors='coerce')

    return raw_costs_df, account_info_df, exchange_rates_df

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
    costs_df = costs_df.copy()  # Avoid mutating original df

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
    daily_spend = (
        costs_df.groupby(['segments_date', 'domain', 'brand', 'channel_type'], as_index=False)
        ['cost_eur']
        .sum()
        .rename(columns={'cost_eur': 'total_spend_eur'})
    )
    return daily_spend


def process_pipeline(raw_costs_path, account_info_path, exchange_rates_path):
    """
    Main function to process the data pipeline.
    Loads, preprocesses, and aggregates the cost data.
    """
    raw_costs_df, account_info_df, exchange_rates_df = load_data(
        raw_costs_path, account_info_path, exchange_rates_path
    )
    
    raw_costs_df, account_info_df, exchange_rates_df = preprocess_data(
        raw_costs_df, account_info_df, exchange_rates_df
    )
    
    costs_with_accounts = join_costs_with_accounts(raw_costs_df, account_info_df)
    
    costs_with_eur = add_eur_cost_column(costs_with_accounts, exchange_rates_df)
    
    daily_spend = aggregate_daily_spend(costs_with_eur)
    
    return daily_spend


if __name__ == "__main__":  
    import sys
    if len(sys.argv) != 4:
        print("Usage: python tranformation.py <raw_costs_path> <account_info_path> <exchange_rates_path>")
        sys.exit(1)

    raw_costs_path = sys.argv[1]
    account_info_path = sys.argv[2]
    exchange_rates_path = sys.argv[3]

    daily_spend_df = process_pipeline(raw_costs_path, account_info_path, exchange_rates_path)
    print(daily_spend_df.head())
    print(daily_spend_df.shape)
    print("Data processing completed successfully.")






