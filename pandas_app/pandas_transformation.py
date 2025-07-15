import pandas as pd
import numpy as np


import logging

logging.basicConfig(
    filename='/app/pandas_app/logs/pandas_job.log', 
    filemode='w',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logger.info(" Starting Logging")

from tests import (
    validate_raw_costs,
    validate_exchange_rates,
    validate_account_info,
    validate_customer_account_realtionship,
    validate_duplicates_in_raw_costs,
    validate_cost_eur_calculation,
    validate_rate_for_eur
)




def run_validations_input_files(raw_costs_path, account_info_path, exchange_rates_path):
    """
    Run all input file validations.

    Validations include:
    - Raw costs schema and content
    - Exchange rates format
    - Account info structure
    - Consistency between raw costs and account info
    - Duplicate records in raw costs
    """

    try:
        validate_raw_costs(raw_costs_path)
        validate_exchange_rates(exchange_rates_path)
        validate_account_info(account_info_path)
        validate_customer_account_realtionship(raw_costs_path, account_info_path)
        validate_duplicates_in_raw_costs(raw_costs_path)
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        sys.exit(1)    
    logger.info("All validations passed!")


def load_data(raw_costs_path, account_info_path, exchange_rates_path):
    try:
        raw_costs_df = pd.read_csv(raw_costs_path)
        account_info_df = pd.read_csv(account_info_path)
        exchange_rates_df = pd.read_csv(exchange_rates_path)
        return raw_costs_df, account_info_df, exchange_rates_df
    except Exception as e:
        logger.error(f"Error loading data: {e}", exc_info=True)
        sys.exit(1)
    


def preprocess_data(raw_costs_df, account_info_df, exchange_rates_df):

    """
    Preprocess input DataFrames:
    - Standardizes column names to lowercase and strips whitespace.
    - Converts relevant columns to datetime.
    - Ensures exchange rates are numeric.
    """    
    dataframes = [raw_costs_df, account_info_df, exchange_rates_df]
    try:
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
    except Exception as e:
        logger.error(f"Error preprocessing the data: {e}", exc_info=True)    
        sys.exit(1)    
    return raw_costs_df, account_info_df, exchange_rates_df

def drop_duplicates_keep_last(raw_costs_df):
    """
    Drops duplicate rows in raw_costs_df, keeping the last occurrence based on the last extrracted at and the index.

    """
    try:
        raw_costs_sorted_df = raw_costs_df.reset_index().sort_values(
            by=["last_extracted_at", "index"],
            ascending=[False, False]
        ).drop(columns="index")


        raw_costs_last_extracted = raw_costs_sorted_df.drop_duplicates(
            subset=["campaign_id", "segments_date", "customer_id"],
            keep="first"
        )
    except Exception as e:
        logger.error(f"Error dropping duplicates: {e}", exc_info=True)
        sys.exit(1)
    return raw_costs_last_extracted

def join_costs_with_accounts(raw_costs_df, account_info_df):
    """
    Merge raw_costs with account_info on CUSTOMER_ID and ACCOUNT_ID.
    """
    try:
        merged_df = raw_costs_df.merge(
            account_info_df,
            left_on='customer_id',
            right_on='account_id',
            how='left'
        )
    except Exception as e:
        logger.error(f"Error joining costs with accounts: {e}", exc_info=True)
        sys.exit(1)    
    return merged_df

def get_exchange_rate(row, exchange_rates_df):
    """
    Get the EUR exchange rate for a given row based on currency and date.

    Parameters:
        row (Series): A row from the main DataFrame with 'currency' and 'segments_date'.
        exchange_rates_df: DataFrame containing exchange rate data 
                                       with 'currency', 'base', 'rate', 'valid_from', 'valid_to'.
    """    

    try:
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
    except Exception as e:
        logger.error(f"Error getting exchange rate: {e}", exc_info=True)
        sys.exit(1)
def add_eur_cost_column(costs_df, exchange_rates_df):
    """Adds rate and cost_eur columns to the input dataframe."""
    try:
    # Apply exchange rate lookup
        costs_df['rate'] = costs_df.apply(
            lambda row: get_exchange_rate(row, exchange_rates_df), axis=1
        )

        # Compute cost in EUR
        costs_df['cost_eur'] = costs_df['metrics_cost_micros'] / costs_df['rate']
    except Exception as e:
        logger.error(f"Error adding EUR cost column: {e}", exc_info=True)
        sys.exit(1)   
    
    return costs_df

def aggregate_daily_spend(costs_df):
    """
    Groups the cost data by segments_date, domain, brand, and channel_type,
    and calculates the total spend in EUR per group.

    Parameters:
        costs_df: DataFrame containing a 'cost_eur' column and the required grouping columns.

    Returns:
        pd.DataFrame: Aggregated daily spend with column 'total_spend_eur'.
    """
    try:
    
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

    except Exception as e:
        logger.error(f"Error aggregating daily spend: {e}", exc_info=True)
        sys.exit(1)
    return (daily_spend,daily_spend_validate)


def run_validations_output(daily_spend_validate):
    """
    Run output validations on the processed data.

    Parameters:
        daily_spend_validate (DataFrame): The DataFrame to validate.

    Validations:
        - Checks if 'cost_eur' is correctly calculated.
        - Verifies that exchange rate to EUR is applied properly.

    Exits:
        Logs error and exits if any validation fails.
    """
    try:    
    # Validate cost_eur calculation
        validate_cost_eur_calculation(daily_spend_validate)
        validate_rate_for_eur(daily_spend_validate)
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        sys.exit(1)
    logger.info("Output validations passed!")

def write_df_to_file(df_spend, path='/app/pandas_app/output/daily_spend.txt'):
    """
    Writes the DataFrame to a text file.
    
    Parameters:
        df_spend (pd.DataFrame): DataFrame to write to file.
        path (str): Path to the output file.
    """
    try:
        df_spend = (
            df_spend
            .sort_values(by=["segments_date", "total_spend_eur"])
            .rename(columns={"domain": "country"})
            .assign(segments_date=lambda x: x['segments_date'].astype(str))
        )

        with open('/app/pandas_app/output/daily_spend.txt', 'w') as f:
            f.write(df_spend.to_string(index=False))    
    except Exception as e:
        logger.error(f"Error writing DataFrame to file: {e}", exc_info=True)
        sys.exit(1)            



def process_pipeline(raw_costs_path, account_info_path, exchange_rates_path):
    """
    Main function to process the data pipeline.
    Loads, preprocesses, and aggregates the cost data.
    """
    logger.info("Starting the validation of input files...")
    run_validations_input_files(raw_costs_path, account_info_path, exchange_rates_path)

    logger.info("Input files validation completed successfully.")

    logger.info("Starting the main pipeline...")
    raw_costs_df, account_info_df, exchange_rates_df = load_data(
        raw_costs_path, account_info_path, exchange_rates_path
    )
    
    raw_costs_df, account_info_df, exchange_rates_df = preprocess_data(
        raw_costs_df, account_info_df, exchange_rates_df
    )
    raw_costs_df = drop_duplicates_keep_last(raw_costs_df)
    
    costs_with_accounts = join_costs_with_accounts(raw_costs_df, account_info_df)
    
    costs_with_eur = add_eur_cost_column(costs_with_accounts, exchange_rates_df)
    
    daily_spend,daily_spend_validate = aggregate_daily_spend(costs_with_eur)
    # Format total_spend_eur to show full numbers without scientific notation
    daily_spend['total_spend_eur'] = daily_spend['total_spend_eur'].map('{:.0f}'.format)
    
    
    logger.info("Daily spend aggregation completed.")
    run_validations_output(daily_spend_validate)
    logger.info("validation on output is starting")
    
    return daily_spend, daily_spend_validate


if __name__ == "__main__":  
    import sys
    
    if len(sys.argv) != 4:
        logger.info("PLEASE provide the paths to the raw costs, account info, and exchange rates CSV files as command line arguments.")
        sys.exit(1)
        
    raw_costs_path = sys.argv[1]
    account_info_path = sys.argv[2]
    exchange_rates_path = sys.argv[3]

    daily_spend_df, daily_spend_validate = process_pipeline(raw_costs_path, account_info_path, exchange_rates_path)

    #logger.info(daily_spend_df.head())
    logger.info("Data processing completed successfully.")

    logger.info("Writing output to file...")
    write_df_to_file(daily_spend_df)
    logger.info("Output written to file successfully, check /app/pandas_app/output/daily_spend.txt")
    logger.info("Script reached the end, exiting with code 0")
    logger.info("Pipeline completed successfully.")
   


    





