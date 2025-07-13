import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, isnan, when, count, lit, abs

# Initialize Spark
spark = SparkSession.builder \
    .appName("ValidationTests") \
    .getOrCreate()

DATA_PATH = os.path.join(os.path.dirname(__file__), 'DATA_FILES')

def validate_raw_costs():
    df = spark.read.option("header", True).csv(os.path.join(DATA_PATH, 'raw_costs.csv'))
    assert 'CUSTOMER_ID' in df.columns
    assert 'SEGMENTS_DATE' in df.columns

    null_count = df.filter(col("CUSTOMER_ID").isNull()).count()
    assert null_count == 0, f"{null_count} rows with null CUSTOMER_ID"

    invalid_dates = df.withColumn("parsed_date", to_date("SEGMENTS_DATE")) \
                      .filter(col("parsed_date").isNull()) \
                      .count()
    assert invalid_dates == 0, f"{invalid_dates} rows with invalid SEGMENTS_DATE"

def validate_exchange_rates():
    df = spark.read.option("header", True).csv(os.path.join(DATA_PATH, 'exchange_rates.csv'))
    assert 'CURRENCY' in df.columns
    assert 'BASE' in df.columns
    assert 'RATE' in df.columns

    # Try casting RATE to float and count invalid ones
    df = df.withColumn("RATE_FLOAT", col("RATE").cast("float"))
    invalid_rate_count = df.filter(col("RATE_FLOAT").isNull()).count()
    assert invalid_rate_count == 0, f"{invalid_rate_count} invalid RATE values (non-numeric)"

def validate_account_info():
    df = spark.read.option("header", True).csv(os.path.join(DATA_PATH, 'accounts_info.csv'))
    assert 'ACCOUNT_ID' in df.columns
    assert 'CURRENCY' in df.columns

    null_count = df.filter(col("ACCOUNT_ID").isNull()).count()
    assert null_count == 0, f"{null_count} rows with null ACCOUNT_ID"

def validate_customer_account_relationship():
    raw_df = spark.read.option("header", True).csv(os.path.join(DATA_PATH, 'raw_costs.csv'))
    accounts_df = spark.read.option("header", True).csv(os.path.join(DATA_PATH, 'accounts_info.csv'))

    raw_ids = raw_df.select(col("CUSTOMER_ID")).distinct()
    account_ids = accounts_df.select(col("ACCOUNT_ID")).distinct()

    missing_accounts = raw_ids.join(account_ids, raw_ids["CUSTOMER_ID"] == account_ids["ACCOUNT_ID"], "left_anti")
    count_missing = missing_accounts.count()
    assert count_missing == 0, f"Missing ACCOUNT_IDs for CUSTOMER_IDs: {count_missing}"

def validate_duplicates_in_raw_costs():
    df = spark.read.option("header", True).csv(os.path.join(DATA_PATH, 'raw_costs.csv'))
    df = df.select([col(c).alias(c.lower()) for c in df.columns])
    duplicates = df.groupBy("campaign_id", "segments_date", "customer_id", "last_extracted_at") \
                   .count().filter(col("count") > 1)
    count_duplicates = duplicates.count()
    assert  not count_duplicates == 0, f"Found {count_duplicates} duplicates in raw_costs.csv"

def validate_cost_eur_calculation(costs_df):
    df = costs_df.withColumn("expected_cost_eur", col("total_metrics_cost") / col("rate"))
    mismatches = df.withColumn("diff", abs(col("expected_cost_eur") - col("total_spend_eur"))) \
                   .filter(col("diff") > 1e-5)
    count_mismatches = mismatches.count()
    assert count_mismatches == 0, f"Found {count_mismatches} mismatches in cost_eur calculation"

def validate_rate_for_eur(costs_df):
    eur_df = costs_df.filter(col("currency") == "EUR")
    non_ones = eur_df.filter(col("rate") != 1.0)
    count_non_ones = non_ones.count()
    assert count_non_ones == 0, f"Find {count_non_ones} EUR rows where rate != 1.0"

if __name__ == "__main__":
    try:
        validate_raw_costs()
        print("raw_costs.csv validation passed.")
        validate_exchange_rates()
        print("exchange_rates.csv validation passed.")
        validate_account_info()
        print("accounts_info.csv validation passed.")
        validate_customer_account_relationship()
        print("customer-account relationship validation passed.")
        validate_duplicates_in_raw_costs()
        print("duplicates found in raw_costs.csv.")
    except AssertionError as e:
        print(f"Validation failed: {e}")
