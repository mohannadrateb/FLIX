import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, isnan, when, count, lit, abs


import logging
logger = logging.getLogger(__name__)

# Initialize Spark
spark = SparkSession.builder \
    .appName("ValidationTests") \
    .getOrCreate()


def validate_raw_costs(raw_costs_path):
    df = spark.read.option("header", True).csv(raw_costs_path)
    df = df.toDF(*[col.lower() for col in df.columns])

    assert 'customer_id' in df.columns
    assert 'segments_date' in df.columns

    null_count = df.filter(col("customer_id").isNull()).count()
    assert null_count == 0, f"{null_count} rows with null CUSTOMER_ID"

    invalid_dates = df.withColumn("parsed_date", to_date("segments_date")) \
                      .filter(col("parsed_date").isNull()) \
                      .count()
    assert invalid_dates == 0, f"{invalid_dates} rows with invalid SEGMENTS_DATE"

    logger.info("Validation passed for raw costs file: customer_id present and segments_date can be converted to dates and there is no null customer_ids.")

def validate_exchange_rates(exchange_rates_path):
    df = spark.read.option("header", True).csv(exchange_rates_path)
    df = df.toDF(*[col.lower() for col in df.columns])
    assert 'currency' in df.columns
    assert 'base' in df.columns
    assert 'rate' in df.columns

    # Try casting RATE to float and count invalid ones
    df = df.withColumn("rate_float", col("rate").cast("float"))
    invalid_rate_count = df.filter(col("rate_float").isNull()).count()
    assert invalid_rate_count == 0, f"{invalid_rate_count} invalid RATE values (non-numeric)"
    logger.info("Validation passed for exchange rates: currency and base and rate are present and rate is numeric.")

def validate_account_info(account_info_path):
    df = spark.read.option("header", True).csv(account_info_path)
    df = df.toDF(*[col.lower() for col in df.columns])
    assert 'account_id' in df.columns
    assert 'currency' in df.columns

    null_count = df.filter(col("account_id").isNull()).count()
    assert null_count == 0, f"{null_count} rows with null ACCOUNT_ID"
    logger.info("Validation passed for accounts file: account_idd and currency are present and there are no null account_ids.")

def validate_customer_account_relationship(raw_costs_path,account_info_path):
    raw_df = spark.read.option("header", True).csv(raw_costs_path)
    accounts_df = spark.read.option("header", True).csv(account_info_path)

    raw_df = raw_df.toDF(*[col.lower() for col in raw_df.columns])
    accounts_df = accounts_df.toDF(*[col.lower() for col in accounts_df.columns])

    raw_ids = raw_df.select(col("customer_id")).distinct()
    account_ids = accounts_df.select(col("account_id")).distinct()

    missing_accounts = raw_ids.join(account_ids, raw_ids["customer_id"] == account_ids["account_id"], "left_anti")
    count_missing = missing_accounts.count()
    assert count_missing == 0, f"Missing ACCOUNT_IDs for CUSTOMER_IDs: {count_missing}"

    logger.info("Validation passed for customer-account relationship: all CUSTOMER_IDs in raw_costs have corresponding ACCOUNT_IDs in accounts_info.")

def validate_duplicates_in_raw_costs(raw_costs_path):
    df = spark.read.option("header", True).csv(raw_costs_path)
    df = df.toDF(*[col.lower() for col in df.columns])
    duplicates = df.groupBy("campaign_id", "segments_date", "customer_id", "last_extracted_at") \
                   .count().filter(col("count") > 1)
    count_duplicates = duplicates.count()
    assert  not count_duplicates == 0, f"Found {count_duplicates} duplicates in raw_costs.csv"
    logger.info(f"Validation passed for duplicates in raw_costs: found {count_duplicates} duplicates based on campaign_id, segments_date, customer_id and last_extracted_at.")

def validate_cost_eur_calculation(costs_df):
    df = costs_df.withColumn("expected_cost_eur", col("total_metrics_cost") / col("rate"))
    mismatches = df.withColumn("diff", abs(col("expected_cost_eur") - col("total_spend_eur"))) \
                   .filter(col("diff") > 1e-5)
    count_mismatches = mismatches.count()
    assert count_mismatches == 0, f"Found {count_mismatches} mismatches in cost_eur calculation"
    logger.info("Validation passed for cost_eur calculation: total_spend_eur matches calculated value.")

def validate_rate_for_eur(costs_df):
    eur_df = costs_df.filter(col("currency") == "EUR")
    non_ones = eur_df.filter(col("rate") != 1.0)
    count_non_ones = non_ones.count()
    assert count_non_ones == 0, f"Find {count_non_ones} EUR rows where rate != 1.0"
    logger.info("Validation passed for EUR rate: all EUR rows have rate = 1.0.")


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
