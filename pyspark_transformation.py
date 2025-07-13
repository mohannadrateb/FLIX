from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    when,
    broadcast,
    row_number,
    monotonically_increasing_id,
    desc,
    sum as _sum,
    avg,
    first
)

from tests_pyspark import (
    validate_raw_costs,
    validate_exchange_rates,
    validate_account_info,
    validate_customer_account_relationship,
    validate_duplicates_in_raw_costs,
    validate_cost_eur_calculation,
    validate_rate_for_eur
)


def create_spark_session():
    return SparkSession.builder.appName("CostDataPipeline").getOrCreate()



def run_validations_input_files():
    validate_raw_costs()
    validate_exchange_rates()
    validate_account_info()
    validate_customer_account_relationship()
    validate_duplicates_in_raw_costs()
    print("All validations passed!")

def load_data(spark, raw_costs_path, account_info_path, exchange_rates_path):
    raw_costs_df = spark.read.option("header", True).csv(raw_costs_path)
    account_info_df = spark.read.option("header", True).csv(account_info_path)
    exchange_rates_df = spark.read.option("header", True).csv(exchange_rates_path)
    return raw_costs_df, account_info_df, exchange_rates_df

def preprocess_data(raw_costs_df, account_info_df, exchange_rates_df):
    def clean_columns(df):
        for col_name in df.columns:
            new_col = col_name.strip().lower().replace(" ", "_")
            df = df.withColumnRenamed(col_name, new_col)
        return df

    raw_costs_df = clean_columns(raw_costs_df)
    account_info_df = clean_columns(account_info_df)
    exchange_rates_df = clean_columns(exchange_rates_df)

    raw_costs_df = raw_costs_df.withColumn("segments_date", col("segments_date").cast("timestamp"))
    raw_costs_df = raw_costs_df.withColumn("last_extracted_at", col("last_extracted_at").cast("timestamp"))
    exchange_rates_df = exchange_rates_df.withColumn("valid_from", col("valid_from").cast("timestamp"))
    exchange_rates_df = exchange_rates_df.withColumn("valid_to", col("valid_to").cast("timestamp"))
    
    return raw_costs_df, account_info_df, exchange_rates_df


def drop_duplicates_keep_last(raw_costs_df):
    """
    Drops duplicate rows in raw_costs_df, keeping the last occurrence based on last_extracted_at.
    """
    # Add a unique row identifier to preserve original order for tie-breaking
    raw_costs_with_order = raw_costs_df.withColumn(
        "original_order", 
        monotonically_increasing_id()
    )

    window_spec = Window.partitionBy(
        "campaign_id", "segments_date", "customer_id"
    ).orderBy(
        desc("last_extracted_at"), 
        desc("original_order")  # Matches Pandas' last-row preference
    )

    
    df_with_rownum = raw_costs_with_order.withColumn("row_num", row_number().over(window_spec))
    raw_costs_last_extracted = df_with_rownum.filter("row_num = 1").drop("row_num","original_order")


    """
    print(f"Original count: {raw_costs_df.count()}")
    print(f"After drop duplicates: {raw_costs_last_extracted.count()}")
    """
    return raw_costs_last_extracted

def join_costs_with_accounts(raw_costs_df, account_info_df):
    return raw_costs_df.join(
        account_info_df,
        raw_costs_df["customer_id"] == account_info_df["account_id"],
        how="left"
    )

def add_eur_cost_column(costs_df, exchange_rates_df):

    joined = costs_df.join(
        broadcast(exchange_rates_df),
        (costs_df.currency == exchange_rates_df.currency) &
        (costs_df.segments_date >= exchange_rates_df.valid_from) &
        (costs_df.segments_date <= exchange_rates_df.valid_to) &
        (exchange_rates_df.base == 'EUR'), how="left").drop(exchange_rates_df.currency)
    
    #joined = joined.filter(col("rate").isNull())
    #print(f"Rows with missing exchange rates: {joined.count()}")
    #print(joined)

    joined = joined.withColumn(
    "rate",
    when(col("currency") == "EUR", lit(1)).otherwise(col("rate"))
    )

    """
    # Rows with EUR currency (should be handled separately)
    print(f"Rows with EUR currency: {joined.filter(col('currency') == 'EUR').count()}")

    # Rows that didn't find a matching exchange rate (rate is null) and currency is NOT EUR
    missing_rate_count = joined.filter((col("rate").isNull()) & (col("currency") != "EUR")).count()
    print(f"Rows with missing exchange rate (non-EUR): {missing_rate_count}")
    different_rate_count = joined.filter((col("rate").isNotNull()) & (col("currency") != "EUR")).count() 
    print(f"Rows with DIIFERNT CURRENCY (non-EUR): {different_rate_count}")
    """
    #joined = joined.withColumn("metrics_cost_micros", col("metrics_cost_micros").cast(FloatType()))
    joined = joined.withColumn("cost_eur", col("metrics_cost_micros") / col("rate"))
    return joined

def aggregate_daily_spend(costs_df):
    daily_spend_validate = costs_df.groupBy("segments_date", "domain", "brand", "channel_type") \
    .agg(
        _sum("cost_eur").alias("total_spend_eur"),
        _sum("metrics_cost_micros").alias("total_metrics_cost"),
        avg("rate").alias("rate"),
        first("currency").alias("currency")

    )
    daily_spend = daily_spend_validate.drop("total_metrics_cost","rate", "currency")
    return daily_spend, daily_spend_validate

def process_pipeline(raw_costs_path, account_info_path, exchange_rates_path):
    spark = create_spark_session()

    run_validations_input_files()

    print("Starting the main pipeline...")
    raw_costs_df, account_info_df, exchange_rates_df = load_data(
        spark, raw_costs_path, account_info_path, exchange_rates_path
    )
    raw_costs_df, account_info_df, exchange_rates_df = preprocess_data(
        raw_costs_df, account_info_df, exchange_rates_df
    )
    raw_costs_df = drop_duplicates_keep_last(raw_costs_df)

    costs_with_accounts = join_costs_with_accounts(raw_costs_df, account_info_df)
    costs_with_eur = add_eur_cost_column(costs_with_accounts, exchange_rates_df)


    """
    filtered_costs = costs_with_eur.filter(
    (col("domain") == "de") &
    (to_date(col("segments_date")) == lit("2025-01-01")) &
    (col("brand") == "FlixTrain") &
    (col("channel_type") == "mixed")
)
    metrics_cost_micros_values = filtered_costs.select("cost_eur").rdd.flatMap(lambda x: x).collect()
    print("values",metrics_cost_micros_values)

    row_count = filtered_costs.count()
    print(f"Number of rows: {row_count}")
    """
    daily_spend,daily_spend_validae = aggregate_daily_spend(costs_with_eur)


    validate_cost_eur_calculation(daily_spend_validae)
    validate_rate_for_eur(daily_spend_validae)
    print("Daily spend aggregation completed.")


    return daily_spend

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: spark-submit transformation.py <raw_costs_path> <account_info_path> <exchange_rates_path>")
        sys.exit(1)

    raw_costs_path = sys.argv[1]
    account_info_path = sys.argv[2]
    exchange_rates_path = sys.argv[3]

    daily_spend_df = process_pipeline(raw_costs_path, account_info_path, exchange_rates_path)
    
    print("---------------------------")
    print(f'processed rows{daily_spend_df.count()}')
    print("---------------------------")
    daily_spend_df.orderBy('segments_date').show(100)

    print("Data processing completed successfully.")
