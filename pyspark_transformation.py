from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, when, lower, regexp_replace
from pyspark.sql.types import FloatType

def create_spark_session():
    return SparkSession.builder.appName("CostDataPipeline").getOrCreate()

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

    raw_costs_df = raw_costs_df.withColumn("segments_date", to_date("segments_date"))
    exchange_rates_df = exchange_rates_df \
        .withColumn("valid_from", to_date("valid_from")) \
        .withColumn("valid_to", to_date("valid_to")) \
        .withColumn("rate", col("rate").cast(FloatType()))

    return raw_costs_df, account_info_df, exchange_rates_df

def join_costs_with_accounts(raw_costs_df, account_info_df):
    return raw_costs_df.join(
        account_info_df,
        raw_costs_df["customer_id"] == account_info_df["account_id"],
        how="left"
    )

def add_eur_cost_column(costs_df, exchange_rates_df):
    # Handle 'EUR' directly
    costs_df = costs_df.withColumn("segments_date", to_date("segments_date"))
    exchange_rates_df = exchange_rates_df.withColumn("valid_from", to_date("valid_from")) \
                                         .withColumn("valid_to", to_date("valid_to"))

    # Cross join workaround - okay for small data
    joined = costs_df.crossJoin(exchange_rates_df).filter(
        (col("currency") == col("currency")) &
        (col("base") == lit("EUR")) &
        (col("segments_date") >= col("valid_from")) &
        (col("segments_date") <= col("valid_to"))
    )

    joined = joined.withColumn("rate", when(col("currency") == "EUR", lit(1.0)).otherwise(col("rate")))
    joined = joined.withColumn("metrics_cost_micros", col("metrics_cost_micros").cast(FloatType()))
    joined = joined.withColumn("cost_eur", col("metrics_cost_micros") / col("rate"))

    return joined

def aggregate_daily_spend(costs_df):
    return costs_df.groupBy("segments_date", "domain", "brand", "channel_type") \
                   .sum("cost_eur") \
                   .withColumnRenamed("sum(cost_eur)", "total_spend_eur")

def process_pipeline(raw_costs_path, account_info_path, exchange_rates_path):
    spark = create_spark_session()
    raw_costs_df, account_info_df, exchange_rates_df = load_data(
        spark, raw_costs_path, account_info_path, exchange_rates_path
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
        print("Usage: spark-submit transformation.py <raw_costs_path> <account_info_path> <exchange_rates_path>")
        sys.exit(1)

    raw_costs_path = sys.argv[1]
    account_info_path = sys.argv[2]
    exchange_rates_path = sys.argv[3]

    daily_spend_df = process_pipeline(raw_costs_path, account_info_path, exchange_rates_path)
    daily_spend_df.show(10)
    print("Data processing completed successfully.")
