from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    when,
    broadcast,
    row_number,
    monotonically_increasing_id,
    desc,
    sum,
    avg,
    first
)




import logging

logging.basicConfig(
    filename='/app/spark_app/logs/spark_job.log', 
    filemode='w',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)
logger.info(" Starting Logging")

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



def run_validations_input_files(raw_costs_path, account_info_path, exchange_rates_path):
    try:
        validate_raw_costs(raw_costs_path)
        validate_exchange_rates(exchange_rates_path)
        validate_account_info(account_info_path)
        validate_customer_account_relationship(raw_costs_path, account_info_path)
        validate_duplicates_in_raw_costs(raw_costs_path)
        logger.info("All validations passed!")
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        sys.exit(1)
        

def load_data(spark, raw_costs_path, account_info_path, exchange_rates_path):
    logger.info("Function load_data called.")
    try:
        raw_costs_df = spark.read.option("header", True).csv(raw_costs_path)
        account_info_df = spark.read.option("header", True).csv(account_info_path)
        exchange_rates_df = spark.read.option("header", True).csv(exchange_rates_path)
        return raw_costs_df, account_info_df, exchange_rates_df
    except Exception as e:
        logger.error(f"Error loading data: {e}", exc_info=True)
        sys.exit(1)
        





def preprocess_data(raw_costs_df, account_info_df, exchange_rates_df):
    try:
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
    except Exception as e:
        logger.error(f"Error preprocessing the data: {e}", exc_info=True)    
        sys.exit(1)
    return raw_costs_df, account_info_df, exchange_rates_df


def drop_duplicates_keep_last(raw_costs_df):
    
    """
    Drops duplicate rows in raw_costs_df, keeping the last occurrence based on last_extracted_at.
    """
    try:
        # Add a unique row identifier to preserve original order for tie-breaking
        raw_costs_with_order = raw_costs_df.withColumn(
            "original_order", 
            monotonically_increasing_id()
        )

        window_spec = Window.partitionBy(
            "campaign_id", "segments_date", "customer_id"
        ).orderBy(
            desc("last_extracted_at"), 
            desc("original_order")  
        )

        
        df_with_rownum = raw_costs_with_order.withColumn("row_num", row_number().over(window_spec))
        raw_costs_last_extracted = df_with_rownum.filter("row_num = 1").drop("row_num","original_order")
    except Exception as e:
        logger.error(f"Error dropping duplicates: {e}", exc_info=True)
        sys.exit(1)

    return raw_costs_last_extracted

def join_costs_with_accounts(raw_costs_df, account_info_df):

    try:
        return raw_costs_df.join(
            account_info_df,
            raw_costs_df["customer_id"] == account_info_df["account_id"],
            how="left"
        )
    except Exception as e:
        logger.error(f"Error joining costs with accounts: {e}", exc_info=True)
        sys.exit(1)
def add_eur_cost_column(costs_df, exchange_rates_df):

    try:
        cost_exchange_rate = costs_df.join(
            broadcast(exchange_rates_df),
            (costs_df.currency == exchange_rates_df.currency) &
            (costs_df.segments_date >= exchange_rates_df.valid_from) &
            (costs_df.segments_date <= exchange_rates_df.valid_to) &
            (exchange_rates_df.base == 'EUR'), how="left").drop(exchange_rates_df.currency)

        cost_exchange_rate = cost_exchange_rate.withColumn(
        "rate",
        when(col("currency") == "EUR", lit(1)).otherwise(col("rate"))
        )
        cost_exchange_rate = cost_exchange_rate.withColumn("cost_eur", col("metrics_cost_micros") / col("rate"))
    except Exception as e:
        logger.error(f"Error adding EUR cost column: {e}", exc_info=True)
        sys.exit(1)    
    return cost_exchange_rate

def aggregate_daily_spend(costs_df):

    try:
        daily_spend_validate = costs_df.groupBy("segments_date", "domain", "brand", "channel_type") \
        .agg(
            sum("cost_eur").alias("total_spend_eur"),
            sum("metrics_cost_micros").alias("total_metrics_cost"),
            avg("rate").alias("rate"),
            first("currency").alias("currency")

        )
        daily_spend = daily_spend_validate.drop("total_metrics_cost","rate", "currency")
    except Exception as e:
        logger.error(f"Error aggregating daily spend: {e}", exc_info=True)
        sys.exit(1)    
    return daily_spend, daily_spend_validate

def write_df_to_file(df_spend, path='/app/spark_app/output/daily_spend_spark.txt'):

    try:
        df_spend = (
        df_spend
        .orderBy(["segments_date", "total_spend_eur"]) \
        .withColumnRenamed("domain", "country")\
        .withColumn("segments_date", col("segments_date").cast("string"))
        )
        df_spend_pandas = df_spend.toPandas()
        
        string_representation = df_spend_pandas.to_string(index=False,float_format="%.2f")
        logger.info(f"Writing output to: {path}")
        with open(path, 'w') as f:
            f.write(string_representation)
    except Exception as e:
        logger.error(f"Error writing DataFrame to file: {e}", exc_info=True)
        sys.exit(1)        
    return None


def process_pipeline(raw_costs_path, account_info_path, exchange_rates_path):
    spark = create_spark_session()

    logger.info("Starting the validation of input files...")
    run_validations_input_files(raw_costs_path, account_info_path, exchange_rates_path)
    logger.info("Input files validation completed successfully.")

    logger.info("Starting the main pipeline...")
    raw_costs_df, account_info_df, exchange_rates_df = load_data(
        spark, raw_costs_path, account_info_path, exchange_rates_path
    )
    raw_costs_df, account_info_df, exchange_rates_df = preprocess_data(
        raw_costs_df, account_info_df, exchange_rates_df
    )
    raw_costs_df = drop_duplicates_keep_last(raw_costs_df)

    costs_with_accounts = join_costs_with_accounts(raw_costs_df, account_info_df)
    costs_with_eur = add_eur_cost_column(costs_with_accounts, exchange_rates_df)

    daily_spend,daily_spend_validate = aggregate_daily_spend(costs_with_eur)
    logger.info("Daily spend aggregation completed.")

    logger.info("validation on output is starting")

    validate_cost_eur_calculation(daily_spend_validate)
    validate_rate_for_eur(daily_spend_validate)
    
    logger.info("Output validation completed successfully.")

    return daily_spend

# Save the result to a CSV file

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        logger.info("Provide the 3 files to the  transformation.py <raw_costs_path> <account_info_path> <exchange_rates_path>")
        sys.exit(1)

    raw_costs_path = sys.argv[1]
    account_info_path = sys.argv[2]
    exchange_rates_path = sys.argv[3]

    daily_spend_df = process_pipeline(raw_costs_path, account_info_path, exchange_rates_path)

    logger.info("Pipeline processing completed. Writing output to file...")
    write_df_to_file(daily_spend_df)
    logger.info("Output written to file successfully, check /app/output/daily_spend_spark.txt")

    logger.info("Pipeline completed successfully.")
