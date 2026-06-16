from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, count, sum as spark_sum, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BASE = Path(__file__).resolve().parents[1]
INPUT_PATH = str(BASE / "data" / "input" / "claims_raw.csv")
BRONZE_PATH = str(BASE / "data" / "output" / "bronze_claims_parquet")
SILVER_PATH = str(BASE / "data" / "output" / "silver_claims_validated_parquet")
GOLD_PATH = str(BASE / "data" / "output" / "gold_claims_summary_parquet")

schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("provider_id", StringType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("procedure_code", StringType(), True),
    StructField("service_date", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("allowed_amount", DoubleType(), True),
    StructField("paid_amount", DoubleType(), True),
    StructField("claim_status", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("HealthcareClaimsIntelligenceLakehouse")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(INPUT_PATH)
        .withColumn("bronze_loaded_at", current_timestamp())
    )
    bronze_df.write.mode("overwrite").parquet(BRONZE_PATH)

    silver_df = (
        bronze_df
        .filter(col("claim_id").isNotNull())
        .filter(col("member_id").isNotNull())
        .filter(col("provider_id").isNotNull())
        .filter(col("claim_amount") > 0)
        .filter(col("allowed_amount") >= 0)
        .filter(col("paid_amount") >= 0)
        .dropDuplicates(["claim_id"])
        .withColumn("silver_loaded_at", current_timestamp())
    )
    silver_df.write.mode("overwrite").parquet(SILVER_PATH)

    gold_df = (
        silver_df
        .groupBy("claim_status", "source_system")
        .agg(
            count("claim_id").alias("claim_count"),
            spark_sum("claim_amount").alias("total_claim_amount"),
            spark_sum("paid_amount").alias("total_paid_amount"),
            avg("claim_amount").alias("avg_claim_amount"),
        )
    )
    gold_df.write.mode("overwrite").parquet(GOLD_PATH)

    print("PySpark claims lakehouse completed")
    print(f"Bronze records: {bronze_df.count()}")
    print(f"Silver records: {silver_df.count()}")
    print(f"Gold records: {gold_df.count()}")
    gold_df.show(truncate=False)


if __name__ == "__main__":
    main()
