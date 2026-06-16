from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

BASE = Path(__file__).resolve().parents[1]
INPUT_PATH = str(BASE / "data" / "input" / "claims_raw.csv")
BRONZE_PATH = str(BASE / "data" / "output" / "bronze_claims_parquet")
SILVER_PATH = str(BASE / "data" / "output" / "silver_claims_validated_parquet")
GOLD_PATH = str(BASE / "data" / "output" / "gold_claims_summary_parquet")


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ClaimsLakehouseProjectSummary")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_safe(spark, path, label, file_type="parquet"):
    try:
        if file_type == "csv":
            df = spark.read.option("header", True).option("inferSchema", True).csv(path)
        else:
            df = spark.read.parquet(path)
        print(f"\n{label} loaded successfully")
        return df
    except Exception as exc:
        print(f"\n{label} not available yet: {exc}")
        return None


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_safe(spark, INPUT_PATH, "Raw claims CSV", "csv")
    bronze_df = read_safe(spark, BRONZE_PATH, "Bronze claims")
    silver_df = read_safe(spark, SILVER_PATH, "Silver validated claims")
    gold_df = read_safe(spark, GOLD_PATH, "Gold claims summary")

    if raw_df is not None:
        print("\n===== RAW CLAIMS SUMMARY =====")
        print("Raw record count:", raw_df.count())

        print("\nDuplicate claim_id proof in raw data:")
        raw_df.groupBy("claim_id").agg(count("*").alias("record_count")) \
            .filter(col("claim_id").isNotNull()) \
            .filter(col("record_count") > 1) \
            .show(truncate=False)

        print("\nInvalid raw claim records:")
        raw_df.filter(
            col("claim_id").isNull()
            | col("member_id").isNull()
            | col("provider_id").isNull()
            | (col("claim_amount") <= 0)
        ).show(truncate=False)

    if bronze_df is not None:
        print("\n===== BRONZE SUMMARY =====")
        print("Bronze count:", bronze_df.count())

    if silver_df is not None:
        print("\n===== SILVER SUMMARY =====")
        print("Silver count:", silver_df.count())
        print("Unique claim_id count:", silver_df.select("claim_id").distinct().count())

        print("\nDuplicate proof after Silver deduplication:")
        silver_df.groupBy("claim_id").agg(count("*").alias("record_count")) \
            .filter(col("record_count") > 1) \
            .show(truncate=False)

        print("\nInvalid record proof after Silver validation:")
        silver_df.filter(
            col("claim_id").isNull()
            | col("member_id").isNull()
            | col("provider_id").isNull()
            | (col("claim_amount") <= 0)
        ).show(truncate=False)

    if gold_df is not None:
        print("\n===== GOLD SUMMARY =====")
        print("Gold count:", gold_df.count())
        print("\nSample Gold output:")
        gold_df.show(20, truncate=False)


if __name__ == "__main__":
    main()
