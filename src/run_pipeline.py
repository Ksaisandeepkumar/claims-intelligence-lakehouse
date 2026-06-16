import os
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
INPUT = BASE / "data" / "input"
OUTPUT = BASE / "data" / "output"
INPUT.mkdir(parents=True, exist_ok=True)
OUTPUT.mkdir(parents=True, exist_ok=True)


def main():
    raw_path = INPUT / "claims_raw.csv"
    if not raw_path.exists():
        print("Run this first: python src/generate_claims.py")
        return

    df = pd.read_csv(raw_path)
    bronze_path = OUTPUT / "bronze_claims.csv"
    df.to_csv(bronze_path, index=False)

    silver = df.dropna(subset=["claim_id", "member_id", "provider_id"])
    silver = silver[silver["claim_amount"] > 0]
    silver = silver.drop_duplicates(subset=["claim_id"])
    silver_path = OUTPUT / "silver_claims_validated.csv"
    silver.to_csv(silver_path, index=False)

    gold = (
        silver.groupby(["claim_status", "source_system"], as_index=False)
        .agg(
            claim_count=("claim_id", "count"),
            total_claim_amount=("claim_amount", "sum"),
            total_paid_amount=("paid_amount", "sum"),
            avg_claim_amount=("claim_amount", "mean"),
        )
    )
    gold_path = OUTPUT / "gold_claims_summary.csv"
    gold.to_csv(gold_path, index=False)

    print("Healthcare claims lakehouse pipeline completed")
    print(f"Bronze: {bronze_path}")
    print(f"Silver: {silver_path}")
    print(f"Gold: {gold_path}")


if __name__ == "__main__":
    main()
