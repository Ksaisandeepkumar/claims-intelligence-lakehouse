import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()

OUTPUT_PATH = "data/input/claims_raw.csv"
os.makedirs("data/input", exist_ok=True)


def generate_claim(claim_id: int) -> dict:
    service_date = datetime.now() - timedelta(days=random.randint(1, 365))
    claim_amount = round(random.uniform(50, 15000), 2)
    allowed_amount = round(claim_amount * random.uniform(0.45, 0.9), 2)
    paid_amount = round(allowed_amount * random.uniform(0.3, 1.0), 2)

    return {
        "claim_id": f"CLM{claim_id:07d}",
        "member_id": f"MBR{random.randint(10000, 99999)}",
        "provider_id": f"PRV{random.randint(1000, 9999)}",
        "diagnosis_code": random.choice(["E11.9", "I10", "J45.909", "M54.5", "R07.9"]),
        "procedure_code": random.choice(["99213", "99214", "80053", "93000", "36415"]),
        "service_date": service_date.strftime("%Y-%m-%d"),
        "claim_amount": claim_amount,
        "allowed_amount": allowed_amount,
        "paid_amount": paid_amount,
        "claim_status": random.choice(["PAID", "DENIED", "PENDING"]),
        "source_system": random.choice(["FACETS", "QNXT", "EDI_837"]),
        "ingestion_timestamp": datetime.now().isoformat(timespec="seconds"),
    }


def main() -> None:
    records = [generate_claim(i) for i in range(1, 1001)]

    # Add duplicate claim examples
    records.append(records[10].copy())
    records.append(records[25].copy())

    # Add invalid examples
    invalid_claim = records[50].copy()
    invalid_claim["claim_id"] = None
    records.append(invalid_claim)

    invalid_amount = records[75].copy()
    invalid_amount["claim_amount"] = -500
    records.append(invalid_amount)

    df = pd.DataFrame(records)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Generated {len(df)} raw claim records")
    print(f"Output written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
