# Healthcare Claims Intelligence Lakehouse

## Business Problem
Healthcare claim systems receive large volumes of claim, member, provider, diagnosis, procedure, and payment data. Raw claim records may include duplicates, invalid amounts, missing identifiers, and inconsistent source-system values.

## Objective
Build a claims intelligence lakehouse that ingests raw claims, validates records, removes duplicates, and produces analytics-ready claim metrics.

## Architecture
```text
Raw Claims CSV
  -> Bronze Claims
  -> Silver Validated Claims
  -> Gold Claims Summary
```

## Key Data Engineering Concepts
- Bronze/Silver/Gold architecture
- Claims validation
- Duplicate removal
- Aggregated reporting layer
- Healthcare domain modeling

## How to Run
```bash
python src/generate_claims.py
python src/run_pipeline.py
```

## Resume Bullet
Built a healthcare claims intelligence lakehouse that ingests raw claim records, applies validation and deduplication rules, and produces Gold-level claim metrics by claim status and source system.
