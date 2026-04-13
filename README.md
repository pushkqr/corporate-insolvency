# IBC Dataset Builder

Compact pipeline to build an India-focused insolvency dataset.

## What it does

1. Extracts company fundamentals and business text from yfinance (NSE symbols via `.NS`).
2. Enriches each row with:
   - `ibc_liquidation_risk` (`High`/`Medium`/`Low`)
   - `asset_illusion_rationale` (one sentence)
3. Uses endpoint failover in enrich mode:
   - Frontier first (`BASE_URL`, `MODEL_NAME`, `GOOGLE_API_KEY`)
   - Local fallback on error (`LOCAL_URL`, `LOCAL_MODEL_NAME`, `OPENAI_API_KEY`)

## Requirements

- Python 3.10+
- `pip install -r requirements.txt`

## Environment

Create a `.env` file:

```env
# Frontier endpoint (primary)
BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai/
MODEL_NAME=gemini-2.5-flash
GOOGLE_API_KEY=your_google_api_key

# Local endpoint (fallback)
LOCAL_URL=http://localhost:11434/v1
LOCAL_MODEL_NAME=qwen2.5:3b
OPENAI_API_KEY=local-placeholder-key

# Optional
DEBUG=false
```

Notes:

- Keep `OPENAI_API_KEY` set for local fallback clients (Ollama accepts placeholder values).
- Enrichment automatically switches to local endpoint if frontier calls fail.

## Seed CSV format

Pass `--seed-csv` with columns:

```csv
ticker,company_name
RELIANCE,Reliance Industries
TCS,Tata Consultancy Services
```

- `ticker` is required.
- `company_name` is optional.

## Run

Extract only:

```powershell
python build_dataset.py --mode extract --output insolvency_dataset.csv --seed-csv ibc_pipeline/seeds.csv --limit 100 --sleep-seconds 0.1
```

Enrich only:

```powershell
python build_dataset.py --mode enrich --input insolvency_dataset.csv --enriched-output insolvency_dataset_enriched.csv --limit 100 --sleep-seconds 0.1
```

Extract + enrich:

```powershell
python build_dataset.py --mode all --output insolvency_dataset.csv --enriched-output insolvency_dataset_enriched.csv --seed-csv ibc_pipeline/seeds.csv --limit 100 --sleep-seconds 0.1
```

Safer local inference mode (reduced generation pressure):

```powershell
python build_dataset.py --mode enrich --input insolvency_dataset.csv --enriched-output insolvency_dataset_enriched.csv --safe-mode
```

## Output files

- Base dataset: `insolvency_dataset.csv`
- Enriched dataset: `insolvency_dataset_enriched.csv`

## Project entrypoint

- `build_dataset.py`
