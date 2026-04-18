# IBC Dataset Builder + Insolvency Modeling Notebook

This repository contains two connected workflows:

1. Dataset construction and enrichment pipeline for India-focused insolvency data.
2. End-to-end notebook analysis and modeling for insolvency risk screening.

## Workflow A: Dataset Builder

The builder script:

1. Extracts company fundamentals and business text from yfinance (NSE symbols via .NS).
2. Enriches each row with:
   - ibc_liquidation_risk (High/Medium/Low)
   - asset_illusion_rationale (one sentence)
3. Uses endpoint failover in enrich mode:
   - Frontier first (BASE_URL, MODEL_NAME, GOOGLE_API_KEY)
   - Local fallback on error (LOCAL_URL, LOCAL_MODEL_NAME, OPENAI_API_KEY)
4. In --safe-mode, frontier calls are throttled to 10 requests/minute before fallback logic applies.

### Requirements

- Python 3.10+
- pip install -r requirements.txt

### Environment

Create a .env file:

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

- Keep OPENAI_API_KEY set for local fallback clients (Ollama accepts placeholder values).
- Enrichment automatically switches to local endpoint if frontier calls fail.
- --safe-mode does two things:
  - Frontier: rate limits requests to 10/min.
  - Local fallback: uses conservative generation settings (max_tokens, reduced local options) for stability.

### Seed CSV format

Pass --seed-csv with columns:

```csv
ticker,company_name
RELIANCE,Reliance Industries
TCS,Tata Consultancy Services
```

- ticker is required.
- company_name is optional.

### Run dataset pipeline

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

Safe mode (frontier rate-limited + safer local fallback):

```powershell
python build_dataset.py --mode enrich --input insolvency_dataset.csv --enriched-output insolvency_dataset_enriched.csv --safe-mode
```

## Workflow B: Notebook Analysis and Modeling

The notebook code.ipynb now contains the complete modeling workflow since the last commit:

1. Data cleaning and imputation from insolvency_raw.csv.
2. Export of cleaned analysis dataset to insolvency_clean.csv.
3. Forensic feature engineering (including tangible asset coverage and cash-flow divergence features).
4. EDA with class balance, missingness checks, scaled ratio distributions, pairwise relationships, and interaction panels.
5. Model comparison:
   - Logistic Regression baseline
   - Random Forest non-linear model
6. Train CV + holdout evaluation with PR-AUC, ROC-AUC, recall, precision, F1, and balanced accuracy.
7. Business-threshold tuning to prioritize high bankrupt-recall screening.
8. Side-by-side confusion matrix comparison (default threshold vs business-tuned threshold).
9. Bootstrap confidence intervals for holdout PR-AUC and tuned recall.

### Run notebook workflow

1. Ensure dependencies are installed:

```powershell
pip install -r requirements.txt
```

2. Open code.ipynb in VS Code or Jupyter.

3. Run cells top-to-bottom so generated variables and outputs are available for later sections.

### Notebook outputs

- Input dataset: insolvency_raw.csv
- Cleaned dataset artifact: insolvency_clean.csv
- Visual outputs: EDA charts, pairplots, confusion matrices, precision-recall plots
- Model outputs: CV summary, holdout summary, tuned threshold metrics, bootstrap CIs

## Key files

- build_dataset.py
- code.ipynb
- insolvency_raw.csv
- insolvency_clean.csv
- requirements.txt
