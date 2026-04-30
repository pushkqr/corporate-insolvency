# Corporate Insolvency Risk Modeling

A data engineering pipeline and machine learning project designed to predict corporate bankruptcy in the Indian stock market. This project identifies distressed companies by analyzing forensic accounting indicators, such as inflated intangible assets and cash flow divergence, rather than relying solely on basic profitability metrics.

## Project Structure

- **`build_dataset.py`**: An automated pipeline that extracts terminal-year financial data using `yfinance` (listed) or PDF parsing + LLM extraction (bankrupt).
- **`code.ipynb`**: A complete Jupyter Notebook that cleans the data, engineers forensic features, evaluates predictive models (Logistic Regression vs. Random Forest), and uses clustering to categorize the different ways companies fail.

## Setup and Installation

1. Install the required dependencies:

   ```powershell
   pip install -r requirements.txt
   ```

2. Create a `.env` file in the root directory to configure the LLM endpoint for PDF parsing:

   ```env
   MODEL_NAME=gemini-2.5-flash
   GOOGLE_API_KEY=your_google_api_key_here
   ```

## Execution Guide

### 1. Build the Dataset

Run the data pipeline to generate `insolvency_raw.csv`.

Standard run:

```powershell
python build_dataset.py
```

Bankrupt pipeline (PDF + Gemini):

```powershell
python build_dataset.py --pipeline bankrupt --output insolvency_dataset.csv --bankrupt-seed-csv ibc_pipeline/bankrupt_seeds.csv
```

Bankrupt PDF modules live in ibc_pipeline/pipeline.py with supporting helpers:

- ibc_pipeline/pdf_fetcher.py
- ibc_pipeline/pdf_extractor.py
- ibc_pipeline/llm_extractor.py

Extract only:

```powershell
python build_dataset.py --mode extract --output insolvency_dataset.csv --seed-csv ibc_pipeline/seeds.csv --limit 100 --sleep-seconds 0.1
```

### 2. Run the Analysis

Open `code.ipynb` in VS Code or Jupyter Notebook and run all cells sequentially from top to bottom. This process will:

1. Output the cleaned dataset (`insolvency_clean.csv`).
2. Generate Exploratory Data Analysis (EDA) charts.
3. Train the predictive models and output performance metrics (PR-AUC, ROC-AUC, Recall).
4. Apply business-logic thresholding for high-sensitivity screening.
5. Generate K-Means/Agglomerative clustering visualizations for failure archetypes.
