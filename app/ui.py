import sys
import asyncio
from pathlib import Path

from nicegui import ui
from schemas import PredictWithAnalysisRequest
from styles import apply_global_styles, show_loader, hide_loader

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ibc_pipeline.models import SeedCompany
from ibc_pipeline.extractor import parse_metrics_with_yfinance

def build_ui(predict_fn):
    @ui.page('/')
    def main_page():
        apply_global_styles()

        # Main Layout Container
        with ui.column().classes('w-full max-w-5xl mx-auto p-8'):
            
            # Header
            ui.markdown("# Insolvency Analysis").classes('text-5xl mb-2 gradient-text drop-shadow-lg')
            ui.markdown("Enter a stock ticker to automatically fetch financial records from yfinance and run AI analyst predictions, or manually input features.").classes('text-lg opacity-80 mb-6')

            # Search Bar Area
            with ui.row().classes('w-full items-end gap-4 p-6 glass-card'):
                ticker_input = ui.input(label="Ticker (e.g., RELIANCE)", placeholder="Enter ticker symbol...").classes('w-72 text-lg')
                fetch_btn = ui.button("Fetch & Predict", icon="search").classes('h-14 px-8 shadow-lg')

            # Advanced Manual Input
            with ui.expansion("Advanced: Manual Feature Input", icon="tune").classes('w-full mt-6 glass-card'):
                ui.markdown("Manually specify the feature values to run predictions.").classes('opacity-80 mb-4')
                
                with ui.row().classes('w-full gap-8 mb-4'):
                    manual_company_name = ui.input(label="Company Name (Optional)", placeholder="e.g. Acme Corp").classes('flex-1')
                    manual_ticker = ui.input(label="Ticker (Optional)", placeholder="e.g. ACME").classes('flex-1')
                
                with ui.row().classes('w-full gap-8'):
                    with ui.column().classes('flex-1 gap-2'):
                        market_cap = ui.number(label="Market Cap", value=0.0).classes('w-full')
                        total_debt = ui.number(label="Total Debt", value=0.0).classes('w-full')
                        intangible_assets = ui.number(label="Intangible Assets", value=0.0).classes('w-full')
                        cash_and_equivalents = ui.number(label="Cash & Equivalents", value=0.0).classes('w-full')
                        current_liabilities = ui.number(label="Current Liabilities", value=0.0).classes('w-full')
                    with ui.column().classes('flex-1 gap-2'):
                        operating_cash_flow = ui.number(label="Operating Cash Flow", value=0.0).classes('w-full')
                        ebitda = ui.number(label="EBITDA", value=0.0).classes('w-full')
                        interest_expense = ui.number(label="Interest Expense", value=0.0).classes('w-full')
                        net_income = ui.number(label="Net Income", value=0.0).classes('w-full')
                        total_assets = ui.number(label="Total Assets", value=0.0).classes('w-full')
                
                manual_inputs = {
                    "market_cap": market_cap, "total_debt": total_debt,
                    "intangible_assets": intangible_assets, "cash_and_equivalents": cash_and_equivalents,
                    "current_liabilities": current_liabilities, "operating_cash_flow": operating_cash_flow,
                    "ebitda": ebitda, "interest_expense": interest_expense,
                    "net_income": net_income, "total_assets": total_assets
                }
                manual_btn = ui.button("Predict from Manual Inputs", icon="science").classes('mt-6')

            results_header = ui.markdown("## Prediction Results").classes('mt-12 text-3xl gradient-text')
            results_header.set_visibility(False)

            results_container = ui.row().classes('w-full gap-6 items-stretch mt-6')
            results_container.set_visibility(False)
            
            with results_container:
                with ui.card().classes('flex-1 glass-card p-6'):
                    ui.markdown("### Optimal Model").classes('text-2xl mb-4 gradient-text')
                    opt_out = ui.markdown().classes('text-lg leading-relaxed')
                with ui.card().classes('flex-1 glass-card p-6'):
                    ui.markdown("### Tuned Model (>=95% Recall)").classes('text-2xl mb-4 gradient-text')
                    tuned_out = ui.markdown().classes('text-lg leading-relaxed')

            def format_output(pred, insight):
                if not pred or not insight:
                    return "No data generated."
                
                pred_dict = pred.model_dump() if hasattr(pred, "model_dump") else pred.dict()
                insight_dict = insight.model_dump() if hasattr(insight, "model_dump") else insight.dict()

                prediction_text = "BANKRUPT 🚨" if pred_dict.get("prediction") == 1 else "HEALTHY ✅"
                prob = pred_dict.get("probability", 0.0)
                
                verdict = insight_dict.get("verdict", "N/A")
                summary = insight_dict.get("summary", "N/A")
                rationale = insight_dict.get("rationale", "N/A")
                
                return f"**Prediction:** {prediction_text} (Prob: {prob:.2f})\n\n**Verdict:** {verdict}\n\n**Summary:** {summary}\n\n**Rationale:** {rationale}"

            async def fetch_and_predict():
                if not ticker_input.value or not ticker_input.value.strip():
                    ui.notify("Please enter a valid ticker.", type="warning", position="top")
                    return
                
                show_loader()
                await asyncio.sleep(0.1)
                
                try:
                    ticker = ticker_input.value.strip()
                    company = SeedCompany(ticker=ticker, hint_name=ticker)
                    try:
                        data = await asyncio.to_thread(parse_metrics_with_yfinance, company)
                    except Exception as e:
                        ui.notify(f"Failed to fetch data for ticker {ticker}: {e}", type="negative", position="top")
                        return
                    
                    features = {
                        "market_cap": data.get("market_cap"),
                        "total_debt": data.get("total_debt"),
                        "intangible_assets": data.get("intangible_assets"),
                        "cash_and_equivalents": data.get("cash_and_equivalents"),
                        "current_liabilities": data.get("current_liabilities"),
                        "operating_cash_flow": data.get("operating_cash_flow"),
                        "ebitda": data.get("ebitda"),
                        "interest_expense": data.get("interest_expense"),
                        "net_income": data.get("net_income"),
                        "total_assets": data.get("total_assets"),
                    }
                    
                    features = {k: (v if v is not None else 0.0) for k, v in features.items()}

                    payload = PredictWithAnalysisRequest(
                        ticker=ticker,
                        company_name=data.get("company_name", ticker),
                        features=features
                    )
                    
                    try:
                        response = await predict_fn(payload)
                    except Exception as e:
                        ui.notify(f"Error running prediction/analysis: {e}", type="negative", position="top")
                        return
                    
                    opt_out.set_content(format_output(response.predictions.get("optimal"), response.insights.get("optimal")))
                    tuned_out.set_content(format_output(response.predictions.get("tuned"), response.insights.get("tuned")))
                    
                    results_header.set_visibility(True)
                    results_container.set_visibility(True)
                finally:
                    hide_loader()

            async def do_predict_manual():
                show_loader()
                await asyncio.sleep(0.1)
                try:
                    features = {k: (v.value if v.value is not None else 0.0) for k, v in manual_inputs.items()}
                    payload = PredictWithAnalysisRequest(
                        ticker=manual_ticker.value.strip() if manual_ticker.value else "MANUAL",
                        company_name=manual_company_name.value.strip() if manual_company_name.value else "Manual Input",
                        features=features
                    )
                    try:
                        response = await predict_fn(payload)
                    except Exception as e:
                        ui.notify(f"Error running prediction/analysis: {e}", type="negative", position="top")
                        return
                    
                    opt_out.set_content(format_output(response.predictions.get("optimal"), response.insights.get("optimal")))
                    tuned_out.set_content(format_output(response.predictions.get("tuned"), response.insights.get("tuned")))
                    
                    results_header.set_visibility(True)
                    results_container.set_visibility(True)
                finally:
                    hide_loader()

            fetch_btn.on('click', fetch_and_predict)
            manual_btn.on('click', do_predict_manual)

