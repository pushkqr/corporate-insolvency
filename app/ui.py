import sys
import asyncio
from pathlib import Path

from nicegui import ui
from schemas import PredictWithAnalysisRequest
from styles import apply_global_styles, show_loader, hide_loader

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import json
import os
import tempfile
from ibc_pipeline.models import SeedCompany
from ibc_pipeline.extractor import parse_metrics_with_yfinance
from ibc_pipeline.pdf_extractor import extract_financial_cherrypick
from ibc_pipeline.llm_extractor import extract_metrics_with_gemini

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
            with ui.row().classes('w-full items-end gap-4 p-6 trading-card'):
                ticker_input = ui.input(label="Ticker (e.g., RELIANCE)", placeholder="Enter ticker symbol...").classes('w-72 text-lg trading-input')
                fetch_btn = ui.button("Fetch & Predict", icon="search").classes('h-14 px-8')

            # Advanced Manual Input
            with ui.expansion("Advanced: Manual Feature Input", icon="tune").classes('w-full mt-6 trading-card'):
                ui.markdown("Manually specify the feature values to run predictions, or drop a PDF Annual Report to auto-extract them using Gemini.").classes('opacity-80 mb-4')
                
                pdf_upload = ui.upload(
                    label="Drop Annual Report PDF Here",
                    auto_upload=True,
                    multiple=False,
                    on_upload=lambda e: handle_pdf_upload(e)
                ).classes('w-full mb-6 border-2 border-dashed border-[#4b5563] bg-transparent hover:bg-[#1f2937] transition-colors')
                
                with ui.row().classes('w-full gap-8 mb-4'):
                    manual_company_name = ui.input(label="Company Name *", placeholder="e.g. Acme Corp").classes('flex-1 trading-input')
                    manual_ticker = ui.input(label="Ticker *", placeholder="e.g. ACME").classes('flex-1 trading-input')
                
                with ui.row().classes('w-full gap-8'):
                    with ui.column().classes('flex-1 gap-2'):
                        market_cap = ui.number(label="Market Cap", value=0.0).classes('w-full trading-input')
                        total_debt = ui.number(label="Total Debt", value=0.0).classes('w-full trading-input')
                        intangible_assets = ui.number(label="Intangible Assets", value=0.0).classes('w-full trading-input')
                        cash_and_equivalents = ui.number(label="Cash & Equivalents", value=0.0).classes('w-full trading-input')
                        current_liabilities = ui.number(label="Current Liabilities", value=0.0).classes('w-full trading-input')
                    with ui.column().classes('flex-1 gap-2'):
                        operating_cash_flow = ui.number(label="Operating Cash Flow", value=0.0).classes('w-full trading-input')
                        ebitda = ui.number(label="EBITDA", value=0.0).classes('w-full trading-input')
                        interest_expense = ui.number(label="Interest Expense", value=0.0).classes('w-full trading-input')
                        net_income = ui.number(label="Net Income", value=0.0).classes('w-full trading-input')
                        total_assets = ui.number(label="Total Assets", value=0.0).classes('w-full trading-input')
                
                manual_inputs = {
                    "market_cap": market_cap, "total_debt": total_debt,
                    "intangible_assets": intangible_assets, "cash_and_equivalents": cash_and_equivalents,
                    "current_liabilities": current_liabilities, "operating_cash_flow": operating_cash_flow,
                    "ebitda": ebitda, "interest_expense": interest_expense,
                    "net_income": net_income, "total_assets": total_assets
                }
                manual_btn = ui.button("Predict from Manual Inputs", icon="science").classes('mt-6 btn-secondary')

            async def handle_pdf_upload(e):
                show_loader()
                ui.notify("Processing PDF... This may take a minute.", type="info", position="top")
                await asyncio.sleep(0.1)
                
                try:
                    pdf_bytes = await e.file.read()
                    
                    with tempfile.TemporaryDirectory() as temp_dir:
                        raw_pdf_path = os.path.join(temp_dir, "raw.pdf")
                        cherry_pdf_path = os.path.join(temp_dir, "cherry.pdf")
                        
                        with open(raw_pdf_path, "wb") as f:
                            f.write(pdf_bytes)
                            
                        success, mode, stmts, pages = await asyncio.to_thread(
                            extract_financial_cherrypick, raw_pdf_path, cherry_pdf_path
                        )
                        
                        target_pdf = cherry_pdf_path if success else raw_pdf_path
                        
                        if success:
                            ui.notify(f"Cherrypicked {mode} statements: {','.join(stmts)}. Extracting metrics...", type="positive", position="top")
                        else:
                            ui.notify("Could not find standard statements. Extracting from raw PDF...", type="warning", position="top")
                        
                        json_str = await asyncio.to_thread(extract_metrics_with_gemini, target_pdf)
                        
                        try:
                            if "```json" in json_str:
                                json_str = json_str.split("```json")[1].split("```")[0].strip()
                            elif "```" in json_str:
                                json_str = json_str.split("```")[1].split("```")[0].strip()
                                
                            extracted = json.loads(json_str)
                            
                            for key, input_field in manual_inputs.items():
                                val = extracted.get(key)
                                if val is not None:
                                    input_field.value = float(val)
                                    
                            ui.notify("Successfully extracted and populated metrics!", type="positive", position="top")
                        except Exception as parse_e:
                            ui.notify(f"Failed to parse Gemini response: {parse_e}", type="negative", position="top")
                            
                except Exception as ex:
                    ui.notify(f"Error processing PDF: {ex}", type="negative", position="top")
                finally:
                    hide_loader()
                    pdf_upload.reset()

            results_header = ui.markdown("## Prediction Results").classes('mt-12 text-3xl')
            results_header.set_visibility(False)

            import math
            def symlog(x):
                return math.copysign(math.log10(abs(x) + 1), x) if x else 0.0

            features_container = ui.row().classes('w-full gap-6 mt-6 items-stretch')
            features_container.set_visibility(False)
            with features_container:
                with ui.card().classes('flex-[3] trading-card p-6'):
                    ui.markdown("### Extracted Financial Metrics").classes('text-2xl mb-4 text-[#9ca3af]')
                    features_table = ui.table(
                        columns=[
                            {'name': 'feature', 'label': 'Metric', 'field': 'feature', 'required': True, 'align': 'left'},
                            {'name': 'value', 'label': 'Value', 'field': 'value', 'sortable': True, 'align': 'right'}
                        ],
                        rows=[],
                        row_key='feature'
                    ).classes('w-full mono-text bg-transparent text-[#e5e7eb]')
                
                with ui.card().classes('flex-[2] trading-card p-6 flex flex-col'):
                    ui.markdown("### Feature Radar Profile").classes('text-2xl mb-4 text-[#9ca3af]')
                    feature_radar = ui.echart({
                        'tooltip': {},
                        'radar': {
                            'indicator': [
                                {'name': 'Market Cap', 'min': -12, 'max': 12},
                                {'name': 'Debt', 'min': -12, 'max': 12},
                                {'name': 'Intangibles', 'min': -12, 'max': 12},
                                {'name': 'Cash', 'min': -12, 'max': 12},
                                {'name': 'Liab.', 'min': -12, 'max': 12},
                                {'name': 'Op. CF', 'min': -12, 'max': 12},
                                {'name': 'EBITDA', 'min': -12, 'max': 12},
                                {'name': 'Int. Exp.', 'min': -12, 'max': 12},
                                {'name': 'Net Inc.', 'min': -12, 'max': 12},
                                {'name': 'Assets', 'min': -12, 'max': 12}
                            ],
                            'splitNumber': 4,
                            'axisName': {'color': '#9ca3af', 'fontFamily': 'Roboto Mono'},
                            'splitArea': {'areaStyle': {'color': ['#1a1a1a', '#222222']}},
                            'splitLine': {'lineStyle': {'color': '#333333'}},
                            'axisLine': {'lineStyle': {'color': '#333333'}}
                        },
                        'series': [{
                            'name': 'Financial Profile',
                            'type': 'radar',
                            'data': [{'value': [0]*10, 'name': 'SymLog Magnitude'}],
                            'itemStyle': {'color': '#2563eb'},
                            'areaStyle': {'color': 'rgba(37, 99, 235, 0.4)'}
                        }]
                    }).classes('w-full flex-1 min-h-[350px]')

            results_container = ui.row().classes('w-full gap-6 items-stretch mt-6')
            results_container.set_visibility(False)
            
            with results_container:
                with ui.card().classes('flex-1 trading-card p-6'):
                    ui.markdown("### Optimal Model").classes('text-2xl mb-4 text-[#9ca3af]')
                    opt_out = ui.html().classes('text-lg leading-relaxed mt-4')
                with ui.card().classes('flex-1 trading-card p-6'):
                    ui.markdown("### Tuned Model (>=95% Recall)").classes('text-2xl mb-4 text-[#9ca3af]')
                    tuned_out = ui.html().classes('text-lg leading-relaxed mt-4')

            def format_output(pred, insight):
                if not pred or not insight:
                    return "No data generated."
                
                pred_dict = pred.model_dump() if hasattr(pred, "model_dump") else pred.dict()
                insight_dict = insight.model_dump() if hasattr(insight, "model_dump") else insight.dict()

                is_bankrupt = pred_dict.get("prediction") == 1
                prediction_text = "BANKRUPT" if is_bankrupt else "HEALTHY"
                color_class = "#ef4444" if is_bankrupt else "#10b981"
                prob = pred_dict.get("probability", 0.0)
                
                verdict = insight_dict.get("verdict", "N/A")
                summary = insight_dict.get("summary", "N/A")
                rationale = insight_dict.get("rationale", "N/A")
                
                return f'''
                <div style="font-family: 'Inter', sans-serif;">
                    <div style="margin-bottom: 20px;">
                        <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
                            <span>
                                <span style="color: #9ca3af; font-weight: 600;">STATUS:</span> 
                                <span style="color: {color_class}; font-weight: bold; font-family: 'Roboto Mono', monospace;">[{prediction_text}]</span>
                            </span>
                            <span style="color: #6b7280; font-family: 'Roboto Mono', monospace;">(Prob: {prob:.2f})</span>
                        </div>
                        <div style="width: 100%; background-color: #333; height: 6px; border-radius: 2px;">
                            <div style="width: {prob*100}%; background-color: {color_class}; height: 100%; border-radius: 2px;"></div>
                        </div>
                    </div>
                    <div style="margin-bottom: 15px;">
                        <span style="color: #9ca3af; font-weight: 600;">VERDICT:</span> {verdict}
                    </div>
                    <div style="margin-bottom: 15px;">
                        <span style="color: #9ca3af; font-weight: 600;">SUMMARY:</span> <span style="color: #e5e7eb;">{summary}</span>
                    </div>
                    <div>
                        <span style="color: #9ca3af; font-weight: 600;">RATIONALE:</span> <span style="color: #e5e7eb;">{rationale}</span>
                    </div>
                </div>
                '''

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
                    
                    opt_pred = response.predictions.get("optimal")
                    tuned_pred = response.predictions.get("tuned")

                    opt_out.set_content(format_output(opt_pred, response.insights.get("optimal")))
                    tuned_out.set_content(format_output(tuned_pred, response.insights.get("tuned")))
                    
                    features_table.rows = [{'feature': k.replace("_", " ").title(), 'value': f"{v:,.2f}"} for k, v in features.items()]
                    features_table.update()
                    
                    radar_values = [
                        symlog(features.get("market_cap", 0)),
                        symlog(features.get("total_debt", 0)),
                        symlog(features.get("intangible_assets", 0)),
                        symlog(features.get("cash_and_equivalents", 0)),
                        symlog(features.get("current_liabilities", 0)),
                        symlog(features.get("operating_cash_flow", 0)),
                        symlog(features.get("ebitda", 0)),
                        symlog(features.get("interest_expense", 0)),
                        symlog(features.get("net_income", 0)),
                        symlog(features.get("total_assets", 0))
                    ]
                    feature_radar.options['series'][0]['data'][0]['value'] = radar_values
                    feature_radar.update()
                    
                    features_container.set_visibility(True)

                    results_header.set_visibility(True)
                    results_container.set_visibility(True)
                finally:
                    hide_loader()

            async def do_predict_manual():
                if not manual_company_name.value or not manual_company_name.value.strip() or not manual_ticker.value or not manual_ticker.value.strip():
                    ui.notify("Company Name and Ticker are required for manual prediction.", type="warning", position="top")
                    return
                    
                show_loader()
                await asyncio.sleep(0.1)
                try:
                    features = {k: (v.value if v.value is not None else 0.0) for k, v in manual_inputs.items()}
                    payload = PredictWithAnalysisRequest(
                        ticker=manual_ticker.value.strip(),
                        company_name=manual_company_name.value.strip(),
                        features=features
                    )
                    try:
                        response = await predict_fn(payload)
                    except Exception as e:
                        ui.notify(f"Error running prediction/analysis: {e}", type="negative", position="top")
                        return
                    
                    opt_pred = response.predictions.get("optimal")
                    tuned_pred = response.predictions.get("tuned")
                    
                    opt_out.set_content(format_output(opt_pred, response.insights.get("optimal")))
                    tuned_out.set_content(format_output(tuned_pred, response.insights.get("tuned")))
                    
                    features_table.rows = [{'feature': k.replace("_", " ").title(), 'value': f"{v:,.2f}"} for k, v in features.items()]
                    features_table.update()
                    
                    radar_values = [
                        symlog(features.get("market_cap", 0)),
                        symlog(features.get("total_debt", 0)),
                        symlog(features.get("intangible_assets", 0)),
                        symlog(features.get("cash_and_equivalents", 0)),
                        symlog(features.get("current_liabilities", 0)),
                        symlog(features.get("operating_cash_flow", 0)),
                        symlog(features.get("ebitda", 0)),
                        symlog(features.get("interest_expense", 0)),
                        symlog(features.get("net_income", 0)),
                        symlog(features.get("total_assets", 0))
                    ]
                    feature_radar.options['series'][0]['data'][0]['value'] = radar_values
                    feature_radar.update()
                    
                    features_container.set_visibility(True)

                    results_header.set_visibility(True)
                    results_container.set_visibility(True)
                finally:
                    hide_loader()

            fetch_btn.on('click', fetch_and_predict)
            manual_btn.on('click', do_predict_manual)

