import os
import json
import pandas as pd
import gspread
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
import google.genai as genai
import re

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Configurações de Ambiente (Segurança e Governança)
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GCP_JSON = os.environ.get("GCP_SERVICE_ACCOUNT")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

def get_db_connection():
    creds_dict = json.loads(GCP_JSON)
    gc = gspread.service_account_from_dict(creds_dict)
    return gc.open_by_key(SPREADSHEET_ID)

def limpar_moeda(valor):
    if pd.isna(valor) or valor == "": return 0.0
    s = str(valor).replace('R$', '').replace('.', '').replace(',', '.').strip()
    try: return float(re.sub(r'[^\d.]', '', s))
    except: return 0.0

def engine_ml_insight(df_vendas, df_gastos):
    # Lógica de Consolidação Mensal do seu predicao_ml.py
    df_vendas['DT'] = pd.to_datetime(df_vendas['DATA E HORA'], dayfirst=True)
    df_gastos['DT'] = pd.to_datetime(df_gastos['DATA E HORA'], dayfirst=True)
    
    vendas_m = df_vendas.set_index('DT').resample('M')['VALOR DA VENDA'].sum()
    gastos_m = df_gastos.set_index('DT').resample('M')['VALOR'].sum()
    
    df_resumo = pd.merge(vendas_m, gastos_m, left_index=True, right_index=True, how='outer').fillna(0)
    df_resumo['Lucro'] = df_resumo['VALOR DA VENDA'] - df_resumo['VALOR']
    
    # ML Naive: Previsão baseada no último lucro real
    previsao_valor = float(df_resumo['Lucro'].iloc[-1]) if not df_resumo.empty else 0.0
    
    # Chamada ao Consultor Sênior (Gemini)
    insight = "O sistema está processando... ou fingindo que trabalha."
    if GEMINI_KEY:
        try:
            client = genai.Client(api_key=GEMINI_KEY)
            prompt = f"Analise este lucro de R$ {previsao_valor:.2f}. Dê um insight curto e muito sarcástico para o dono do negócio."
            response = client.models.generate_content(model='gemini-2.0-flash', contents=prompt)
            insight = response.text
        except Exception as e:
            insight = f"O Gemini teve um burnout: {str(e)}"
            
    return {"previsao": previsao_valor, "insight": insight}

@app.get("/api/ml_insights")
async def api_ml():
    try:
        sh = get_db_connection()
        df_vendas = pd.DataFrame(sh.worksheet("VENDAS").get_all_records())
        df_gastos = pd.DataFrame(sh.worksheet("GASTOS").get_all_records())

        df_vendas['VALOR DA VENDA'] = df_vendas['VALOR DA VENDA'].apply(limpar_moeda)
        df_gastos['VALOR'] = df_gastos['VALOR'].apply(limpar_moeda)

        return engine_ml_insight(df_vendas, df_gastos)
    except Exception as e:
        return {"erro": str(e)}

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
