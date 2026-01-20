import os
import json
import pandas as pd
import gspread
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
import google.genai as genai # Integrando seu código de ML
from sklearn.metrics import mean_absolute_error
import re

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Configurações de Governança
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GCP_JSON = os.environ.get("GCP_SERVICE_ACCOUNT")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")

def get_db_connection():
    creds_dict = json.loads(GCP_JSON)
    gc = gspread.service_account_from_dict(creds_dict)
    return gc.open_by_key(SPREADSHEET_ID)

# --- ENGINE DE ML (MIGRADO DO SEU predicao_ml.py) ---
def realizar_previsao_ml(df_vendas, df_gastos):
    # Consolidação Mensal (Lógica do seu script)
    df_vendas['Mes_Ano'] = pd.to_datetime(df_vendas['DATA E HORA'], dayfirst=True).dt.to_period('M')
    vendas_m = df_vendas.groupby('Mes_Ano')['VALOR DA VENDA'].sum()
    
    df_gastos['Mes_Ano'] = pd.to_datetime(df_gastos['DATA E HORA'], dayfirst=True).dt.to_period('M')
    gastos_m = df_gastos.groupby('Mes_Ano')['VALOR'].sum()
    
    df_ml = pd.merge(vendas_m, gastos_m, left_index=True, right_index=True, how='outer').fillna(0)
    df_ml['Lucro'] = df_ml['VALOR DA VENDA'] - df_ml['VALOR']
    
    # Modelo Naive: Próximo mês = Último mês
    previsao = float(df_ml['Lucro'].iloc[-1])
    
    # Insight Sênior via Gemini (Sua lógica visionária)
    insight = "O Sênior está sem café."
    if GEMINI_KEY:
        try:
            client = genai.Client(api_key=GEMINI_KEY)
            prompt = f"Gere um insight sarcástico sobre um lucro previsto de R$ {previsao:.2f}."
            response = client.models.generate_content(model='gemini-2.0-flash', contents=prompt)
            insight = response.text
        except: pass
        
    return {"valor": previsao, "insight": insight}

@app.get("/api/status")
async def api_status():
    sh = get_db_connection()
    # Carregando dados brutos para o ML
    df_vendas = pd.DataFrame(sh.worksheet("vendas").get_all_records())
    df_gastos = pd.DataFrame(sh.worksheet("gastos").get_all_records())
    
    # Limpeza básica (Regex do seu predicao_ml.py)
    for df, col in [(df_vendas, 'VALOR DA VENDA'), (df_gastos, 'VALOR')]:
        df[col] = df[col].replace(r'[R\$\s\.]', '', regex=True).replace(',', '.', regex=True).astype(float)

    # Executa o ML em tempo real
    ml_results = realizar_previsao_ml(df_vendas, df_gastos)

    return {
        "vendas_mes": float(df_vendas['VALOR DA VENDA'].sum()),
        "previsao_ml": ml_results["valor"],
        "insight_senior": ml_results["insight"],
        "ultima_atualizacao": datetime.now().strftime("%H:%M:%S")
    }

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
