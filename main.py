import os
import json
import pandas as pd
import gspread
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
import re

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- CONFIGURAÇÕES DE GOVERNANÇA (SECRETS DO RENDER) ---
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GCP_JSON = os.environ.get("GCP_SERVICE_ACCOUNT")

def get_db_connection():
    creds_dict = json.loads(GCP_JSON)
    gc = gspread.service_account_from_dict(creds_dict)
    return gc.open_by_key(SPREADSHEET_ID)

def limpar_valor(valor):
    if pd.isna(valor) or valor == "": return 0.0
    s = str(valor).replace('R$', '').replace('.', '').replace(',', '.').strip()
    try:
        return float(re.sub(r'[^\d.]', '', s))
    except:
        return 0.0

@app.get("/api/v1/ml_visionario")
async def api_ml():
    try:
        sh = get_db_connection()
        df_vendas = pd.DataFrame(sh.worksheet("VENDAS").get_all_records())
        df_gastos = pd.DataFrame(sh.worksheet("GASTOS").get_all_records())

        # Processamento de Dados
        df_vendas['DT'] = pd.to_datetime(df_vendas['DATA E HORA'], dayfirst=True)
        df_vendas['VALOR'] = df_vendas['VALOR DA VENDA'].apply(limpar_valor)
        df_gastos['VALOR'] = df_gastos['VALOR'].apply(limpar_valor)

        # Agrupamento Mensal
        vendas_m = df_vendas.set_index('DT').resample('M')['VALOR'].sum()
        gastos_m = df_gastos.set_index('DT').resample('M')['VALOR'].sum()
        
        df_resumo = pd.DataFrame({'Vendas': vendas_m, 'Gastos': gastos_m}).fillna(0)
        df_resumo['Lucro'] = df_resumo['Vendas'] - df_resumo['Gastos']
        
        # --- MÉTRICAS RECOMENDADAS (6 MESES) ---
        
        # 1. Previsão Próximo Mês (Média Ponderada: peso maior para o mês atual)
        if len(df_resumo) >= 2:
            peso_atual = 0.7
            peso_anterior = 0.3
            previsao = (df_resumo['Lucro'].iloc[-1] * peso_atual) + (df_resumo['Lucro'].iloc[-2] * peso_anterior)
        else:
            previsao = df_resumo['Lucro'].mean()

        # 2. KPI Trimestral (Últimos 3 meses vs 3 meses anteriores)
        ticket_medio = df_vendas['VALOR'].mean()
        crescimento_mensal = df_resumo['Vendas'].pct_change().iloc[-1] * 100 if len(df_resumo) > 1 else 0

        # 3. Produto "Estrela" (O que mais traz grana, não o que mais sai)
        produto_estrela = df_vendas.groupby('SABORES')['VALOR'].sum().idxmax()

        return {
            "previsao_lucro": round(previsao, 2),
            "ticket_medio": round(ticket_medio, 2),
            "crescimento_mensal_perc": round(crescimento_mensal, 2),
            "produto_estrela": produto_estrela,
            "total_vendas_periodo": round(df_resumo['Vendas'].sum(), 2),
            "meses_analisados": len(df_resumo)
        }
    except Exception as e:
        return {"erro": str(e)}

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("visionario.html", {"request": request})
