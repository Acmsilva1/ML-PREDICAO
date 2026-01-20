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

def limpar_moeda(v):
    if not v or v == "": return 0.0
    # Limpeza robusta: remove R$, pontos de milhar e ajusta a vírgula decimal
    s = str(v).replace('R$', '').replace('.', '').replace(',', '.').strip()
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

        # Tratamento de Valores
        df_vendas['VALOR'] = df_vendas['VALOR DA VENDA'].apply(limpar_moeda)
        df_gastos['VALOR'] = df_gastos['VALOR'].apply(limpar_moeda)

        # Tratamento de Datas (Sincronizado com seu Flow do GitHub)
        df_vendas['DT'] = pd.to_datetime(df_vendas['DATA E HORA'], dayfirst=True, errors='coerce')
        df_gastos['DT'] = pd.to_datetime(df_gastos['DATA E HORA'], dayfirst=True, errors='coerce')
        
        # Remove linhas com datas corrompidas para não quebrar o agrupamento
        df_vendas = df_vendas.dropna(subset=['DT'])
        df_gastos = df_gastos.dropna(subset=['DT'])

        # Agrupamento Mensal (Cruzamento Raiz)
        # 'ME' é o novo padrão do Pandas para Month End
        vendas_m = df_vendas.set_index('DT').resample('ME')['VALOR'].sum()
        gastos_m = df_gastos.set_index('DT').resample('ME')['VALOR'].sum()
        
        # Criação da Tabela de Performance
        df_resumo = pd.DataFrame({'Vendas': vendas_m, 'Gastos': gastos_m}).fillna(0)
        df_resumo['Lucro'] = df_resumo['Vendas'] - df_resumo['Gastos']

        # --- LÓGICA DE PREDIÇÃO DINÂMICA ---
        if len(df_resumo) >= 2:
            # Tendência baseada no deslocamento dos últimos meses
            ultimo_lucro = df_resumo['Lucro'].iloc[-1]
            penultimo_lucro = df_resumo['Lucro'].iloc[-2]
            tendencia = ultimo_lucro - penultimo_lucro
            previsao = ultimo_lucro + tendencia
        else:
            previsao = df_resumo['Lucro'].sum()

        # KPIs Estratégicos
        ticket_medio = df_vendas['VALOR'].mean() if not df_vendas.empty else 0
        produto_estrela = df_vendas.groupby('SABORES')['VALOR'].sum().idxmax() if not df_vendas.empty else "N/A"
        crescimento = df_resumo['Vendas'].pct_change().iloc[-1] * 100 if len(df_resumo) > 1 else 0

        return {
            "previsao_lucro": round(previsao, 2),
            "ticket_medio": round(ticket_medio, 2),
            "crescimento_mensal_perc": round(crescimento, 2),
            "produto_estrela": produto_estrela,
            "lucro_total_acumulado": round(df_resumo['Lucro'].sum(), 2),
            "meses_analisados": len(df_resumo)
        }
    except Exception as e:
        print(f"Erro Crítico: {e}")
        return {"erro": str(e)}

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("visionario.html", {"request": request})
