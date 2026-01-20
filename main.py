import os
import json
import pandas as pd
import gspread
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import re

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- SECRETS DO RENDER ---
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GCP_JSON = os.environ.get("GCP_SERVICE_ACCOUNT")

def get_db_connection():
    creds_dict = json.loads(GCP_JSON)
    gc = gspread.service_account_from_dict(creds_dict)
    return gc.open_by_key(SPREADSHEET_ID)

def limpar_moeda(v):
    if not v or v == "": return 0.0
    s = str(v).replace('R$', '').replace('.', '').replace(',', '.').strip()
    try: return float(re.sub(r'[^\d.-]', '', s))
    except: return 0.0

@app.get("/api/v1/ml_visionario")
async def api_ml():
    try:
        sh = get_db_connection()
        df_vendas = pd.DataFrame(sh.worksheet("VENDAS").get_all_records())
        df_gastos = pd.DataFrame(sh.worksheet("GASTOS").get_all_records())

        # Normalização de Colunas (Evita erros de espaços extras)
        df_vendas.columns = [c.strip() for c in df_vendas.columns]
        df_gastos.columns = [c.strip() for c in df_gastos.columns]

        # Tratamento de Valores e Datas
        df_vendas['VAL_NUM'] = df_vendas['VALOR DA VENDA'].apply(limpar_moeda)
        df_gastos['VAL_NUM'] = df_gastos['VALOR'].apply(limpar_moeda)
        
        df_vendas['DT'] = pd.to_datetime(df_vendas['DATA E HORA'], dayfirst=True, errors='coerce')
        df_gastos['DT'] = pd.to_datetime(df_gastos['DATA E HORA'], dayfirst=True, errors='coerce')
        
        df_vendas = df_vendas.dropna(subset=['DT'])
        df_gastos = df_gastos.dropna(subset=['DT'])

        # Agrupamento Mensal (Auditoria)
        vendas_m = df_vendas.set_index('DT').resample('ME')['VAL_NUM'].sum()
        gastos_m = df_gastos.set_index('DT').resample('ME')['VAL_NUM'].sum()
        
        df_resumo = pd.DataFrame({'vendas': vendas_m, 'gastos': gastos_m}).fillna(0)
        df_resumo['lucro'] = df_resumo['vendas'] - df_resumo['gastos']
        df_resumo['mes'] = df_resumo.index.strftime('%b/%Y')

        # Lógica de Predição Nova (Tendência dos últimos 2 meses)
        if len(df_resumo) >= 2:
            ultimo = df_resumo['lucro'].iloc[-1]
            tendencia = ultimo - df_resumo['lucro'].iloc[-2]
            previsao = ultimo + tendencia
        else:
            previsao = df_resumo['lucro'].mean()

        # Rankings (Aqui tratamos a coluna "DADOS DO COMPRADOR")
        col_cliente = "DADOS DO COMPRADOR" if "DADOS DO COMPRADOR" in df_vendas.columns else df_vendas.columns[1]
        
        top_compradores = df_vendas.groupby(col_cliente)['VAL_NUM'].sum().nlargest(5).reset_index()
        top_compradores.columns = ['CLIENTE', 'VALOR']
        
        top_produtos = df_vendas.groupby('SABORES')['VAL_NUM'].sum().nlargest(5).reset_index()
        top_produtos.columns = ['SABOR', 'VALOR']

        return {
            "previsao": round(previsao, 2),
            "auditoria_mensal": df_resumo.tail(12).to_dict(orient='records'),
            "ranking_compradores": top_compradores.to_dict(orient='records'),
            "ranking_produtos": top_produtos.to_dict(orient='records')
        }
    except Exception as e:
        return {"erro": str(e)}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("visionario.html", {"request": request})
