import os
import json
import pandas as pd
import gspread
import re
import gc
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- CONFIGURAÇÕES DE AMBIENTE ---
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

        # Limpeza Básica
        df_vendas.columns = [c.strip() for c in df_vendas.columns]
        df_gastos.columns = [c.strip() for c in df_gastos.columns]
        
        df_vendas['VAL_NUM'] = df_vendas['VALOR DA VENDA'].apply(limpar_moeda)
        df_gastos['VAL_NUM'] = df_gastos['VALOR'].apply(limpar_moeda)
        
        df_vendas['DT'] = pd.to_datetime(df_vendas['DATA E HORA'], dayfirst=True, errors='coerce')
        df_gastos['DT'] = pd.to_datetime(df_gastos['DATA E HORA'], dayfirst=True, errors='coerce')
        
        df_vendas = df_vendas.dropna(subset=['DT'])
        df_gastos = df_gastos.dropna(subset=['DT'])

        # --- INTELIGÊNCIA DE EXPLOSÃO (Onde o filho chora e a mãe não vê) ---
        # 1. Separamos os sabores por vírgula
        df_vendas['SABORES_LIST'] = df_vendas['SABORES'].astype(str).str.split(',')
        
        # 2. Explodimos para que cada sabor tenha sua própria linha
        df_exploded = df_vendas.explode('SABORES_LIST')
        df_exploded['SABORES_LIST'] = df_exploded['SABORES_LIST'].str.strip().str.upper()

        # 3. Cálculo de Valor Proporcional (Evita duplicar faturamento no ranking)
        df_exploded['QT_ITENS_LINHA'] = df_exploded.groupby(level=0)['SABORES_LIST'].transform('count')
        df_exploded['VAL_PROPORCIONAL'] = df_exploded['VAL_NUM'] / df_exploded['QT_ITENS_LINHA']

        # --- Agrupamento Histórico Mensal ---
        vendas_m = df_vendas.set_index('DT').resample('ME')['VAL_NUM'].sum()
        gastos_m = df_gastos.set_index('DT').resample('ME')['VAL_NUM'].sum()
        
        # Contagem real de itens por mês (usando o df explodido)
        itens_m = df_exploded.set_index('DT').resample('ME')['SABORES_LIST'].count()
        
        df_resumo = pd.DataFrame({
            'vendas': vendas_m, 
            'gastos': gastos_m,
            'itens': itens_m
        }).fillna(0)
        
        df_resumo['lucro'] = df_resumo['vendas'] - df_resumo['gastos']
        # Cálculo de Ticket Médio Mensal
        df_resumo['ticket_medio'] = df_resumo['vendas'] / df_resumo['itens']
        df_resumo['ticket_medio'] = df_resumo['ticket_medio'].fillna(0)
        df_resumo['mes'] = df_resumo.index.strftime('%m/%Y')

        # Totais Gerais
        totais = {
            "faturamento": round(df_resumo['vendas'].sum(), 2),
            "custos": round(df_resumo['gastos'].sum(), 2),
            "lucro": round(df_resumo['lucro'].sum(), 2),
            "total_itens": int(df_resumo['itens'].sum())
        }

        # Rankings de Performance (Agora com dados limpos!)
        top_produtos = df_exploded.groupby('SABORES_LIST').agg(
            total=('VAL_PROPORCIONAL', 'sum'),
            quantidade=('SABORES_LIST', 'count')
        ).nlargest(10, 'quantidade').reset_index()
        top_produtos.columns = ['SABOR', 'VALOR', 'QTD']

        return {
            "totais": totais,
            "auditoria_mensal": df_resumo.to_dict(orient='records'),
            "ranking_produtos": top_produtos.to_dict(orient='records')
        }
    except Exception as e:
        return {"erro": str(e)}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("visionario.html", {"request": request})
