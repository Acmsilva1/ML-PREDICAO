"""
SISTEMA DE GESTÃO FINANCEIRA - ANALISTA VISÃO MACRO (ML_VISIONARIO)
Foco: Auditoria Mensal, Ticket Médio e Ranking Global de Produtos.
"""

import os
import json
import pandas as pd
import gspread
import re
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- [V1] INFRAESTRUTURA: CONEXÃO SEGURA ---
def get_db_connection():
    """Gerencia a autenticação com a Service Account do Google."""
    GCP_JSON = os.environ.get("GCP_SERVICE_ACCOUNT")
    SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
    creds_dict = json.loads(GCP_JSON)
    gc = gspread.service_account_from_dict(creds_dict)
    return gc.open_by_key(SPREADSHEET_ID)

# --- [V2] SANITIZAÇÃO: LIMPEZA DE DADOS CORROMPIDOS ---
def limpar_moeda(v):
    """Converte strings financeiras 'R$ 1.200,50' em float puro."""
    if not v or v == "": return 0.0
    s = str(v).replace('R$', '').replace('.', '').replace(',', '.').strip()
    try: 
        return float(re.sub(r'[^\d.-]', '', s))
    except: 
        return 0.0

# --- [V3] CORE: INTELIGÊNCIA GLOBAIS (VERSÃO ADITIVA) ---
@app.get("/api/v1/ml_visionario")
async def api_ml():
    try:
        sh = get_db_connection()
        df_vendas = pd.DataFrame(sh.worksheet("VENDAS").get_all_records())
        df_gastos = pd.DataFrame(sh.worksheet("GASTOS").get_all_records())

        # [V2] Sanitização (Mantida)
        df_vendas['VAL_NUM'] = df_vendas['VALOR DA VENDA'].apply(limpar_moeda)
        df_gastos['VAL_NUM'] = df_gastos['VALOR'].apply(limpar_moeda)
        df_vendas['DT'] = pd.to_datetime(df_vendas['DATA E HORA'], dayfirst=True, errors='coerce')
        df_gastos['DT'] = pd.to_datetime(df_gastos['DATA E HORA'], dayfirst=True, errors='coerce')
        df_vendas = df_vendas.dropna(subset=['DT'])

        # --- PROCESSAMENTO DE VENDAS (MANTIDO) ---
        df_vendas['SAB_LIST'] = df_vendas['SABORES'].astype(str).str.split(',')
        df_exploded = df_vendas.explode('SAB_LIST') 
        df_exploded['SAB_LIST'] = df_exploded['SAB_LIST'].str.strip().str.upper()
        df_exploded['COUNT_ITENS'] = df_exploded.groupby(level=0)['SAB_LIST'].transform('count')
        df_exploded['VAL_UNITARIO'] = df_exploded['VAL_NUM'] / df_exploded['COUNT_ITENS']

        # Auditoria Mensal (KPIs Originais Mantidos)
        vendas_m = df_vendas.set_index('DT').resample('ME')['VAL_NUM'].sum()
        gastos_m = df_gastos.set_index('DT').resample('ME')['VAL_NUM'].sum()
        itens_m = df_exploded.set_index('DT').resample('ME')['SAB_LIST'].count()

        df_resumo = pd.DataFrame({'vendas': vendas_m, 'gastos': gastos_m, 'itens': itens_m}).fillna(0)
        df_resumo['lucro'] = df_resumo['vendas'] - df_resumo['gastos']
        df_resumo['ticket_medio'] = df_resumo['vendas'] / df_resumo['itens'].replace(0, 1)
        df_resumo['mes'] = df_resumo.index.strftime('%m/%Y')

        # Ranking de Produtos (KPI Original Mantido)
        top_produtos = df_exploded.groupby('SAB_LIST').agg(
            total=('VAL_UNITARIO', 'sum'),
            qtd=('SAB_LIST', 'count')
        ).nlargest(10, 'qtd').reset_index()

        # --- [NOVIDADE] RANKING DE GASTOS (ACRESCENTADO) ---
        ranking_gastos = df_gastos.groupby('PRODUTO').agg(
            total_gasto=('VAL_NUM', 'sum'),
            qtd_compras=('PRODUTO', 'count'),
            volume_total=('QUANTIDADE', 'sum')
        ).nlargest(10, 'total_gasto').reset_index()

        return {
            "totais": {
                "faturamento": float(df_resumo['vendas'].sum()),
                "custos": float(df_resumo['gastos'].sum()),
                "lucro": float(df_resumo['lucro'].sum()),
                "total_itens": int(df_resumo['itens'].sum())
            },
            "auditoria_mensal": df_resumo.to_dict(orient='records'),
            "ranking_produtos": top_produtos.to_dict(orient='records'),
            "ranking_gastos": ranking_gastos.to_dict(orient='records') # <-- Novo dado
        }
    except Exception as e:
        return {"erro": str(e)}

# --- [V4] ROTEAMENTO: INTERFACE MACRO ---
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("visionario.html", {"request": request})
