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

# --- [V3] CORE: INTELIGÊNCIA GLOBAIS (FOCO EM VOLUME REAL) ---
@app.get("/api/v1/ml_visionario")
async def api_ml():
    try:
        sh = get_db_connection()
        df_vendas = pd.DataFrame(sh.worksheet("VENDAS").get_all_records())
        df_gastos = pd.DataFrame(sh.worksheet("GASTOS").get_all_records())

        # Sanitização e Datas (Mantidos)
        df_vendas['VAL_NUM'] = df_vendas['VALOR DA VENDA'].apply(limpar_moeda)
        df_gastos['VAL_NUM'] = df_gastos['VALOR'].apply(limpar_moeda)
        df_vendas['DT'] = pd.to_datetime(df_vendas['DATA E HORA'], dayfirst=True, errors='coerce')
        df_gastos['DT'] = pd.to_datetime(df_gastos['DATA E HORA'], dayfirst=True, errors='coerce')

        # --- RANKING DE GASTOS: O DADO REAL ---
        # Garantimos que QUANTIDADE é numérico para somar o volume real
        df_gastos['QUANTIDADE'] = pd.to_numeric(df_gastos['QUANTIDADE'], errors='coerce').fillna(0)

        ranking_gastos = df_gastos.groupby('PRODUTO').agg(
            total_gasto=('VAL_NUM', 'sum'),
            volume_real=('QUANTIDADE', 'sum') # <-- Aqui está o seu dado real acumulado
        ).nlargest(10, 'total_gasto').reset_index()

        # ... (Restante da lógica de Auditoria e Vendas mantida conforme catálogo)

        return {
            "totais": {
                "faturamento": float(df_vendas['VAL_NUM'].sum()),
                "custos": float(df_gastos['VAL_NUM'].sum()),
                "lucro": float(df_vendas['VAL_NUM'].sum() - df_gastos['VAL_NUM'].sum()),
                "total_itens": int(len(df_vendas))
            },
            "auditoria_mensal": df_resumo.to_dict(orient='records'), # (Lógica de resample omitida aqui para brevidade)
            "ranking_produtos": top_produtos.to_dict(orient='records'),
            "ranking_gastos": ranking_gastos.to_dict(orient='records') 
        }
    except Exception as e:
        return {"erro": str(e)}

# --- [V4] ROTEAMENTO: INTERFACE MACRO ---
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("visionario.html", {"request": request})
