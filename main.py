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

# --- [V1] CONEXÃO ---
def get_db_connection():
    creds_dict = json.loads(os.environ.get("GCP_SERVICE_ACCOUNT"))
    gc = gspread.service_account_from_dict(creds_dict)
    return gc.open_by_key(os.environ.get("SPREADSHEET_ID"))

# --- [V2] SANITIZAÇÃO FINANCEIRA ---
def limpar_moeda(v):
    if not v or v == "": return 0.0
    s = str(v).replace('R$', '').replace('.', '').replace(',', '.').strip()
    try: return float(re.sub(r'[^\d.-]', '', s))
    except: return 0.0

# --- [V3] CORE: INTELIGÊNCIA MACRO ---
@app.get("/api/v1/ml_visionario")
async def api_ml():
    try:
        sh = get_db_connection()
        df_vendas = pd.DataFrame(sh.worksheet("VENDAS").get_all_records())
        df_gastos = pd.DataFrame(sh.worksheet("GASTOS").get_all_records())

        # Limpeza de valores e datas
        df_vendas['VAL_NUM'] = df_vendas['VALOR DA VENDA'].apply(limpar_moeda)
        df_gastos['VAL_NUM'] = df_gastos['VALOR'].apply(limpar_moeda)
        
        # Garantindo que QUANTIDADE seja numérica para o dado real
        df_gastos['QTD_NUM'] = pd.to_numeric(df_gastos['QUANTIDADE'], errors='coerce').fillna(0)
        
        df_vendas['DT'] = pd.to_datetime(df_vendas['DATA E HORA'], dayfirst=True, errors='coerce')
        df_gastos['DT'] = pd.to_datetime(df_gastos['DATA E HORA'], dayfirst=True, errors='coerce')
        df_vendas = df_vendas.dropna(subset=['DT'])

        # Explosão de Sabores para Ranking
        df_exploded = df_vendas.assign(SAB_LIST=df_vendas['SABORES'].str.split(',')).explode('SAB_LIST')
        df_exploded['SAB_LIST'] = df_exploded['SAB_LIST'].str.strip().str.upper()
        df_exploded['VAL_UNIT'] = df_exploded['VAL_NUM'] / df_exploded.groupby(level=0)['SAB_LIST'].transform('count')

        # Agrupamento Mensal
        resumo = pd.DataFrame({
            'vendas': df_vendas.set_index('DT').resample('ME')['VAL_NUM'].sum(),
            'gastos': df_gastos.set_index('DT').resample('ME')['VAL_NUM'].sum(),
            'itens': df_exploded.set_index('DT').resample('ME')['SAB_LIST'].count()
        }).fillna(0)
        resumo['lucro'] = resumo['vendas'] - resumo['gastos']
        resumo['ticket'] = resumo['vendas'] / resumo['itens'].replace(0, 1)
        resumo['mes'] = resumo.index.strftime('%m/%Y')

        # Ranking de Produtos (Vendas)
        top_prod = df_exploded.groupby('SAB_LIST').agg(total=('VAL_UNIT', 'sum'), qtd=('SAB_LIST', 'count')).nlargest(10, 'qtd').reset_index()

        # --- RANKING DE GASTOS (VOLUME REAL ACUMULADO) ---
        top_gastos = df_gastos.groupby('PRODUTO').agg(
            total_gasto=('VAL_NUM', 'sum'),
            volume_real=('QTD_NUM', 'sum') # Soma real da coluna Quantidade
        ).nlargest(10, 'total_gasto').reset_index()

        return {
            "totais": {
                "faturamento": float(resumo['vendas'].sum()),
                "custos": float(resumo['gastos'].sum()),
                "lucro": float(resumo['lucro'].sum()),
                "total_itens": int(resumo['itens'].sum())
            },
            "auditoria_mensal": resumo.to_dict(orient='records'),
            "ranking_produtos": top_prod.to_dict(orient='records'),
            "ranking_gastos": top_gastos.to_dict(orient='records')
        }
    except Exception as e: return {"erro": str(e)}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("visionario.html", {"request": request})
