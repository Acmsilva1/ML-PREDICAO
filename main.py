import os
import json
import pandas as pd
import gspread
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import re

app = FastAPI()

# Configuração de templates (certifique-se de que a pasta 'templates' existe)
templates = Jinja2Templates(directory="templates")

# --- GOVERNANÇA DE AMBIENTE (SECRETS DO RENDER) ---
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GCP_JSON = os.environ.get("GCP_SERVICE_ACCOUNT")

def get_db_connection():
    """Estabelece conexão segura com o Google Sheets via Service Account."""
    try:
        creds_dict = json.loads(GCP_JSON)
        gc = gspread.service_account_from_dict(creds_dict)
        return gc.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        print(f"Erro na conexão com Google Sheets: {e}")
        raise

def limpar_moeda(v):
    """Transforma strings financeiras (R$ 1.234,56) em float puro (1234.56)."""
    if not v or v == "": return 0.0
    # Remove R$, pontos de milhar e troca vírgula por ponto
    s = str(v).replace('R$', '').replace('.', '').replace(',', '.').strip()
    try:
        # Regex para manter apenas números, pontos decimais e sinal de menos
        return float(re.sub(r'[^\d.-]', '', s))
    except:
        return 0.0

@app.get("/api/v1/ml_visionario")
async def api_ml():
    try:
        sh = get_db_connection()
        
        # Carregamento das abas raiz
        df_vendas = pd.DataFrame(sh.worksheet("VENDAS").get_all_records())
        df_gastos = pd.DataFrame(sh.worksheet("GASTOS").get_all_records())

        # 1. Tratamento de Dados de Vendas
        df_vendas['VALOR'] = df_vendas['VALOR DA VENDA'].apply(limpar_moeda)
        df_vendas['DT'] = pd.to_datetime(df_vendas['DATA E HORA'], dayfirst=True, errors='coerce')
        df_vendas = df_vendas.dropna(subset=['DT'])

        # 2. Tratamento de Dados de Gastos
        df_gastos['VALOR'] = df_gastos['VALOR'].apply(limpar_moeda)
        df_gastos['DT'] = pd.to_datetime(df_gastos['DATA E HORA'], dayfirst=True, errors='coerce')
        df_gastos = df_gastos.dropna(subset=['DT'])

        # 3. Agrupamento Mensal (Cruzamento para Tabela de Auditoria)
        # 'ME' garante compatibilidade com as versões novas do Pandas no Render
        vendas_m = df_vendas.set_index('DT').resample('ME')['VALOR'].sum()
        gastos_m = df_gastos.set_index('DT').resample('ME')['VALOR'].sum()
        
        # Merge para alinhar meses de vendas e gastos
        df_resumo = pd.DataFrame({'vendas': vendas_m, 'gastos': gastos_m}).fillna(0)
        df_resumo['lucro'] = df_resumo['vendas'] - df_resumo['gastos']
        df_resumo['mes'] = df_resumo.index.strftime('%b/%Y')

        # 4. Lógica de Predição Nova (Tendência Linear Simples)
        # Se o lucro do último mês foi maior que o anterior, projeta o mesmo crescimento.
        if len(df_resumo) >= 2:
            ultimo_lucro = df_resumo['lucro'].iloc[-1]
            penultimo_lucro = df_resumo['lucro'].iloc[-2]
            tendencia = ultimo_lucro - penultimo_lucro
            previsao = ultimo_lucro + tendencia
        else:
            previsao = df_resumo['lucro'].mean() if not df_resumo.empty else 0.0

        # 5. Rankings (Top 5) para as Tabelas Profissionais
        # Usamos nlargest para garantir que pegamos os valores mais altos
        ranking_compradores = df_vendas.groupby('COMPRADOR')['VALOR'].sum().nlargest(5).reset_index()
        # Renomeamos para o JSON ficar limpo
        ranking_compradores.columns = ['COMPRADOR', 'VALOR']

        ranking_produtos = df_vendas.groupby('SABORES')['VALOR'].sum().nlargest(5).reset_index()
        ranking_produtos.columns = ['SABORES', 'VALOR']

        return {
            "previsao": round(previsao, 2),
            "auditoria_mensal": df_resumo.tail(12).to_dict(orient='records'), # Últimos 12 meses
            "ranking_compradores": ranking_compradores.to_dict(orient='records'),
            "ranking_produtos": ranking_produtos.to_dict(orient='records'),
            "lucro_total_acumulado": round(df_resumo['lucro'].sum(), 2)
        }

    except Exception as e:
        print(f"Erro no processamento: {e}")
        return {"erro": str(e)}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serve a interface Visionário BI."""
    return templates.TemplateResponse("visionario.html", {"request": request})

if __name__ == "__main__":
    import uvicorn
    # Porta padrão do Render ou 8000 para teste local
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
