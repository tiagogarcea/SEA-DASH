# ==============================================================================
# 1. IMPORTAÇÃO DAS BIBLIOTECAS (COM ADIÇÕES)
# ==============================================================================
import dash
from dash import dcc, html, Input, Output, State, ALL, clientside_callback, ctx, callback, no_update
import plotly.express as px
import pandas as pd
import numpy as np
import dash_bootstrap_components as dbc
import urllib.parse
import math
import os
from datetime import datetime
import plotly.graph_objects as go
import flask  # Para obter o IP do usuário
import requests  # Para geolocalização
from dotenv import load_dotenv  # <-- ADICIONE ESTA LINHA
load_dotenv()                   # <-- ADICIONE ESTA LINHA

# --- MÓDULOS DE AUTENTICAÇÃO E BANCO DE DADOS ---
import psycopg2 # pyright: ignore[reportMissingModuleSource]
import psycopg2.extras # pyright: ignore[reportMissingModuleSource]
from werkzeug.security import generate_password_hash, check_password_hash

# ==============================================================================
# CONFIGURAÇÕES DE AUTENTICAÇÃO
# ==============================================================================
# Pega a URL do banco de dados das variáveis de ambiente do Render
DATABASE_URL = os.environ.get('DATABASE_URL')
ADMIN_USERS = ['tgr', 'lfdl']
NORMAL_USERS = ['hmc', 'hes', 'jbg', 'anln', 'tcj', 'cmf', 'mss']
ALL_PREDEFINED_USERS = ADMIN_USERS + NORMAL_USERS

# ==============================================================================
# FUNÇÕES DO BANCO DE DADOS (VERSÃO POSTGRESQL)
# ==============================================================================
def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados PostgreSQL."""
    if not DATABASE_URL:
        raise ValueError("A variável de ambiente DATABASE_URL não foi configurada.")
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def initialize_database():
    """Cria as tabelas se não existirem no PostgreSQL."""
    print("Verificando e inicializando banco de dados PostgreSQL...")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT,
            role TEXT NOT NULL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS access_logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            username TEXT NOT NULL,
            ip_address TEXT,
            location TEXT
        )
    ''')
    # Insere usuários se eles ainda não existirem, usando a sintaxe do PostgreSQL
    for user in ADMIN_USERS + NORMAL_USERS:
        role = 'admin' if user in ADMIN_USERS else 'user'
        cur.execute("INSERT INTO users (username, role) VALUES (%s, %s) ON CONFLICT (username) DO NOTHING", (user, role))

    conn.commit()
    cur.close()
    conn.close()
    print("Banco de dados PostgreSQL inicializado/verificado com sucesso.")

def get_user(username):
    """Busca um usuário no banco de dados PostgreSQL."""
    conn = get_db_connection()
    # Usar DictCursor para retornar resultados como dicionários (ex: user['password_hash'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT * FROM users WHERE username = %s', (username,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return user

def update_user_password(username, password):
    """Atualiza a senha de um usuário no PostgreSQL."""
    password_hash = generate_password_hash(password)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('UPDATE users SET password_hash = %s WHERE username = %s', (password_hash, username))
    conn.commit()
    cur.close()
    conn.close()

def log_access(username):
    """Registra um evento de login no PostgreSQL."""
    try:
        ip_address = flask.request.headers.get('X-Forwarded-For', flask.request.remote_addr)
        response = requests.get(f'http://ip-api.com/json/{ip_address}?fields=city,regionName,country', timeout=2)
        if response.status_code == 200:
            data = response.json()
            location = f"{data.get('city', 'N/A')}, {data.get('regionName', 'N/A')}, {data.get('country', 'N/A')}"
        else:
            location = "Localização não encontrada"
    except requests.exceptions.RequestException:
        ip_address = "N/A"
        location = "Falha ao obter localização"
    except Exception:
        ip_address = "localhost"
        location = "Local"

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO access_logs (username, ip_address, location) VALUES (%s, %s, %s)',
                 (username, ip_address, location))
    conn.commit()
    cur.close()
    conn.close()

def get_all_logs():
    """Busca todos os registros de logs de acesso do PostgreSQL."""
    conn = get_db_connection()
    query = """
    SELECT
        timestamp AS "HORA DO ACESSO",
        username AS "LOGIN",
        ip_address AS "IP DO COMPUTADOR",
        location AS "LOCALIZAÇÃO"
    FROM access_logs
    ORDER BY timestamp DESC
    """
    df_logs = pd.read_sql_query(query, conn)
    conn.close()
    return df_logs

# ==============================================================================
# 2. FUNÇÃO PARA GERAR O DATAFRAME COMPARATIVO (SEU CÓDIGO ORIGINAL)
# ==============================================================================
def gerar_df_comparativo_robusto(df_base):
    """
    Compara o carro mais barato por localidade/retirada/categoria entre os dois
    planos mais recentes de CADA localidade, retornando um DataFrame consolidado.
    """
    print("\nIniciando a geração dos dados para a aba 'Comparativo' (versão corrigida)...")
    df = df_base.copy()

    # --- 2.1. Limpeza e padronização ---
    df = df.loc[:, ~df.columns.duplicated()]
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()
    df.dropna(subset=['categoria', 'locadora', 'plano', 'preço', 'localidade'], inplace=True)
    categorias_invalidas = ['-', 'L+']
    df = df[~df['categoria'].isin(categorias_invalidas)]
    print(f"Dados limpos. {len(df)} linhas válidas para análise.")

    # --- 2.2. Encontrar os 2 planos mais recentes para CADA localidade ---
    planos_por_localidade = df.groupby('localidade')['plano'].unique().apply(lambda x: sorted(x, reverse=True)[:2])
    planos_validos = planos_por_localidade[planos_por_localidade.apply(len) == 2]

    if planos_validos.empty:
        print("Nenhuma localidade encontrada com pelo menos 2 planos para comparação.")
        return pd.DataFrame()

    # --- 2.3. Processar cada localidade individualmente ---
    lista_dfs_comparados = []

    for localidade, planos in planos_validos.items():
        plano_recente = planos[0]
        plano_anterior = planos[1]

        df_local = df[(df['localidade'] == localidade) & (df['plano'].isin(planos))]

        try:
            idx_min = df_local.groupby(['plano', 'retirada', 'duração', 'categoria'])['preço'].idxmin()
            df_mais_baratos = df_local.loc[idx_min]
        except ValueError:
            continue

        df_recente = df_mais_baratos[df_mais_baratos['plano'] == plano_recente]
        df_anterior = df_mais_baratos[df_mais_baratos['plano'] == plano_anterior]

        merge_cols = ['localidade', 'retirada', 'duração', 'categoria']

        df_merged = pd.merge(df_recente, df_anterior, on=merge_cols, suffixes=('_atual', '_anterior'))

        if not df_merged.empty:
            lista_dfs_comparados.append(df_merged)

    # --- 2.4. Consolidar e formatar o resultado final ---
    if not lista_dfs_comparados:
        print("Nenhuma correspondência de categoria/retirada encontrada entre os planos das localidades.")
        return pd.DataFrame()

    df_final = pd.concat(lista_dfs_comparados, ignore_index=True)

    # --- 2.5. Calcular a variação e formatar ---
    df_final['variacao_preco'] = (df_final['preço_atual'] / df_final['preço_anterior']) - 1

    novos_nomes = {
        'localidade': 'LOCALIDADE', 'retirada': 'RETIRADA', 'duração': 'DURAÇÃO', 'categoria': 'CATEGORIA',
        'preço_anterior': 'PREÇO ANTERIOR', 'preço_atual': 'PREÇO ATUAL',
        'locadora_anterior': 'LOCADORA MAIS BARATA (ANTERIOR)',
        'locadora_atual': 'LOCADORA MAIS BARATA (ATUAL)',
        'plano_anterior': 'PLANO ANTERIOR',
        'plano_atual': 'PLANO ATUAL',
        'variacao_preco': 'VARIAÇÃO %'
    }
    df_relatorio_final = df_final.rename(columns=novos_nomes)

    # --- 2.6. Formatação final ---
    df_relatorio_final['RETIRADA'] = pd.to_datetime(df_relatorio_final['RETIRADA']).dt.strftime('%Y-%m-%d')
    for col in ['PREÇO ANTERIOR', 'PREÇO ATUAL']:
        df_relatorio_final[col] = df_relatorio_final[col].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    df_relatorio_final['VARIAÇÃO %'] = df_relatorio_final['VARIAÇÃO %'].apply(lambda x: f"{x:.2%}")

    colunas_finais = ['LOCALIDADE', 'RETIRADA', 'DURAÇÃO', 'CATEGORIA', 'PREÇO ANTERIOR', 'PREÇO ATUAL', 'VARIAÇÃO %',
                      'LOCADORA MAIS BARATA (ANTERIOR)', 'LOCADORA MAIS BARATA (ATUAL)', 'PLANO ANTERIOR', 'PLANO ATUAL']

    df_relatorio_final = df_relatorio_final[[col for col in colunas_finais if col in df_relatorio_final.columns]]

    print(f"Análise comparativa concluída! {len(df_relatorio_final)} variações encontradas em múltiplas localidades.")
    return df_relatorio_final

# ==============================================================================
# 3. CARREGAMENTO E LIMPEZA DOS DADOS (SEU CÓDIGO ORIGINAL)
# ==============================================================================
last_update_string = "N/A"
df_comparativo = pd.DataFrame()
plano_recente = "N/A"

try:
    script_dir = os.path.dirname(__file__)
    caminho_arquivo = os.path.join(script_dir, 'dados_consolidados.parquet')
    df_original = pd.read_parquet(caminho_arquivo)

    mod_time_timestamp = os.path.getmtime(caminho_arquivo)
    mod_time_datetime = datetime.fromtimestamp(mod_time_timestamp)
    last_update_string = mod_time_datetime.strftime('%d/%m/%Y %H:%M:%S')

    print("Arquivo Parquet carregado com sucesso!")
    print(f"Total de {len(df_original)} linhas carregadas.")
    print(f"Última modificação do arquivo: {last_update_string}")

    df_para_comparativo = df_original.copy()
    df_para_comparativo.columns = [str(col).lower() for col in df_para_comparativo.columns]
    df_comparativo = gerar_df_comparativo_robusto(df_para_comparativo)

    df = df_original.copy()
    df.columns = [str(col).upper() for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    if not df_comparativo.empty and 'PLANO ATUAL' in df_comparativo.columns:
        plano_recente_series = df_comparativo['PLANO ATUAL']
        if not plano_recente_series.empty:
            plano_recente = str(plano_recente_series.iloc[0])
            print(f"Plano mais recente detectado: {plano_recente}")

except FileNotFoundError:
    print(f"ERRO: O arquivo '{caminho_arquivo}' não foi encontrado.")
    df = pd.DataFrame()
    last_update_string = "Arquivo não encontrado"
except Exception as e:
    print(f"Ocorreu um erro inesperado ao ler o arquivo Parquet: {e}")
    df = pd.DataFrame()
    last_update_string = "Erro ao carregar dados"

# --- Tratamento e Limpeza dos Dados ---
if not df.empty:
    if 'PREÇO' in df.columns and pd.api.types.is_object_dtype(df['PREÇO']):
        df['PREÇO'] = pd.to_numeric(df['PREÇO'], errors='coerce')

    df.rename(columns={'DATA': 'DATA_HORA'}, inplace=True)
    df['DATA_HORA'] = pd.to_datetime(df['DATA_HORA'], errors='coerce')
    if 'HORA' in df.columns:
        df['DATA_HORA'] = pd.to_datetime(df['DATA_HORA'].dt.date.astype(str) + ' ' + df['HORA'].astype(str), errors='coerce')

    if 'RETIRADA' in df.columns:
        df['RETIRADA'] = pd.to_datetime(df['RETIRADA'], errors='coerce')

    df.dropna(subset=['PREÇO', 'DATA_HORA', 'RETIRADA', 'LOCALIDADE', 'LOCADORA', 'CATEGORIA'], inplace=True)

    df_calculos = df.copy()
    df_calculos['RETIRADA'] = df_calculos['RETIRADA'].dt.date

    df_tabela = df.copy()
    df_tabela.rename(columns={'DATA_HORA': 'DATA'}, inplace=True)
    for col in df_tabela.columns:
        if pd.api.types.is_datetime64_any_dtype(df_tabela[col]):
            if col == 'DATA':
                df_tabela[col] = df_tabela[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                df_tabela[col] = df_tabela[col].dt.strftime('%Y-%m-%d')
    print(f"Total de {len(df)} linhas após a limpeza.")
else:
    print("Dashboard iniciado com dados de exemplo.")
    cols = ['LOCALIDADE', 'RETIRADA', 'CRIAÇÃO', 'DURAÇÃO', 'MODELO', 'PREÇO', 'LOCADORA', 'PLANO', 'HORA', 'OTA', 'CAMBIO', 'CATEGORIA', 'DATA_HORA']
    df = pd.DataFrame(columns=cols).astype({'RETIRADA': 'datetime64[ns]', 'DATA_HORA': 'datetime64[ns]'})
    df_calculos = df.copy()
    df_tabela = df.copy()
    df_tabela.rename(columns={'DATA_HORA': 'DATA'}, inplace=True)

# ==============================================================================
# 4. INICIALIZAÇÃO E ESTILO DO APP (SEU CÓDIGO ORIGINAL)
# ==============================================================================
CUSTOM_CSS = """
    body, .dash-bootstrap { background-color: #2b2b2b !important; color: #f0f0f0 !important; font-family: 'Segoe UI', sans-serif !important; font-size: 10pt; }
    .h1, .h4, h1, h4 { font-weight: bold; color: #f0f0f0 !important; }
    .text-primary { color: #42a5f5 !important; }
    .custom-table { border-collapse: collapse; width: 100%; }
    .custom-table th, .custom-table td { border: 1px solid #444; padding: 10px; text-align: left; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .custom-table th { background-color: #3c3c3c; padding: 0; vertical-align: middle; }
    .custom-table td { background-color: #2b2b2b; }
    .popover { background-color: #3c3c3c !important; border: 1px solid #555 !important; }
    .popover-body { background-color: #3c3c3c !important; color: #f0f0f0 !important; }
    .bs-popover-auto[data-popper-placement^=bottom]>.popover-arrow::before, .bs-popover-bottom>.popover-arrow::before { border-bottom-color: #3c3c3c !important; }
    .btn-primary, .btn-secondary { background-color: #444 !important; border-color: #555 !important; border-radius: 4px; font-weight: bold; }
    .btn-primary:hover, .btn-secondary:hover { background-color: #555 !important; border-color: #666 !important; }
    .card { background-color: #3c3c3c !important; border: 1px solid #444 !important; }
    .Select-control, .Select-menu-outer, .Select-value { background-color: #3c3c3c !important; color: #f0f0f0 !important; border: 1px solid #444 !important; }
    .Select--multi .Select-value { background-color: #555 !important; }
    .nav-pills .nav-link.active, .nav-pills .show>.nav-link { background-color: #444; }
    .nav-link { color: #f0f0f0 !important; }
    .nav-link:hover { color: #42a5f5 !important; }
    hr { background-color: #444; }
"""
encoded_css = urllib.parse.quote(CUSTOM_CSS)
css_data_uri = f"data:text/css;charset=utf-8,{encoded_css}"
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, css_data_uri], suppress_callback_exceptions=True)
server = app.server

# ==============================================================================
# 5. DEFINIÇÃO DOS LAYOUTS E NAVEGAÇÃO
# ==============================================================================
PAGE_SIZE = 20
INITIAL_SCALE = 0.8
INVERSE_WIDTH = (1 / INITIAL_SCALE) * 100

# --- LAYOUTS DE LOGIN E REGISTRO (NOVOS) ---
login_layout = dbc.Container([
    dbc.Row(
        dbc.Col(
            html.Div([
                dbc.Card([
                    dbc.CardBody([
                        html.H3("Login", className="card-title text-center"),
                        html.Div(id='login-alert', className="mb-2"),
                        dbc.Input(id="login-username", type="text", placeholder="Usuário", className="mb-3", autoFocus=True),
                        dbc.Input(id="login-password", type="password", placeholder="Senha", className="mb-3"),
                        dbc.Button("Entrar", id="login-button", color="primary", className="w-100"),
                        html.Div(
                            dcc.Link("Primeiro login?", href="/register", style={'font-style': 'italic', 'color': 'lightblue'}),
                            className="text-center mt-3"
                        )
                    ])
                ])
            ], className="w-100", style={'maxWidth': '400px'}),
            width="auto"
        ),
        className="vh-100 d-flex align-items-center justify-content-center"
    )
], fluid=True, style={"backgroundColor": "#2b2b2b", "height": "100vh"})

register_layout = dbc.Container([
    dbc.Row(
        dbc.Col(
            html.Div([
                dbc.Card([
                    dbc.CardBody([
                        html.H3("Primeiro Acesso", className="card-title text-center"),
                        html.P("Cadastre sua senha.", className="text-center text-muted"),
                        html.Div(id='register-alert', className="mb-2"),
                        dbc.Input(id="register-username", type="text", placeholder="Seu login predefinido", className="mb-3", autoFocus=True),
                        dbc.Input(id="register-password", type="password", placeholder="Crie uma senha", className="mb-3"),
                        dbc.Input(id="register-confirm-password", type="password", placeholder="Confirme a senha", className="mb-3"),
                        dbc.Button("Confirmar", id="register-button", color="success", className="w-100"),
                         html.Div(
                            dcc.Link("Voltar para o Login", href="/login", style={'font-style': 'italic', 'color': 'lightblue'}),
                            className="text-center mt-3"
                        )
                    ])
                ])
            ], className="w-100", style={'maxWidth': '400px'}),
            width="auto"
        ),
        className="vh-100 d-flex align-items-center justify-content-center"
    )
], fluid=True, style={"backgroundColor": "#2b2b2b", "height": "100vh"})


# --- LAYOUT DA ABA DE LOGS (ADMIN - NOVO) ---
layout_admin_logs = dbc.Container([
    html.H1("Logs de Acesso", className="text-center text-primary mb-4"),
    html.P("Registros de login no sistema."),
    html.Hr(),
    dcc.Loading(
        id="loading-logs",
        type="circle",
        children=[
             html.Div(id='log-table-container', style={'overflowX': 'auto'})
        ]
    )
], fluid=True)

# --- SIDEBAR DINÂMICA (NOVA) ---
def create_sidebar(user_role):
    SIDEBAR_STYLE = { "position": "fixed", "top": 0, "left": 0, "bottom": 0, "width": "18rem", "padding": "2rem 1rem", "background-color": "#2b2b2b", "border-right": "1px solid #444" }

    nav_links = [
        dbc.NavLink("Base", href="/", active="exact"),
        dbc.NavLink("Comparativo", href="/comparativo", active="exact"),
        dbc.NavLink("Big Picture", href="/dashboard", active="exact"),
        dbc.NavLink("Posicionamento por Loja", href="/posicionamento", active="exact"),
        dbc.NavLink("Posicionamento por Categoria", href="/posicionamento-categoria", active="exact"),
        dbc.NavLink("Movimentação Horário", href="/movimentacao-horario", active="exact"),
    ]
    if user_role == 'admin':
        nav_links.append(dbc.NavLink("Logs de Acesso", href="/admin-logs", active="exact", className="text-warning font-weight-bold"))

    sidebar = html.Div([
        html.H2("SEA-DASH", className="display-6"), html.Hr(),
        dbc.Nav(nav_links, vertical=True, pills=True),
        html.Div([
            html.Hr(),
            html.P("Controles", style={'textAlign': 'center', 'fontWeight':'bold'}),
            dbc.Button("Recarregar Dados 🔄", href="/", color="success", className="w-100 mb-2"),
            dbc.ButtonGroup([
                dbc.Button('-', id='zoom-out-btn', color='primary'),
                dbc.Button('Reset', id='zoom-reset-btn', color='secondary'),
                dbc.Button('+', id='zoom-in-btn', color='primary')
            ], size="sm", className="d-flex"),
            dbc.Button("Logout", id="logout-button", color="danger", className="w-100 mt-3"),
            html.Small(
                f"Última Atualização: {last_update_string}",
                style={'color': '#999', 'fontSize': '0.75rem', 'display': 'block', 'textAlign': 'center', 'marginTop': '10px'}
            )
        ], style={'position': 'absolute', 'bottom': '1rem', 'width': 'calc(100% - 2rem)'})
    ], style=SIDEBAR_STYLE)
    return sidebar


# --- SEUS LAYOUTS ORIGINAIS (INTACTOS) ---
def criar_cabecalho_de_filtros(df_para_filtros, page_prefix):
    if df_para_filtros.empty:
        return html.Thead(html.Tr(html.Th("Nenhum dado para exibir.")))

    colunas_para_exibir = ['#'] + df_para_filtros.columns.tolist()
    header_rows = []
    for coluna in colunas_para_exibir:
        if coluna == '#':
            header_cell = html.Th("#", style={'width': '50px', 'minWidth': '50px', 'padding': '10px', 'textAlign': 'center'})
            header_rows.append(header_cell)
            continue

        header_len = len(coluna)
        try:
            max_content_len = df_para_filtros[coluna].astype(str).str.len().max()
            if pd.isna(max_content_len): max_content_len = 0
        except:
            max_content_len = 0

        optimal_len = max(header_len, int(max_content_len))
        width_px = max(120, min(400, optimal_len * 9 + 30))
        width_str = f'{width_px}px'

        opcoes_unicas = sorted(df_para_filtros[coluna].dropna().astype(str).unique())
        header_cell = html.Th([
            dbc.Button(coluna, id={'type': f'filter-btn-{page_prefix}', 'index': coluna}, className="w-100 h-100 text-truncate", style={'borderRadius': 0, 'textAlign': 'left', 'padding': '10px', 'backgroundColor': '#3c3c3c', 'border': 'none', 'fontWeight': 'bold'}),
            dbc.Popover(dbc.PopoverBody([
                        dcc.Checklist(id={'type': f'select-all-{page_prefix}', 'index': coluna}, options=[{'label': 'Selecionar Tudo', 'value': 'all'}], value=['all'], className="mb-2 fw-bold"),
                        html.Hr(className="my-1"),
                        dcc.Checklist(id={'type': f'options-list-{page_prefix}', 'index': coluna}, options=[{'label': i, 'value': i} for i in opcoes_unicas], value=opcoes_unicas, style={'maxHeight': '200px', 'overflowY': 'auto', 'overflowX': 'hidden'}, labelClassName="d-block text-truncate")
                    ]), target={'type': f'filter-btn-{page_prefix}', 'index': coluna}, trigger="legacy")
        ], style={'width': width_str, 'maxWidth': width_str, 'minWidth': width_str})
        header_rows.append(header_cell)
    return html.Thead(html.Tr(header_rows))

layout_visao_geral = dbc.Container([
    dcc.Store(id='store-pagina-atual-geral', data=1),
    html.H1("Base", className="text-center text-primary mb-4"),
    html.P("Clique nos cabeçalhos abaixo para filtrar os dados da tabela."),
    html.Hr(),
    dbc.Button(
        "Limpar Todos os Filtros",
        id="btn-limpar-filtros-geral",
        color="danger",
        className="mb-3"
    ),
    html.Div([
        dcc.Loading(
            id="loading-geral",
            type="circle",
            children=[
                html.Table([
                    html.Thead(id='tabela-header-geral'),
                    html.Tbody(id='tabela-body-geral')
                ], className='custom-table')
            ]
        )], id='scrollable-container-geral', style={'overflowX': 'auto', 'width': f'{INVERSE_WIDTH:.2f}%'}),
    dbc.Row([
        dbc.Col(dbc.Button("<< Primeira", id="btn-primeira-geral", color="secondary"), width="auto"),
        dbc.Col(dbc.Button("< Anterior", id="btn-anterior-geral", color="primary"), width="auto"),
        dbc.Col(html.Div(id='texto-pagina-geral', style={'textAlign': 'center', 'padding': '0.5rem'}), width="auto"),
        dbc.Col(dbc.Button("Próxima >", id="btn-proxima-geral", color="primary"), width="auto"),
        dbc.Col(dbc.Button("Última >>", id="btn-ultima-geral", color="secondary"), width="auto"),
    ], justify="center", align="center", className="mt-4"),
    html.P("by Tiago Garcéa e Felipe Dias", style={"color": "gray", "font-size": "9pt", "margin-top": "20px"})
], fluid=True)


layout_comparativo = dbc.Container([
    dcc.Store(id='store-pagina-atual-comp', data=1),
    html.H1("Comparativo de Planos", className="text-center text-primary mb-4"),
    html.P("Comparação do plano mais recente vs. o plano anterior para cada localidade. Clique nos cabeçalhos para filtrar."),
    html.Hr(),
    dbc.Button(
        "Limpar Todos os Filtros",
        id="btn-limpar-filtros-comp",
        color="danger",
        className="mb-3"
    ),
    html.Div([
        dcc.Loading(
            id="loading-comp",
            type="circle",
            children=[
                html.Table([
                    html.Thead(id='tabela-header-comp'),
                    html.Tbody(id='tabela-body-comp')
                ], className='custom-table')
            ]
        )], id='scrollable-container-comp', style={'overflowX': 'auto', 'width': f'{INVERSE_WIDTH:.2f}%'}),
    dbc.Row([
        dbc.Col(dbc.Button("<< Primeira", id="btn-primeira-comp", color="secondary"), width="auto"),
        dbc.Col(dbc.Button("< Anterior", id="btn-anterior-comp", color="primary"), width="auto"),
        dbc.Col(html.Div(id='texto-pagina-comp', style={'textAlign': 'center', 'padding': '0.5rem'}), width="auto"),
        dbc.Col(dbc.Button("Próxima >", id="btn-proxima-comp", color="primary"), width="auto"),
        dbc.Col(dbc.Button("Última >>", id="btn-ultima-comp", color="secondary"), width="auto"),
    ], justify="center", align="center", className="mt-4"),
    html.P("by Tiago Garcéa e Felipe Dias", style={"color": "gray", "font-size": "9pt", "margin-top": "20px"})
], fluid=True)

layout_dashboard = dbc.Container([
    html.H1("Big Picture", className="text-center text-primary mb-4"),
    html.Hr(),
    dbc.Row([
        dbc.Col([html.Label("Localidade (Loja):"),
                 dcc.Dropdown(id='filtro-localidade', options=[{'label': i, 'value': i} for i in sorted(df['LOCALIDADE'].dropna().unique())], multi=True, placeholder="Selecione...")], width=6),
        dbc.Col([html.Label("Locadora:"),
                 dcc.Dropdown(id='filtro-locadora', options=[{'label': i, 'value': i} for i in sorted(df['LOCADORA'].dropna().unique())], multi=True, placeholder="Selecione...")], width=6)
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Preço Médio da Diária"), html.H2(id='kpi-preco-medio')]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Total de Pesquisas"), html.H2(id='kpi-total-pesquisas')]))),
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Número de Locadoras"), html.H2(id='kpi-num-locadoras')]))),
    ], className="mb-4 g-3"),
    dbc.Row([
        dbc.Col(dcc.Graph(id='grafico-preco-locadora'), width=8),
        dbc.Col(dcc.Graph(id='grafico-dist-categoria'), width=4)
    ]),
    dbc.Row([dbc.Col(dcc.Graph(id='grafico-preco-tempo'), width=12)]),
    html.Div(id='scrollable-container-dashboard', style={'display': 'none'}), # ID Único
    html.P("by Tiago Garcéa e Felipe Dias", style={"color": "gray", "font-size": "9pt", "margin-top": "20px"})
], fluid=True)

layout_posicionamento = dbc.Container([
    html.H1("Posicionamento por Loja", className="text-center text-primary mb-4"),
    html.P("Utilize os filtros nos cabeçalhos para analisar o posicionamento de preços."),
    html.Hr(),
    html.Div([
        html.Table([
            html.Thead(id='tabela-header-pos-loja'),
        ], className='custom-table', style={'marginBottom': '20px'})
    ]),
    dbc.Row([
        dbc.Col([
            html.H4("Matriz 1: Locadora com Menor Preço", className="text-center mb-3"),
            dbc.Spinner(html.Div(id='matriz-menor-preco-container'))
        ], width=12)
    ], className="mb-5"),
    dbc.Row([
        dbc.Col([
            html.H4("Matriz 2: Diferença Percentual da Foco", className="text-center mb-3"),
            html.P("Valores negativos indicam que o menor preço é X% mais barato que a Foco. Valores positivos indicam que a Foco é o menor preço, e o segundo menor é Y% mais caro.", style={'textAlign': 'center', 'fontSize': '0.9em', 'color': 'gray'}),
            dbc.Spinner(html.Div(id='matriz-diferenca-foco-container'))
        ], width=12)
    ]),
    html.Div(id='scrollable-container-pos-loja', style={'display': 'none'}), # ID Único
    html.P("by Tiago Garcéa e Felipe Dias", style={"color": "gray", "font-size": "9pt", "margin-top": "20px"})
], fluid=True)

layout_posicionamento_categoria = dbc.Container([
    html.H1("Posicionamento por Categoria", className="text-center text-primary mb-4"),
    html.P("Utilize os filtros nos cabeçalhos para analisar o posicionamento de preços por categoria."),
    html.Hr(),
    html.Div([
        html.Table([
            html.Thead(id='tabela-header-pos-cat'),
        ], className='custom-table', style={'marginBottom': '20px'})
    ]),
    dbc.Row([
        dbc.Col([
            html.H4("Matriz 1: Locadora com Menor Preço", className="text-center mb-3"),
            dbc.Spinner(html.Div(id='matriz-menor-preco-categoria-container'))
        ], width=12)
    ], className="mb-5"),
    dbc.Row([
        dbc.Col([
            html.H4("Matriz 2: Diferença Percentual da Foco", className="text-center mb-3"),
            html.P("Valores negativos indicam que o menor preço é X% mais barato que a Foco. Valores positivos indicam que a Foco é o menor preço, e o segundo menor é Y% mais caro.", style={'textAlign': 'center', 'fontSize': '0.9em', 'color': 'gray'}),
            dbc.Spinner(html.Div(id='matriz-diferenca-foco-categoria-container'))
        ], width=12)
    ]),
    html.Div(id='scrollable-container-pos-cat', style={'display': 'none'}), # ID Único
    html.P("by Tiago Garcéa e Felipe Dias", style={"color": "gray", "font-size": "9pt", "margin-top": "20px"})
], fluid=True)

layout_movimentacao_horario = dbc.Container([
    html.H1("Movimentação por Horário", className="text-center text-primary mb-4"),
    html.P("Selecione uma data para analisar a flutuação dos preços das locadoras ao longo daquele dia."),
    html.Hr(),
    dbc.Row([
        dbc.Col([
            html.Label("Data da Pesquisa:"),
            dcc.DatePickerSingle(
                id='filtro-data-horario',
                min_date_allowed=df['DATA_HORA'].min().date() if not df.empty else None,
                max_date_allowed=df['DATA_HORA'].max().date() if not df.empty else None,
                initial_visible_month=df['DATA_HORA'].max().date() if not df.empty else None,
                date=df['DATA_HORA'].max().date() if not df.empty else None,
                display_format='DD/MM/YYYY',
                className="w-100"
            )
        ], width=12, lg=6, className="mb-3"),
        dbc.Col([
            html.Label("Data de Retirada:"),
            dcc.DatePickerSingle(
                id='filtro-retirada-horario',
                min_date_allowed=df['RETIRADA'].min().date() if not df.empty else None,
                max_date_allowed=df['RETIRADA'].max().date() if not df.empty else None,
                initial_visible_month=df['RETIRADA'].max().date() if not df.empty else None,
                date=None,
                display_format='DD/MM/YYYY',
                placeholder="Selecione a Retirada...",
                className="w-100"
            )
        ], width=12, lg=6, className="mb-3"),
    ]),
    dbc.Row([
        dbc.Col([
            html.Label("Localidade (Loja):"),
            dcc.Dropdown(
                id='filtro-localidade-horario',
                options=[{'label': i, 'value': i} for i in sorted(df['LOCALIDADE'].dropna().unique())],
                multi=True,
                placeholder="Todas as localidades"
            )
        ], width=12, lg=3, className="mb-3"),
        dbc.Col([
            html.Label("Locadora:"),
            dcc.Dropdown(
                id='filtro-locadora-horario',
                options=[{'label': i, 'value': i} for i in sorted(df['LOCADORA'].dropna().unique())],
                multi=True,
                placeholder="Todas as locadoras"
            )
        ], width=12, lg=3, className="mb-3"),
        dbc.Col([
            html.Label("LOR:"),
            dcc.Dropdown(
                id='filtro-lor-horario',
                options=[{'label': i, 'value': i} for i in sorted(df['DURAÇÃO'].dropna().unique())] if 'DURAÇÃO' in df.columns else [],
                multi=True,
                placeholder="Todas os LORs"
            )
        ], width=12, lg=3, className="mb-3"),
        dbc.Col([
            html.Label("Categoria:"),
            dcc.Dropdown(
                id='filtro-categoria-horario',
                options=[{'label': i, 'value': i} for i in sorted(df['CATEGORIA'].dropna().unique())],
                multi=True,
                placeholder="Todas as categorias"
            )
        ], width=12, lg=3, className="mb-3"),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id='grafico-movimentacao-horario'), width=12)
    ]),
    html.Div(id='scrollable-container-mov-horario', style={'display': 'none'}), # ID Único
    html.P("by Tiago Garcéa e Felipe ", style={"color": "gray", "font-size": "9pt", "margin-top": "20px"})
], fluid=True)

# ==============================================================================
# INICIALIZAÇÃO DO BANCO DE DADOS EXTERNO
# ==============================================================================
if DATABASE_URL:
    initialize_database()

# --- LAYOUT PRINCIPAL DO APP (MODIFICADO) ---
app.layout = html.Div([
    dcc.Store(id='session-store', storage_type='session'),
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-container') # Este Div conterá ou a tela de login ou o dashboard
])


# ==============================================================================
# 6. CALLBACKS
# ==============================================================================

# --- CALLBACK PRINCIPAL DE ROTEAMENTO E EXIBIÇÃO (NOVO) ---
@app.callback(
    Output('page-container', 'children'),
    Input('url', 'pathname'),
    State('session-store', 'data')
)
def main_router_and_display(pathname, session_data):
    # Se não estiver logado, redireciona para a tela de login, a menos que já esteja nela ou na de registro
    if not session_data or 'username' not in session_data:
        if pathname == '/register':
            return register_layout
        return login_layout

    # Se estiver logado, monta o layout do dashboard
    user_role = session_data.get('role', 'user')
    CONTENT_STYLE = { "marginLeft": "18rem", "padding": "2rem 1rem", "transform": f"scale({INITIAL_SCALE})", "transformOrigin": "top left" }

    # Determina qual página de conteúdo mostrar
    page_content = layout_visao_geral
    if pathname == '/comparativo':
        page_content = layout_comparativo
    elif pathname == '/dashboard':
        page_content = layout_dashboard
    elif pathname == '/posicionamento':
        page_content = layout_posicionamento
    elif pathname == '/posicionamento-categoria':
        page_content = layout_posicionamento_categoria
    elif pathname == '/movimentacao-horario':
        page_content = layout_movimentacao_horario
    elif pathname == '/admin-logs' and user_role == 'admin':
        page_content = layout_admin_logs

    return html.Div([
        create_sidebar(user_role),
        html.Div(page_content, id="page-content", style=CONTENT_STYLE)
    ])


# --- NOVOS CALLBACKS DE AUTENTICAÇÃO ---
@app.callback(
    Output('url', 'pathname', allow_duplicate=True),
    Output('session-store', 'data'),
    Output('login-alert', 'children'),
    Input('login-button', 'n_clicks'),
    State('login-username', 'value'),
    State('login-password', 'value'),
    prevent_initial_call=True
)
def handle_login(n_clicks, username, password):
    if not username or not password:
        return no_update, no_update, dbc.Alert("Preencha todos os campos.", color="warning")

    user = get_user(username)

    if not user:
        return no_update, no_update, dbc.Alert("Usuário ou senha inválidos.", color="danger")

    if user['password_hash'] and check_password_hash(user['password_hash'], password):
        log_access(username)
        session_data = {'username': user['username'], 'role': user['role']}
        return '/', session_data, None
    elif not user['password_hash']:
         return no_update, no_update, dbc.Alert(html.Div(["Parece ser seu primeiro acesso. ", dcc.Link("Clique aqui para cadastrar sua senha.", href="/register")]), color="info")
    else:
        return no_update, no_update, dbc.Alert("Usuário ou senha inválidos.", color="danger")

@app.callback(
    Output('url', 'pathname', allow_duplicate=True),
    Output('session-store', 'clear_data', allow_duplicate=True),
    Input('logout-button', 'n_clicks'),
    prevent_initial_call=True
)
def handle_logout(n_clicks):
    if n_clicks:
        return '/login', True
    return no_update, no_update

@app.callback(
    Output('register-alert', 'children'),
    Output('url', 'pathname', allow_duplicate=True),
    Input('register-button', 'n_clicks'),
    State('register-username', 'value'),
    State('register-password', 'value'),
    State('register-confirm-password', 'value'),
    prevent_initial_call=True
)
def handle_register(n_clicks, username, pw1, pw2):
    if not all([username, pw1, pw2]):
        return dbc.Alert("Preencha todos os campos.", color="warning"), no_update

    username = username.strip().lower()

    if username not in [u.lower() for u in ALL_PREDEFINED_USERS]:
        return dbc.Alert(f"O login '{username}' não existe ou não está na lista de usuários predefinidos.", color="danger"), no_update

    user = get_user(username)
    if not user:
         return dbc.Alert(f"Erro interno: usuário '{username}' não encontrado no banco de dados, embora esteja predefinido.", color="danger"), no_update

    if user['password_hash']:
        return dbc.Alert("Este usuário já possui uma senha cadastrada.", color="warning"), no_update

    if len(pw1) < 4:
        return dbc.Alert("A senha deve ter pelo menos 4 caracteres.", color="warning"), no_update

    if pw1 != pw2:
        return dbc.Alert("As senhas não coincidem.", color="danger"), no_update

    update_user_password(username, pw1)
    success_message = html.Div([
        dbc.Alert("Senha cadastrada! Você será redirecionado para o login...", color="success"),
        dcc.Location(id='redirect-to-login', pathname='/login', refresh=True)
    ])
    return success_message, no_update

@app.callback(
    Output('log-table-container', 'children'),
    Input('url', 'pathname'),
    State('session-store', 'data')
)
def load_log_table(pathname, session_data):
    if pathname == '/admin-logs' and session_data and session_data.get('role') == 'admin':
        df_logs = get_all_logs()
        if df_logs.empty:
            return dbc.Alert("Nenhum registro de acesso encontrado.", color="info")

        df_logs['HORA DO ACESSO'] = pd.to_datetime(df_logs['HORA DO ACESSO']).dt.strftime('%d/%m/%Y %H:%M:%S')
        return dbc.Table.from_dataframe(df_logs, striped=True, bordered=True, hover=True, dark=True, responsive=True)
    return no_update

# ==============================================================================
# SEUS CALLBACKS E FUNÇÕES ORIGINAIS (INTACTOS)
# ==============================================================================

@app.callback(
    Output('tabela-header-geral', 'children'),
    Output('tabela-body-geral', 'children'),
    Output('store-pagina-atual-geral', 'data'),
    Output('texto-pagina-geral', 'children'),
    Output('btn-primeira-geral', 'disabled'),
    Output('btn-anterior-geral', 'disabled'),
    Output('btn-proxima-geral', 'disabled'),
    Output('btn-ultima-geral', 'disabled'),
    Input({'type': 'options-list-geral', 'index': ALL}, 'value'),
    Input('btn-primeira-geral', 'n_clicks'),
    Input('btn-anterior-geral', 'n_clicks'),
    Input('btn-proxima-geral', 'n_clicks'),
    Input('btn-ultima-geral', 'n_clicks'),
    Input("btn-limpar-filtros-geral", "n_clicks"),
    State('store-pagina-atual-geral', 'data'),
    State({'type': 'options-list-geral', 'index': ALL}, 'id')
)
def update_dynamic_table_geral(
    valores_dos_filtros, n_first, n_prev, n_next, n_last, n_limpar,
    pagina_atual, ids_dos_filtros):

    triggered_id = ctx.triggered_id
    if df_tabela.empty:
        return html.Tr(html.Th("Nenhum dado carregado")), html.Tr(html.Td("Nenhum dado para exibir.", colSpan=10, style={'textAlign': 'center'})), 1, "Página 1 de 1", True, True, True, True

    filtros_ativos = {id_filtro['index']: valores for id_filtro, valores in zip(ids_dos_filtros, valores_dos_filtros) if valores}

    if triggered_id == 'btn-limpar-filtros-geral':
        filtros_ativos = {}

    dff = df_tabela.copy()
    if filtros_ativos:
        for nome_da_coluna, valores_selecionados in filtros_ativos.items():
            if nome_da_coluna in dff.columns:
                opcoes_todas = df_tabela[nome_da_coluna].dropna().astype(str).unique()
                if len(valores_selecionados) < len(opcoes_todas):
                    dff = dff[dff[nome_da_coluna].astype(str).isin(valores_selecionados)]

    page_prefix = 'geral'
    colunas_para_exibir_header = ['#'] + df_tabela.columns.tolist()
    header_rows = []

    for coluna in colunas_para_exibir_header:
        if coluna == '#':
            header_rows.append(html.Th("#", style={'width': '50px', 'minWidth': '50px', 'padding': '10px', 'textAlign': 'center'}))
            continue

        df_para_opcoes = df_tabela.copy()
        for outra_coluna, valores in filtros_ativos.items():
            if outra_coluna != coluna and outra_coluna in df_para_opcoes.columns:
                df_para_opcoes = df_para_opcoes[df_para_opcoes[outra_coluna].astype(str).isin(valores)]

        opcoes_unicas = sorted(df_para_opcoes[coluna].dropna().astype(str).unique())
        valores_selecionados_atuais = filtros_ativos.get(coluna, opcoes_unicas)

        header_len = len(coluna)
        max_content_len = df_tabela[coluna].astype(str).str.len().max() if not df_tabela.empty and coluna in df_tabela.columns else 0
        if pd.isna(max_content_len): max_content_len = 0
        optimal_len = max(header_len, int(max_content_len))
        width_px = max(120, min(400, optimal_len * 9 + 30))
        width_str = f'{width_px}px'

        header_cell = html.Th([
            dbc.Button(coluna, id={'type': f'filter-btn-{page_prefix}', 'index': coluna}, className="w-100 h-100 text-truncate", style={'borderRadius': 0, 'textAlign': 'left', 'padding': '10px', 'backgroundColor': '#3c3c3c', 'border': 'none', 'fontWeight': 'bold'}),
            dbc.Popover(dbc.PopoverBody([
                dcc.Checklist(id={'type': f'select-all-{page_prefix}', 'index': coluna}, options=[{'label': 'Selecionar Tudo', 'value': 'all'}], value=['all'] if len(valores_selecionados_atuais) == len(opcoes_unicas) else [], className="mb-2 fw-bold"),
                html.Hr(className="my-1"),
                dcc.Checklist(id={'type': f'options-list-{page_prefix}', 'index': coluna}, options=[{'label': i, 'value': i} for i in opcoes_unicas], value=valores_selecionados_atuais, style={'maxHeight': '200px', 'overflowY': 'auto', 'overflowX': 'hidden'}, labelClassName="d-block text-truncate")
            ]), target={'type': f'filter-btn-{page_prefix}', 'index': coluna}, trigger="legacy")
        ], style={'width': width_str, 'maxWidth': width_str, 'minWidth': width_str})
        header_rows.append(header_cell)

    cabecalho_final = html.Tr(header_rows)

    total_linhas = len(dff)
    total_paginas = math.ceil(total_linhas / PAGE_SIZE) if total_linhas > 0 else 1

    nova_pagina = pagina_atual
    if isinstance(triggered_id, dict) or triggered_id == 'btn-limpar-filtros-geral':
        nova_pagina = 1
    elif isinstance(triggered_id, str):
        if 'btn-primeira' in triggered_id: nova_pagina = 1
        elif 'btn-anterior' in triggered_id: nova_pagina = max(1, pagina_atual - 1)
        elif 'btn-proxima' in triggered_id: nova_pagina = min(total_paginas, pagina_atual + 1)
        elif 'btn-ultima' in triggered_id: nova_pagina = total_paginas

    nova_pagina = min(nova_pagina, total_paginas) if total_paginas > 0 else 1

    dff = dff.reset_index(drop=True)
    dff.insert(0, '#', dff.index + 1)

    start_index = (nova_pagina - 1) * PAGE_SIZE
    end_index = start_index + PAGE_SIZE
    dff_paginado = dff.iloc[start_index:end_index]

    colunas_para_exibir_body = ['#'] + df_tabela.columns.tolist()
    table_rows = []
    if not dff_paginado.empty:
        for _, row in dff_paginado.iterrows():
            table_rows.append(html.Tr([html.Td(row.get(col, '')) for col in colunas_para_exibir_body]))
    else:
        table_rows.append(html.Tr(html.Td("Nenhum dado encontrado.", colSpan=len(colunas_para_exibir_body), style={'textAlign': 'center'})))

    texto_paginacao = f"Página {nova_pagina} de {total_paginas}"
    disable_first = disable_prev = nova_pagina == 1
    disable_last = disable_next = nova_pagina == total_paginas

    return cabecalho_final, table_rows, nova_pagina, texto_paginacao, disable_first, disable_prev, disable_next, disable_last


@app.callback(
    Output('tabela-header-comp', 'children'),
    Output('tabela-body-comp', 'children'),
    Output('store-pagina-atual-comp', 'data'),
    Output('texto-pagina-comp', 'children'),
    Output('btn-primeira-comp', 'disabled'),
    Output('btn-anterior-comp', 'disabled'),
    Output('btn-proxima-comp', 'disabled'),
    Output('btn-ultima-comp', 'disabled'),
    Input({'type': 'options-list-comp', 'index': ALL}, 'value'),
    Input('btn-primeira-comp', 'n_clicks'),
    Input('btn-anterior-comp', 'n_clicks'),
    Input('btn-proxima-comp', 'n_clicks'),
    Input('btn-ultima-comp', 'n_clicks'),
    Input("btn-limpar-filtros-comp", "n_clicks"),
    State('store-pagina-atual-comp', 'data'),
    State({'type': 'options-list-comp', 'index': ALL}, 'id')
)
def update_dynamic_table_comparativo(
    valores_dos_filtros, n_first, n_prev, n_next, n_last, n_limpar,
    pagina_atual, ids_dos_filtros):

    triggered_id = ctx.triggered_id
    if df_comparativo.empty:
        return html.Tr(html.Th("Nenhum dado para comparar")), html.Tr(html.Td("Nenhum dado para exibir.", colSpan=10, style={'textAlign': 'center'})), 1, "Página 1 de 1", True, True, True, True

    filtros_ativos = {id_filtro['index']: valores for id_filtro, valores in zip(ids_dos_filtros, valores_dos_filtros) if valores}

    if triggered_id == 'btn-limpar-filtros-comp':
        filtros_ativos = {}

    dff = df_comparativo.copy()
    if filtros_ativos:
        for nome_da_coluna, valores_selecionados in filtros_ativos.items():
            if nome_da_coluna in dff.columns:
                opcoes_todas = df_comparativo[nome_da_coluna].dropna().astype(str).unique()
                if len(valores_selecionados) < len(opcoes_todas):
                    dff = dff[dff[nome_da_coluna].astype(str).isin(valores_selecionados)]

    page_prefix = 'comp'
    colunas_para_exibir_header = ['#'] + df_comparativo.columns.tolist()
    header_rows = []

    for coluna in colunas_para_exibir_header:
        if coluna == '#':
            header_rows.append(html.Th("#", style={'width': '50px', 'minWidth': '50px', 'padding': '10px', 'textAlign': 'center'}))
            continue

        df_para_opcoes = df_comparativo.copy()
        for outra_coluna, valores in filtros_ativos.items():
            if outra_coluna != coluna and outra_coluna in df_para_opcoes.columns:
                df_para_opcoes = df_para_opcoes[df_para_opcoes[outra_coluna].astype(str).isin(valores)]

        opcoes_unicas = sorted(df_para_opcoes[coluna].dropna().astype(str).unique())
        valores_selecionados_atuais = filtros_ativos.get(coluna, opcoes_unicas)

        header_len = len(coluna)
        max_content_len = df_comparativo[coluna].astype(str).str.len().max() if not df_comparativo.empty and coluna in df_comparativo.columns else 0
        if pd.isna(max_content_len): max_content_len = 0
        optimal_len = max(header_len, int(max_content_len))
        width_px = max(120, min(400, optimal_len * 9 + 30))
        width_str = f'{width_px}px'

        header_cell = html.Th([
            dbc.Button(coluna, id={'type': f'filter-btn-{page_prefix}', 'index': coluna}, className="w-100 h-100 text-truncate", style={'borderRadius': 0, 'textAlign': 'left', 'padding': '10px', 'backgroundColor': '#3c3c3c', 'border': 'none', 'fontWeight': 'bold'}),
            dbc.Popover(dbc.PopoverBody([
                dcc.Checklist(id={'type': f'select-all-{page_prefix}', 'index': coluna}, options=[{'label': 'Selecionar Tudo', 'value': 'all'}], value=['all'] if len(valores_selecionados_atuais) == len(opcoes_unicas) else [], className="mb-2 fw-bold"),
                html.Hr(className="my-1"),
                dcc.Checklist(id={'type': f'options-list-{page_prefix}', 'index': coluna}, options=[{'label': i, 'value': i} for i in opcoes_unicas], value=valores_selecionados_atuais, style={'maxHeight': '200px', 'overflowY': 'auto', 'overflowX': 'hidden'}, labelClassName="d-block text-truncate")
            ]), target={'type': f'filter-btn-{page_prefix}', 'index': coluna}, trigger="legacy")
        ], style={'width': width_str, 'maxWidth': width_str, 'minWidth': width_str})
        header_rows.append(header_cell)

    cabecalho_final = html.Tr(header_rows)

    total_linhas = len(dff)
    total_paginas = math.ceil(total_linhas / PAGE_SIZE) if total_linhas > 0 else 1

    nova_pagina = pagina_atual
    if isinstance(triggered_id, dict) or triggered_id == 'btn-limpar-filtros-comp':
        nova_pagina = 1
    elif isinstance(triggered_id, str):
        if 'btn-primeira' in triggered_id: nova_pagina = 1
        elif 'btn-anterior' in triggered_id: nova_pagina = max(1, pagina_atual - 1)
        elif 'btn-proxima' in triggered_id: nova_pagina = min(total_paginas, pagina_atual + 1)
        elif 'btn-ultima' in triggered_id: nova_pagina = total_paginas

    nova_pagina = min(nova_pagina, total_paginas) if total_paginas > 0 else 1

    dff = dff.reset_index(drop=True)
    dff.insert(0, '#', dff.index + 1)

    start_index = (nova_pagina - 1) * PAGE_SIZE
    end_index = start_index + PAGE_SIZE
    dff_paginado = dff.iloc[start_index:end_index]

    colunas_para_exibir_body = ['#'] + df_comparativo.columns.tolist()
    table_rows = []
    if not dff_paginado.empty:
        for _, row in dff_paginado.iterrows():
            table_rows.append(html.Tr([html.Td(row.get(col, '')) for col in colunas_para_exibir_body]))
    else:
        table_rows.append(html.Tr(html.Td("Nenhum dado encontrado.", colSpan=len(colunas_para_exibir_body), style={'textAlign': 'center'})))

    texto_paginacao = f"Página {nova_pagina} de {total_paginas}"
    disable_first = disable_prev = nova_pagina == 1
    disable_last = disable_next = nova_pagina == total_paginas

    return cabecalho_final, table_rows, nova_pagina, texto_paginacao, disable_first, disable_prev, disable_next, disable_last

# ==============================================================================
# SEÇÃO DE FUNÇÕES E CALLBACKS DE POSICIONAMENTO (ORIGINAL E CORRIGIDO)
# ==============================================================================
def dataframe_to_html_table(df, is_percent=False):
    table_header = [html.Th("RETIRADA")] + [html.Th(col) for col in df.columns]
    table_body = []

    FOCO_CHEAPEST_STYLE = {'backgroundColor': '#28a745', 'color': 'white', 'fontWeight': 'bold'}

    for index, row in df.iterrows():
        cells = [html.Td(index.strftime('%d/%m/%Y'))]

        for val in row:
            cell_style = {}
            cell_content = ""

            if not is_percent and val == 'Foco':
                cell_style = FOCO_CHEAPEST_STYLE
            elif is_percent:
                is_foco_cheapest = False
                if isinstance(val, (int, float)) and not pd.isna(val) and val >= 0:
                    is_foco_cheapest = True
                elif val == 'Único':
                    is_foco_cheapest = True

                if is_foco_cheapest:
                    cell_style = FOCO_CHEAPEST_STYLE

            if pd.isna(val):
                cell_content = "-"
            elif is_percent:
                if isinstance(val, str):
                    cell_content = val
                else:
                    cell_content = f"{val:.1%}"
            else:
                cell_content = val

            cells.append(html.Td(cell_content, style=cell_style))

        table_body.append(html.Tr(cells))

    return html.Div(
        html.Table([
            html.Thead(html.Tr(table_header)),
            html.Tbody(table_body)
        ], className='custom-table'),
        style={'overflowX': 'auto'}
    )

def dataframe_to_html_table_categoria(df, is_percent=False):
    table_header = [html.Th("CATEGORIA")] + [html.Th(col) for col in df.columns]
    table_body = []

    FOCO_CHEAPEST_STYLE = {'backgroundColor': '#28a745', 'color': 'white', 'fontWeight': 'bold'}

    for index, row in df.iterrows():
        cells = [html.Td(index)]

        for val in row:
            cell_style = {}
            cell_content = ""

            if not is_percent and val == 'Foco':
                cell_style = FOCO_CHEAPEST_STYLE
            elif is_percent:
                is_foco_cheapest = False
                if isinstance(val, (int, float)) and not pd.isna(val) and val >= 0:
                    is_foco_cheapest = True
                elif val == 'Único':
                    is_foco_cheapest = True

                if is_foco_cheapest:
                    cell_style = FOCO_CHEAPEST_STYLE

            if pd.isna(val):
                cell_content = "-"
            elif is_percent:
                if isinstance(val, str):
                    cell_content = val
                else:
                    cell_content = f"{val:.1%}"
            else:
                cell_content = val

            cells.append(html.Td(cell_content, style=cell_style))

        table_body.append(html.Tr(cells))

    return html.Div(
        html.Table([
            html.Thead(html.Tr(table_header)),
            html.Tbody(table_body)
        ], className='custom-table'),
        style={'overflowX': 'auto'}
    )

def calculate_foco_diff(group):
    preco_menor_geral = group['PREÇO'].min()
    foco_prices = group[group['LOCADORA'] == 'Foco']['PREÇO']
    if foco_prices.empty:
        return np.nan

    preco_menor_foco = foco_prices.min()

    if preco_menor_foco == preco_menor_geral:
        outras_locadoras = group[group['PREÇO'] > preco_menor_geral]
        if outras_locadoras.empty:
            return "Único"

        segundo_menor_preco = outras_locadoras['PREÇO'].min()
        return (segundo_menor_preco / preco_menor_foco) - 1
    else:
        return (preco_menor_geral / preco_menor_foco) - 1


@app.callback(
    Output('tabela-header-pos-loja', 'children'),
    Output('matriz-menor-preco-container', 'children'),
    Output('matriz-diferenca-foco-container', 'children'),
    Input({'type': 'options-list-pos-loja', 'index': ALL}, 'value'),
    State({'type': 'options-list-pos-loja', 'index': ALL}, 'id')
)
def update_dynamic_posicionamento_loja(valores_dos_filtros, ids_dos_filtros):
    if df_tabela.empty:
        return html.Tr(html.Th("Nenhum dado carregado")), "", ""

    filtros_ativos = {id_filtro['index']: valores for id_filtro, valores in zip(ids_dos_filtros, valores_dos_filtros) if valores}

    is_initial_load = not ctx.triggered_id and not filtros_ativos
    if is_initial_load and plano_recente != "N/A":
        filtros_ativos = {'PLANO': [plano_recente]}

    page_prefix = 'pos-loja'
    header_rows = []
    colunas_de_filtro = df_tabela.columns.tolist()

    for coluna in colunas_de_filtro:
        df_para_opcoes = df_tabela.copy()
        for outra_coluna, valores in filtros_ativos.items():
            if outra_coluna != coluna and outra_coluna in df_para_opcoes.columns:
                df_para_opcoes = df_para_opcoes[df_para_opcoes[outra_coluna].astype(str).isin(valores)]

        opcoes_unicas = sorted(df_para_opcoes[coluna].dropna().astype(str).unique())
        valores_selecionados_atuais = filtros_ativos.get(coluna, opcoes_unicas)

        width_px = max(120, min(400, len(coluna) * 9 + 60))
        width_str = f'{width_px}px'
        header_cell = html.Th([
            dbc.Button(coluna, id={'type': f'filter-btn-{page_prefix}', 'index': coluna}, className="w-100 h-100 text-truncate", style={'borderRadius': 0, 'textAlign': 'left', 'padding': '10px', 'backgroundColor': '#3c3c3c', 'border': 'none', 'fontWeight': 'bold'}),
            dbc.Popover(dbc.PopoverBody([
                dcc.Checklist(id={'type': f'select-all-{page_prefix}', 'index': coluna}, options=[{'label': 'Selecionar Tudo', 'value': 'all'}], value=['all'] if len(valores_selecionados_atuais) == len(opcoes_unicas) else [], className="mb-2 fw-bold"),
                html.Hr(className="my-1"),
                dcc.Checklist(id={'type': f'options-list-{page_prefix}', 'index': coluna}, options=[{'label': i, 'value': i} for i in opcoes_unicas], value=valores_selecionados_atuais, style={'maxHeight': '200px', 'overflowY': 'auto', 'overflowX': 'hidden'}, labelClassName="d-block text-truncate")
            ]), target={'type': f'filter-btn-{page_prefix}', 'index': coluna}, trigger="legacy")
        ], style={'width': width_str, 'maxWidth': width_str, 'minWidth': width_str})
        header_rows.append(header_cell)
    cabecalho_final = html.Tr(header_rows)

    dff = df_calculos.copy()
    if filtros_ativos:
        for nome_da_coluna, valores in filtros_ativos.items():
            if not valores:
                msg_vazia = html.P("Nenhum dado para a seleção (um filtro está vazio).")
                return cabecalho_final, msg_vazia, msg_vazia
            if nome_da_coluna in dff.columns:
                dff = dff[dff[nome_da_coluna].astype(str).isin([str(v) for v in valores])]

    if dff.empty:
        msg_vazia = html.P("Nenhum dado encontrado para os filtros aplicados.")
        return cabecalho_final, msg_vazia, msg_vazia

    try:
        idx_min_preco = dff.loc[dff.groupby(['RETIRADA', 'LOCALIDADE'])['PREÇO'].idxmin()]
        matriz1_df = idx_min_preco.pivot_table(index='RETIRADA', columns='LOCALIDADE', values='LOCADORA', aggfunc='first').fillna("-")
        tabela1_html = dataframe_to_html_table(matriz1_df)
    except Exception as e:
        tabela1_html = dbc.Alert(f"Erro ao gerar Matriz 1: {e}", color="danger")

    try:
        matriz2_series = dff.groupby(['RETIRADA', 'LOCALIDADE']).apply(calculate_foco_diff, include_groups=False)
        matriz2_df = matriz2_series.unstack(level='LOCALIDADE')
        tabela2_html = dataframe_to_html_table(matriz2_df, is_percent=True)
    except Exception as e:
        tabela2_html = dbc.Alert(f"Erro ao gerar Matriz 2: {e}", color="danger")

    return cabecalho_final, tabela1_html, tabela2_html

@app.callback(
    Output('tabela-header-pos-cat', 'children'),
    Output('matriz-menor-preco-categoria-container', 'children'),
    Output('matriz-diferenca-foco-categoria-container', 'children'),
    Input({'type': 'options-list-pos-cat', 'index': ALL}, 'value'),
    State({'type': 'options-list-pos-cat', 'index': ALL}, 'id')
)
def update_dynamic_posicionamento_categoria(valores_dos_filtros, ids_dos_filtros):
    if df_tabela.empty:
        return html.Tr(html.Th("Nenhum dado carregado")), "", ""

    filtros_ativos = {id_filtro['index']: valores for id_filtro, valores in zip(ids_dos_filtros, valores_dos_filtros) if valores}

    is_initial_load = not ctx.triggered_id and not filtros_ativos
    if is_initial_load and plano_recente != "N/A":
        filtros_ativos = {'PLANO': [plano_recente]}

    page_prefix = 'pos-cat'
    header_rows = []
    colunas_de_filtro = df_tabela.columns.tolist()

    for coluna in colunas_de_filtro:
        df_para_opcoes = df_tabela.copy()
        for outra_coluna, valores in filtros_ativos.items():
            if outra_coluna != coluna and outra_coluna in df_para_opcoes.columns:
                df_para_opcoes = df_para_opcoes[df_para_opcoes[outra_coluna].astype(str).isin(valores)]

        opcoes_unicas = sorted(df_para_opcoes[coluna].dropna().astype(str).unique())
        valores_selecionados_atuais = filtros_ativos.get(coluna, opcoes_unicas)

        width_px = max(120, min(400, len(coluna) * 9 + 60))
        width_str = f'{width_px}px'
        header_cell = html.Th([
            dbc.Button(coluna, id={'type': f'filter-btn-{page_prefix}', 'index': coluna}, className="w-100 h-100 text-truncate", style={'borderRadius': 0, 'textAlign': 'left', 'padding': '10px', 'backgroundColor': '#3c3c3c', 'border': 'none', 'fontWeight': 'bold'}),
            dbc.Popover(dbc.PopoverBody([
                dcc.Checklist(id={'type': f'select-all-{page_prefix}', 'index': coluna}, options=[{'label': 'Selecionar Tudo', 'value': 'all'}], value=['all'] if len(valores_selecionados_atuais) == len(opcoes_unicas) else [], className="mb-2 fw-bold"),
                html.Hr(className="my-1"),
                dcc.Checklist(id={'type': f'options-list-{page_prefix}', 'index': coluna}, options=[{'label': i, 'value': i} for i in opcoes_unicas], value=valores_selecionados_atuais, style={'maxHeight': '200px', 'overflowY': 'auto', 'overflowX': 'hidden'}, labelClassName="d-block text-truncate")
            ]), target={'type': f'filter-btn-{page_prefix}', 'index': coluna}, trigger="legacy")
        ], style={'width': width_str, 'maxWidth': width_str, 'minWidth': width_str})
        header_rows.append(header_cell)
    cabecalho_final = html.Tr(header_rows)

    dff = df_calculos.copy()
    if filtros_ativos:
        for nome_da_coluna, valores in filtros_ativos.items():
            if not valores:
                msg_vazia = html.P("Nenhum dado para a seleção (um filtro está vazio).")
                return cabecalho_final, msg_vazia, msg_vazia
            if nome_da_coluna in dff.columns:
                dff = dff[dff[nome_da_coluna].astype(str).isin([str(v) for v in valores])]

    if dff.empty:
        msg_vazia = html.P("Nenhum dado encontrado para os filtros aplicados.")
        return cabecalho_final, msg_vazia, msg_vazia

    try:
        idx_min_preco = dff.loc[dff.groupby(['CATEGORIA', 'RETIRADA'])['PREÇO'].idxmin()]
        matriz1_df = idx_min_preco.pivot_table(
            index='CATEGORIA', columns='RETIRADA', values='LOCADORA', aggfunc='first'
        )
        matriz1_df.columns = [col.strftime('%d/%m') for col in matriz1_df.columns]
        matriz1_df.fillna("-", inplace=True)
        tabela1_html = dataframe_to_html_table_categoria(matriz1_df)
    except Exception as e:
        tabela1_html = dbc.Alert(f"Erro ao gerar Matriz 1: {e}", color="danger")

    try:
        matriz2_series = dff.groupby(['CATEGORIA', 'RETIRADA']).apply(calculate_foco_diff, include_groups=False)
        matriz2_df = matriz2_series.unstack(level='RETIRADA')
        matriz2_df.columns = [col.strftime('%d/%m') for col in matriz2_df.columns]
        tabela2_html = dataframe_to_html_table_categoria(matriz2_df, is_percent=True)
    except Exception as e:
        tabela2_html = dbc.Alert(f"Erro ao gerar Matriz 2: {e}", color="danger")

    return cabecalho_final, tabela1_html, tabela2_html


@app.callback(
    Output('grafico-movimentacao-horario', 'figure'),
    Output('filtro-localidade-horario', 'options'),
    Output('filtro-locadora-horario', 'options'),
    Output('filtro-categoria-horario', 'options'),
    Output('filtro-lor-horario', 'options'),
    Input('filtro-data-horario', 'date'),
    Input('filtro-retirada-horario', 'date'),
    Input('filtro-localidade-horario', 'value'),
    Input('filtro-locadora-horario', 'value'),
    Input('filtro-categoria-horario', 'value'),
    Input('filtro-lor-horario', 'value'),
)
def update_movimentacao_horario(selected_date, selected_retirada_date, localidades, locadoras, categorias, lor):
    fig_vazia = go.Figure().update_layout(paper_bgcolor="#3c3c3c", plot_bgcolor="#2b2b2b", font_color="#f0f0f0", xaxis={"visible": False}, yaxis={"visible": False})

    if not selected_date or df.empty:
        fig_vazia.update_layout(title_text='Por favor, selecione uma data de pesquisa para começar')
        return fig_vazia, [], [], [], []

    dff = df.copy()
    start_date = pd.to_datetime(selected_date).normalize()
    end_date = start_date + pd.Timedelta(days=1)
    dff = dff[(dff['DATA_HORA'] >= start_date) & (dff['DATA_HORA'] < end_date)]

    if selected_retirada_date:
        start_retirada = pd.to_datetime(selected_retirada_date).normalize()
        end_retirada = start_retirada + pd.Timedelta(days=1)
        dff = dff[(dff['RETIRADA'] >= start_retirada) & (dff['RETIRADA'] < end_retirada)]

    opcoes_localidade_dinamicas = [{'label': i, 'value': i} for i in sorted(dff['LOCALIDADE'].dropna().unique())]
    opcoes_locadora_dinamicas = [{'label': i, 'value': i} for i in sorted(dff['LOCADORA'].dropna().unique())]
    opcoes_categoria_dinamicas = [{'label': i, 'value': i} for i in sorted(dff['CATEGORIA'].dropna().unique())]
    opcoes_lor_dinamicas = [{'label': i, 'value': i} for i in sorted(dff['DURAÇÃO'].dropna().unique())] if 'DURAÇÃO' in dff.columns else []

    if localidades: dff = dff[dff['LOCALIDADE'].isin(localidades)]
    if locadoras: dff = dff[dff['LOCADORA'].isin(locadoras)]
    if categorias: dff = dff[dff['CATEGORIA'].isin(categorias)]
    if lor and 'DURAÇÃO' in dff.columns: dff = dff[dff['DURAÇÃO'].isin(lor)]

    if dff.empty:
        fig_vazia.update_layout(title_text='Nenhum dado encontrado para os filtros selecionados')
        return fig_vazia, opcoes_localidade_dinamicas, opcoes_locadora_dinamicas, opcoes_categoria_dinamicas, opcoes_lor_dinamicas

    dff = dff.sort_values('DATA_HORA')
    title_date = pd.to_datetime(selected_date).strftime('%d/%m/%Y')

    fig = px.line(dff, x='DATA_HORA', y='PREÇO', color='LOCADORA', title=f'Variação de Preço ao Longo do Dia - {title_date}', markers=True, labels={'DATA_HORA': 'Horário da Pesquisa', 'PREÇO': 'Preço (R$)', 'LOCADORA': 'Locadora'})
    fig.update_xaxes(tickformat='%H:%M')
    fig.update_layout(paper_bgcolor="#3c3c3c", plot_bgcolor="#2b2b2b", font_color="#f0f0f0", xaxis_gridcolor="#444", yaxis_gridcolor="#444", legend_title_text='Locadora', xaxis_title="Horário da Pesquisa", yaxis_title="Preço (R$)")

    return fig, opcoes_localidade_dinamicas, opcoes_locadora_dinamicas, opcoes_categoria_dinamicas, opcoes_lor_dinamicas


@app.callback(
    Output('kpi-preco-medio', 'children'),
    Output('kpi-total-pesquisas', 'children'),
    Output('kpi-num-locadoras', 'children'),
    Output('grafico-preco-locadora', 'figure'),
    Output('grafico-dist-categoria', 'figure'),
    Output('grafico-preco-tempo', 'figure'),
    Output('filtro-localidade', 'options'),
    Output('filtro-locadora', 'options'),
    Input('filtro-localidade', 'value'),
    Input('filtro-locadora', 'value')
)
def update_dashboard(localidades, locadoras):
    fig_vazia = go.Figure().update_layout(title_text='Nenhum dado para os filtros', paper_bgcolor="#3c3c3c", plot_bgcolor="#2b2b2b", font_color="#f0f0f0", xaxis={"visible": False}, yaxis={"visible": False})

    if df.empty:
         return "R$ 0,00", "0", "0", fig_vazia, fig_vazia, fig_vazia, [], []

    df_op_loc = df.copy()
    if locadoras:
        df_op_loc = df_op_loc[df_op_loc['LOCADORA'].isin(locadoras)]
    opcoes_localidade = [{'label': i, 'value': i} for i in sorted(df_op_loc['LOCALIDADE'].dropna().unique())]

    df_op_locadora = df.copy()
    if localidades:
        df_op_locadora = df_op_locadora[df_op_locadora['LOCALIDADE'].isin(localidades)]
    opcoes_locadora = [{'label': i, 'value': i} for i in sorted(df_op_locadora['LOCADORA'].dropna().unique())]

    dff = df.copy()
    if localidades: dff = dff[dff['LOCALIDADE'].isin(localidades)]
    if locadoras: dff = dff[dff['LOCADORA'].isin(locadoras)]

    if dff.empty:
        return "R$ 0,00", "0", "0", fig_vazia, fig_vazia, fig_vazia, opcoes_localidade, opcoes_locadora

    custom_template = { "layout": { "paper_bgcolor": "#3c3c3c", "plot_bgcolor": "#2b2b2b", "font": {"color": "#f0f0f0"}, "xaxis": {"gridcolor": "#444"}, "yaxis": {"gridcolor": "#444"}, "colorway": px.colors.sequential.Plotly3 } }
    preco_medio = dff['PREÇO'].mean()
    fig_preco_loc = px.bar(dff.groupby('LOCADORA')['PREÇO'].mean().sort_values(ascending=False).reset_index(), x='LOCADORA', y='PREÇO', title='Preço Médio por Locadora', text_auto='.2f', template=custom_template).update_traces(marker_color='#42a5f5', textposition='outside')
    fig_dist_cat = px.pie(dff, names='CATEGORIA', title='Distribuição por Categoria', hole=0.4, template=custom_template).update_traces(textposition='inside', textinfo='percent+label')
    df_preco_tempo = dff.groupby(dff['DATA_HORA'].dt.date)['PREÇO'].mean().reset_index()
    fig_preco_tmp = px.line(df_preco_tempo, x='DATA_HORA', y='PREÇO', title='Evolução do Preço Médio', markers=True, template=custom_template)

    return (f"R$ {preco_medio:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            f"{len(dff):,}".replace(",", "."), f"{dff['LOCADORA'].nunique()}",
            fig_preco_loc, fig_dist_cat, fig_preco_tmp,
            opcoes_localidade, opcoes_locadora)


# --- CALLBACKS CLIENTSIDE ---
def create_clientside_filter_callback(page_prefix):
    clientside_callback(
    f"""
    function(option_values, select_all_values, option_options, btn_ids) {{
        const ctx = dash_clientside.callback_context;
        if (!ctx.triggered.length) return [dash_clientside.no_update, dash_clientside.no_update, dash_clientside.no_update];

        const new_option_values = [...option_values].map(() => dash_clientside.no_update);
        const new_select_all_values = [...select_all_values].map(() => dash_clientside.no_update);
        const new_btn_labels = btn_ids.map(() => dash_clientside.no_update);

        const trigger_id_str = ctx.triggered[0].prop_id.split('.')[0];
        if (!trigger_id_str) return [dash_clientside.no_update, dash_clientside.no_update, dash_clientside.no_update];

        const trigger_id = JSON.parse(trigger_id_str);
        if (!trigger_id.index) return [dash_clientside.no_update, dash_clientside.no_update, dash_clientside.no_update];

        const idx = trigger_id.index;
        const array_idx = btn_ids.findIndex(id => id.index === idx);
        if (array_idx === -1) return [dash_clientside.no_update, dash_clientside.no_update, dash_clientside.no_update];

        const all_options = option_options[array_idx].map(opt => opt.value);
        let final_selected_values;

        if (trigger_id.type.startsWith('select-all')) {{
            final_selected_values = select_all_values[array_idx].length > 0 ? all_options : [];
            new_option_values[array_idx] = final_selected_values;
        }} else {{
            final_selected_values = option_values[array_idx];
            new_select_all_values[array_idx] = (final_selected_values.length === all_options.length) ? ['all'] : [];
        }}

        let label = `${{idx}}`;
        const selected_count = final_selected_values.length;
        if (selected_count === 0) label += ' (Nenhum)';
        else if (selected_count < all_options.length) label += ` (${{selected_count}} de ${{all_options.length}})`;

        new_btn_labels[array_idx] = label;
        return [new_option_values, new_select_all_values, new_btn_labels];
    }}
    """,
        Output({ 'type': f'options-list-{page_prefix}', 'index': ALL }, 'value'),
        Output({ 'type': f'select-all-{page_prefix}', 'index': ALL }, 'value'),
        Output({ 'type': f'filter-btn-{page_prefix}', 'index': ALL }, 'children'),
        Input({ 'type': f'options-list-{page_prefix}', 'index': ALL }, 'value'),
        Input({ 'type': f'select-all-{page_prefix}', 'index': ALL }, 'value'),
        State({ 'type': f'options-list-{page_prefix}', 'index': ALL }, 'options'),
        State({ 'type': f'filter-btn-{page_prefix}', 'index': ALL }, 'id'),
        prevent_initial_call=True
    )

create_clientside_filter_callback('geral')
create_clientside_filter_callback('comp')
create_clientside_filter_callback('pos-loja')
create_clientside_filter_callback('pos-cat')

clientside_callback(
    """
    function(n_in, n_out, n_reset, page_style, s_geral, s_comp, s_dash, s_pos_l, s_pos_c, s_mov_h) {
        const ctx = dash_clientside.callback_context;
        if (!ctx.triggered.length || !ctx.triggered[0].prop_id) return [dash_clientside.no_update, ...Array(7).fill(dash_clientside.no_update)];

        const button_id = ctx.triggered[0]['prop_id'].split('.')[0];

        // This callback should only fire when a user is logged in and the page-content exists
        if (!page_style) return [dash_clientside.no_update, ...Array(7).fill(dash_clientside.no_update)];

        let new_page_style = {...page_style};

        const transform_str = new_page_style.transform || 'scale(1.0)';
        const scale_match = transform_str.match(/scale\\(([^)]+)\\)/);
        let current_scale = scale_match ? parseFloat(scale_match[1]) : 0.8;

        if (button_id === 'zoom-in-btn') current_scale += 0.1;
        else if (button_id === 'zoom-out-btn') current_scale -= 0.1;
        else if (button_id === 'zoom-reset-btn') current_scale = 0.8; // Reset para o valor inicial

        current_scale = Math.max(0.5, Math.min(2.0, current_scale));
        new_page_style.transform = `scale(${current_scale.toFixed(2)})`;
        new_page_style.transformOrigin = 'top left';

        const new_width = `${((1 / current_scale) * 100).toFixed(2)}%`;

        const update_style = (style_obj) => {
            if (style_obj && typeof style_obj.display !== 'undefined' && style_obj.display !== 'none') {
                let new_style = {...style_obj};
                new_style.width = new_width;
                return new_style;
            }
            return dash_clientside.no_update;
        };

        return [new_page_style, update_style(s_geral), update_style(s_comp), update_style(s_dash), update_style(s_pos_l), update_style(s_pos_c), update_style(s_mov_h)];
    }
    """,
    Output('page-content', 'style'),
    Output('scrollable-container-geral', 'style'),
    Output('scrollable-container-comp', 'style'),
    Output('scrollable-container-dashboard', 'style'),
    Output('scrollable-container-pos-loja', 'style'),
    Output('scrollable-container-pos-cat', 'style'),
    Output('scrollable-container-mov-horario', 'style'),
    Input('zoom-in-btn', 'n_clicks'),
    Input('zoom-out-btn', 'n_clicks'),
    Input('zoom-reset-btn', 'n_clicks'),
    State('page-content', 'style'),
    State('scrollable-container-geral', 'style'),
    State('scrollable-container-comp', 'style'),
    State('scrollable-container-dashboard', 'style'),
    State('scrollable-container-pos-loja', 'style'),
    State('scrollable-container-pos-cat', 'style'),
    State('scrollable-container-mov-horario', 'style'),
    prevent_initial_call=True
)


if __name__ == '__main__':
    app.run_server(debug=True)