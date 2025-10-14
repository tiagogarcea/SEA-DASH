# ==============================================================================
# 1. IMPORTAÇÃO DAS BIBLIOTECAS
# ==============================================================================
import dash
from dash import dcc, html, Input, Output, State, ALL, clientside_callback, ctx, callback, no_update
import plotly.express as px
import pandas as pd
import numpy as np
import dash_bootstrap_components as dbc
import socket
import urllib.parse
import math
import os
from datetime import datetime
import plotly.graph_objects as go

# ==============================================================================
# 2. FUNÇÃO PARA GERAR O DATAFRAME COMPARATIVO (VERSÃO ROBUSTA E CORRIGIDA)
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

        # Filtra o DF base para a localidade e os dois planos relevantes
        df_local = df[(df['localidade'] == localidade) & (df['plano'].isin(planos))]

        # Acha o preço MÍNIMO para cada grupo (categoria, retirada, etc.)
        idx_min = df_local.groupby(['plano', 'retirada', 'duração', 'categoria'])['preço'].idxmin()
        df_mais_baratos = df_local.loc[idx_min]

        # Separa os dados do plano recente e do anterior
        df_recente = df_mais_baratos[df_mais_baratos['plano'] == plano_recente]
        df_anterior = df_mais_baratos[df_mais_baratos['plano'] == plano_anterior]

        # Colunas para fazer a junção (o que define um "carro" para comparação)
        merge_cols = ['localidade', 'retirada', 'duração', 'categoria']

        # Junta os dois dataframes para ter os preços lado a lado
        df_merged = pd.merge(
            df_recente,
            df_anterior,
            on=merge_cols,
            suffixes=('_atual', '_anterior')
        )

        if not df_merged.empty:
            lista_dfs_comparados.append(df_merged)

    # --- 2.4. Consolidar e formatar o resultado final ---
    if not lista_dfs_comparados:
        print("Nenhuma correspondência de categoria/retirada encontrada entre os planos das localidades.")
        return pd.DataFrame()

    df_final = pd.concat(lista_dfs_comparados, ignore_index=True)

    # --- 2.5. Calcular a variação e formatar ---
    df_final['variacao_preco'] = (df_final['preço_atual'] / df_final['preço_anterior']) - 1

    # Renomear colunas para o relatório final
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

    # Garante que apenas as colunas finais existam e estejam na ordem correta
    df_relatorio_final = df_relatorio_final[colunas_finais]

    print(f"Análise comparativa concluída! {len(df_relatorio_final)} variações encontradas em múltiplas localidades.")
    return df_relatorio_final

# ==============================================================================
# 3. CARREGAMENTO E LIMPEZA DOS DADOS
# ==============================================================================
last_update_string = "N/A"
df_comparativo = pd.DataFrame()

try:
    # --- CÓDIGO MODIFICADO ---
    script_dir = os.path.dirname(__file__)
    caminho_arquivo = os.path.join(script_dir, 'dados_consolidados.parquet')
    df_original = pd.read_parquet(caminho_arquivo)

    mod_time_timestamp = os.path.getmtime(caminho_arquivo)
    mod_time_datetime = datetime.fromtimestamp(mod_time_timestamp)
    last_update_string = mod_time_datetime.strftime('%d/%m/%Y %H:%M:%S')

    print("Arquivo Parquet carregado com sucesso!")
    print(f"Total de {len(df_original)} linhas carregadas.")
    print(f"Última modificação do arquivo: {last_update_string}")

    # Gera o DataFrame comparativo a partir de uma cópia com colunas minúsculas
    df_para_comparativo = df_original.copy()
    df_para_comparativo.columns = [str(col).lower() for col in df_para_comparativo.columns]
    df_comparativo = gerar_df_comparativo_robusto(df_para_comparativo)

    # Continua o tratamento normal com 'df' usando colunas maiúsculas
    df = df_original.copy()
    df.columns = [str(col).upper() for col in df.columns]

    # ### LINHA CORRETIVA ADICIONADA ###
    # Garante que o DataFrame principal não tenha colunas duplicadas
    df = df.loc[:, ~df.columns.duplicated()]

    plano_recente = df_comparativo['PLANO ATUAL']
    plano_recente = plano_recente.tolist()
    plano_recente = plano_recente[0]
    plano_recente = f'{plano_recente}'
    print(plano_recente)

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

    # Renomeando 'DATA' para 'DATA_HORA' para evitar conflito com .dt.date
    df.rename(columns={'DATA': 'DATA_HORA'}, inplace=True)
    df['DATA_HORA'] = pd.to_datetime(df['DATA_HORA'], errors='coerce')
    df['DATA_HORA'] = pd.to_datetime(df['DATA_HORA'].dt.date.astype(str) + ' ' + df['HORA'].astype(str), errors='coerce')

    if 'RETIRADA' in df.columns:
        df['RETIRADA'] = pd.to_datetime(df['RETIRADA'], errors='coerce')

    df.dropna(subset=['PREÇO', 'DATA_HORA', 'RETIRADA', 'LOCALIDADE', 'LOCADORA', 'CATEGORIA'], inplace=True)

    df_calculos = df.copy()
    df_calculos['RETIRADA'] = df_calculos['RETIRADA'].dt.date

    df_tabela = df.copy()
    # Renomeia DATA_HORA de volta para DATA para exibição na tabela
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
    df = pd.DataFrame({
        'LOCALIDADE': ['N/A'], 'RETIRADA': [pd.to_datetime('2025-09-26')], 'CRIAÇÃO': [None], 'LOR': [0],
        'MODELO': ['Erro ao carregar'], 'PREÇO': [0], 'LOCADORA': ['N/A'], 'PLANO': ['N/A'],
        'HORA': ['00:00:00'], 'OTA': ['N/A'], 'CAMBIO': ['N/A'], 'CATEGORIA': ['N/A'], 'DATA_HORA': [pd.to_datetime('2025-09-26')]
    })
    df_calculos = df.copy()
    df_tabela = df.copy()
    df_tabela.rename(columns={'DATA_HORA': 'DATA'}, inplace=True)
    df_tabela['RETIRADA'] = df_tabela['RETIRADA'].dt.strftime('%Y-%m-%d')
    df_tabela['DATA'] = df_tabela['DATA'].dt.strftime('%Y-%m-%d %H:%M:%S')

# ==============================================================================
# 4. INICIALIZAÇÃO E ESTILO DO APP
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

SIDEBAR_STYLE = { "position": "fixed", "top": 0, "left": 0, "bottom": 0, "width": "18rem", "padding": "2rem 1rem", "background-color": "#2b2b2b", "border-right": "1px solid #444" }
CONTENT_STYLE = { "marginLeft": "18rem", "padding": "2rem 1rem", "transform": f"scale({INITIAL_SCALE})", "transformOrigin": "top left", }

sidebar = html.Div([
    html.H2("SEA-DASH", className="display-6"), html.Hr(),
    dbc.Nav([
        dbc.NavLink("Base", href="/", active="exact"),
        dbc.NavLink("Comparativo", href="/comparativo", active="exact"),
        dbc.NavLink("Big Picture", href="/dashboard", active="exact"),
        dbc.NavLink("Posicionamento por Loja", href="/posicionamento", active="exact"),
        dbc.NavLink("Posicionamento por Categoria", href="/posicionamento-categoria", active="exact"),
        dbc.NavLink("Movimentação Horário", href="/movimentacao-horario", active="exact"),
    ], vertical=True, pills=True),
    html.Div([
        html.Hr(),
        html.P("Controles", style={'textAlign': 'center', 'fontWeight':'bold'}),
        dbc.Button("Recarregar Dados 🔄", href="/", color="success", className="w-100 mb-2"),
        dbc.ButtonGroup([
            dbc.Button('-', id='zoom-out-btn', color='primary'),
            dbc.Button('Reset', id='zoom-reset-btn', color='secondary'),
            dbc.Button('+', id='zoom-in-btn', color='primary')
        ], size="sm", className="d-flex"),
        html.Small(
            f"Última Atualização: {last_update_string}",
            style={'color': '#999', 'fontSize': '0.75rem', 'display': 'block', 'textAlign': 'center', 'marginTop': '10px'}
        )
    ], style={'position': 'absolute', 'bottom': '1rem', 'width': 'calc(100% - 2rem)'})
], style=SIDEBAR_STYLE)

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
    # ADICIONE O BOTÃO AQUI
    dbc.Button(
        "Limpar Todos os Filtros",
        id="btn-limpar-filtros-geral",
        color="danger",
        className="mb-3"
    ),
    html.Div([
        # Adicionamos o dcc.Loading envolvendo a tabela
        dcc.Loading(
            id="loading-geral",
            type="circle", # Você pode escolher outros tipos como "dot", "cube", etc.
            children=[
                html.Table([
                    html.Thead(id='tabela-header-geral'),
                    html.Tbody(id='tabela-body-geral')
                ], className='custom-table')
            ]
        )], id='scrollable-container-comp', style={'overflowX': 'auto', 'width': f'{INVERSE_WIDTH:.2f}%'}),
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
    # ADICIONE O BOTÃO AQUI
    dbc.Button(
        "Limpar Todos os Filtros",
        id="btn-limpar-filtros-comp",
        color="danger",
        className="mb-3"
    ),
    html.Div([
        # Adicionamos o dcc.Loading envolvendo a tabela
        dcc.Loading(
            id="loading-comp",
            type="circle", # Você pode escolher outros tipos como "dot", "cube", etc.
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
            html.Thead(id='tabela-header-pos-loja'), # <-- LINHA MODIFICADA COM ID ÚNICO
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
            html.Thead(id='tabela-header-pos-cat'), # <-- LINHA MODIFICADA COM ID ÚNICO
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
                min_date_allowed=df['DATA_HORA'].min().date(),
                max_date_allowed=df['DATA_HORA'].max().date(),
                initial_visible_month=df['DATA_HORA'].max().date(),
                date=df['DATA_HORA'].max().date(),
                display_format='DD/MM/YYYY',
                className="w-100"
            )
        ], width=12, lg=6, className="mb-3"),
        dbc.Col([
            html.Label("Data de Retirada:"),
            dcc.DatePickerSingle(
                id='filtro-retirada-horario',
                min_date_allowed=df['RETIRADA'].min().date(),
                max_date_allowed=df['RETIRADA'].max().date(),
                initial_visible_month=df['RETIRADA'].max().date(),
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
                options=[{'label': i, 'value': i} for i in sorted(df['DURAÇÃO'].dropna().unique())],
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


app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    sidebar,
    html.Div(id="page-content", style=CONTENT_STYLE)
])

# ==============================================================================
# 6. CALLBACKS
# ==============================================================================
@app.callback(Output('page-content', 'children'), [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/comparativo':
        return layout_comparativo
    elif pathname == '/dashboard':
        return layout_dashboard
    elif pathname == '/posicionamento':
        return layout_posicionamento
    elif pathname == '/posicionamento-categoria':
        return layout_posicionamento_categoria
    elif pathname == '/movimentacao-horario':
        return layout_movimentacao_horario
    else:
        return layout_visao_geral

@app.callback(
    # IDs atualizados conforme sua nova implementação
    Output('tabela-header-geral', 'children'),
    Output('tabela-body-geral', 'children'),
    Output('store-pagina-atual-geral', 'data'),
    Output('texto-pagina-geral', 'children'),
    Output('btn-primeira-geral', 'disabled'),
    Output('btn-anterior-geral', 'disabled'),
    Output('btn-proxima-geral', 'disabled'),
    Output('btn-ultima-geral', 'disabled'),

    # Inputs conforme sua nova implementação
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

    # Mapeia colunas para os valores selecionados nos filtros
    filtros_ativos = {id_filtro['index']: valores for id_filtro, valores in zip(ids_dos_filtros, valores_dos_filtros) if valores}

    # LÓGICA DE RESET: Se o botão de limpar foi clicado, esvazie os filtros
    if triggered_id == 'btn-limpar-filtros-geral':
        filtros_ativos = {}

    # --- Aplica os filtros ao DataFrame principal ---
    dff = df_tabela.copy()
    if filtros_ativos:
        for nome_da_coluna, valores_selecionados in filtros_ativos.items():
            opcoes_todas = df_tabela[nome_da_coluna].dropna().astype(str).unique()
            if len(valores_selecionados) < len(opcoes_todas):
                dff = dff[dff[nome_da_coluna].astype(str).isin(valores_selecionados)]

    # --- ETAPA 1: GERAÇÃO DO CABEÇALHO DINÂMICO ---
    page_prefix = 'geral'
    colunas_para_exibir_header = ['#'] + df_tabela.columns.tolist()
    header_rows = []

    for coluna in colunas_para_exibir_header:
        if coluna == '#':
            header_cell = html.Th("#", style={'width': '50px', 'minWidth': '50px', 'padding': '10px', 'textAlign': 'center'})
            header_rows.append(header_cell)
            continue

        # LÓGICA PRINCIPAL: Filtra o DF por TODAS AS OUTRAS colunas para obter as opções corretas
        df_para_opcoes = df_tabela.copy()
        for outra_coluna, valores in filtros_ativos.items():
            if outra_coluna != coluna:
                df_para_opcoes = df_para_opcoes[df_para_opcoes[outra_coluna].astype(str).isin(valores)]

        opcoes_unicas = sorted(df_para_opcoes[coluna].dropna().astype(str).unique())
        valores_selecionados_atuais = filtros_ativos.get(coluna, opcoes_unicas)

        # Lógica para calcular a largura da coluna
        header_len = len(coluna)
        max_content_len = df_tabela[coluna].astype(str).str.len().max()
        if pd.isna(max_content_len): max_content_len = 0
        optimal_len = max(header_len, int(max_content_len))
        width_px = max(120, min(400, optimal_len * 9 + 30))
        width_str = f'{width_px}px'

        header_cell = html.Th([
            dbc.Button(coluna, id={'type': f'filter-btn-{page_prefix}', 'index': coluna}, className="w-100 h-100 text-truncate", style={'borderRadius': 0, 'textAlign': 'left', 'padding': '10px', 'backgroundColor': '#3c3c3c', 'border': 'none', 'fontWeight': 'bold'}),
            dbc.Popover(dbc.PopoverBody([
                dcc.Checklist(
                    id={'type': f'select-all-{page_prefix}', 'index': coluna},
                    options=[{'label': 'Selecionar Tudo', 'value': 'all'}],
                    value=['all'] if len(valores_selecionados_atuais) == len(opcoes_unicas) else [],
                    className="mb-2 fw-bold"
                ),
                html.Hr(className="my-1"),
                dcc.Checklist(
                    id={'type': f'options-list-{page_prefix}', 'index': coluna},
                    options=[{'label': i, 'value': i} for i in opcoes_unicas],
                    value=valores_selecionados_atuais,
                    style={'maxHeight': '200px', 'overflowY': 'auto', 'overflowX': 'hidden'},
                    labelClassName="d-block text-truncate"
                )
            ]), target={'type': f'filter-btn-{page_prefix}', 'index': coluna}, trigger="legacy")
        ], style={'width': width_str, 'maxWidth': width_str, 'minWidth': width_str})
        header_rows.append(header_cell)

    cabecalho_final = html.Tr(header_rows)

    # --- ETAPA 2: LÓGICA DE PAGINAÇÃO ---
    total_linhas = len(dff)
    total_paginas = math.ceil(total_linhas / PAGE_SIZE) if total_linhas > 0 else 1

    nova_pagina = pagina_atual
    # Reseta a página se um filtro foi alterado ou limpo
    if isinstance(triggered_id, dict) or triggered_id == 'btn-limpar-filtros-comp':
        nova_pagina = 1
    elif isinstance(triggered_id, str):
        if 'btn-primeira' in triggered_id: nova_pagina = 1
        elif 'btn-anterior' in triggered_id: nova_pagina = max(1, pagina_atual - 1)
        elif 'btn-proxima' in triggered_id: nova_pagina = min(total_paginas, pagina_atual + 1)
        elif 'btn-ultima' in triggered_id: nova_pagina = total_paginas

    nova_pagina = min(nova_pagina, total_paginas)

    # --- ETAPA 3: GERAÇÃO DO CORPO DA TABELA ---
    dff = dff.reset_index(drop=True)
    dff.insert(0, '#', dff.index + 1)

    start_index = (nova_pagina - 1) * PAGE_SIZE
    end_index = start_index + PAGE_SIZE
    dff_paginado = dff.iloc[start_index:end_index]

    colunas_para_exibir_body = ['#'] + df_tabela.columns.tolist()
    table_rows = []
    if not dff_paginado.empty:
        for _, row in dff_paginado.iterrows():
            table_rows.append(html.Tr([html.Td(row[col]) for col in colunas_para_exibir_body]))
    else:
        table_rows.append(html.Tr(html.Td("Nenhum dado encontrado.", colSpan=len(colunas_para_exibir_body), style={'textAlign': 'center'})))

    # --- ETAPA 4: ATUALIZAÇÃO DOS ELEMENTOS DE PAGINAÇÃO ---
    texto_paginacao = f"Página {nova_pagina} de {total_paginas}"
    disable_first = disable_prev = nova_pagina == 1
    disable_last = disable_next = nova_pagina == total_paginas

    # --- ETAPA 5: RETORNO DE TODOS OS OUTPUTS NA ORDEM CORRETA ---
    return cabecalho_final, table_rows, nova_pagina, texto_paginacao, disable_first, disable_prev, disable_next, disable_last


# --- Callback Tabela COMPARATIVO ---
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

    # Mapeia colunas para os valores selecionados nos filtros
    filtros_ativos = {id_filtro['index']: valores for id_filtro, valores in zip(ids_dos_filtros, valores_dos_filtros) if valores}

    # LÓGICA DE RESET: Se o botão de limpar foi clicado, esvazie os filtros
    if triggered_id == 'btn-limpar-filtros-comp':
        filtros_ativos = {}

    # --- Aplica os filtros ao DataFrame principal ---
    dff = df_comparativo.copy()
    if filtros_ativos:
        for nome_da_coluna, valores_selecionados in filtros_ativos.items():
            opcoes_todas = df_comparativo[nome_da_coluna].dropna().astype(str).unique()
            if len(valores_selecionados) < len(opcoes_todas):
                dff = dff[dff[nome_da_coluna].astype(str).isin(valores_selecionados)]

    # --- ETAPA 1: GERAÇÃO DO CABEÇALHO DINÂMICO ---
    page_prefix = 'comp'
    colunas_para_exibir_header = ['#'] + df_comparativo.columns.tolist()
    header_rows = []

    for coluna in colunas_para_exibir_header:
        if coluna == '#':
            header_cell = html.Th("#", style={'width': '50px', 'minWidth': '50px', 'padding': '10px', 'textAlign': 'center'})
            header_rows.append(header_cell)
            continue

        # LÓGICA PRINCIPAL: Filtra o DF por TODAS AS OUTRAS colunas para obter as opções corretas
        df_para_opcoes = df_comparativo.copy()
        for outra_coluna, valores in filtros_ativos.items():
            if outra_coluna != coluna:
                df_para_opcoes = df_para_opcoes[df_para_opcoes[outra_coluna].astype(str).isin(valores)]

        opcoes_unicas = sorted(df_para_opcoes[coluna].dropna().astype(str).unique())
        valores_selecionados_atuais = filtros_ativos.get(coluna, opcoes_unicas)

        # Lógica para calcular a largura da coluna
        header_len = len(coluna)
        max_content_len = df_comparativo[coluna].astype(str).str.len().max()
        if pd.isna(max_content_len): max_content_len = 0
        optimal_len = max(header_len, int(max_content_len))
        width_px = max(120, min(400, optimal_len * 9 + 30))
        width_str = f'{width_px}px'

        header_cell = html.Th([
            dbc.Button(coluna, id={'type': f'filter-btn-{page_prefix}', 'index': coluna}, className="w-100 h-100 text-truncate", style={'borderRadius': 0, 'textAlign': 'left', 'padding': '10px', 'backgroundColor': '#3c3c3c', 'border': 'none', 'fontWeight': 'bold'}),
            dbc.Popover(dbc.PopoverBody([
                dcc.Checklist(
                    id={'type': f'select-all-{page_prefix}', 'index': coluna},
                    options=[{'label': 'Selecionar Tudo', 'value': 'all'}],
                    value=['all'] if len(valores_selecionados_atuais) == len(opcoes_unicas) else [],
                    className="mb-2 fw-bold"
                ),
                html.Hr(className="my-1"),
                dcc.Checklist(
                    id={'type': f'options-list-{page_prefix}', 'index': coluna},
                    options=[{'label': i, 'value': i} for i in opcoes_unicas],
                    value=valores_selecionados_atuais,
                    style={'maxHeight': '200px', 'overflowY': 'auto', 'overflowX': 'hidden'},
                    labelClassName="d-block text-truncate"
                )
            ]), target={'type': f'filter-btn-{page_prefix}', 'index': coluna}, trigger="legacy")
        ], style={'width': width_str, 'maxWidth': width_str, 'minWidth': width_str})
        header_rows.append(header_cell)

    cabecalho_final = html.Tr(header_rows)

    # --- ETAPA 2: LÓGICA DE PAGINAÇÃO ---
    total_linhas = len(dff)
    total_paginas = math.ceil(total_linhas / PAGE_SIZE) if total_linhas > 0 else 1

    nova_pagina = pagina_atual
    # Reseta a página se um filtro foi alterado ou limpo
    if isinstance(triggered_id, dict) or triggered_id == 'btn-limpar-filtros-comp':
        nova_pagina = 1
    elif isinstance(triggered_id, str):
        if 'btn-primeira' in triggered_id: nova_pagina = 1
        elif 'btn-anterior' in triggered_id: nova_pagina = max(1, pagina_atual - 1)
        elif 'btn-proxima' in triggered_id: nova_pagina = min(total_paginas, pagina_atual + 1)
        elif 'btn-ultima' in triggered_id: nova_pagina = total_paginas

    nova_pagina = min(nova_pagina, total_paginas)

    # --- ETAPA 3: GERAÇÃO DO CORPO DA TABELA ---
    dff = dff.reset_index(drop=True)
    dff.insert(0, '#', dff.index + 1)

    start_index = (nova_pagina - 1) * PAGE_SIZE
    end_index = start_index + PAGE_SIZE
    dff_paginado = dff.iloc[start_index:end_index]

    colunas_para_exibir_body = ['#'] + df_comparativo.columns.tolist()
    table_rows = []
    if not dff_paginado.empty:
        for _, row in dff_paginado.iterrows():
            table_rows.append(html.Tr([html.Td(row[col]) for col in colunas_para_exibir_body]))
    else:
        table_rows.append(html.Tr(html.Td("Nenhum dado encontrado.", colSpan=len(colunas_para_exibir_body), style={'textAlign': 'center'})))

    # --- ETAPA 4: ATUALIZAÇÃO DOS ELEMENTOS DE PAGINAÇÃO ---
    texto_paginacao = f"Página {nova_pagina} de {total_paginas}"
    disable_first = disable_prev = nova_pagina == 1
    disable_last = disable_next = nova_pagina == total_paginas

    # --- ETAPA 5: RETORNO DE TODOS OS OUTPUTS NA ORDEM CORRETA ---
    return cabecalho_final, table_rows, nova_pagina, texto_paginacao, disable_first, disable_prev, disable_next, disable_last

# ==============================================================================
# SEÇÃO DE FUNÇÕES E CALLBACKS DE POSICIONAMENTO
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


###################################################### POSICIONAMENTO POR LOJA #################################################

@app.callback(
    # Outputs: Adicionamos o cabeçalho como o primeiro Output
    Output('tabela-header-pos-loja', 'children'),
    Output('matriz-menor-preco-container', 'children'),
    Output('matriz-diferenca-foco-container', 'children'),

    # Inputs e States
    Input({'type': 'options-list-pos-loja', 'index': ALL}, 'value'),
    State({'type': 'options-list-pos-loja', 'index': ALL}, 'id')
)
def update_dynamic_posicionamento_loja(valores_dos_filtros, ids_dos_filtros):

    # Mapeia colunas para os valores selecionados nos filtros
    filtros_ativos = {id_filtro['index']: valores for id_filtro, valores in zip(ids_dos_filtros, valores_dos_filtros) if valores}

    is_initial_load = not ctx.triggered_id and not filtros_ativos
    if is_initial_load:
        # Defina seu filtro padrão aqui.
        # Formato: {'Nome_da_Coluna': ['Valor_a_Filtrar']}
        filtros_ativos = {'PLANO': [plano_recente]}
    # --- FIM DA LÓGICA DO FILTRO PADRÃO ---
    # --- ETAPA 1: GERAÇÃO DO CABEÇALHO DINÂMICO ---
    page_prefix = 'pos-loja'
    header_rows = []
    # Usamos df_tabela.columns para definir QUAIS filtros existem.
    colunas_de_filtro = df_tabela.columns.tolist()

    for coluna in colunas_de_filtro:
        # LÓGICA PRINCIPAL: Filtra o df_tabela por TODAS AS OUTRAS colunas para obter as opções corretas
        df_para_opcoes = df_tabela.copy()
        for outra_coluna, valores in filtros_ativos.items():
            if outra_coluna != coluna and outra_coluna in df_para_opcoes.columns:
                df_para_opcoes = df_para_opcoes[df_para_opcoes[outra_coluna].astype(str).isin(valores)]

        opcoes_unicas = sorted(df_para_opcoes[coluna].dropna().astype(str).unique())
        valores_selecionados_atuais = filtros_ativos.get(coluna, opcoes_unicas)

        # Lógica de estilo e criação do componente Popover/Checklist
        width_px = max(120, min(400, len(coluna) * 9 + 60))
        width_str = f'{width_px}px'

        header_cell = html.Th([
            dbc.Button(coluna, id={'type': f'filter-btn-{page_prefix}', 'index': coluna}, className="w-100 h-100 text-truncate", style={'borderRadius': 0, 'textAlign': 'left', 'padding': '10px', 'backgroundColor': '#3c3c3c', 'border': 'none', 'fontWeight': 'bold'}),
            dbc.Popover(dbc.PopoverBody([
                dcc.Checklist(
                    id={'type': f'select-all-{page_prefix}', 'index': coluna},
                    options=[{'label': 'Selecionar Tudo', 'value': 'all'}],
                    value=['all'] if len(valores_selecionados_atuais) == len(opcoes_unicas) else [],
                    className="mb-2 fw-bold"
                ),
                html.Hr(className="my-1"),
                dcc.Checklist(
                    id={'type': f'options-list-{page_prefix}', 'index': coluna},
                    options=[{'label': i, 'value': i} for i in opcoes_unicas],
                    value=valores_selecionados_atuais,
                    style={'maxHeight': '200px', 'overflowY': 'auto', 'overflowX': 'hidden'},
                    labelClassName="d-block text-truncate"
                )
            ]), target={'type': f'filter-btn-{page_prefix}', 'index': coluna}, trigger="legacy")
        ], style={'width': width_str, 'maxWidth': width_str, 'minWidth': width_str})
        header_rows.append(header_cell)

    cabecalho_final = html.Tr(header_rows)


    # --- ETAPA 2: FILTRAGEM E GERAÇÃO DAS MATRIZES ---
    # Esta parte é a lógica do seu callback original.
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

    # Gera a Matriz 1 (Menor Preço)
    try:
        idx_min_preco = dff.loc[dff.groupby(['RETIRADA', 'LOCALIDADE'])['PREÇO'].idxmin()]
        matriz1_df = idx_min_preco.pivot_table(
            index='RETIRADA', columns='LOCALIDADE', values='LOCADORA', aggfunc='first'
        ).fillna("-")
        tabela1_html = dataframe_to_html_table(matriz1_df)
    except Exception as e:
        tabela1_html = dbc.Alert(f"Erro ao gerar Matriz 1: {e}", color="danger")

    # Gera a Matriz 2 (Diferença para o Foco)
    try:
        # Adicionado include_groups=False para evitar DeprecationWarning e garantir compatibilidade
        matriz2_series = dff.groupby(['RETIRADA', 'LOCALIDADE']).apply(calculate_foco_diff, include_groups=False)
        matriz2_df = matriz2_series.unstack(level='LOCALIDADE')
        tabela2_html = dataframe_to_html_table(matriz2_df, is_percent=True)
    except Exception as e:
        tabela2_html = dbc.Alert(f"Erro ao gerar Matriz 2: {e}", color="danger")

    # --- ETAPA 3: RETORNO DE TODOS OS OUTPUTS ---
    return cabecalho_final, tabela1_html, tabela2_html

############################################## POSICIONAMENTO CAT ##################################################




@app.callback(
    # Outputs: O novo cabeçalho e as duas matrizes existentes
    Output('tabela-header-pos-cat', 'children'),
    Output('matriz-menor-preco-categoria-container', 'children'),
    Output('matriz-diferenca-foco-categoria-container', 'children'),

    # Inputs e States
    Input({'type': 'options-list-pos-cat', 'index': ALL}, 'value'),
    State({'type': 'options-list-pos-cat', 'index': ALL}, 'id')
)
def update_dynamic_posicionamento_categoria(valores_dos_filtros, ids_dos_filtros):

    # Mapeia colunas para os valores selecionados nos filtros
    filtros_ativos = {id_filtro['index']: valores for id_filtro, valores in zip(ids_dos_filtros, valores_dos_filtros) if valores}

    is_initial_load = not ctx.triggered_id and not filtros_ativos
    if is_initial_load:
        # Defina seu filtro padrão aqui.
        # Formato: {'Nome_da_Coluna': ['Valor_a_Filtrar']}
        filtros_ativos = {'PLANO': [plano_recente]}
    # --- ETAPA 1: GERAÇÃO DO CABEÇALHO DINÂMICO ---
    # Nota: O cabeçalho é gerado a partir do df_tabela original para obter todas as colunas de filtro.
    page_prefix = 'pos-cat'
    header_rows = []
    # Usamos df_tabela.columns para definir QUAIS filtros existem.
    colunas_de_filtro = df_tabela.columns.tolist()

    for coluna in colunas_de_filtro:
        # LÓGICA PRINCIPAL: Filtra o df_tabela por TODAS AS OUTRAS colunas para obter as opções corretas
        df_para_opcoes = df_tabela.copy()
        for outra_coluna, valores in filtros_ativos.items():
            if outra_coluna != coluna and outra_coluna in df_para_opcoes.columns:
                df_para_opcoes = df_para_opcoes[df_para_opcoes[outra_coluna].astype(str).isin(valores)]

        opcoes_unicas = sorted(df_para_opcoes[coluna].dropna().astype(str).unique())
        valores_selecionados_atuais = filtros_ativos.get(coluna, opcoes_unicas)

        # Lógica de estilo e criação do componente Popover/Checklist
        width_px = max(120, min(400, len(coluna) * 9 + 60))
        width_str = f'{width_px}px'

        header_cell = html.Th([
            dbc.Button(coluna, id={'type': f'filter-btn-{page_prefix}', 'index': coluna}, className="w-100 h-100 text-truncate", style={'borderRadius': 0, 'textAlign': 'left', 'padding': '10px', 'backgroundColor': '#3c3c3c', 'border': 'none', 'fontWeight': 'bold'}),
            dbc.Popover(dbc.PopoverBody([
                dcc.Checklist(
                    id={'type': f'select-all-{page_prefix}', 'index': coluna},
                    options=[{'label': 'Selecionar Tudo', 'value': 'all'}],
                    value=['all'] if len(valores_selecionados_atuais) == len(opcoes_unicas) else [],
                    className="mb-2 fw-bold"
                ),
                html.Hr(className="my-1"),
                dcc.Checklist(
                    id={'type': f'options-list-{page_prefix}', 'index': coluna},
                    options=[{'label': i, 'value': i} for i in opcoes_unicas],
                    value=valores_selecionados_atuais,
                    style={'maxHeight': '200px', 'overflowY': 'auto', 'overflowX': 'hidden'},
                    labelClassName="d-block text-truncate"
                )
            ]), target={'type': f'filter-btn-{page_prefix}', 'index': coluna}, trigger="legacy")
        ], style={'width': width_str, 'maxWidth': width_str, 'minWidth': width_str})
        header_rows.append(header_cell)

    cabecalho_final = html.Tr(header_rows)

    # --- ETAPA 2: FILTRAGEM E GERAÇÃO DAS MATRIZES ---
    # Esta parte é a lógica do seu callback original, agora usando o 'filtros_ativos' que já definimos.
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

    # Gera a Matriz 1 (Menor Preço)
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

    # Gera a Matriz 2 (Diferença para o Foco)
    try:
        matriz2_series = dff.groupby(['CATEGORIA', 'RETIRADA']).apply(calculate_foco_diff, include_groups=False)
        matriz2_df = matriz2_series.unstack(level='RETIRADA')
        matriz2_df.columns = [col.strftime('%d/%m') for col in matriz2_df.columns]
        tabela2_html = dataframe_to_html_table_categoria(matriz2_df, is_percent=True)
    except Exception as e:
        tabela2_html = dbc.Alert(f"Erro ao gerar Matriz 2: {e}", color="danger")

    # --- ETAPA 3: RETORNO DE TODOS OS OUTPUTS ---
    return cabecalho_final, tabela1_html, tabela2_html

############################################# MOVIMENTAÇÃO POR HÓRARIO --- AJUSTADO #####################################################################
@app.callback(
    Output('grafico-movimentacao-horario', 'figure'),
    Output('filtro-data-horario', 'options'),
    Output('filtro-retirada-horario', 'options'),
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
    # Cria uma figura vazia padrão para casos sem dados
    fig_vazia = go.Figure()
    fig_vazia.update_layout(
        paper_bgcolor="#3c3c3c", plot_bgcolor="#2b2b2b",
        font_color="#f0f0f0",
        xaxis={"visible": False}, yaxis={"visible": False}
    )

    # CORREÇÃO 1: Se a data não for selecionada, retorna um valor para cada um dos 6 outputs.
    if not selected_date:
        fig_vazia.update_layout(title_text='Por favor, selecione uma data de pesquisa para começar')
        # dash.no_update evita re-renderizar componentes desnecessariamente.
        return fig_vazia, no_update, no_update, no_update, no_update, no_update, no_update

    dff = df.copy()

    # AJUSTE DE PERFORMANCE: Filtrar por range é muito mais rápido que usar .dt.date
    start_date = pd.to_datetime(selected_date).normalize()
    end_date = start_date + pd.Timedelta(days=1)
    dff = dff[(dff['DATA_HORA'] >= start_date) & (dff['DATA_HORA'] < end_date)]

    # Aplica os outros filtros
    if selected_retirada_date:
        start_retirada = pd.to_datetime(selected_retirada_date).normalize()
        end_retirada = start_retirada + pd.Timedelta(days=1)
        dff = dff[(dff['RETIRADA'] >= start_retirada) & (dff['RETIRADA'] < end_retirada)]

    # --- Lógica para Filtros Dinâmicos ---
    # Para cada filtro, calculamos as opções válidas com base nos outros filtros já aplicados.

    # Opções para Lor
    df_lor = dff.copy()
    if localidades: df_lor = df_lor[df_lor['LOCALIDADE'].isin(localidades)]
    if locadoras: df_lor = df_lor[df_lor['LOCADORA'].isin(locadoras)]
    if categorias: df_lor = df_lor[df_lor['CATEGORIA'].isin(categorias)]
    opcoes_lor = [{'label': i, 'value': i} for i in sorted(df_lor['DURAÇÃO'].dropna().unique())]


    # Opções para Localidade
    df_op_loc = dff.copy()
    if lor: df_op_loc = df_op_loc[df_op_loc['DURAÇÃO'].isin(lor)]
    if locadoras: df_op_loc = df_op_loc[df_op_loc['LOCADORA'].isin(locadoras)]
    if categorias: df_op_loc = df_op_loc[df_op_loc['CATEGORIA'].isin(categorias)]
    opcoes_localidade = [{'label': i, 'value': i} for i in sorted(df_op_loc['LOCALIDADE'].dropna().unique())]

    # Opções para Locadora
    df_op_locadora = dff.copy()
    if lor: df_op_locadora = df_op_locadora[df_op_locadora['DURAÇÃO'].isin(lor)]
    if localidades: df_op_locadora = df_op_locadora[df_op_locadora['LOCALIDADE'].isin(localidades)]
    if categorias: df_op_locadora = df_op_locadora[df_op_locadora['CATEGORIA'].isin(categorias)]
    opcoes_locadora = [{'label': i, 'value': i} for i in sorted(df_op_locadora['LOCADORA'].dropna().unique())]

    # Opções para Categoria
    df_op_cat = dff.copy()
    if lor: df_op_cat = df_op_cat[df_op_cat['DURAÇÃO'].isin(lor)]
    if localidades: df_op_cat = df_op_cat[df_op_cat['LOCALIDADE'].isin(localidades)]
    if locadoras: df_op_cat = df_op_cat[df_op_cat['LOCADORA'].isin(locadoras)]
    opcoes_categoria = [{'label': i, 'value': i} for i in sorted(df_op_cat['CATEGORIA'].dropna().unique())]

    # Aplica os filtros dos dropdowns para o gráfico final
    if localidades:
        dff = dff[dff['LOCALIDADE'].isin(localidades)]
    if locadoras:
        dff = dff[dff['LOCADORA'].isin(locadoras)]
    if categorias:
        dff = dff[dff['CATEGORIA'].isin(categorias)]
    if lor:
        dff = dff[dff['DURAÇÃO'].isin(lor)]

    # CORREÇÃO 2: Se o DataFrame ficar vazio, retorna um gráfico de aviso e as opções calculadas.
    if dff.empty:
        fig_vazia.update_layout(title_text='Nenhum dado encontrado para os filtros selecionados')
        return fig_vazia, no_update, no_update, opcoes_localidade, opcoes_locadora, opcoes_categoria, opcoes_lor

    dff = dff.sort_values('DATA_HORA')
    title_date = pd.to_datetime(selected_date).strftime('%d/%m/%Y')

    fig = px.line(
        dff,
        x='DATA_HORA',
        y='PREÇO',
        color='LOCADORA',
        title=f'Variação de Preço ao Longo do Dia - {title_date}',
        markers=True,
        labels={'DATA_HORA': 'Horário da Pesquisa', 'PREÇO': 'Preço (R$)', 'LOCADORA': 'Locadora'}
    )

    fig.update_xaxes(tickformat='%H:%M')
    fig.update_layout(
        paper_bgcolor="#3c3c3c", plot_bgcolor="#2b2b2b",
        font_color="#f0f0f0",
        xaxis_gridcolor="#444", yaxis_gridcolor="#444",
        legend_title_text='Locadora',
        xaxis_title="Horário da Pesquisa",
        yaxis_title="Preço (R$)"
    )

    # CORREÇÃO 3: Retorna a figura e as opções para todos os filtros, na ordem correta.
    return fig, no_update, no_update, opcoes_localidade, opcoes_locadora, opcoes_categoria, opcoes_lor


##################################################### BIG PICTURE -----  ###################################################################################


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
    # --- LÓGICA PARA FILTROS DINÂMICOS ---
    # Primeiro, calculamos as opções válidas para cada filtro com base na seleção do outro.

    # Opções para Localidade (baseado na seleção de Locadora)
    df_op_loc = df.copy()
    if locadoras:
        df_op_loc = df_op_loc[df_op_loc['LOCADORA'].isin(locadoras)]
    opcoes_localidade = [{'label': i, 'value': i} for i in sorted(df_op_loc['LOCALIDADE'].dropna().unique())]

    # Opções para Locadora (baseado na seleção de Localidade)
    df_op_locadora = df.copy()
    if localidades:
        df_op_locadora = df_op_locadora[df_op_locadora['LOCALIDADE'].isin(localidades)]
    opcoes_locadora = [{'label': i, 'value': i} for i in sorted(df_op_locadora['LOCADORA'].dropna().unique())]

    # Agora, aplicamos os filtros para os KPIs e gráficos
    dff = df.copy()
    if localidades: dff = dff[dff['LOCALIDADE'].isin(localidades)]
    if locadoras: dff = dff[dff['LOCADORA'].isin(locadoras)]

    # CORREÇÃO 1: Trata o caso de DataFrame vazio retornando um valor para CADA um dos 8 Outputs.
    if dff.empty:
        fig_vazia = go.Figure()
        fig_vazia.update_layout(
            title_text='Nenhum dado encontrado para os filtros selecionados',
            paper_bgcolor="#3c3c3c", plot_bgcolor="#2b2b2b", font_color="#f0f0f0",
            xaxis={"visible": False}, yaxis={"visible": False}
        )
        # Retorna 8 valores: 3 KPIs, 3 gráficos vazios e as 2 listas de opções calculadas.
        return "R$ 0,00", "0", "0", fig_vazia, fig_vazia, fig_vazia, opcoes_localidade, opcoes_locadora

    # Lógica para criar KPIs e gráficos (seu código original)
    custom_template = { "layout": { "paper_bgcolor": "#3c3c3c", "plot_bgcolor": "#2b2b2b", "font": {"color": "#f0f0f0"}, "xaxis": {"gridcolor": "#444"}, "yaxis": {"gridcolor": "#444"}, "colorway": px.colors.sequential.Plotly3 } }

    preco_medio = dff['PREÇO'].mean()
    fig_preco_loc = px.bar(dff.groupby('LOCADORA')['PREÇO'].mean().sort_values(ascending=False).reset_index(),
                           x='LOCADORA', y='PREÇO', title='Preço Médio por Locadora', text_auto='.2f', template=custom_template)
    fig_preco_loc.update_traces(marker_color='#42a5f5', textposition='outside')

    fig_dist_cat = px.pie(dff, names='CATEGORIA', title='Distribuição por Categoria', hole=0.4, template=custom_template)
    fig_dist_cat.update_traces(textposition='inside', textinfo='percent+label')

    df_preco_tempo = dff.groupby(dff['DATA_HORA'].dt.date)['PREÇO'].mean().reset_index()
    fig_preco_tmp = px.line(df_preco_tempo, x='DATA_HORA', y='PREÇO', title='Evolução do Preço Médio', markers=True, template=custom_template)

    # CORREÇÃO 2: Garante que o retorno principal também tenha 8 elementos, incluindo as opções dos filtros.
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
        if (!ctx.triggered.length) return [dash_clientside.no_update, ...Array(6).fill(dash_clientside.no_update)];

        const button_id = ctx.triggered[0]['prop_id'].split('.')[0];
        let new_page_style = {...(page_style || {})};

        const transform_str = new_page_style.transform || 'scale(1.0)';
        const scale_match = transform_str.match(/scale\\(([^)]+)\\)/);
        let current_scale = scale_match ? parseFloat(scale_match[1]) : 1.0;

        if (button_id === 'zoom-in-btn') current_scale += 0.1;
        else if (button_id === 'zoom-out-btn') current_scale -= 0.1;
        else if (button_id === 'zoom-reset-btn') current_scale = 1.0;

        current_scale = Math.max(0.5, Math.min(2.0, current_scale));
        new_page_style.transform = `scale(${current_scale.toFixed(2)})`;
        new_page_style.transformOrigin = 'top left';

        const new_width = `${((1 / current_scale) * 100).toFixed(2)}%`;

        const update_style = (style_obj) => {
            if (style_obj && style_obj.display !== 'none') {
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
