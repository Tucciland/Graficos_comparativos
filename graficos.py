import firebirdsql as fb
import pandas as pd
import plotly.express as px
import streamlit as st
import datetime as dt
import calendar
from dateutil.relativedelta import relativedelta
import os

# Obtém o diretório do script em execução
script_dir = os.path.dirname(os.path.abspath(__file__))

# Caminho completo para o arquivo de banco de dados
db_path = os.path.join(script_dir, 'bancos\\MODELO_2.FDB')

def seleciona_dados():
    conn = fb.connect(user='SYSDBA', password='masterkey', database=db_path, host='localhost', charset='ISO8859_1')

    #Select's utilizados
    sql_vendas_totais = "SELECT total, data_emissao FROM vendas_master WHERE situacao = 'F'"
    sql_produtos_vendidos = "select p.descricao, vd.id_produto, p.grupo, g.descricao as NOME_GRUPO, vd.qtd, vd.total, vm.data_emissao,vd.unidade from vendas_detalhe vd, vendas_master vm, produto p, grupo g where vd.fkvenda = vm.codigo and p.codigo = vd.id_produto and p.grupo = g.codigo"
    sql_entrada_saida = "select EMISSAO, TIPO_MOVIMENTO, ENTRADA, SAIDA from CAIXA where TIPO_MOVIMENTO = 'RE'"

    #Aplicando os selects
    #Vendas Totais
    cursor = conn.cursor()
    cursor.execute(sql_vendas_totais)
    resultados_total_vendas = cursor.fetchall()
    colunas_vendas_totais = [desc[0] for desc in cursor.description]
    df_TotalVenda = pd.DataFrame(resultados_total_vendas, columns=colunas_vendas_totais)
    cursor.close()

    #Produtos Vendidos
    cursor = conn.cursor()
    cursor.execute(sql_produtos_vendidos)
    resultados_produtos_vendidos = cursor.fetchall()
    colunas_produtos_vendidos = [desc[0] for desc in cursor.description]
    df_produtos_vendidos = pd.DataFrame(resultados_produtos_vendidos, columns=colunas_produtos_vendidos)
    cursor.close()

    #Recebimentos
    cursor = conn.cursor()
    cursor.execute(sql_entrada_saida)
    resultados_entrada_saida = cursor.fetchall()
    colunas_produtos_vendidos = [desc[0] for desc in cursor.description]
    df_entrada_saida = pd.DataFrame(resultados_entrada_saida, columns=colunas_produtos_vendidos)
    cursor.close()

    return df_TotalVenda, df_produtos_vendidos, df_entrada_saida

@st.cache_data
def trata_base_dados(data_inicial_selecionada, data_final_selecionada):

    df_TotalVenda, df_produtos_vendidos, df_entrada_saida = seleciona_dados()

    #Tratamento das Bases
    #Vendas totais
    df_TotalVenda['DATA_EMISSAO'] = pd.to_datetime(df_TotalVenda['DATA_EMISSAO'])
    df_TotalVenda['ANO'] = df_TotalVenda['DATA_EMISSAO'].dt.year
    df_TotalVenda['MES'] = df_TotalVenda['DATA_EMISSAO'].dt.month
    df_TotalVenda['DIA'] = df_TotalVenda['DATA_EMISSAO'].dt.day

    #Filtragem pelo periodo da SIDEBAR
    df_TotalVenda_filtrado = df_TotalVenda[(df_TotalVenda['DATA_EMISSAO'] >= data_inicial_selecionada) & (df_TotalVenda['DATA_EMISSAO'] <= data_final_selecionada)]
    cont_vendas = df_TotalVenda_filtrado['TOTAL'].count()

    res_total_vendas = df_TotalVenda_filtrado.groupby('DIA')['TOTAL'].sum().reset_index()
    res_total_vendas = res_total_vendas.rename(columns={'total': 'TOTAL'})


    #Produtos Vendidos
    df_produtos_vendidos['DATA_EMISSAO'] = pd.to_datetime(df_produtos_vendidos['DATA_EMISSAO'])
    df_produtos_vendidos['ANO'] = df_produtos_vendidos['DATA_EMISSAO'].dt.year
    df_produtos_vendidos['MES'] = df_produtos_vendidos['DATA_EMISSAO'].dt.month
    df_produtos_vendidos['DIA'] = df_produtos_vendidos['DATA_EMISSAO'].dt.day
    df_produtos_vendidos['QTD'] = pd.to_numeric(df_produtos_vendidos['QTD'], errors='coerce')
    df_produtos_vendidos['TOTAL'] = pd.to_numeric(df_produtos_vendidos['TOTAL'], errors='coerce')

    #Filtragem pelo periodo da SIDEBAR
    df_produtos_vendidos_filtrado = df_produtos_vendidos[(df_produtos_vendidos['DATA_EMISSAO'] >= data_inicial_selecionada) & (df_produtos_vendidos['DATA_EMISSAO'] <= data_final_selecionada)]

    res_produtos_vendidos_qte = (df_produtos_vendidos_filtrado.groupby(['ID_PRODUTO']).agg(
        descricao=('DESCRICAO', 'first'),
        unidade=('UNIDADE', 'first'),
        qtd_total=('QTD', 'sum'),
        total_total=('TOTAL', 'sum'),
        NOME_GRUPO=('NOME_GRUPO', 'first')  # Adiciona a coluna NOME_GRUPO
        )
        ).reset_index()

    top_10_un = res_produtos_vendidos_qte[res_produtos_vendidos_qte['unidade'] == 'UN'].nlargest(10, 'qtd_total')
    top_10_kg = res_produtos_vendidos_qte[res_produtos_vendidos_qte['unidade'] == 'KG'].nlargest(10, 'qtd_total')


    #Grupo de produtos
    res_porcentage_venda_grupos = res_produtos_vendidos_qte.groupby('NOME_GRUPO')['total_total'].sum().reset_index()

    #Recebimentos
    df_entrada_saida['EMISSAO'] = pd.to_datetime(df_entrada_saida['EMISSAO'])
    df_entrada_saida['ANO'] = df_entrada_saida['EMISSAO'].dt.year
    df_entrada_saida['MES'] = df_entrada_saida['EMISSAO'].dt.month
    df_entrada_saida['DIA'] = df_entrada_saida['EMISSAO'].dt.day
    df_entrada_saida['ENTRADA'] = pd.to_numeric(df_entrada_saida['ENTRADA'], errors='coerce')
    df_entrada_saida['SAIDA'] = pd.to_numeric(df_entrada_saida['SAIDA'], errors='coerce')

    #Filtragem pelo periodo da SIDEBAR
    df_entrada_saida_filtrado = df_entrada_saida[(df_entrada_saida['EMISSAO'] >= data_inicial_selecionada) & (df_entrada_saida['EMISSAO'] <= data_final_selecionada)]

    res_entrada_saida = df_entrada_saida_filtrado.groupby(['DIA', 'TIPO_MOVIMENTO']).size().reset_index(name='COUNT')
    res_entrada_saida = pd.merge(df_entrada_saida_filtrado, res_entrada_saida, on=['DIA', 'TIPO_MOVIMENTO'], how='left')
    res_entrada_saida = res_entrada_saida.groupby(['DIA', 'MES']).agg({'ENTRADA': 'sum'}).reset_index()


    return res_total_vendas, cont_vendas, top_10_kg, top_10_un, res_entrada_saida, res_porcentage_venda_grupos

#Trabalhando com datas
data_atual = dt.datetime.now()
mes_atual = data_atual.month
mes_passado = (data_atual.month -1)
ano_atual = data_atual.year
data_mes_anterior = str(data_atual - relativedelta(months=1))
data_mes_anterior = int(data_mes_anterior[:4])

if mes_atual == 1:
    mes_passado = 12
    ano_atual = data_mes_anterior

num_dias = calendar.monthrange(ano_atual, mes_passado)[1]

#Config da Pagina - INI
st.set_page_config(layout="wide", page_title="DashBoards", page_icon="🌍")
st.subheader("📉 Análise Descritiva Gerencial")
#FIM

#Config da SIDEBAR - INI
st.sidebar.image("img/tuuci-logo-1.jpg", caption="Tucci Analytics")
st.sidebar.title('Selecione o Período:')
st.sidebar.text("*(Por padrão o mes passado)")

data_inicial_selecionado = st.sidebar.date_input("Data Inicial",value=dt.date(ano_atual,mes_passado,1))
data_final_selecionado = st.sidebar.date_input("Data Final",value=dt.date(ano_atual,mes_passado,num_dias))
data_inicial_selecionada = pd.Timestamp(data_inicial_selecionado)
data_final_selecionada = pd.Timestamp(data_final_selecionado)
#FIM

#Chamada da função (para plotar e calcular as informações)
return_total_vendas, qtd_vendas_periodo, top_10_kg, top_10_un, return_entrada_saida, return_porcentage_venda_grupos = trata_base_dados(data_inicial_selecionada, data_final_selecionada)
total_vendas_periodo = float(return_total_vendas['TOTAL'].sum())
total_recebimebtos_periodo = float(return_entrada_saida['ENTRADA'].sum())


#Plotagem dos Graficos e Exibição das informações
#Graficos
#Vendas totais
fig_total_vendas = px.bar(return_total_vendas, x='DIA', y='TOTAL', text='TOTAL', 
            labels={'DIA': 'Dia', 'TOTAL': 'Total Vendido'}, 
            title='Total Vendido no Periodo', orientation='v', width=660, height=400)

#Ajustes e Plot
fig_total_vendas.update_traces(texttemplate='%{text:,.2f}', textposition='outside', textangle=0, marker_color='yellow')


#Grafico Produtos RECEBIMENTOS
fig_recebimentos = px.bar(return_entrada_saida, x='DIA', y='ENTRADA', text='ENTRADA',
            labels={'ENTRADA': 'Total de Entrada', 'DIA': 'Dia do Mês'},
            title='Total de Recebimentos no periodo', width=660, height=400)

#Ajustes e Plot
fig_recebimentos.update_traces(texttemplate='%{text:,.2f}', textposition='outside', textangle=0, marker_color='yellow')
fig_recebimentos.update_layout(yaxis=dict(tickformat=".0f"))



#Grafico Produtos por QUANTIDADE
#UN
fig_produtos_vendidos_qteun = px.bar(top_10_un, x='qtd_total', y='descricao', text='qtd_total',
                labels={'qtd_total': 'Quantidade', 'descricao': 'Produto'},
                title='Produtos UN',width=675, height=400)

#Ajustes e Plot
fig_produtos_vendidos_qteun.update_yaxes(categoryorder='total ascending')
fig_produtos_vendidos_qteun.update_traces(texttemplate='%{text:,.2f}', textposition='outside', marker_color='yellow')
fig_produtos_vendidos_qteun.update_layout(yaxis=dict(tickformat=".1f"))


#KG
fig_produtos_vendidos_qtekg = px.bar(top_10_kg, x='qtd_total', y='descricao', text='qtd_total', 
                labels={'qtd_total': 'Quantidade', 'descricao': 'Produto'},
                title='Produtos KG',width=675, height=400)

#Ajustes e Plot
fig_produtos_vendidos_qtekg.update_yaxes(categoryorder='total ascending')
fig_produtos_vendidos_qtekg.update_traces(texttemplate='%{text:,.2f}', textposition='outside', marker_color='yellow')
fig_produtos_vendidos_qtekg.update_layout(yaxis=dict(tickformat=".1f"), margin=dict(r=0))



#Grafico Produtos por VALOR
#UN
fig_produtos_vendidos_valun = px.bar(top_10_un, x='total_total', y='descricao', text='total_total',
                labels={'total_total': 'Valor total', 'descricao': 'Produto'},
                title='Produtos UN', width=675, height=400)

#Ajustes e Plot
fig_produtos_vendidos_valun.update_yaxes(categoryorder='total ascending')
fig_produtos_vendidos_valun.update_traces(texttemplate='%{text:,.2f}', textposition='outside', marker_color='yellow')
fig_produtos_vendidos_valun.update_layout(yaxis=dict(tickformat=".1f"))


#KG
fig_produtos_vendidos_valkg = px.bar(top_10_kg, x='total_total', y='descricao', text='total_total', 
                labels={'total_total': 'Valor total', 'descricao': 'Produto'},
                title='Produtos KG',width=675, height=400)

#Ajustes e Plot
fig_produtos_vendidos_valkg.update_yaxes(categoryorder='total ascending')
fig_produtos_vendidos_valkg.update_traces(texttemplate='%{text:,.2f}', textposition='outside', marker_color='yellow')
fig_produtos_vendidos_valkg.update_layout(yaxis=dict(tickformat=".1f"))

#Grafico porcentagem venda grupos
cores_personalizadas = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
fig_pizza = px.pie(
    return_porcentage_venda_grupos,
    values='total_total',
    names='NOME_GRUPO',
    hover_data=['total_total'],
    labels={'total_total': 'Total Vendido', 'NOME_GRUPO': 'Grupo'},
    template='seaborn', width=660, height=400, color_discrete_sequence=cores_personalizadas
)

#Ajustes
fig_pizza.update_traces(textinfo='percent+label', hoverinfo='label+percent', texttemplate='%{label}: %{percent:.2f}%')
fig_pizza.update_layout(
    showlegend=True,  # Se você deseja exibir a legenda
    legend=dict(title='Grupos', font=dict(size=10)),  # Posiciona a legenda
    margin=dict(l=0, r=0, b=0, t=0),
    font=dict(family='Arial', size=12)  # Margens do layout
)


#Ajustes CONTAINERS e PLOTS
#Container INFO's
with st.container(border=True):
    info1, info2, info3, info4 = st.columns([1, 1, 1, 1], gap='large')

    with info1:
        st.info(' Valor Total de Vendas',icon='💵')
        st.metric(label='Somatorio:', value=f"{total_vendas_periodo:,.0f}")

    with info2:
        st.info(' Numero Total de Vendas',icon='🛒')
        st.metric(label='Somatorio:', value=f"{qtd_vendas_periodo:,.0f}")

    with info3:
        st.info(' Valor Total de Recebimentos',icon='📦')
        st.metric(label='Somatorio:', value=f"{total_recebimebtos_periodo:,.0f}")

    with info4:
        st.info(' Quarta info',icon='📌')
        st.metric(label='Somatorio:', value=40)

    st.markdown("""---""")


col1,col2 = st.columns([1,1])

#Container TOTAL VENDAS
with col1:
    with st.container(border=True):
        fig_total_vendas

#Container RECEBIMENTOS
with col2:
    with st.container(border=True):
        fig_recebimentos

#Container TOP 10 POR VALOR
with st.container(border=True):
    st.header("Top 10 Produtos mais Vendidos por Valor", divider="orange")
    x1, x2 = st.columns([1, 1])

    with x1:
        fig_produtos_vendidos_valun

    with x2:
        fig_produtos_vendidos_valkg

#Container TOP 10 POR QUANTIDADE
with st.container(border=True):
    st.header("Top 10 Produtos mais Vendidos por Quantidade", divider="orange")
    x3, x4 = st.columns([1, 1])

    with x3:
        fig_produtos_vendidos_qteun

    with x4:
        fig_produtos_vendidos_qtekg

col3, col4 = st.columns([1,1])
with col3:
    with st.container(border=True):
        st.header("Porcentagem de Vendas por Grupos", divider="orange")
        fig_pizza