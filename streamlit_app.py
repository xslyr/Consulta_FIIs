
import sys, requests, random_header, json, time, base64, random
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed, wait



@st.cache_data
def inicializacao():
    
    def coletar_tickers():
        url = base64.b64decode('aHR0cHM6Ly9maWlzLmNvbS5ici9saXN0YS1kZS1mdW5kb3MtaW1vYmlsaWFyaW9zLw==').decode()
        req = requests.get( url, headers=random_header.RandomHeader.get() )
        parser = BeautifulSoup(req.text, 'html.parser')
        cards = parser.find_all( attrs={ 'data-element':"content-list-ticker" } )
        tickers = []
        for ticker in cards:
            aux = {}
            aux['Ticker'] = ticker.div.text
            aux['Categoria'], aux['Subcategoria'] = map( lambda x: x.strip(), ticker.span.text.split(':'))
            aux['Preço'] = None
            aux['Link'] = ticker.a['href']
            tickers.append( aux )
        return pd.DataFrame(tickers)
    
    
    def coletar_historico(ticker, link):
        try:
            req = requests.get( link, headers=random_header.RandomHeader.get() )
            bs = BeautifulSoup(req.text, 'html.parser')
            preco = float(bs.find( attrs={ 'class':'value' }).text.replace('.','').replace(',','.'))
            tabela = bs.find_all(attrs={'class':'yieldChart__table__bloco'})
            rendimentos = []
            for linha in tabela[1:]:
                a,b,c,d,e = list(map(lambda x:x.text.strip(), linha.find_all(attrs={'class':'table__linha'})))
                if not all(list(map( lambda x: ('{{' in x) or (x==''), [a,b,c,d,e] ))):
                    rendimentos.append({
                        'Ticker':ticker,
                        'Data_base': datetime.strptime(a,'%d.%m.%Y'), 
                        'Data_pagamento': datetime.strptime(b,'%d.%m.%Y'), 
                        'Cotacao_base': float(c.replace('R$','').replace('.','').replace(',','.').strip()), 
                        'Dividend_yield': float(d.replace('%','').replace(',','.').strip()), 
                        'Rendimentos': float(e.replace('R$','').replace('.','').replace(',','.').strip()), 
                    })
            return ticker, preco, rendimentos
        except Exception as e: print(e)  
            
    tickers = coletar_tickers()
    lista_tarefas = list(tickers[['Ticker','Link']].itertuples(index=False, name=None))
    historico = pd.DataFrame()
    e, s = 0, len(lista_tarefas)
    mybar = st.progress(0, text='Carregando dados ...') # não permitido para funcoes decoradas com o @st.cache
    with ThreadPoolExecutor(max_workers=15) as executor: 
        threads = [ executor.submit(coletar_historico, *item) for item in lista_tarefas ]
        
        for task_done in as_completed(threads):
            try:
                t,p,r = task_done.result()    
                tickers.loc[ tickers['Ticker']==t, 'Preço'] = p
                historico = pd.concat([historico, pd.DataFrame(r)],axis=0)
            except Exception as e: print(e)
            e += 1
            mybar.progress(e/s, text=f'Carregando dados ... {e}/{s}')
        
        wait(threads)
        mybar.progress(1.0, text=f'Carregando dados ... {s}/{s}')
        mybar.empty()
    
    historico.reset_index(drop=True, inplace=True)
    return tickers, historico


if __name__== '__main__':

    # --- carregando os dados ---#
    if any(map(lambda x: '--offline' in x, sys.argv)):
        try:
            tickers = pd.read_csv('tickers.csv')
            historico = pd.read_csv('historico.csv')
        except:
            tickers, historico = inicializacao()
            tickers.to_csv('tickers.csv', index=False)
            historico.to_csv('historico.csv', index=False)
    else:
        tickers, historico = inicializacao()
            
            
    # --- sidebar --- #
    st.sidebar.title('Opções')
    st.markdown(
        """
        <style>
            div[data-testid=stSidebarUserContent] { padding-top: 0px; }
            div[data-testid=stAppViewBlockContainer] { width:90%!important; max-width:100%!important; padding-top:1rem!important;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    preco_maximo = st.sidebar.slider('Preço Máximo da FIIs', min_value=1, max_value=1000, value=120)
    
    st.sidebar.write('Considerar rendimentos entre:')
    data_atual = datetime.now()
    data_antiga = data_atual + timedelta(days=-6*30)
    data_inicio = st.sidebar.date_input('Data Inicial', value=data_antiga)
    data_fim = st.sidebar.date_input('Data Final', value=data_atual)
    
    historico['Data_pagamento'] = pd.to_datetime(historico['Data_pagamento'])
    chart_data = historico.loc[ historico.Data_pagamento.between(data_antiga, data_atual) ]
    all_tickers = chart_data['Ticker'].unique()
    default_tickers = tickers.loc[ (tickers['Preço'].between(1,preco_maximo)) & (tickers.Ticker.isin(chart_data.Ticker))].Ticker.tolist()
    
    selected_tickers = st.sidebar.multiselect('Selecione as ações:', all_tickers, default=default_tickers )
    filtered_data = chart_data[chart_data['Ticker'].isin(selected_tickers)]
    
    # --- pagina principal --- #
    
    with st.container():
            
        st.header(':chart_with_upwards_trend: Comparativo de FIIs')
        tab1, tab2 = st.tabs([' Rendimentos Mensais ',' Preços e Categoria  |'])
        
        with tab1:
            st.line_chart(data=filtered_data, x='Data_pagamento', y='Rendimentos', color='Ticker')
        
        with tab2:
            st.dataframe(tickers.loc[ tickers.Ticker.isin(selected_tickers)])



