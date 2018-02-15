import time
import datetime

import ccxt

import pandas as pd
import sqlite3

from IPython.display import clear_output

class DataLoader:

    def __init__(self, sql_db, table_name, show_connection = False):
        self.sql_db = sql_db
        self.table_name = table_name
        conn = sqlite3.connect(self.sql_db)
        self.conn = conn
        self.cur = conn.cursor()
        if show_connection == True:
            print('Connected to ',self.sql_db, ', table ', self.table_name.upper(), sep='')
    
    #load process #2 - check db if load step fails
    def check_db(self):
        cur = self.cur
        cur.execute('SELECT name FROM sqlite_master WHERE type="table";')
        sql_tables = cur.fetchall() 
        print('The database has the following tables: ', sql_tables)
        if len(sql_tables) == 0: 
            create_data_input = input("Your database is empty. Do you want to fetch data from the API? ['y' - yes; 'n' - no]")
            create_data_input = create_data_input.lower()
            if create_data_input == 'y':
                print("Fetching data from Kucoin API. All markets' data specified in this project (ETH, LTC, NEO) will be extracted in this order. You might follow the counter below.")
                print("\033[91m", "Atention. All output in this display will be cleared during data retrieval phase.", sep="")
                DataRetriever.main_builder()
            elif create_data_input == 'n':
                print("You decided to avoid creating the database. It won't be possible to proceed to the next steps for now.")
            else: 
                print('Sorry. Unrecognized command.')
    
    #load process #1
    def load_sql_table(self):
        conn = self.conn
        try: 
            df = pd.read_sql_query('SELECT * FROM {} ORDER BY date DESC'.format(self.table_name), conn)
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            return df # new
        
        except Exception as error:
            error = str(error)
            if 'no such table' in error:
                print("\033[91m", 'YOUR SELECTED TABLE "', self.table_name, '" DOES NOT EXIST. PLEASE CHECK TABLE NAMES ON DB.\r\n', 
                        'DATAFRAME WAS NOT GENERATED.', sep='')
                DataLoader.check_db()
            else:
                print(error)
        
    def check_sql_table(self):
        conn = self.conn
        cur = self.cur
        cur.execute('SELECT COUNT(date) FROM {}'.format(self.table_name))
        eth_count_lines = cur.fetchone() 
        print('There are', eth_count_lines[0], 'lines on', self.table_name, 'table. See below the 5 most recent data:')
        recent_rows = pd.read_sql_query('SELECT * FROM {} ORDER BY date DESC LIMIT 5'.format(self.table_name), conn)
        print(recent_rows)
        print('\r\n')

class DataRetriever:
    '''
    #load process #3 - retrieve data if DB is empty
    This function has the following dependencies: 
    fetch_data: retrieve data from API
    check_dict_consistency: check retrieved data
    build_df: use pandas to create dataframe
    build_sql_table: export data to sqlite
    '''
    def main_builder():
        time_end = datetime.datetime.now()
        time_start_limit = datetime.datetime(2017, 9, 16, 6, 50, 0, 0) # data limite de dados da tabela
        time_delta = (time_end - time_start_limit).days
        
        total_of_days = time_delta+5
        total_of_days = 5 # temp
        timeframe = '5m'
        exchange = ccxt.kucoin()
        
        print('The retrieval parameters are: number of days = 365, timeframe = 5m, exchange = Kucoin')
        user_input = input("Do you want to change the parameters? ['y' - yes; 'n' - no]")
        if user_input.lower() == 'y':
            user_input = input("Do you want to change number of days? ['y' - yes; 'n' - no]")
            if user_input.lower() == 'y':
                total_of_days = int(input('Set number of days (365 or below): '))
            user_input = input("Do you want to change timeframe? ['y' - yes; 'n' - no]")
            if user_input.lower() == 'y':
                timeframe = input('Set valid timeframe (1m, 5m...): ')
            user_input = input("Do you want to change the exchange? ['y' - yes; 'n' - no]")
            if user_input.lower() == 'y':
                exchange = input('Set exchange: ')
            print('please verify the new parameters:', 'number of days =',total_of_days, ', timeframe =', timeframe, ', exchange =', exchange)
            print('If you need to change again standard parameters or if you want to modify the data retrieval and db storage process itself, go to file: local_tools, class: DataRetriever, function: main_builder')
            exchange = 'ccxt.'+exchange.lower()+'()'
            exchange = eval(exchange)
            input('Press any key to continue')
        else: print('No parameters were changed')
        
        ohlcv_eth_btc = []
        ohlcv_ltc_btc = []
        ohlcv_neo_btc = []
        global api_results_dict
        
        '''
        ETH/BTC = ether / bitcoin
        LTC/BTC = litecoin / bitcoin
        NEO/BTC = neo / bitcoin
        '''
        api_results_dict = {'ETH/BTC': ohlcv_eth_btc, 'LTC/BTC': ohlcv_ltc_btc, 'NEO/BTC': ohlcv_neo_btc}
        
        for key,val in api_results_dict.items():
            api_results_dict[key] = DataRetriever.fetch_data(key, total_of_days, timeframe, exchange)
            
        DataRetriever.check_dict_consistency(api_results_dict)
        
        dataframes_dict = {}

        dataframes_dict['ETH'] = DataRetriever.build_df(api_results_dict['ETH/BTC'])
        dataframes_dict['LTC'] = DataRetriever.build_df(api_results_dict['LTC/BTC'])
        dataframes_dict['NEO'] = DataRetriever.build_df(api_results_dict['NEO/BTC'])
        
        sql_db = 'marcelobbribeiro_ohls_cryptos.sqlite'
        conn = sqlite3.connect(sql_db)
        cur = conn.cursor()
        DataRetriever.build_sql_table(dataframes_dict['ETH'], 'ETH')
        DataRetriever.build_sql_table(dataframes_dict['LTC'], 'LTC')
        DataRetriever.build_sql_table(dataframes_dict['NEO'], 'NEO')
        
        print('\r\n3 tables were created and your database was saved in your local folder.')
        
    '''
    CAPTURA DADOS DE API
    Primeiramente, tentei usar a exchange bitstamp. No entanto, sua API é extremamente problemática. Não funciona a função 
    fetch_ohlcv e há diversas restrições na API. Ver discussão em: https://github.com/ccxt/ccxt/issues/306
    
    Portanto, foi decidido usar a API da kucoin, cuja API é mais apropriada, além de permitir aplicar a função fetch_ohlcv.
    
    Sobre a API da kucoin:
    * Limite = 1440 linhas = 5 dias de dados (a cada 5 minutos)
    * Dados por 5 minutos em 1 dia = 24*12 = 288
    * Dados por 5 minutos em 5 dias = 288*5 = 1440
    
    Observações: 
    * A função sleep não é necessária no momento, mas é bom manter no código para evitar chance de mal funcionamento no futuro, caso surjam restrições de request/tempo no API.
    * Podem surgir valores duplicados no momento do fetch, quando ultrapassamos a data de início dos dados da API. Os mesmos são facilmente removidos mais à frente no momento da criação do df.
    * A API possui dados até: September 16, 2017 6:50:00 PM
    * Faz loop em cada lote de 5 dias de dados. 
    * Dias = Hoje - 16/setembro (data de início da série). 
    * Cada lote = 5 days (timeframe=5m). 
    * Total de lotes = 27
    '''
    def fetch_data(market_pair, total_days=365, timeframe='5m', exchange = ccxt.kucoin()):
        
        ohlcv_data = []
        partial_list = []
        
        time_end = datetime.datetime.now()
        api_parts = range(1,int(total_days/5)+1)
        
        for i in api_parts:
            days = 5*i
            time_start = time_end - datetime.timedelta(days)
            time_start = int(time_start.timestamp()*1000)
            
            partial_list = exchange.fetch_ohlcv(market_pair, timeframe, since=time_start)
            partial_list = list(reversed(partial_list))
            ohlcv_data.extend(partial_list)
            
            clear_output()
            time_remaining = api_parts[-1] - i
            print('Slot ', i, ' was appended to the list ', market_pair,'. Only ', time_remaining, ' of ', api_parts[-1], ' to complete.', sep='')
            time.sleep(5) # aumentar o número de segundos caso surja alta restrição de request/tempo no futuro
            
            '''
            verifica último item para saber se alcançou início da API. 
            1505587800000 = Saturday, September 16, 2017 6:50:00 PM
            '''
            if partial_list[-1][0] == 1505587800000: 
                print('Extração de dados terminou.')
                break
        return ohlcv_data

    def check_dict_consistency(dict):
        clear_output()
        print('Data retrieval has finished. Checking consistency of generated lists', '\r\n')
        for key,val in dict.items():
            print('length of', key, ':',len(val))
            for line in val[:5]:
                print(line)
                if line == val[4]: print('')
                
    def build_df(series_name):
        df = pd.DataFrame(series_name, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        df = df.sort_values('date')
        df['date'] = pd.to_datetime(df['date']*1000000)
        df = df.drop_duplicates(subset=['date']) # remove algumas linhas duplicadas (da extração dos dados)
        df = df.set_index('date')
        return df
        
    def build_sql_table(dataframe, table_name):   
        try: 
            dataframe.to_sql(table_name, conn)
        except Exception as error:
            print("\033[91m", error)
            error = str(error)
            if 'already exists' in error:
                user_input = input("Table already exists. Continue? Type 'yes' or 'no' on your keyboard: ")
                if user_input == 'yes': 
                    dataframe.to_sql(table_name, conn, if_exists="replace")
                    print('Table',table_name, 'was created on database', sql_db)
                else:
                    print('Table was not created')
            pass