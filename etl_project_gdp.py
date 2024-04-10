import requests
import sqlite3
import numpy as np 
import pandas as pd 
from datetime import datetime 
from bs4 import BeautifulSoup as bs 

URL = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDB'
csv_path = './Countries_by_GDB.csv'

def extract():
    response = requests.get(URL)
    respinse_text = response.text 
    response_html = bs(respinse_text, 'html.parser') 
    dataframe = pd.DataFrame(columns=table_attribs)
    table = response_html.find_all('table')[2]
    rows = table.find_all('tr')
    i = 0;
    for row in rows:
        i += 1
        if not row:
            print(i, "row is empty")
        else:
            cells = row.find_all('td') 
            if len(cells) > 2 and cells[0].find('a') and "—" not in cells[2] :
                data_dict = {'Country': cells[0].a.contents[0], "GDP_USD_millions": cells[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                dataframe = pd.concat([dataframe,df1], ignore_index=True)
            else:
                print(i, "not having link or emty column")
    return dataframe     


def Transform(dataframe):
    dataframe['GDP_USD_millions'] = pd.to_numeric(dataframe['GDP_USD_millions'].str.replace(',', ''), errors='coerce')  / 1000    
    dataframe['GDP_USD_millions'] = dataframe['GDP_USD_millions'].round(2) 
    dataframe = dataframe.rename(columns={'GDP_USD_millions': 'GDP_USD_billions'})
    return dataframe

def load_to_csv(dataframe, csv_path):
    dataframe.to_csv(csv_path)

def load_to_db(dataframe, sql_connection, table_name):
    dataframe.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Initiating ETL process')

extracted_df = extract()

log_progress('Data extraction complete. Initiating Transformation process')

transformed_df = Transform(extracted_df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(transformed_df, csv_path)

log_progress('Data saved to CSV file')

with sqlite3.connect('World_Economies.db') as sql_connection:

    log_progress('SQL Connection initiated.')

    load_to_db(transformed_df, sql_connection, table_name)

    log_progress('Data loaded to Database as table. Running the query')

    query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
    run_query(query_statement, sql_connection)

log_progress('SQL connection is closed.')
log_progress('ETL Process Complete.')


