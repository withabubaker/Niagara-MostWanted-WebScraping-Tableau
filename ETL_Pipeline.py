import requests
import datetime
from helpers import *


source = requests.get('https://www.niagarapolice.ca/en/news-and-events/Niagara-s-Wanted.aspx').text
current_time = datetime.datetime.now()  
formatted_time = current_time.strftime('%Y%m%d_%H%M%S') # use to generate new cvs file name
formatted_time_sql = current_time.strftime('%Y%m%d') # use to generate new SQL table
file_name = f'NRP_{formatted_time}.csv'
to_csv_file_name = f'afterclean_{formatted_time}.csv'
table_name = f'datafor_{formatted_time_sql}'

    
######## Call the functions - ETL Piplines ########

    
## 1 Extract the data
scrap_data(source, file_name)

## 2. Transform the date
df_age = clean_age(file_name)
df_location = clean_location(df_age)
df_date = clean_date(df_location)
df_crime = clean_crime(df_date)
data = get_gender(df_crime)

## 3. Load the data
load_to_csv(data, to_csv_file_name) # To CSV file
load_to_sqldb(data, table_name ) # To SQL DB