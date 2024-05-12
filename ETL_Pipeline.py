from bs4 import BeautifulSoup
import requests
import csv
import datetime
import pandas as pd
import pyodbc as odbc
from sqlalchemy import create_engine

current_time = datetime.datetime.now()
formatted_time = current_time.strftime('%Y%m%d_%H%M%S')
formatted_time_sql = current_time.strftime('%Y%m%d')
file_name = f'NRP_{formatted_time}.csv'
to_csv_file_name = f'afterclean_{formatted_time}.csv'
table_name = f'datafor_{formatted_time_sql}'


#csv_file = open(f'NRP_{formatted_time}.csv', 'w') 

## scrap the data from NRPS website
def scrap_data():
    
    source = requests.get('https://www.niagarapolice.ca/en/news-and-events/Niagara-s-Wanted.aspx').text
    soup = BeautifulSoup(source, 'lxml') # use lxml parser
    csv_file = open(file_name, 'w') 
    csv_writer = csv.writer(csv_file) # write the result into csv file
    csv_writer.writerow(['Name', 'Age', 'Location', 'Crime', 'Date']) # these are the data we need to collect

    for match in soup.find_all('tr',{"class":['row','altrow']}): # extract all text in row or altrow class
        try:
            #result = match.text.strip() 
            result = match.get_text(separator="\n").strip() #replace <br> with new line, this will help us to select items from info list
            #print(result)
            info = result.splitlines()
            name = info[0]
            for i in range(1,1,5):
                if info[i] != '':
                    year = info[i]
                    i+=1
                    break
                else:
                    pass
            year = info [1]
            location = info [2]
            crime = info [3:-1]
            date = info[-1]
            print(info)
        except Exception as e:
            result = None
        csv_writer.writerow([name,year,location,crime,date])
    print(f'Extract completed... filename{file_name}')  
    csv_file.close()


                 ######## clean the data ######## 

def clean_data():
    #### 1- load the data into df ####
    df = pd.read_csv(file_name, encoding='windows-1252')

    #### 2- fill out age Null values from Crime column #####
    # update null values in Age column from Crime column
    df['Crime']=df['Crime'].str.split(',')
    
    for i in range(len(df)):
            if df.isnull().iloc[i,1]:
                    df.iloc[i,1]= df['Crime'][i][0]
    
    # Remove unwatned chars from Age column
    char_remove = ['[', '\'', 'Yrs','yrs', '.','Years','old', '\\xa0','`' ]
    for char in char_remove:
        df['Age']=df['Age'].str.replace(char, '')
    
    #Make sure no white spaces
    df['Age']=df['Age'].str.strip()


    #### 3- Fill Out Location Null Values From the Crime Column ####
    # update null values in Location column from Crime column                
    for i in range(len(df)):
            if df.isnull().iloc[i,2]:
                    df.iloc[i,2]= df['Crime'][i][3]

    # remove location for Crime list
    df['Crime'] = df.apply(lambda row: [x for x in row.Crime if x != row.Location],axis=1)

    # Remove unwatned chars from Location column
    char_remove = ['[', '\'','.', '\\xa0','`' ]
    for char in char_remove:
        df['Location']=df['Location'].str.replace(char, '')
        
    #Make sure no white spaces
    df['Location']=df['Location'].str.strip()

    # List all unique locations
    df.loc[df['Location'].apply(str.lower).str.contains('fall'), 'Location'] = 'Niagara Falls'
    df.loc[df['Location'].apply(str.lower).str.contains('cath'), 'Location'] = 'St.Catharines'
    df.loc[df['Location'].apply(str.lower).str.contains('fix'),'Location'] = 'No Fixed Address'
    df.loc[df['Location'].apply(str.lower).str.contains('port'),'Location'] = 'Port Colborne'
    df.loc[df['Location'].apply(str.lower).str.contains('lha'),'Location'] = 'Pelham'
    df.loc[df['Location'].apply(str.lower).str.contains('fort'),'Location'] = 'Fort Erie'
    df.loc[df['Location'].apply(str.lower).str.contains('fte'),'Location'] = 'Fort Erie'
    df.loc[df['Location'].apply(str.lower).str.contains('notl|lake'),'Location'] = 'Niagara-on-the-Lake'
    df.loc[df['Location'].apply(str.lower).str.contains('pole'),'Location'] = 'Walpole Island'

    #### 4- Clean the Date Column ####
    df['Date']=df['Date'].str.strip()
    df['Date']=df['Date'].str.lower().str.replace('updated:','')
    df['Date']=df['Date'].str.lower().str.replace('updated','')

#### 5- Clean the Crime Column ####
    ##df['Crime'] = df['Crime'].apply(lambda x: [x.strip(" '") for x in x]) # keep as list
    df['Crime'] = df['Crime'].apply(lambda x: ' '.join(x.strip(" '") for x in x)) # keep as strin

   

    '''

     df['Crime'] = df['Crime'].apply(
         lambda lst: [
              ''.join(char for char in item.replace("''", '')if char.isalnum() or char == ',')
              for item in lst
         ]
    )

    
        match = ["'']","''","[''","''","'']","''","''","'']","'\\xa0'","'\\xa0'", "" ''""]
        prefixes = ["'23-","'23-","'22-","'20","'21","'16-","'19-","'18-","'updated","'added"]
        suffixes = ["yrs'", "Yrs'","yrs.'","yrs old'"]
        for i in range(len(df)):
            lst = df['Crime'][i]
            for i in lst:
                if i.strip() in match or i.lower().strip().startswith(tuple(prefixes)) or i.lower().strip().endswith(tuple(suffixes)):
                    lst.remove(i)
        
            
    '''
    return df
    

                    ######## Load the Data ########



def load_to_csv(df_data): 
    df_data.to_csv(to_csv_file_name, index=False)


def load_to_sqldb(df_data):
    server_name = 'MYMAD\SQLEXPRESS'
    database = 'NRPS'
 
    connection_url = (
        f"mssql+pyodbc://{server_name}/{database}?"
        f"trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
    )

    engine = create_engine(connection_url)
    df_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
    


             ######## call the functions - ETL Piplines ########

    
## 1 Extract the data
scrap_data()

## 2 load the data into df
df = clean_data()


## 3 load the data in SQL database
load_to_csv(df)

### -- load data into sql server
load_to_sqldb(df)

### python library 'gender_guesser'
### -- use chatgpt to identify gender from name