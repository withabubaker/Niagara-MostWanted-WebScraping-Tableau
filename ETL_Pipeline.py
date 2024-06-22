from bs4 import BeautifulSoup
import requests
import csv
import datetime
import pandas as pd
#import pyodbc as odbc
from sqlalchemy import create_engine
import gender_guesser.detector as gender
import re
from helpers import *


source = requests.get('https://www.niagarapolice.ca/en/news-and-events/Niagara-s-Wanted.aspx').text
current_time = datetime.datetime.now()  
formatted_time = current_time.strftime('%Y%m%d_%H%M%S') # use to generate new cvs file name
formatted_time_sql = current_time.strftime('%Y%m%d') # use to generate new SQL table
file_name = f'NRP_{formatted_time}.csv'
to_csv_file_name = f'afterclean_{formatted_time}.csv'
table_name = f'datafor_{formatted_time_sql}'


###### 1. SCRAPE THE DATA FROM NRPS WEBSITE ######



scrap_data(source, file_name)
    
    
   


######## 2. CLEAN THE DATA ######## 

def clean_data():
    df = pd.read_csv(file_name, encoding='windows-1252')


    ## for some rows the Age and Location values are included in the Crime column, here we will extract the age value
    df['Crime']=df['Crime'].apply(lambda x: eval(x)) # convert crime to actual list

    #### 1- Clean the Age Column ####
    for i in range(len(df)):
        if df.isnull().iloc[i,1]:
            for x in range(len(df['Crime'][i])):
                items = df.iloc[i,3][x]
                items = items.lower()
                if 'yrs' in items or 'years' in items:
                    parts = items.split()
                    for part in parts:
                        if part.isdigit():
                            df.iloc[i,1] = part
                            break

     
    df['Age']=df['Age'].str.strip() # remove white spaces
    df['Age']= df['Age'].apply(lambda x: ''.join(re.findall(r'\d+', x)) ) ## Now need to remove strings from Age column 

    #### 2- Clean the Location Column ####
    patterns = ['Niagara','Catharine','Pelham', 'Fort','Walpole','Fixed', 'Welland','Wainfleet','Colborne','Grimsby',
                'Lincoln','NFA','NOTL','Sherbrooke','Thorold','Montreal','Lachute','Burlington','Hamilton','Scarborough']
    comp_pattern = re.compile('|'.join(patterns), re.IGNORECASE)
    for i in range(len(df)):
        if df.isnull().iloc[i,2]:
            for x in range(len(df['Crime'][i])):
                item = df.iloc[i,3][x]
                if comp_pattern.search(item):
                    df.iloc[i,2] = item
            
    
    df['Location']=df['Location'].str.strip() #Make sure no white spaces

    # Normalize Location names
    df.loc[df['Location'].apply(str.lower).str.contains('fall'), 'Location'] = 'Niagara Falls'
    df.loc[df['Location'].apply(str.lower).str.contains('cath'), 'Location'] = 'St.Catharines'
    df.loc[df['Location'].apply(str.lower).str.contains('fix'),'Location'] = 'No Fixed Address'
    df.loc[df['Location'].apply(str.lower).str.contains('port'),'Location'] = 'Port Colborne'
    df.loc[df['Location'].apply(str.lower).str.contains('lha'),'Location'] = 'Pelham'
    df.loc[df['Location'].apply(str.lower).str.contains('fort'),'Location'] = 'Fort Erie'
    df.loc[df['Location'].apply(str.lower).str.contains('fte'),'Location'] = 'Fort Erie'
    df.loc[df['Location'].apply(str.lower).str.contains('notl|lake'),'Location'] = 'Niagara-on-the-Lake'
    df.loc[df['Location'].apply(str.lower).str.contains('pole'),'Location'] = 'Walpole Island'

    #### 3- Clean the Date Column ####
    df['Date']=df['Date'].str.strip()
    df['Date']=df['Date'].str.lower().str.replace('updated:','')
    df['Date']=df['Date'].str.lower().str.replace('updated','')

    #### 4- Clean the Crime Column ####
    

    df['Commited_Crime'] = ''
    patterns = ['Fai','Shoplifting','Theft', 'Instrument','Assault','Break', 'Possession',
                'Threat','Breach','Mischief','Traffick','Kidnap','Warrant','Fraud','Surety','Impaired','Arson','Unlawful']
    comp_pattern = re.compile('|'.join(patterns), re.IGNORECASE)
    for i in range(len(df)):
        for x in range(len(df['Crime'][i])):
            item = df.iloc[i,3][x]
            if comp_pattern.search(item):
                df.loc[i, 'Commited_Crime'] = item
                break
    
    
    df['Commited_Crime']=df['Commited_Crime'].str.strip()

    # Normalize the Crimes
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('fail to attend court|fail to appear'), 'Commited_Crime'] = 'Fail To Attend Court'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('fail to attend fingerprint'), 'Commited_Crime'] = 'Fail to Attend Fingerprint'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('assault'),'Commited_Crime'] = 'Assault'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('fail to comply'),'Commited_Crime'] = 'Fail to Comply'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('break'),'Commited_Crime'] = 'Break and Enter'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('shoplifting'),'Commited_Crime'] = 'Shoplifting'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('flight police dangerous operation'),'Commited_Crime'] = 'Flight Police Dangerous Operation'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('fraud'),'Commited_Crime'] = 'Fraud'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('mischief'),'Commited_Crime'] = 'Mischief'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('breach'),'Commited_Crime'] = 'Breach Probation'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('allegation'),'Commited_Crime'] = 'Allegation of Breach Conditional Sentence Order'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('allegation'),'Commited_Crime'] = 'Allegation of Breach Conditional Sentence Order'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('kidnap'),'Commited_Crime'] = 'Kidnapping'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('theft of motor'),'Commited_Crime'] = 'Theft of Motor Vehicle'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('theft under|theft over'),'Commited_Crime'] = 'Theft'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('traffick'),'Commited_Crime'] = 'Trafficking'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('utter'),'Commited_Crime'] = 'Utter Threats'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('possession of property'),'Commited_Crime'] = 'Possession of Property Obtained by Crime'
    df.loc[df['Commited_Crime'].apply(str.lower).str.contains('possession over'),'Commited_Crime'] = 'Possession'



    df.drop('Crime', axis=1, inplace=True)

    return df

    ######## 3. Feature Engineering ########

    ## add gender feature
def det_gender(x):
     detector = gender.Detector(case_sensitive=False)
     x['gender'] = x['Name'].apply(lambda x: detector.get_gender(x.split()[0]))
     x['gender'] = x['gender'].replace('andy', 'unknown')
     x['gender'] = x['gender'].replace('mostly_male', 'male')
     x['gender'] = x['gender'].replace('mostly_female', 'female')

     return df


    ######## 4. Load Data ########
    ## load to CSV

def load_to_csv(df_data): 
    df_data.to_csv(to_csv_file_name, index=False)

    ## load to SQL DB
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

## 3 assign gender feature
df_gender = det_gender(df)

## 4 load the data to CSV
load_to_csv(df_gender)

## 5 load data to SQL DB
load_to_sqldb(df_gender)