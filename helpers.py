from bs4 import BeautifulSoup
import csv
import pandas as pd
import re
import gender_guesser.detector as gender
from sqlalchemy import create_engine

# 1. Extract the raw data from NRPS website
def scrap_data(url, file_name):  
    soup = BeautifulSoup(url, 'lxml') # use lxml parser
    csv_file = open(file_name, 'w') 
    csv_writer = csv.writer(csv_file) # write the raw date into csv file
    csv_writer.writerow(['Name', 'Age', 'Location', 'Crime', 'Date']) # these are the data we need to collect

    for match in soup.find_all('tr',{"class":['row','altrow']}): # extract all text in row or altrow class
        try:
            result = match.get_text(separator="\n").strip() #replace <br> with new line, this will help us to select items from info list
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
            Location = info [2]
            crime = info [3:-1]
            date = info[-1]
            print(info)
        except Exception as e:
            result = None
        csv_writer.writerow([name,year,Location,crime,date])
    print(f'Extract completed... filename{file_name}')  
    csv_file.close()


### 2. Transform the date

# I. clean the age column
def clean_age(file_name):
    df = pd.read_csv(file_name, encoding='windows-1252')
    # for some rows the Age and Location values are included in the Crime column, here we will extract the age value
    df['Crime']=df['Crime'].apply(lambda x: eval(x)) # convert crime to actual list
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
    df['Age']= df['Age'].apply(lambda x: ''.join(re.findall(r'\d+', x)) ) # remove strings from Age column 

    return df



# II. Clean the location column
def clean_location(df_age):
    patterns = ['Niagara','Catharine','Pelham', 'Fort','Walpole','Fixed', 'Welland','Wainfleet','Colborne','Grimsby',
                'Lincoln','NFA','NOTL','Sherbrooke','Thorold','Montreal','Lachute','Burlington','Hamilton','Scarborough']
    comp_pattern = re.compile('|'.join(patterns), re.IGNORECASE)
    for i in range(len(df_age)):
        if df_age.isnull().iloc[i,2]:
            for x in range(len(df_age['Crime'][i])):
                item = df_age.iloc[i,3][x]
                if comp_pattern.search(item):
                    df_age.iloc[i,2] = item
    df_age['Location']=df_age['Location'].str.strip() #Make sure no white spaces
    # Normalize Location names
    df_age.loc[df_age['Location'].apply(str.lower).str.contains('fall'), 'Location'] = 'Niagara Falls'
    df_age.loc[df_age['Location'].apply(str.lower).str.contains('cath'), 'Location'] = 'St.Catharines'
    df_age.loc[df_age['Location'].apply(str.lower).str.contains('fix'),'Location'] = 'No Fixed Address'
    df_age.loc[df_age['Location'].apply(str.lower).str.contains('port'),'Location'] = 'Port Colborne'
    df_age.loc[df_age['Location'].apply(str.lower).str.contains('lha'),'Location'] = 'Pelham'
    df_age.loc[df_age['Location'].apply(str.lower).str.contains('fort'),'Location'] = 'Fort Erie'
    df_age.loc[df_age['Location'].apply(str.lower).str.contains('fte'),'Location'] = 'Fort Erie'
    df_age.loc[df_age['Location'].apply(str.lower).str.contains('notl|lake'),'Location'] = 'Niagara-on-the-Lake'
    df_age.loc[df_age['Location'].apply(str.lower).str.contains('pole'),'Location'] = 'Walpole Island'

    return df_age



# III. Clean the date column
def clean_date(df_loc):
    df_loc['Date']=df_loc['Date'].str.strip()
    df_loc['Date']=df_loc['Date'].str.lower().str.replace('updated:','')
    df_loc['Date']=df_loc['Date'].str.lower().str.replace('updated','')

    return df_loc



# IV. Clean the crime column
def clean_crime(df_date):
    df_date['Commited_Crime'] = ''
    patterns = ['Fai','Shoplifting','Theft', 'Instrument','Assault','Break', 'Possession',
                'Threat','Breach','Mischief','Traffick','Kidnap','Warrant','Fraud','Surety','Impaired','Arson','Unlawful']
    comp_pattern = re.compile('|'.join(patterns), re.IGNORECASE)
    for i in range(len(df_date)):
        for x in range(len(df_date['Crime'][i])):
            item = df_date.iloc[i,3][x]
            if comp_pattern.search(item):
                df_date.loc[i, 'Commited_Crime'] = item
                break
    
    
    df_date['Commited_Crime']=df_date['Commited_Crime'].str.strip()

    # Normalize the Crimes
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('fail to attend court|fail to appear'), 'Commited_Crime'] = 'Fail To Attend Court'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('fail to attend fingerprint'), 'Commited_Crime'] = 'Fail to Attend Fingerprint'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('assault'),'Commited_Crime'] = 'Assault'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('fail to comply'),'Commited_Crime'] = 'Fail to Comply'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('break'),'Commited_Crime'] = 'Break and Enter'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('shoplifting'),'Commited_Crime'] = 'Shoplifting'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('flight police dangerous operation'),'Commited_Crime'] = 'Flight Police Dangerous Operation'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('fraud'),'Commited_Crime'] = 'Fraud'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('mischief'),'Commited_Crime'] = 'Mischief'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('breach'),'Commited_Crime'] = 'Breach Probation'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('allegation'),'Commited_Crime'] = 'Allegation of Breach Conditional Sentence Order'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('allegation'),'Commited_Crime'] = 'Allegation of Breach Conditional Sentence Order'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('kidnap'),'Commited_Crime'] = 'Kidnapping'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('theft of motor'),'Commited_Crime'] = 'Theft of Motor Vehicle'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('theft under|theft over'),'Commited_Crime'] = 'Theft'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('traffick'),'Commited_Crime'] = 'Trafficking'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('utter'),'Commited_Crime'] = 'Utter Threats'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('possession of property'),'Commited_Crime'] = 'Possession of Property Obtained by Crime'
    df_date.loc[df_date['Commited_Crime'].apply(str.lower).str.contains('possession over'),'Commited_Crime'] = 'Possession'

    df_date.drop('Crime', axis=1, inplace=True)

    return df_date


# V. Add gender feature
def get_gender(data):
    detector = gender.Detector(case_sensitive=False)
    data['gender'] = data['Name'].apply(lambda x: detector.get_gender(x.split()[0]))
    data['gender'] = data['gender'].replace('andy', 'unknown')
    data['gender'] = data['gender'].replace('mostly_male', 'male')
    data['gender'] = data['gender'].replace('mostly_female', 'female')

    return data


## 3. Load the data
# I. Load the data to CSV file
def load_to_csv(data, to_csv_file_name): 
    data.to_csv(to_csv_file_name, index=False)

# II. Load the data to SQL DB
def load_to_sqldb(data, table_name):
    server_name = 'MYMAD\SQLEXPRESS'
    database = 'NRPS'
 
    connection_url = (
        f"mssql+pyodbc://{server_name}/{database}?"
        f"trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
    )

    engine = create_engine(connection_url)
    data.to_sql(table_name, con=engine, if_exists='replace', index=False)