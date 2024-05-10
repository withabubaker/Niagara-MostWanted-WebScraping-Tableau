from bs4 import BeautifulSoup
import requests
import csv
import datetime
import pandas as pd

current_time = datetime.datetime.now()
formatted_time = current_time.strftime('%Y%m%d_%H%M%S')
file_name = f'NRP_{formatted_time}.csv'

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

def df_data():
    df = pd.read_csv(file_name, encoding='windows-1252')
    print(df.head())

## call the functions - ETL Piplines

## 1 Extract the data
scrap_data()

## 2 load the data into df
df_data()

## 3 load the data in SQL database
