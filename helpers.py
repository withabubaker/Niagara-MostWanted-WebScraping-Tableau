from bs4 import BeautifulSoup
import csv

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