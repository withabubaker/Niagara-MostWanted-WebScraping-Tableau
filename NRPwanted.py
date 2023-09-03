from bs4 import BeautifulSoup
import requests
import csv

source = requests.get('https://www.niagarapolice.ca/en/news-and-events/Niagara-s-Wanted.aspx').text
soup = BeautifulSoup(source, 'lxml')

csv_file = open('NRP10.csv', 'w')
csv_writer = csv.writer(csv_file)
csv_writer.writerow(['Name', 'Age', 'Location', 'Crime', 'Date'])

for match in soup.find_all('tr',{"class":['row','altrow']}):
    try:
        result = match.text.strip()
        info = result.splitlines()
        name = info[0]
        year = info [1]
        location = info [2]
        crime = info [3:-1]
        date = info[-1]
        print(info)
    except Exception as e:
        result = None
        info = None
        name = None
        year = None
        location = None
        crime = None
        date = None
    csv_writer.writerow([name,year,location,crime,date])
print('Done')  
csv_file.close()
    


