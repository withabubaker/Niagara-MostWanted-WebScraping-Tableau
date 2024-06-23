# Analyze and Visualize Niagara Most Wanted Data Using Web Scraping and Tableau

![alt text](https://github.com/withabubaker/NiagaraWanted-WebScraping-Tableau/blob/main/img/NRPS-Wanted-logo.jpg)



## Project Goals:

- Used web scraping techniques to pull data from the [Niagara Regional Police Service website](https://www.niagarapolice.ca/en/news-and-events/Niagara-s-Wanted.aspx) for the most wanted criminals.
- Followed the ETL process (Extract, Transform, Load).
- Utilized the pandas library to explore and clean the data.
- Loaded the results into CSV file and SQL database.
- Used Tableau to build an informative dashboard.

## Libraries & Tools:

1. Python 3.12.1
2. Tableau
3. BeautifulSoup
4. Requests
5. Pandas
6. Numpy

## Files:

- ***NPRwanted.py***: contains the codes for extracting the data from the NRPS website.
- ***NPR.csv***: contains the data extracted from NRPS (before cleaning).
- ***dataCleaning.ipynb***: exploring and cleaning the data.
- ***Final_dfwithID.csv***: contains the data after cleaning (used for Tableau virtualization)

## Tableau Dashboard

Here is the link to the Tableau [Dashboard](https://public.tableau.com/app/profile/mohammed.abubaker/viz/NRPSMostWantedOct2023/Dashboard1?publish=yes)


## Results:

- ***Niagara city*** has the most wanted criminals, 37 wanted, then ***Welland city*** with 30 wanted.
- ***Failed to comply with release order***, is the most committed crime
- Most criminal's age is between 30 and 39.



