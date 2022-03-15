from datetime import datetime
from sqlite3 import Date
import sys ,argparse, csv
import requests
import psycopg2

conn = psycopg2.connect(user="postgres",password="root",host="127.0.0.1",port="5432",database="covidDataDB")
def download_file():
    file_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
    req = requests.get(file_url)
    url_content = req.content
    with open('covid_data.csv', 'wb') as csv_file:
        csv_file.write(url_content)

def processing_data():
    #parser = argparse.ArgumentParser(description='csv to postgres', fromfile_prefix_chars="@" )
    #parser.add_argument('file', default='covid_data.csv', help='csv file to import', action='store')
    #args = parser.parse_args()
  
    query = "SELECT MAX(Date) FROM covid_data_table"  

    last_date = execute_query(query)
                
    csv_file = "covid_data.csv"
    # open csv file
    with open(csv_file, 'r') as csvfile:
    # get number of columns
        for i, line in enumerate(csvfile):
            values = line.split(',')
            if i == 0:
                header = values
                continue
            

            included_headers = ["location","date","total_cases","new_cases","total_deaths","new_deaths","total_vaccinations"]
            row = list(values[header.index(h)] for h in included_headers)

            if last_date[0] is None or datetime.strptime(row[1],'%Y-%m-%d').date() > last_date[0]:
                non_query = """INSERT INTO covid_data_table 
                    (location,date,total_cases,new_cases,total_deaths,new_deaths,total_vaccinations) VALUES('{0}', '{1}', {2})""".format(
                    row[0].replace("'", ""), row[1], ",".join((f if f else '0' for f in row[2:]))     
                )
                execute_non_query(non_query)
                print(i)
                
                

def execute_non_query(non_query):
    cur = conn.cursor()
    cur.execute(non_query)
    conn.commit()
    


def execute_query(query):
    cur = conn.cursor()
    cur.execute(query)
    date = cur.fetchone()
    return date

if __name__ == "__main__":
    download_file()
    processing_data()
    #query = "SELECT MAX(Date) FROM covid_data_table"  
    # print(execute_query(query)[1])
    
