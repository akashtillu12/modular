# -*- coding: utf-8 -*-
"""
web scraping - Akash Tillu

Code has 2 functions:
1. Generate backfill of all data from url: 'https://www.ccilindia.com/FPI_ARCV.aspx'
2. Generate a custom run of the latest data on the url: 'https://www.ccilindia.com/FPI_ARCV.aspx'

In the process, script also posts/appends data to the Azure sql server and can fetch data from the sql server
"""

"""
Declaration: modules used: pandas, selenium, pyodbc in the main script as they are globally used
"""
import os

import pandas as pd
import copy
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options 
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException

import pyodbc
from sqlalchemy import create_engine
import urllib

"""
Use chrome & chromedriver installation on machine (PC, ubuntu) to look for the table element
## 

define driver and url, which is globally used across functions in the main script
"""
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("no-sandbox")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--headless")
path = os.path.join("/usr/bin/","chromedriver")

driver = webdriver.Chrome(executable_path=path,chrome_options=chrome_options)

url_to_scrape = 'https://www.ccilindia.com/FPI_ARCV.aspx'

"""
Function run_backfill: Get backfill for all dates for the table requested. 
Table --> ::Security Wise Holdings - General Limit::

## function selects the dropdown option and loops through each date option to parse through the table
## table is created for each page number for each date and concat with parent final_bfill_table
## backfill table is checked for exact duplicates and sorted
"""
def run_backfill(driver,url_to_scrape):
    
    """Params
    driver: required param. This is the chrome driver selenium instance defined in the main script to parse webpage. 
    url_to_scrape: required param. url to pass. This is fixed to ccil for this exercise.
    return: final backfill table
    """
    
    driver.get(url_to_scrape)
    
    dropdown_id = driver.find_element_by_id('drpArchival')
    options_drop = Select(dropdown_id).options
    final_bfill_table = pd.DataFrame()
    
    for option in range(0,len(options_drop)):
        
        if option == 0:
            continue
        elif option == 1:
            Select(driver.find_element_by_id('drpArchival')).select_by_index(option)
        else:
            ignored_exceptions=(NoSuchElementException,StaleElementReferenceException,)
            WebDriverWait(driver, 10,ignored_exceptions=ignored_exceptions).until(EC.presence_of_element_located((By.ID, 'grdFPISWH')))        
            
            Select(driver.find_element_by_id('drpArchival')).select_by_index(option)
        
        try:
            value = Select(driver.find_element_by_id('drpArchival')).options[option].text
            datetime_object = datetime.strptime(value, '%d-%b-%Y').date()
        except:
            pass
        
        table_by_date = pd.DataFrame()
        page_select_id = driver.find_elements_by_xpath("//td[@colspan]")
    
        for j in range(1, len(page_select_id)+1):
            try:
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, str(j)))).click()
            except:
                pass
        
            table_holdings = driver.find_element_by_id("grdFPISWH")
            table_holdings_html = table_holdings.get_attribute('outerHTML')
            table_df = pd.read_html(table_holdings_html)[0]
            table_df.columns = table_df.iloc[:1].values.tolist()
            table_df.columns = table_df.columns.get_level_values(0)
            table_df = table_df.drop(table_df.index[0])
            table_df['Date'] = datetime_object
            table_df = table_df.set_index('Date')
            table_df=table_df[table_df.nunique(1)>1]
            table_by_date = pd.concat([table_by_date,table_df],axis=0)
    
        final_bfill_table = pd.concat([final_bfill_table,table_by_date],axis=0)
        final_bfill_table_no_dups = copy.deepcopy(final_bfill_table)
        final_bfill_table_no_dups['Date_unique'] = final_bfill_table_no_dups.index
        final_bfill_table_no_dups = final_bfill_table_no_dups.drop_duplicates().sort_index()
        final_bfill_table_no_dups = final_bfill_table_no_dups.drop(columns='Date_unique')
        
    return final_bfill_table_no_dups


"""
Function get_latest: Get table data for latest available date. 
Table --> ::Security Wise Holdings - General Limit::

## function selects the last available date in the dropdown to parse through the table
## table is created for each page number for each date and concat across all pages
## table data is then compared with the historical data from the database 
## new data is only unique values meant to be appended to the DB
"""

def get_latest(driver,url_to_scrape,historic_data):

    """Params
    driver: required param. This is the chrome driver selenium instance defined in the main script to parse webpage. 
    url_to_scrape: required param. url to pass. This is fixed to ccil for this exercise.
    historical_data: existing data from the database is passed in this function
    return: new_data: unique dataframe of rows to be appended
    """    
    driver.get(url_to_scrape)
    
    historic_data['Date_unique'] = historic_data.index
    
    latest_table = pd.DataFrame()
    new_data = pd.DataFrame()
    
    Select(driver.find_element_by_id('drpArchival')).select_by_index(1)
    value = Select(driver.find_element_by_id('drpArchival')).options[1].text
    datetime_object = datetime.strptime(value, '%d-%b-%Y').date()    
    
    page_select_id = driver.find_elements_by_xpath("//td[@colspan]")

    for j in range(1, len(page_select_id)+1):
        try:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, str(j)))).click()
        except:
            pass
    
        table_holdings = driver.find_element_by_id("grdFPISWH")
        table_holdings_html = table_holdings.get_attribute('outerHTML')
        table_df = pd.read_html(table_holdings_html)[0]
        table_df.columns = table_df.iloc[:1].values.tolist()
        table_df.columns = table_df.columns.get_level_values(0)
        table_df = table_df.drop(table_df.index[0])
        table_df['Date'] = datetime_object
        table_df = table_df.set_index('Date')
        table_df=table_df[table_df.nunique(1)>1]
        latest_table = pd.concat([latest_table,table_df],axis=0)
    
    print('table retrieved')
    
    latest_table_no_dups = latest_table
    latest_table_no_dups['Date_unique'] = latest_table_no_dups.index
    latest_table_no_dups = latest_table_no_dups.drop_duplicates()
    latest_table_no_dups.columns = historic_data.columns
       
    merge = latest_table_no_dups.merge(historic_data, how='left', indicator=True)
    new_data =merge[merge._merge == 'left_only'].iloc[:,:-1]
    new_data['Date'] = new_data['Date_unique']
    new_data = new_data.set_index('Date')
    new_data = new_data.drop(columns='Date_unique')

    return new_data


"""
Function get_data_from_db: Get data from db
## function queries the azure database to get data stored
"""
def get_data_from_db():

    """Params
    return: data stored in the DB
    """        
    server = 'tcp:akash-test.database.windows.net'
    database = 'web_scrape'
    username = 'admin_1'
    password = 'AkashTillu123!' 
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password) 
    
    print('connection established')
    
    query = 'SELECT * FROM dbo.SecurityHoldings'
    historic_data = pd.read_sql(query, conn,index_col='Date')
    historic_data = historic_data.sort_index()
    historic_data = historic_data.astype(object)
    
    return historic_data

"""
Function post_to_db: Post data to database
## function to append data to the database
## data is transformed into the right format to post and appended to the azure database
"""
def post_to_db(data_to_append,db='SecurityHoldings',replace=False):

    """Params
    data_to_append: submit data to append. This is typically the latest data we get after parsing table for the last date
    db: enter the table to append data
    replace: if True, replace existing data, if False, append.
    """       
    server = 'tcp:akash-test.database.windows.net'
    database = 'web_scrape'
    username = 'admin_1'
    password = 'AkashTillu123!' 
    
    quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

    column_names = ['ISIN','SecurityDescription','AggregateHoldingOfFPIS','OutstandingPositionOfGovtSecurities','SecHoldings']
    data_to_append.columns = column_names

    if replace:
        data_to_append.to_sql(db, schema='dbo', con = engine, chunksize=200, method='multi', index=True, if_exists='replace')
    else:
        data_to_append.to_sql(db, schema='dbo', con = engine, chunksize=200, method='multi', index=True, if_exists='append')

    print('Data posted')

if __name__ == '__main__':
    
    historic_data = get_data_from_db()
    new_data = get_latest(driver,url_to_scrape,historic_data)
    post_to_db(new_data,db='SecurityHoldings')