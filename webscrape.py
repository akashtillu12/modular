# -*- coding: utf-8 -*-
"""
web scraping - Akash Tillu
"""
import pandas as pd
import copy
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException


import pyodbc
from sqlalchemy import create_engine
import urllib



driver = webdriver.Chrome(executable_path=r'C:\bin\chromedriver')

url_to_scrape = 'https://www.ccilindia.com/FPI_ARCV.aspx'

#from selenium.webdriver.chrome.options import Options 

#chrome_options = webdriver.ChromeOptions()
#chrome_options.add_argument("no-sandbox")
#chrome_options.add_argument("--disable-extensions")
#chrome_options.add_argument("--headless")
#path = os.path.join("/usr/bin/","chromedriver")

#driver = webdriver.Chrome(executable_path=path,chrome_options=chrome_options)
#browser.get("https://www.example.com")
#print(browser.title)

#driver = webdriver.Chrome(executable_path=r'/usr/bin/chromedriver')


def run_backfill(driver,url_to_scrape):
            
    driver.get(url_to_scrape)
    
    dropdown_id = driver.find_element_by_id('drpArchival')
    options_drop = Select(dropdown_id).options
    #wait = WebDriverWait(driver, 10)
    final_bfill_table = pd.DataFrame()
    
    #for option in options_drop:
    
    for option in range(0,len(options_drop)):
        
        #select = Select(wait.until(EC.presence_of_element_located(By.id,dropdown_id)))
        #dropdown_id = driver.find_element_by_id('drpArchival')
        #Select(driver.find_element_by_id('drpArchival')).select_by_value(value)
      #  dropdown = Select(dropdown_id)
      #  dropdown.select_by_index(index)
      #  WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.LINK_TEXT, str(j)))).click()
        if option == 0:
            continue
        elif option == 1:
            Select(driver.find_element_by_id('drpArchival')).select_by_index(option)
        else:
            ignored_exceptions=(NoSuchElementException,StaleElementReferenceException,)
            WebDriverWait(driver, 10,ignored_exceptions=ignored_exceptions).until(EC.presence_of_element_located((By.ID, 'grdFPISWH')))        
            
            Select(driver.find_element_by_id('drpArchival')).select_by_index(option)
        
       # WebDriverWait(driver, 5).until(EC.element_to_be_selected((By.ID,'drpArchival')))
    #    if value == '--Select--':
    #        continue
    #    
    #    else:
    #        Select(driver.find_element_by_id('drpArchival')).select_by_value(value)
       
        try:
            value = Select(driver.find_element_by_id('drpArchival')).options[option].text
            datetime_object = datetime.strptime(value, '%d-%b-%Y').date()
        except:
            pass
        
        table_by_date = pd.DataFrame()
        page_select_id = driver.find_elements_by_xpath("//td[@colspan]")
    
        for j in range(1, len(page_select_id)+1):
          #  page_select_individual_id = driver.find_elements_by_xpath("//td[@colspan]")[j]
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

def get_latest(driver,url_to_scrape,historic_data):
    
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

def get_data_from_db():
    
    server = 'tcp:akash-test.database.windows.net'
    database = 'web_scrape'
    username = 'admin_1'
    password = 'AkashTillu123!' 
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password) 
    
    query = 'SELECT * FROM dbo.SecurityHoldings'
    historic_data = pd.read_sql(query, conn,index_col='Date')
    historic_data = historic_data.sort_index()
    historic_data = historic_data.astype(object)
    
    return historic_data


def post_to_db(data_to_append,db='SecurityHoldings',replace=False):
    
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
    post_to_db(new_data,db='SecurityHoldings_test')

#def create_new_test_db():
#
#    for driver_test in pyodbc.drivers():
#        print(driver_test)
#    
#    server = 'tcp:akash-test.database.windows.net'
#    database = 'web_scrape'
#    username = 'admin_1'
#    password = 'AkashTillu123!'   
#    
#    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
#    
#    cursor = conn.cursor()
#    cursor.execute('''
#                    CREATE TABLE SecurityHoldings_test (Date datetime, 
#                                                   ISIN nvarchar(50), 
#                                                   SecurityDescription nvarchar(50),
#                                                   AggregateHoldingOfFPIS Float(8), 
#                                                   OutstandingPositionOfGovtSecurities Float(8),
#                                                   SecHoldings Float(8))
#                    ''')
#    conn.commit()
