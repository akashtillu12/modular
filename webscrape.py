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


driver = webdriver.Chrome(executable_path=r'C:\bin\chromedriver')

url_to_scrape = 'https://www.ccilindia.com/FPI_ARCV.aspx'

            
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
    
