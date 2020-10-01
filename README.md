# modular
# webscrape_ubuntu.py file to execute webscrape backfill code, get latest webscrape and append to database

# you need to install dependencies: pip install selenium, pip install pyodbc, pip install dash and dependences

# please make sure your chromedriver path is correctly specified. This is done for the ubuntu instance, but needs to be checked if you run the code on your local machine. Please make sure pyodbc connection requirements (odbc server etc) are ok

# the backfill data can either be manually extracted into data.csv or data_upload.txt & bcp imported into the SQL db. I have also enabled a direct "append_to_sql" function which can be used to bulk insert if necessary

# the __main__ file looks only at updates: the default triggers are: 1) get data from DB; 2) check if there is new data to be updated; 3) appended new data to DB

# application.py file is the dash app used to perform dropdown and select ISIN + display line chart functionality. This file works in localhost but not deployed to the webapp.
