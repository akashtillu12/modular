# -*- coding: utf-8 -*-
"""
Dash_app - Akash Tillu
Code has 2 functions:
1. Choose columns Indicative Value Of Aggregate Holding of FPIS and ISIN; then combining the data
2. Displaying a dropdown using Dash and line chart using ploty px
"""

"""
Declaration: modules used: dash, webscrape and css style sheet to improve the look of the table
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px

import pandas as pd
import pyodbc

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
dash_app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app = dash_app.server


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

df_choose = historic_data[['ISIN','AggregateHoldingOfFPIS']]
df_choose['As_of_Date'] = df_choose.index
df_choose_grp = df_choose.groupby(['ISIN','As_of_Date'], as_index=False)['AggregateHoldingOfFPIS'].sum()
df_choose_grp = df_choose_grp.set_index('As_of_Date')


dash_app.layout = html.Div([

    html.Div([dcc.Graph(id='indicator-graphic'),

        dcc.Dropdown(id='ISIN',
            options=[{'label':x, 'value':x} for x in df_choose_grp.sort_values('ISIN')['ISIN'].unique()],
            value='IN0020000025',
            style={'width':"90%"},
            ),]),
])


@dash_app.callback(
    Output('indicator-graphic','figure'),
    [Input('ISIN','value')]
)

def build_graph(ISIN):
    dff=df_choose_grp[(df_choose_grp['ISIN']==ISIN)]
    dff['As_of_Date'] = dff.index 

    fig = px.line(dff, x="As_of_Date", y="AggregateHoldingOfFPIS", height=600)
    fig.update_layout(yaxis={'title':'Aggregate Holdings in INR CR'},
                      title={'text':'Agg holdings by ISIN'})
    return fig


if __name__ == '__main__':
    dash_app.run_server(debug=False)