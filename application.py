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
import webscrape as wb

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


historic_data = wb.get_data_from_db()
df_choose = historic_data[['ISIN','AggregateHoldingOfFPIS']]
df_choose['As_of_Date'] = df_choose.index
df_choose_grp = df_choose.groupby(['ISIN','As_of_Date'], as_index=False)['AggregateHoldingOfFPIS'].sum()
df_choose_grp = df_choose_grp.set_index('As_of_Date')


app.layout = html.Div([

    html.Div([dcc.Graph(id='indicator-graphic'),

        dcc.Dropdown(id='ISIN',
            options=[{'label':x, 'value':x} for x in df_choose_grp.sort_values('ISIN')['ISIN'].unique()],
            value='IN0020000025',
            style={'width':"90%"},
            ),]),
])


@app.callback(
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
    app.run_server(debug=True)