# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output
import sqlalchemy
from sqlalchemy.dialects import postgresql
import pandas as pd
import psycopg2
import sys
import os
from flask import Flask

def connect():
    #Returns a connection and a metadata object
    # We connect with the help of the PostgreSQL URL
    user = 'gaurav'
    password = 'Ichwisenicht_31'
    host = 'ec2-54-244-2-49.us-west-2.compute.amazonaws.com'
    db = 'postgres'
    port = 5432
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8')

    # We then bind the connection to MetaData()
    meta = sqlalchemy.MetaData(bind=con, reflect=True)

    return con, meta

con, meta = connect()

df_matplotlib = pd.read_sql_query('SELECT * FROM matplotlib ORDER BY datetime ASC',con)
df_plotly = pd.read_sql_query('SELECT * FROM plotly ORDER BY datetime ASC',con)
df_seaborn = pd.read_sql_query('SELECT * FROM seaborn ORDER BY datetime ASC',con)

df_numpy = pd.read_sql_query('SELECT * FROM numpy ORDER BY datetime ASC',con)
df_scipy = pd.read_sql_query('SELECT * FROM scipy ORDER BY datetime ASC',con)
df_pandas = pd.read_sql_query('SELECT * FROM pandas ORDER BY datetime ASC',con)

df_sklearn = pd.read_sql_query('SELECT * FROM sklearn ORDER BY datetime ASC',con)
df_eli5 = pd.read_sql_query('SELECT * FROM eli5 ORDER BY datetime ASC',con)
df_xgboost = pd.read_sql_query('SELECT * FROM xgboost ORDER BY datetime ASC',con)

df_nltk = pd.read_sql_query('SELECT * FROM nltk ORDER BY datetime ASC',con)
df_gensim = pd.read_sql_query('SELECT * FROM gensim ORDER BY datetime ASC',con)
df_spacy = pd.read_sql_query('SELECT * FROM spacy ORDER BY datetime ASC',con)

df_keras = pd.read_sql_query('SELECT * FROM keras ORDER BY datetime ASC',con)
df_tensorflow = pd.read_sql_query('SELECT * FROM tensorflow ORDER BY datetime ASC',con)
df_theano = pd.read_sql_query('SELECT * FROM theano ORDER BY datetime ASC',con)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

server = app.server

tabs_styles = {
    'height': '44px'
}
tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '8px',
    'fontWeight': 'bold'
}

tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#4582c2',
    'color': '#fcfcfb',
    'padding': '8px',
    'fontWeight': 'bold'
}

app.layout = html.Div([
    html.H1('ActiveTools',style={'text-align': 'center', 'padding': '40px',}),
    dcc.Tabs(id="tabs-navigation", value='tab-1', children=[
        dcc.Tab(label='Visualization', value='tab-1', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='Data Wrangling', value='tab-2', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='Machine Learning', value='tab-3', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='Natural Language Processing', value='tab-4', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='Deep Learning', value='tab-5', style=tab_style, selected_style=tab_selected_style),
    ], style=tabs_styles),
    html.Div(id='tabs-content-inline')
])

@app.callback(Output('tabs-content-inline', 'children'),
              [Input('tabs-navigation', 'value')])
def render_content(tab):
    if tab == 'tab-1':
        return html.Div([
            html.H3('Libraries For Graphical Representation Of Data',style={'text-align': 'center','padding': '50px 0px 0px 0px'}),
            dcc.Graph(
                id='graph-1-tabs',
                figure={
                    'data': [
                    {'x': df_matplotlib['datetime'], 'y': df_matplotlib['lib_counts'], 'type': 'line', 'name': 'matplotlib'},
                    {'x': df_plotly['datetime'], 'y': df_plotly['lib_counts'], 'type': 'line', 'name': 'plotly'},
                    {'x': df_seaborn['datetime'], 'y': df_seaborn['lib_counts'], 'type': 'line', 'name': 'seaborn'}
                    ],
                    'layout':{
                            'xaxis':{
                                'title':'Date',
                                },
                            'yaxis':{
                                'title':'Users',
                                }
                    }
                }, style={'height': '600px'}
            )
        ], style={'marginLeft': '100px', 'marginRight': '100px'})
    elif tab == 'tab-2':
        return html.Div([
            html.H3('Libraries For Transforming And Mapping Data',style={'text-align': 'center','padding': '50px 0px 0px 0px'}),
            dcc.Graph(
                id='graph-1-tabs',
                figure={
                    'data': [
                    {'x': df_numpy['datetime'], 'y': df_numpy['lib_counts'], 'type': 'line', 'name': 'numpy'},
                    {'x': df_scipy['datetime'], 'y': df_scipy['lib_counts'], 'type': 'line', 'name': 'scipy'},
                    {'x': df_pandas['datetime'], 'y': df_pandas['lib_counts'], 'type': 'line', 'name': 'pandas'}
                    ],
                    'layout':{
                            'xaxis':{
                                'title':'Date'
                                },
                            'yaxis':{
                                'title':'Users'
                                }
                    }
                }, style={'height': '600px'}
            )
        ], style={'marginLeft': '100px', 'marginRight': '100px'})
    elif tab == 'tab-3':
        return html.Div([
            html.H3('Libraries For Learning From Data',style={'text-align': 'center','padding': '50px 0px 0px 0px'}),
            dcc.Graph(
                id='graph-1-tabs',
                figure={
                    'data': [
                    {'x': df_sklearn['datetime'], 'y': df_sklearn['lib_counts'], 'type': 'line', 'name': 'sklearn'},
                    {'x': df_eli5['datetime'], 'y': df_eli5['lib_counts'], 'type': 'line', 'name': 'eli5'},
                    {'x': df_xgboost['datetime'], 'y': df_xgboost['lib_counts'], 'type': 'line', 'name': 'xgboost'}
                    ],
                    'layout':{
                            'xaxis':{
                                'title':'Date'
                                },
                            'yaxis':{
                                'title':'Users'
                                }
                    }
                }, style={'height': '600px'}
            )
        ], style={'marginLeft': '100px', 'marginRight': '100px'})
    elif tab == 'tab-4':
        return html.Div([
            html.H3('Libraries For Understanding Human Language',style={'text-align': 'center','padding': '50px 0px 0px 0px'}),
            dcc.Graph(
                id='graph-1-tabs',
                figure={
                    'data': [
                    {'x': df_nltk['datetime'], 'y': df_nltk['lib_counts'], 'type': 'line', 'name': 'nltk'},
                    {'x': df_gensim['datetime'], 'y': df_gensim['lib_counts'], 'type': 'line', 'name': 'gensim'},
                    {'x': df_spacy['datetime'], 'y': df_spacy['lib_counts'], 'type': 'line', 'name': 'spacy'}
                    ],
                    'layout':{
                            'xaxis':{
                                'title':'Date'
                                },
                            'yaxis':{
                                'title':'Users'
                                }
                    }
                }, style={'height': '600px'}
            )
        ], style={'marginLeft': '100px', 'marginRight': '100px'})
    elif tab == 'tab-5':
        return html.Div([
            html.H3('Libraries For Doing Tasks That Require Human Intelligence',style={'text-align': 'center','padding': '50px 0px 0px 0px'}),
            dcc.Graph(
                id='graph-1-tabs',
                figure={
                    'data': [
                    {'x': df_keras['datetime'], 'y': df_keras['lib_counts'], 'type': 'line', 'name': 'keras'},
                    {'x': df_tensorflow['datetime'], 'y': df_tensorflow['lib_counts'], 'type': 'line', 'name': 'tensorflow'},
                    {'x': df_theano['datetime'], 'y': df_theano['lib_counts'], 'type': 'line', 'name': 'theano'}
                    ],
                    'layout':{
                            'xaxis':{
                                'title':'Date'
                                },
                            'yaxis':{
                                'title':'Users'
                                }
                    }
                }, style={'height': '600px'}
            )
        ], style={'marginLeft': '100px', 'marginRight': '100px'})

# app.layout = html.Div(children=[
#     html.H1(children='ActiveTools'),
#
#     html.Div(children='''
#         Dash: A web application framework for Python.
#     '''),
#
#     dcc.Graph(
#         id='example-graph',
#         figure={
#             'data': [
#                 {'x': df_matplotlib['datetime'], 'y': df_matplotlib['lib_counts'], 'type': 'line', 'name': 'matplotlib'},
#                 {'x': df_plotly['datetime'], 'y': df_plotly['lib_counts'], 'type': 'line', 'name': 'plotly'},
#                 {'x': df_seaborn['datetime'], 'y': df_seaborn['lib_counts'], 'type': 'line', 'name': 'seaborn'}
#             ],
#             'layout': {
#                 'title': 'Dash Data Visualization'
#             }
#         }
#     )
# ])

if __name__ == '__main__':
    app.run_server(debug=True)
