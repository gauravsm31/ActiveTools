# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output, State
import sqlalchemy
from sqlalchemy.dialects import postgresql
import pandas as pd
import psycopg2
import sys
import os
from flask import Flask
from itertools import combinations

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

def GetLibraryPairs(lib_list):
    return_list = lib_list
    lib_list_sorted = sorted(lib_list)
    # get list of all subsets of length 2
    # to deal with duplicate subsets use
    # set(list(combinations(arr, r)))
    comb_list = list(combinations(lib_list_sorted, 2))
    for lib_pair in comb_list:
        return_list.append(str(lib_pair[0])+'_'+str(lib_pair[1]))
    return return_list

con, meta = connect()
df_libinfo = pd.read_csv('LibraryInfo.csv')
lib_list = df_libinfo['Libraries'].values.tolist()
all_libs_list = GetLibraryPairs(lib_list)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.config['suppress_callback_exceptions'] = True

available_categories = df_libinfo['Category'].unique()
available_libraries = df_libinfo['Libraries'].unique()


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
    html.H1('ActiveTools',style={'text-align': 'center', 'padding': '20px',}),

    dcc.Tabs(id="tabs-navigation", value='tab-1', children=[
        dcc.Tab(label='Explore Categories', value='tab-1', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='Explore Collocation', value='tab-2', style=tab_style, selected_style=tab_selected_style),
    ], style=tabs_styles),
    html.Div(id='tabs-content-inline')
])



@app.callback(Output('tabs-content-inline', 'children'),
              [Input('tabs-navigation', 'value')])

def render_content(tab):

    libs_in_category = df_libinfo[df_libinfo['Category']=='Visualization']['Libraries'].values.tolist()

    table = libs_in_category[0]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_lib1 = pd.read_sql_query(qry,con)

    table = libs_in_category[1]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_lib2 = pd.read_sql_query(qry,con)
    
    table = libs_in_category[2]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_lib3 = pd.read_sql_query(qry,con)


    libs_in_coll_category = df_libinfo[df_libinfo['Category']=='Data Wrangling']['Libraries'].values.tolist()
    coll_cats = df_libinfo[df_libinfo['Category']!=df_libinfo[df_libinfo['Libraries']=='matplotlib'].iloc[0]['Category']]['Category'].unique()
    tables = []
    lib_main = 'matplotlib'
    for lib in libs_in_coll_category:
        if lib < 'matplotlib':
            tables.append(lib+'_'+'matplotlib')
        else:
            tables.append('matplotlib'+'_'+lib)
    table = tables[0]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_coll_lib1 = pd.read_sql_query(qry,con)

    table = tables[1]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_coll_lib2 = pd.read_sql_query(qry,con)

    table = tables[2]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_coll_lib3 = pd.read_sql_query(qry,con)

    if tab == 'tab-1':
        return html.Div([
            html.H4('choose category',style={'text-align': 'center','padding': '20px 0px 0px 0px'}),

                html.Div([
                dcc.Dropdown(
                    id='choose-category',
                    options=[{'label': i, 'value': i} for i in available_categories],
                    value='Visualization'
                ),
            ],
            style={'width': '50%', 'text-align': 'center','marginLeft': '420px','display': 'inline-block','fontWeight': 'bold'}),
            # ,'left': '50%','right': 'auto'

            dcc.Graph(
                id='graph-1-tabs',
                figure={
                    'data': [
                    {'x': df_lib1['datetime'], 'y': df_lib1['users'], 'type': 'line', 'name': libs_in_category[0]},
                    {'x': df_lib2['datetime'], 'y': df_lib2['users'], 'type': 'line', 'name': libs_in_category[1]},
                    {'x': df_lib3['datetime'], 'y': df_lib3['users'], 'type': 'line', 'name': libs_in_category[2]}
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
            # html.H3('Visualize Trends On Active Users For Libraries in Different Categories',style={'text-align': 'center','padding': '20px 0px 0px 0px'}),
            html.Div([
                html.H4('choose library',style={'text-align': 'center','padding': '20px 0px 0px 0px'}),
                html.Div([
                dcc.Dropdown(
                    id='choose-library',
                    options=[{'label': i, 'value': i} for i in available_libraries],
                    value='matplotlib'
                ),
            ],
            style={'width': '80%', 'text-align': 'center','marginLeft': '100px','display': 'inline-block','fontWeight': 'bold'}),

                html.H4('choose category',style={'text-align': 'center','padding': '34px 0px 0px 0px'}),
                html.Div([
                dcc.Dropdown(
                    id='choose-collocation-category',
                    options=[{'label': i, 'value': i} for i in coll_cats],
                    value='Data Wrangling'
                ),
            ],
            style={'width': '80%', 'text-align': 'center','marginLeft': '100px','display': 'inline-block','fontWeight': 'bold'}),
            ], style={'columnCount': 2}),

            dcc.Graph(
                id='graph-2-tabs',
                figure={
                    'data': [
                    {'x': df_coll_lib1['datetime'], 'y': df_coll_lib1['users'], 'type': 'line', 'name': lib_main + ' + ' + libs_in_coll_category[0]},
                    {'x': df_coll_lib2['datetime'], 'y': df_coll_lib2['users'], 'type': 'line', 'name': lib_main + ' + ' + libs_in_coll_category[1]},
                    {'x': df_coll_lib3['datetime'], 'y': df_coll_lib3['users'], 'type': 'line', 'name': lib_main + ' + ' + libs_in_coll_category[2]}
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


@app.callback(Output('graph-1-tabs', 'figure'),
              [Input('choose-category', 'value')])

def update_graph(lib_category):

    libs_in_category = df_libinfo[df_libinfo['Category']==lib_category]['Libraries'].values.tolist()

    table = libs_in_category[0]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_lib1 = pd.read_sql_query(qry,con)

    table = libs_in_category[1]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_lib2 = pd.read_sql_query(qry,con)

    table = libs_in_category[2]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_lib3 = pd.read_sql_query(qry,con)

    return {
        'data': [
        {'x': df_lib1['datetime'], 'y': df_lib1['users'], 'type': 'line', 'name': libs_in_category[0]},
        {'x': df_lib2['datetime'], 'y': df_lib2['users'], 'type': 'line', 'name': libs_in_category[1]},
        {'x': df_lib3['datetime'], 'y': df_lib3['users'], 'type': 'line', 'name': libs_in_category[2]}
        ],
        'layout':{
                'xaxis':{
                    'title':'Date',
                    },
                'yaxis':{
                    'title':'Users',
                    }
        }
    }

@app.callback([Output('choose-collocation-category', 'options'),
                Output('choose-collocation-category', 'value')],
              [Input('choose-library', 'value')])

def update_dropdown(libr):
    coll_cats = df_libinfo[df_libinfo['Category']!=df_libinfo[df_libinfo['Libraries']==libr].iloc[0]['Category']]['Category'].unique()
    return [{'label': i, 'value': i} for i in coll_cats], coll_cats[0]




@app.callback(Output('graph-2-tabs', 'figure'),
              [Input('choose-collocation-category', 'value')],
              [State('choose-library', 'value')])

def update_graph(coll_category,libr):

    lib_main = libr
    libs_in_coll_category = df_libinfo[df_libinfo['Category']==coll_category]['Libraries'].values.tolist()
    coll_cats = df_libinfo[df_libinfo['Category']!=df_libinfo[df_libinfo['Libraries']==libr].iloc[0]['Category']]['Category'].unique()
    tables = []
    for lib in libs_in_coll_category:
        if lib < libr:
            tables.append(lib+'_'+libr)
        else:
            tables.append(libr+'_'+lib)

    table = tables[0]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_coll_lib1 = pd.read_sql_query(qry,con)

    table = tables[1]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_coll_lib2 = pd.read_sql_query(qry,con)

    table = tables[2]
    qry = 'select datetime,SUM(CAST(lib_counts AS int)) as users from ' + table + ' GROUP BY datetime ORDER BY datetime ASC'
    df_coll_lib3 = pd.read_sql_query(qry,con)


    return {
        'data': [
        {'x': df_coll_lib1['datetime'], 'y': df_coll_lib1['users'], 'type': 'line', 'name': lib_main + ' + ' + libs_in_coll_category[0]},
        {'x': df_coll_lib2['datetime'], 'y': df_coll_lib2['users'], 'type': 'line', 'name': lib_main + ' + ' + libs_in_coll_category[1]},
        {'x': df_coll_lib3['datetime'], 'y': df_coll_lib3['users'], 'type': 'line', 'name': lib_main + ' + ' + libs_in_coll_category[2]}
        ],
        'layout':{
                'xaxis':{
                    'title':'Date',
                    },
                'yaxis':{
                    'title':'Users',
                    }
        }
    }




if __name__ == '__main__':
    app.run_server(debug=True)
