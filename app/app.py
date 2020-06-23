import os
import dash
from dash.dependencies import Input, Output, State
import dash_table
import dash_html_components as html
import dash_core_components as dcc
from dash.exceptions import PreventUpdate
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
import plotly.express as px
import plotly.graph_objs as go
from dotenv import load_dotenv
import os
load_dotenv()

db_user = os.getenv("db_user")
db_password = os.getenv("db_password")
UI_host = os.getenv("UI_host")

# connect to database
try:
    conn = psycopg2.connect(dbname='reddit', user=db_user, password=db_password, host='10.0.0.10')
except:
    print "I am unable to connect to the database."

# create dashboard
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash('ModDash', external_stylesheets = external_stylesheets)
server = app.server

app.layout = html.Div([

    html.H1('Subreddit: r/Politics',style={'font-family':'arial', 'color':'#ff5722','font-weight':'bold'}),
    html.Div([
    html.Label('Year',style={'font-family':'arial', 'font-weight':'bold'}),
    dcc.Dropdown(
        id='year',
        options=[{'label': i, 'value': i} for i in [2013,2014,2015,2016,2017]],
        style={'font-family':'arial','margin-right':'40px'},
	placeholder='Year'
	)],
    style={'display':'inline-block'}),

    html.Div([
    html.Label('Month',style={'font-family':'arial','margin-top':'5px','font-weight':'bold'}),
    dcc.Dropdown(
        id='month',
        options=[{'label': i, 'value': i} for i in [1,2,3,4,5,6,7,8,9,10,11,12]],
        style={'font-family':'arial','margin-right':'40px'},
        placeholder='Month',
	)],
    style={'display':'inline-block'}),

    html.Div([
    html.Label('Day',style={'font-family':'arial','margin-top':'5px', 'font-weight':'bold'}),
    dcc.Dropdown(
        id='day',
        options=[{'label': i, 'value': i} for i in [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]],
        style={'font-family':'arial', 'margin-right':'40px'},
        placeholder='Day'
	)],
    style={'display':'inline-block'}),

    html.Div([
    html.Button('Submit',n_clicks=0, style={'margin-top':'10px'}, id='submit')]),

    html.H4('TOP COMMENTS',style={'font-family':'arial', 'font-weight':'bold'}),
    dash_table.DataTable(
    id='comments',
    columns=[{"name": i, "id": i} for i in ['time','post_id','author','score','sentiment','comment']],
    style_cell_conditional=[{'if':{'column_id':'comment'},'overflowX':'scroll', 'textAlign':'left','height':'auto'}, \
                            {'if':{'column_id':'time'}, 'width':'150px', 'textAlign':'left'}, \
                            {'if':{'column_id':'post_id'},'width':'90px', 'textAlign':'left'},\
			    {'if':{'column_id':'author'}, 'width':'140px', 'textAlign':'left'},
                            {'if':{'column_id':'score'}, 'width':'50px', 'textAlign':'left'},
                            {'if':{'column_id':'sentiment'}, 'width':'80px', 'textAlign':'left'}],
    style_cell={'font-family':'sans-serif','maxWidth':'40px'},
    style_header={
        'backgroundColor': 'white',
        'fontWeight': 'bold'
    	}
    ),

    html.H4('TOP POSTS',style={'font-family':'arial','font-weight':'bold'}),
    dash_table.DataTable(
    id='posts',
    columns=[{"name": i, "id": i} for i in ['post_id','avg_sentiment','total_comments']],
    style_cell_conditional=[{'if':{'column_id':'post_id'},'width':'50px', 'textAlign':'left'},\
                            {'if':{'column_id':'avg_sentiment'}, 'width':'10px', 'textAlign':'left'},
                            {'if':{'column_id':'total_comments'}, 'width':'10px', 'textAlign':'left'},
                            ],
    style_header={
        'backgroundColor': 'white',
        'fontWeight': 'bold'
   	 },
    style_cell={'textAlign':'left','font-family':'sans-serif'},
    ),


    html.H4('USER HISTORY',style={'font-family':'arial','font-weight':'bold'}),
    dcc.Input(id='user', placeholder='Enter author here', type='text'),
    html.Div([
    html.Button('View User',n_clicks=0, style={'margin-top':'10px'}, id='user-button')
		]),
   
    dcc.Graph(id='user-graph'),
   
    dash_table.DataTable(
    id='user-comments',
    columns=[{"name": i, "id": i} for i in ['time','author','sentiment','score','comment']],
    style_cell_conditional=[{'if':{'column_id':'time'},'width':'50px', 'textAlign':'left'},\
                            {'if':{'column_id':'author'}, 'width':'10px', 'textAlign':'left'},
                            {'if':{'column_id':'sentiment'}, 'width':'10px', 'textAlign':'left'},
                            {'if':{'column_id':'score'}, 'width':'10px', 'textAlign':'left'},
                            {'if':{'column_id':'comment'},'overflowX':'scroll', 'textAlign':'left','height':'auto'},

                            ],
    style_header={
        'backgroundColor': 'white',
        'fontWeight': 'bold'
         },
    style_cell={'textAlign':'left','font-family':'sans-serif'},
    ),


], style={'width':'100%','display':'inline-block'})



@app.callback(Output('comments','data'),[Input('submit','n_clicks')], [State('year','value'),State('month','value')])
def update_comments(n_clicks, year, month):
    if n_clicks is None:
	raise PreventUpdate
    if n_clicks > 0:
	select = "SELECT time,post_id,author,score,sentiment,comment FROM comments "
        year = "WHERE year = {} AND ".format(year)
        month = "month = {} AND ".format(month)
	subreddit = "subreddit = 'politics' "
        order_by = "ORDER BY sentiment ASC LIMIT 5;"
        sql = select + year + month + subreddit + order_by
        df = sqlio.read_sql_query(sql, conn)
    	return df.to_dict('records')

@app.callback(Output('posts','data'),[Input('submit','n_clicks')], [State('year','value'),State('month','value')])
def update_posts(n_clicks, year, month):
    if n_clicks is None:
	raise PreventUpdate
    if n_clicks > 0:
        select = "SELECT post_id,AVG(sentiment) AS avg_sentiment, COUNT(comment) AS total_comments FROM comments "
        year = "WHERE year = {} AND ".format(year)
        month = "month = {} AND ".format(month)
	subreddit = "subreddit = 'politics' "
	groupby = "GROUP BY post_id HAVING COUNT(comment) > 5 " 
        order_by = "ORDER BY avg_sentiment ASC LIMIT 5;"
        sql = select + year + month + subreddit + groupby + order_by
        df = sqlio.read_sql_query(sql, conn)
        return df.to_dict('records')

@app.callback([Output('user-graph','figure'),Output('user-comments','data')],[Input('user-button','n_clicks')], [State('user','value'),State('year','value'),State('month','value')])
def update_user(n_clicks, user, year, month):
    if n_clicks is None or user is None:
	raise PreventUpdate
    elif n_clicks > 0:
	select = "SELECT time, author, sentiment, score, comment FROM comments "
	year = "WHERE year = {} AND ".format(year)
	month = "month = {} AND ".format(month)
	subreddit = "subreddit = 'politics' AND "
	author_str = "'" + user + "'" 
	author = " author = {} ".format(author_str)
	sql = select + year + month + subreddit + author
	df = sqlio.read_sql_query(sql, conn).sort_values('time')
	fig = {'data': [go.Scatter(x=df.time, y=df.sentiment,name='Hi')], 'layout': go.Layout(title='User Sentiment')}
	return fig, df.to_dict('records')


if __name__ == '__main__':
    app.run_server(debug=True, port=8050, host=UI_host)

