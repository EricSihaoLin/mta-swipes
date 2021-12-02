import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import altair as alt
import numpy as np
import datetime

'# MTA Swipes Viewer'

@st.cache(allow_output_mutation=True)
def get_connection():
  return create_engine("sqlite:///mta-aggregate.db")

@st.cache
def query_db(sql: str):
  # Connect to our file db
  conn = get_connection()
  # Execute a command: this creates a new table
  # Obtain data
  cur = conn.execute(sql)
  df = pd.DataFrame(data=cur.fetchall(), columns=cur.keys())
  return df

all_tool = "All System"
line_tool = "View by Line"
station_tool = "View by Station"
comparison_tool = "Comparison"
tools = [all_tool, line_tool, station_tool, comparison_tool]

selected_tool = st.selectbox('Please select a tool', tools)

############
# ALL SYSTEM TOOL
############
if selected_tool == all_tool:
  system = st.selectbox('Choose a system', ['NYCT', 'PATH', 'TRAM', 'AIRTRAIN'])
  year = st.selectbox('Choose a year', [2021,2020,2019,2018,2017,2016])
  metric = st.selectbox('Choose a metric', ['Entries', 'Exits'])
  if year and system and metric:
    f'### Yearly Ridership Table ({metric})'
    sql_table = f'select date, sum(graveyard_{metric.lower()}) as graveyard, sum(morning_{metric.lower()}) as morning, sum(afternoon_{metric.lower()}) as afternoon, sum(night_{metric.lower()}) as night from daily_count where date like "{year}%" and station_id in (select id from station_data where type = "{system}") group by date'
    df = query_db(sql_table)
    g_mean = int(df['graveyard'].mean())
    m_mean = int(df['morning'].mean())
    a_mean = int(df['afternoon'].mean())
    n_mean = int(df['night'].mean())
    f'Average Graveyard (12 AM - 6 AM): {g_mean}'
    f'Average Morning (6 AM - Noon): {m_mean}'
    f'Average Graveyard (Noon - 6 PM): {a_mean}'
    f'Average Graveyard (6 PM - 12 AM): {n_mean}'
    df = df.rename(columns={'date':'index'}).set_index('index')
    st.line_chart(df)

    month_filter = 'Filter by Month'
    week_filter = 'Filter by Weekday/Weekend'
    filter_tool = st.selectbox('Choose a filter', [month_filter, week_filter])
    if filter_tool == month_filter:
      ## FILTERING BY MONTH
      months = {'01':'January','02':'February','03':'March','04':'April','05':'May','06':'June','07':'July','08':'August','09':'September','10':'October','11':'November','12':'December'}
      def format_func(option):
        return months[option]
      month = st.selectbox('Choose a month', options=list(months.keys()), format_func=format_func)
      if month:
        f'### {format_func(month)} Ridership Table ({metric})'
        sql_table_month = f'select date, sum(graveyard_{metric.lower()}) as graveyard, sum(morning_{metric.lower()}) as morning, sum(afternoon_{metric.lower()}) as afternoon, sum(night_{metric.lower()}) as night from daily_count where date like "{year}/{month}%" and station_id in (select id from station_data where type = "{system}") group by date'
        df_month = query_db(sql_table_month)
        g_mean_month = int(df_month['graveyard'].mean())
        m_mean_month = int(df_month['morning'].mean())
        a_mean_month = int(df_month['afternoon'].mean())
        n_mean_month = int(df_month['night'].mean())
        f'Average Graveyard (12 AM - 6 AM): {g_mean_month}'
        f'Average Morning (6 AM - Noon): {m_mean_month}'
        f'Average Graveyard (Noon - 6 PM): {a_mean_month}'
        f'Average Graveyard (6 PM - 12 AM): {n_mean_month}'
        df_month = df_month.rename(columns={'date':'index'}).set_index('index')
        st.line_chart(df_month)
    if filter_tool == week_filter:
      dayofweek = st.selectbox('Choose a filter', ['Weekday', 'Weekend'])
      dates = pd.to_datetime(df.index, format="%Y/%m/%d")
      df["weekend"] = dates.dayofweek > 4
      if dayofweek == 'Weekday':
        f'### {dayofweek} Ridership Table ({metric})'
        df_weekday = df[df["weekend"] == "false"]
        df_weekday.drop(['weekend'], axis=1)
        g_mean_week = int(df_weekday['graveyard'].mean())
        m_mean_week = int(df_weekday['morning'].mean())
        a_mean_week = int(df_weekday['afternoon'].mean())
        n_mean_week = int(df_weekday['night'].mean())
        f'Average Graveyard (12 AM - 6 AM): {g_mean_week}'
        f'Average Morning (6 AM - Noon): {m_mean_week}'
        f'Average Graveyard (Noon - 6 PM): {a_mean_week}'
        f'Average Graveyard (6 PM - 12 AM): {n_mean_week}'
        st.line_chart(df_weekday)
      if dayofweek == 'Weekend':
        df_weekday = df[df["weekend"] == "true"]
        df_weekday.drop(['weekend'], axis=1)
        f'### {dayofweek} Ridership Table ({metric})'
        df_weekday = df[df["weekend"] == "false"]
        df_weekday.drop(['weekend'], axis=1)
        g_mean_week = int(df_weekday['graveyard'].mean())
        m_mean_week = int(df_weekday['morning'].mean())
        a_mean_week = int(df_weekday['afternoon'].mean())
        n_mean_week = int(df_weekday['night'].mean())
        f'Average Graveyard (12 AM - 6 AM): {g_mean_week}'
        f'Average Morning (6 AM - Noon): {m_mean_week}'
        f'Average Graveyard (Noon - 6 PM): {a_mean_week}'
        f'Average Graveyard (6 PM - 12 AM): {n_mean_week}'
        st.line_chart(df_weekday)