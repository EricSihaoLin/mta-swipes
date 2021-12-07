from attr import define
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import altair as alt
import numpy as np
import datetime
import pydeck as pdk

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

all_tool = "View All System"
line_tool = "View by Line"
station_tool = "View by Station"
comparison_tool = "View Comparison"
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
    f'### {year} Ridership Table ({metric})'
    sql_table = f'''select date, sum(graveyard_{metric.lower()}) as graveyard, 
                    sum(morning_{metric.lower()}) as morning, sum(afternoon_{metric.lower()}) 
                    as afternoon, sum(night_{metric.lower()}) as night from daily_count 
                    where date like "{year}%" and station_id in 
                    (select id from station_data where type = "{system}") group by date'''
    df = query_db(sql_table)
    g_mean = int(df['graveyard'].mean())
    m_mean = int(df['morning'].mean())
    a_mean = int(df['afternoon'].mean())
    n_mean = int(df['night'].mean())
    f'Average Graveyard (12 AM - 6 AM): {g_mean}'
    f'Average Morning (6 AM - Noon): {m_mean}'
    f'Average Graveyard (Noon - 6 PM): {a_mean}'
    f'Average Graveyard (6 PM - 12 AM): {n_mean}'
    chart = alt.Chart(df).transform_fold(
      ['graveyard', 'morning', 'afternoon', 'night']
    ).mark_line().encode(
      x=alt.X('yearmonthdate(date):O', axis=alt.Axis(title='Date')),
      y=alt.Y('value:Q', axis=alt.Axis(title=metric)),
      color='key:N'
    )
    st.altair_chart(chart, use_container_width=True)

    f'### {year} Ridership Map ({metric})'
    map_table = f'''select id, graveyard + morning + afternoon + night as total, name, lon, lat
                    from (select station_id, sum(graveyard_{metric.lower()}) as graveyard, 
                    sum(morning_{metric.lower()}) as morning, sum(afternoon_{metric.lower()}) 
                    as afternoon, sum(night_{metric.lower()}) as night from daily_count 
                    where date like "{year}%" and station_id in 
                    (select id from station_data where type = "{system}")
                    group by station_id)
                    inner join station_data on station_data.id = station_id'''
    map_df = query_db(map_table).copy()
    map_df['qt'] = map_df.total.rank(pct = True)
    layer = pdk.Layer(
      "ColumnLayer",
      data=map_df,
      get_position="[lon, lat]",
      get_elevation_value="total",
      elevation_scale=1,
      radius=50,
      auto_highlight=True,
      get_fill_color=["qt * 255", 0, 0, "qt * 255"],
      elevation_range=[map_df.total.min, map_df.total.max],
      pickable=True,
      extruded=True,
    )
    # Set the viewport location
    view_state = pdk.ViewState(
      longitude=-73.987495, latitude=40.75529, zoom=10, min_zoom=5, max_zoom=15, pitch=40.5, bearing=60
    )
    # Combined all of it and render a viewport
    r = pdk.Deck(
      map_style="mapbox://styles/mapbox/light-v9",
      layers=[layer],
      initial_view_state=view_state,
      tooltip={"html": "<b>{name}</b><br>Total metric {total}", "style": {"color": "white"}},
    )
    st.pydeck_chart(r)


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
        chart = alt.Chart(df_month).transform_fold(
          ['graveyard', 'morning', 'afternoon', 'night']
        ).mark_line().encode(
          x=alt.X('yearmonthdate(date):O', axis=alt.Axis(title='Date')),
          y=alt.Y('value:Q', axis=alt.Axis(title=metric)),
          color='key:N'
        )
        st.altair_chart(chart, use_container_width=True)
        f'### {format_func(month)} Ridership Map ({metric})'
        map_table_month = f'''select id, graveyard + morning + afternoon + night as total, name, lon, lat
                        from (select station_id, sum(graveyard_{metric.lower()}) as graveyard, 
                        sum(morning_{metric.lower()}) as morning, sum(afternoon_{metric.lower()}) 
                        as afternoon, sum(night_{metric.lower()}) as night from daily_count 
                        where date like "{year}/{month}%" and station_id in 
                        (select id from station_data where type = "{system}")
                        group by station_id)
                        inner join station_data on station_data.id = station_id'''
        map_df_month = query_db(map_table_month).copy()
        map_df_month['qt'] = map_df_month.total.rank(pct = True)
        layer = pdk.Layer(
          "ColumnLayer",
          data=map_df_month,
          get_position="[lon, lat]",
          get_elevation_value="total",
          elevation_scale=1,
          radius=50,
          auto_highlight=True,
          get_fill_color=["qt * 255", 0, 0, "qt * 255"],
          elevation_range=[map_df_month.total.min, map_df_month.total.max],
          pickable=True,
          extruded=True,
        )
        # Set the viewport location
        view_state = pdk.ViewState(
          longitude=-73.987495, latitude=40.75529, zoom=10, min_zoom=5, max_zoom=15, pitch=40.5, bearing=60
        )
        # Combined all of it and render a viewport
        r = pdk.Deck(
          map_style="mapbox://styles/mapbox/light-v9",
          layers=[layer],
          initial_view_state=view_state,
          tooltip={"html": "<b>{name}</b><br>Total metric {total}", "style": {"color": "white"}},
        )
        st.pydeck_chart(r)
    if filter_tool == week_filter:
      dayofweek = st.selectbox('Choose a filter', ['Weekday', 'Weekend'])
      dates = pd.to_datetime(df['date'], format="%Y/%m/%d")
      df_weekday = df.copy()
      df_weekday["weekend"] = dates.dt.dayofweek > 4
      f'### {dayofweek} Ridership Table ({metric})'
      if dayofweek == 'Weekday':
        df_weekday = df_weekday[df_weekday["weekend"] == False]
      if dayofweek == 'Weekend':
        df_weekday = df_weekday[df_weekday["weekend"] == True]
      df_weekday = df_weekday.drop(labels=['weekend'], axis=1)
      g_mean_week = int(df_weekday['graveyard'].mean())
      m_mean_week = int(df_weekday['morning'].mean())
      a_mean_week = int(df_weekday['afternoon'].mean())
      n_mean_week = int(df_weekday['night'].mean())
      f'Average Graveyard (12 AM - 6 AM): {g_mean_week}'
      f'Average Morning (6 AM - Noon): {m_mean_week}'
      f'Average Graveyard (Noon - 6 PM): {a_mean_week}'
      f'Average Graveyard (6 PM - 12 AM): {n_mean_week}'
      chart = alt.Chart(df_weekday).transform_fold(
        ['graveyard', 'morning', 'afternoon', 'night']
      ).mark_line().encode(
        x=alt.X('yearmonthdate(date):O', axis=alt.Axis(title='Date')),
        y=alt.Y('value:Q', axis=alt.Axis(title=metric)),
        color='key:N'
      )
      st.altair_chart(chart, use_container_width=True)

############
# VIEW BY LINE
############
if selected_tool == line_tool:
  year = st.selectbox('Choose a year', [2021,2020,2019,2018,2017,2016])
  metric = st.selectbox('Choose a metric', ['Entries', 'Exits'])
  line = st.selectbox('Choose a line', ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'J', 'L', 'M', 'N', 'Q', 'R', 'S', 'Z', '1', '2', '3', '4', '5', '6', '7'])
  if year and metric and line:
    ## extract line data from station name
    sql_table = f'''select date, sum(graveyard_{metric.lower()}) as graveyard, sum(morning_{metric.lower()}) as morning, 
                    sum(afternoon_{metric.lower()}) as afternoon, sum(night_{metric.lower()}) as night from daily_count 
                    where date like "{year}%" and station_id in (select id from station_data where name REGEXP "-[A-Z0-9]*{line}[A-Z0-9]*$") group by date'''
    df = query_db(sql_table)
    f'### {line} Ridership Table ({metric})'
    g_mean = int(df['graveyard'].mean())
    m_mean = int(df['morning'].mean())
    a_mean = int(df['afternoon'].mean())
    n_mean = int(df['night'].mean())
    f'Average Graveyard (12 AM - 6 AM): {g_mean}'
    f'Average Morning (6 AM - Noon): {m_mean}'
    f'Average Graveyard (Noon - 6 PM): {a_mean}'
    f'Average Graveyard (6 PM - 12 AM): {n_mean}'
    chart = alt.Chart(df).transform_fold(
      ['graveyard', 'morning', 'afternoon', 'night']
    ).mark_line().encode(
      x=alt.X('yearmonthdate(date):O', axis=alt.Axis(title='Date')),
      y=alt.Y('value:Q', axis=alt.Axis(title=metric)),
      color='key:N'
    )
    st.altair_chart(chart, use_container_width=True)
    map_table_line = f'''select id, graveyard + morning + afternoon + night as total, name, lon, lat
                        from (select station_id, sum(graveyard_{metric.lower()}) as graveyard, 
                        sum(morning_{metric.lower()}) as morning, sum(afternoon_{metric.lower()}) 
                        as afternoon, sum(night_{metric.lower()}) as night from daily_count 
                        where date like "{year}%" and station_id in 
                        (select id from station_data where name REGEXP "-[A-Z0-9]*{line}[A-Z0-9]*$")
                        group by station_id)
                        inner join station_data on station_data.id = station_id'''
    map_df_line = query_db(map_table_line).copy()
    map_df_line['qt'] = map_df_line.total.rank(pct = True)
    layer = pdk.Layer(
      "ColumnLayer",
      data=map_df_line,
      get_position="[lon, lat]",
      get_elevation_value="total",
      elevation_scale=1,
      radius=50,
      auto_highlight=True,
      get_fill_color=["qt * 140 + 100", 0, 0, "qt * 140"],
      elevation_range=[map_df_line.total.min, map_df_line.total.max],
      pickable=True,
      extruded=True,
    )
    # Set the viewport location
    view_state = pdk.ViewState(
      longitude=-73.987495, latitude=40.75529, zoom=10, min_zoom=5, max_zoom=15, pitch=40.5, bearing=60
    )
    # Combined all of it and render a viewport
    r = pdk.Deck(
      map_style="mapbox://styles/mapbox/light-v9",
      layers=[layer],
      initial_view_state=view_state,
      tooltip={"html": "<b>{name}</b><br>Total metric {total}", "style": {"color": "white"}},
    )
    st.pydeck_chart(r)

############
# VIEW BY STATION
############
if selected_tool == station_tool:
  system = st.selectbox('Choose a system', ['NYCT', 'PATH', 'TRAM', 'AIRTRAIN'])
  year = st.selectbox('Choose a year', [2021,2020,2019,2018,2017,2016])
  metric = st.selectbox('Choose a metric', ['Entries', 'Exits'])
  if system and year and metric:
    station_table = f'''select name from station_data where type = "{system}"'''
    station_names = query_db(station_table)['name'].tolist()
    station_selection = st.selectbox('Choose a station', station_names)
    if station_selection:
      sql_table = f'''select date, sum(graveyard_{metric.lower()}) as graveyard, sum(morning_{metric.lower()}) as morning, 
                    sum(afternoon_{metric.lower()}) as afternoon, sum(night_{metric.lower()}) as night from daily_count 
                    where date like "{year}%" and station_id in (select id from station_data where name = "{station_selection}") group by date'''
      df = query_db(sql_table)
      f'### {station_selection} Ridership Table ({metric})'
      g_mean = int(df['graveyard'].mean())
      m_mean = int(df['morning'].mean())
      a_mean = int(df['afternoon'].mean())
      n_mean = int(df['night'].mean())
      f'Average Graveyard (12 AM - 6 AM): {g_mean}'
      f'Average Morning (6 AM - Noon): {m_mean}'
      f'Average Graveyard (Noon - 6 PM): {a_mean}'
      f'Average Graveyard (6 PM - 12 AM): {n_mean}'
      chart = alt.Chart(df).transform_fold(
        ['graveyard', 'morning', 'afternoon', 'night']
      ).mark_line().encode(
        x=alt.X('yearmonthdate(date):O', axis=alt.Axis(title='Date')),
        y=alt.Y('value:Q', axis=alt.Axis(title=metric)),
        color='key:N'
      )
      st.altair_chart(chart, use_container_width=True)

############
# COMPARISON TOOL
############
if selected_tool == comparison_tool:
  by_year = 'Compare Years'
  by_line = 'Compare Lines'
  by_station = 'Compare Stations'
  comparison_methods = [by_year, by_line, by_station]
  select_comparison = st.selectbox('Please select a comparison method', comparison_methods)
  if select_comparison == by_year:
    system = st.selectbox('Choose a system', ['NYCT', 'PATH', 'TRAM', 'AIRTRAIN'])
    metric = st.selectbox('Choose a metric', ['Entries', 'Exits'])
    list_years = [2021,2020,2019,2018,2017,2016]
    year1 = st.selectbox('Choose a year', list_years)
    if year1 and system and metric:
      list_years2 = list_years.copy()
      list_years2.remove(year1)
      year2 = st.selectbox('Choose a year to compare with', list_years2)

      if year2:
        year1_sql = f'''select date, sum(graveyard_{metric.lower()}) as graveyard, 
          sum(morning_{metric.lower()}) as morning, sum(afternoon_{metric.lower()}) 
          as afternoon, sum(night_{metric.lower()}) as night from daily_count 
          where date like "{year1}%" and station_id in 
          (select id from station_data where type = "{system}") group by date'''
        year2_sql = f'''select date, sum(graveyard_{metric.lower()}) as graveyard, 
          sum(morning_{metric.lower()}) as morning, sum(afternoon_{metric.lower()}) 
          as afternoon, sum(night_{metric.lower()}) as night from daily_count 
          where date like "{year2}%" and station_id in 
          (select id from station_data where type = "{system}") group by date'''
        
        df1 = query_db(year1_sql)
        df2 = query_db(year2_sql)

        g1_mean = int(df1['graveyard'].mean())
        m1_mean = int(df1['morning'].mean())
        a1_mean = int(df1['afternoon'].mean())
        n1_mean = int(df1['night'].mean())
        g2_mean = int(df2['graveyard'].mean())
        m2_mean = int(df2['morning'].mean())
        a2_mean = int(df2['afternoon'].mean())
        n2_mean = int(df2['night'].mean())

        g_change = (g1_mean - g2_mean) / g2_mean
        if g_change < 0:
          g_change = ((g2_mean - g1_mean) / g1_mean) * -1
        m_change = (m1_mean - m2_mean) / m2_mean
        if m_change < 0:
          m_change = ((m2_mean - m1_mean) / m1_mean) * -1
        a_change = (a1_mean - a2_mean) / a2_mean
        if a_change < 0:
          a_change = ((a2_mean - a1_mean) / a1_mean) * -1
        n_change = (n1_mean - n2_mean) / n2_mean
        if n_change < 0:
          n_change = ((n2_mean - n1_mean) / n1_mean) * -1
        f'Average Graveyard Change: **{g_change:.2%}**----{g1_mean}({year1}) vs {g2_mean}({year2})'
        f'Average Morning Change: **{m_change:.2%}**----{m1_mean}({year1}) vs {m2_mean}({year2})'
        f'Average Graveyard Change: **{a_change:.2%}**----{a1_mean}({year1}) vs {a2_mean}({year2})'
        f'Average Graveyard Change: **{n_change:.2%}**----{n1_mean}({year1}) vs {n2_mean}({year2})'
  if select_comparison == by_line:
    year = st.selectbox('Choose a year', [2021,2020,2019,2018,2017,2016])
    metric = st.selectbox('Choose a metric', ['Entries', 'Exits'])
    list_lines = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'J', 'L', 'M', 'N', 'Q', 'R', 'S', 'Z', '1', '2', '3', '4', '5', '6', '7']
    line1 = st.selectbox('Choose a line', list_lines)
    if line1 and year and metric:
      list_lines2 = list_lines.copy()
      list_lines2.remove(line1)
      line2 = st.selectbox('Choose a line to compare with', list_lines2)
      if line2:
        line1_sql = f'''select date, sum(graveyard_{metric.lower()}) as graveyard, sum(morning_{metric.lower()}) as morning, 
                        sum(afternoon_{metric.lower()}) as afternoon, sum(night_{metric.lower()}) as night from daily_count 
                        where date like "{year}%" and station_id in (select id from station_data where name REGEXP "-[A-Z0-9]*{line1}[A-Z0-9]*$") group by date'''
        line2_sql = f'''select date, sum(graveyard_{metric.lower()}) as graveyard, sum(morning_{metric.lower()}) as morning, 
                        sum(afternoon_{metric.lower()}) as afternoon, sum(night_{metric.lower()}) as night from daily_count 
                        where date like "{year}%" and station_id in (select id from station_data where name REGEXP "-[A-Z0-9]*{line2}[A-Z0-9]*$") group by date'''

        df1 = query_db(line1_sql)
        df2 = query_db(line2_sql)

        g1_mean = int(df1['graveyard'].mean())
        m1_mean = int(df1['morning'].mean())
        a1_mean = int(df1['afternoon'].mean())
        n1_mean = int(df1['night'].mean())
        g2_mean = int(df2['graveyard'].mean())
        m2_mean = int(df2['morning'].mean())
        a2_mean = int(df2['afternoon'].mean())
        n2_mean = int(df2['night'].mean())

        g_change = (g1_mean - g2_mean) / g2_mean
        if g_change < 0:
          g_change = ((g2_mean - g1_mean) / g1_mean) * -1
        m_change = (m1_mean - m2_mean) / m2_mean
        if m_change < 0:
          m_change = ((m2_mean - m1_mean) / m1_mean) * -1
        a_change = (a1_mean - a2_mean) / a2_mean
        if a_change < 0:
          a_change = ((a2_mean - a1_mean) / a1_mean) * -1
        n_change = (n1_mean - n2_mean) / n2_mean
        if n_change < 0:
          n_change = ((n2_mean - n1_mean) / n1_mean) * -1
        f'Average Graveyard Change: **{g_change:.2%}**----{g1_mean}({line1}) vs {g2_mean}({line2})'
        f'Average Morning Change: **{m_change:.2%}**----{m1_mean}({line1}) vs {m2_mean}({line2})'
        f'Average Graveyard Change: **{a_change:.2%}**----{a1_mean}({line1}) vs {a2_mean}({line2})'
        f'Average Graveyard Change: **{n_change:.2%}**----{n1_mean}({line1}) vs {n2_mean}({line2})'
  if select_comparison == by_station:
    system = st.selectbox('Choose a system', ['NYCT', 'PATH', 'TRAM', 'AIRTRAIN'])
    year = st.selectbox('Choose a year', [2021,2020,2019,2018,2017,2016])
    metric = st.selectbox('Choose a metric', ['Entries', 'Exits'])
    if system and year and metric:
      station_table = f'''select name from station_data where type = "{system}"'''
      station_names = query_db(station_table)['name'].tolist()
      station_selection = st.selectbox('Choose a station', station_names)
      if station_selection:
        station_names2 = station_names.copy()
        station_names2.remove(station_selection)
        station_selection2 = st.selectbox('Choose a station to compare with', station_names2)
        if station_selection2:
          station1 = f'''select date, sum(graveyard_{metric.lower()}) as graveyard, sum(morning_{metric.lower()}) as morning, 
                    sum(afternoon_{metric.lower()}) as afternoon, sum(night_{metric.lower()}) as night from daily_count 
                    where date like "{year}%" and station_id in (select id from station_data where name = "{station_selection}") group by date'''
          station2 = f'''select date, sum(graveyard_{metric.lower()}) as graveyard, sum(morning_{metric.lower()}) as morning, 
                    sum(afternoon_{metric.lower()}) as afternoon, sum(night_{metric.lower()}) as night from daily_count 
                    where date like "{year}%" and station_id in (select id from station_data where name = "{station_selection2}") group by date'''
          df1 = query_db(station1)
          df2 = query_db(station2)

          g1_mean = int(df1['graveyard'].mean())
          m1_mean = int(df1['morning'].mean())
          a1_mean = int(df1['afternoon'].mean())
          n1_mean = int(df1['night'].mean())
          g2_mean = int(df2['graveyard'].mean())
          m2_mean = int(df2['morning'].mean())
          a2_mean = int(df2['afternoon'].mean())
          n2_mean = int(df2['night'].mean())

          g_change = (g1_mean - g2_mean) / g2_mean
          if g_change < 0:
            g_change = ((g2_mean - g1_mean) / g1_mean) * -1
          m_change = (m1_mean - m2_mean) / m2_mean
          if m_change < 0:
            m_change = ((m2_mean - m1_mean) / m1_mean) * -1
          a_change = (a1_mean - a2_mean) / a2_mean
          if a_change < 0:
            a_change = ((a2_mean - a1_mean) / a1_mean) * -1
          n_change = (n1_mean - n2_mean) / n2_mean
          if n_change < 0:
            n_change = ((n2_mean - n1_mean) / n1_mean) * -1
          f'Average Graveyard Change: **{g_change:.2%}**----{g1_mean}({station_selection}) vs {g2_mean}({station_selection2})'
          f'Average Morning Change: **{m_change:.2%}**----{m1_mean}({station_selection}) vs {m2_mean}({station_selection2})'
          f'Average Graveyard Change: **{a_change:.2%}**----{a1_mean}({station_selection}) vs {a2_mean}({station_selection2})'
          f'Average Graveyard Change: **{n_change:.2%}**----{n1_mean}({station_selection}) vs {n2_mean}({station_selection2})'


