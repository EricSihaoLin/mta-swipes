import os
import re
import csv
from sqlalchemy import create_engine, insert, table, column
from sqlalchemy.schema import CreateTable
import datetime

START_DATE = '01/01/2016'
END_DATE = '11/26/2021'

# helper function to find closest
def closest(lst, K):
  return lst[min(range(len(lst)), key = lambda i: abs(lst[i]-K))]

mtadb=create_engine("sqlite:///mta.db")
daily=create_engine("sqlite:///mta-daily.db")
# daily.execute('''
# CREATE TABLE IF NOT EXISTS station_data (
# 	id INTEGER PRIMARY KEY,
#   name TEXT NOT NULL,
#   lon REAL NOT NULL,
#   lat REAL NOT NULL,
#   type TEXT NOT NULL
# )
# ''')

# station_table = table("station_data",
#   column("id"),
#   column("name"),
#   column("lon"),
#   column("lat"),
#   column("type")
# )

# with open('turnstile.csv') as file:
#   data = csv.DictReader(file)
#   for row in data:
#     id = row['id']
#     name = row['name']
#     lon = row['lon']
#     lat = row['lat']
#     t = row['type']
#     stmt = (
#       insert(station_table).
#       values(id=id, name=name, lon=lon, lat=lat, type=t)
#     )
#     daily.execute(stmt)

# print('created station_data in daily db')

r_set=daily.execute('select * from station_data ')

#daily count is split into 4 times
# graveyard - 12 AM to 6 AM (not guaranteed to be within that day)
# morning - 6 AM to 12 PM
# afternoon - 12 PM to 6 PM
# night - 6 PM to 11:59 PM (not guaranteed to be within that day)

stations = []
for row in r_set:
  stations.append(row['id'])

# for id in stations:
#   daily.execute(f'''
#   CREATE TABLE IF NOT EXISTS count_{id} (
#     date TEXT PRIMARY KEY,
#     graveyard_entries INTEGER DEFAULT -1,
#     morning_entries INTEGER DEFAULT -1,
#     afternoon_entries INTEGER DEFAULT -1,
#     night_entries INTEGER DEFAULT -1,
#     graveyard_exits INTEGER DEFAULT -1,
#     morning_exits INTEGER DEFAULT -1,
#     afternoon_exits INTEGER DEFAULT -1,
#     night_exits INTEGER DEFAULT -1
#   )
#   ''')

# keep track of how many rows added
rows_added = -1

date_start = datetime.datetime.strptime(START_DATE, "%m/%d/%Y")
date_end = datetime.datetime.strptime(END_DATE, "%m/%d/%Y")
current_date = date_start
while current_date <= date_end:
  next_day = current_date + datetime.timedelta(days=1)
  print(f'processing data for {current_date}')
  # get data for that day (with 2 hour grace period on each end)
  day_start_unix = int(current_date.timestamp()) - 7200
  day_end_unix = int(next_day.timestamp()) + 7200
  result = mtadb.execute(f'select * from data_{current_date.year} where unix_timestamp >= {day_start_unix} and unix_timestamp <= {day_end_unix}')
  # load data for that day into dict
  turnstiles = {}
  for row in result:
    station_id = row['station_id']
    turnstile_id = row['turnstile_id']
    timestamp = row['unix_timestamp']
    entries = row['entries']
    exits = row['exits']
    if station_id not in turnstiles:
      turnstiles[station_id] = {}
    if turnstile_id not in turnstiles[station_id]:
      turnstiles[station_id][turnstile_id] = {}
    if timestamp not in turnstiles[station_id][turnstile_id]:
      turnstiles[station_id][turnstile_id][timestamp] = {}
    turnstiles[station_id][turnstile_id][timestamp]['entries'] = entries
    turnstiles[station_id][turnstile_id][timestamp]['exits'] = exits
  
  # go through all stations
  for id in stations:
    # default counts
    graveyard_entries = 0
    morning_entries = 0
    afternoon_entries = 0
    night_entries = 0
    graveyard_exits = 0
    morning_exits = 0
    afternoon_exits = 0
    night_exits = 0
    # if id in turnstile data
    if id in turnstiles:
      tdata = turnstiles[id]
      # iterate through every single turnstile
      for td in tdata:
        timestamps = list(tdata[td].keys())
        # get key closest to midnight
        am0 = current_date.timestamp()
        am6 = am0 + 21600
        noon = am6 + 21600
        pm6 = noon + 21600
        pm12 = pm6 + 21600

        am0 = closest(timestamps, am0)
        am6 = closest(timestamps, am6)
        noon = closest(timestamps, noon)
        pm6 = closest(timestamps, pm6)
        pm12 = closest(timestamps, pm12)

        graveyard_entries += tdata[td][am6]['entries'] - tdata[td][am0]['entries']
        morning_entries += tdata[td][noon]['entries'] - tdata[td][am6]['entries']
        afternoon_entries += tdata[td][pm6]['entries'] - tdata[td][noon]['entries']
        night_entries += tdata[td][pm12]['entries'] - tdata[td][pm6]['entries']

        graveyard_exits += tdata[td][am6]['exits'] - tdata[td][am0]['exits']
        morning_exits += tdata[td][noon]['exits'] - tdata[td][am6]['exits']
        afternoon_exits += tdata[td][pm6]['exits'] - tdata[td][noon]['exits']
        night_exits += tdata[td][pm12]['exits'] - tdata[td][pm6]['exits']
    else:
      graveyard_entries = -1
      morning_entries = -1
      afternoon_entries = -1
      night_entries = -1
      graveyard_exits = -1
      morning_exits = -1
      afternoon_exits = -1
      night_exits = -1
        
    # insert final count into database
    data_table = table(f'count_{id}',
      column("date"),
      column("graveyard_entries"),
      column("morning_entries"),
      column("afternoon_entries"),
      column("night_entries"),
      column("graveyard_exits"),
      column("morning_exits"),
      column("afternoon_exits"),
      column("night_exits")
    )

    stmt = (
      insert(data_table).
      values(
        date=current_date.strftime('%Y/%m/%d'),
        graveyard_entries=graveyard_entries,
        morning_entries=morning_entries,
        afternoon_entries=afternoon_entries,
        night_entries=night_entries,
        graveyard_exits=graveyard_exits,
        morning_exits=morning_exits,
        afternoon_exits=afternoon_exits,
        night_exits=night_exits
      )
    )
    daily.execute(stmt)
    rows_added += 1
    if rows_added % 250 == 0:
      print(f'{rows_added + 1} have been inserted to the DB')
      print(f'Last added info was for station {id} with g+: {graveyard_entries}, m+: {morning_entries}, an+: {afternoon_entries}, n+: {night_entries}, g-: {graveyard_exits}, m-: {morning_exits}, an-: {afternoon_exits}, n-: {night_exits}')
  # increment day
  current_date = next_day