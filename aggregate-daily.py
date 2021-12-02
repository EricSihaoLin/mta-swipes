import os
import re
import csv
from sqlalchemy import create_engine, insert, table, column, update
from sqlalchemy.schema import CreateTable
import datetime

daily=create_engine("sqlite:///mta-daily.db")
aggregate=create_engine("sqlite:///mta-aggregate.db")

aggregate.execute('''
CREATE TABLE IF NOT EXISTS station_data (
	id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  lon REAL NOT NULL,
  lat REAL NOT NULL,
  type TEXT NOT NULL
)
''')

station_table = table("station_data",
  column("id"),
  column("name"),
  column("lon"),
  column("lat"),
  column("type")
)

with open('turnstile.csv') as file:
  data = csv.DictReader(file)
  for row in data:
    id = row['id']
    name = row['name']
    lon = row['lon']
    lat = row['lat']
    t = row['type']
    stmt = (
      insert(station_table).
      values(id=id, name=name, lon=lon, lat=lat, type=t)
    )
    aggregate.execute(stmt)

print('created station_data in daily db')

r_set=daily.execute('select * from station_data ')

stations = []
for row in r_set:
  stations.append(row['id'])

rows_added = 0

aggregate.execute(f'''
CREATE TABLE IF NOT EXISTS daily_count (
  station_id INTEGER NOT NULL,
  date TEXT NOT NULL,
  graveyard_entries INTEGER DEFAULT -1,
  morning_entries INTEGER DEFAULT -1,
  afternoon_entries INTEGER DEFAULT -1,
  night_entries INTEGER DEFAULT -1,
  graveyard_exits INTEGER DEFAULT -1,
  morning_exits INTEGER DEFAULT -1,
  afternoon_exits INTEGER DEFAULT -1,
  night_exits INTEGER DEFAULT -1,
  PRIMARY KEY (station_id, date)
)
''')

# prepare to update table
data_table = table('daily_count',
  column("station_id"),
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

# iterating through all the stations
for id in stations:
  print(f'processing station id {id}')
  single_station = daily.execute(f'select * from count_{id}').fetchall()
  for row in single_station:
    stmt = (
      insert(data_table).
      values(
        station_id=id,
        date=row['date'],
        graveyard_entries=row['graveyard_entries'],
        morning_entries=row['morning_entries'],
        afternoon_entries=row['afternoon_entries'],
        night_entries=row['night_entries'],
        graveyard_exits=row['graveyard_exits'],
        morning_exits=row['morning_exits'],
        afternoon_exits=row['afternoon_exits'],
        night_exits=row['night_exits']
      )
    )
    aggregate.execute(stmt)
    rows_added += 1
    if rows_added % 500 == 0:
      print(f'{rows_added} have been inserted to the DB')
