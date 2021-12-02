import os
import re
import csv
from sqlalchemy import create_engine, insert, table, column
import datetime

mtadb=create_engine("sqlite:///mta.db")
mtadb.execute('''
  CREATE TABLE IF NOT EXISTS data_2017 ( 
  station_id INTEGER NOT NULL,
  turnstile_id TEXT NOT NULL,
  unix_timestamp INTEGER NOT NULL,
  entries INTEGER NOT NULL,
  exits INTEGER NOT NULL,
  FOREIGN KEY(station_id) REFERENCES station_data(id)
)
''')

r_set=mtadb.execute('select * from station_data ')

stations = {}
for row in r_set:
  name = row['name']
  stations[name] = row['id']

with open('lookup.csv') as lookup:
  lu = csv.DictReader(lookup)
  for row in lu:
    stations[row['key']] = int(row['id'])

data_table = table("data_2017",
  column("station_id"),
  column("turnstile_id"),
  column("unix_timestamp"),
  column("entries"),
  column("exits")
)

filenames = []
for filename in os.listdir('mta_data/'):
  date = int(re.match('turnstile_([0-9]+).txt', filename)[1])
  if date < 180107 and date > 170101:
    filenames.append(filename)

filenames.sort(reverse=True)

rows = 0

for filename in filenames:
  with open('mta_data/' + filename) as file:
    print("reading", filename)
    data = csv.DictReader(file)
    header = []
    for head in data.fieldnames:
      header.append(head.strip())
    data.fieldnames = header
    for row in data:
      station = row['STATION']
      line = row['LINENAME']
      name = station + '-' + line
      timestamp = row['DATE'] + ' ' + row['TIME']
      turnstile_id = row['SCP']
      time = datetime.datetime.strptime(timestamp, "%m/%d/%Y %H:%M:%S")
      unix = int(time.timestamp())
      entries = row['ENTRIES']
      exits = int(row['EXITS'])
      stmt = (
        insert(data_table).
        values(station_id=stations[name], unix_timestamp=unix, turnstile_id=turnstile_id, entries=entries, exits=exits)
      )
      mtadb.execute(stmt)
      rows += 1
      if rows % 1000 == 0:
        print(f'{rows} have been inserted to the DB')
        