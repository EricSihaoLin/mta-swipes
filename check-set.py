import os
import re
import csv
from sqlalchemy import create_engine, insert, table, column

mtadb=create_engine("sqlite:///mta.db")

r_set=mtadb.execute('select * from station_data ')

stations = {}
for row in r_set:
  name = row['name']
  stations[name] = row['id']

with open('lookup.csv') as lookup:
  lu = csv.DictReader(lookup)
  for row in lu:
    stations[row['key']] = int(row['id'])

filenames = []
for filename in os.listdir('mta_data/'):
  date = int(re.match('turnstile_([0-9]+).txt', filename)[1])
  if date < 170108 and date > 160101:
    filenames.append(filename)

filenames.sort(reverse=True)
station_name_set = set()
for filename in filenames:
  with open('mta_data/' + filename) as file:
    print("reading", filename)
    data = csv.DictReader(file)
    for row in data:
      station = row['STATION']
      line = row['LINENAME']
      name = station + '-' + line
      if name not in stations:
        station_name_set.add(name)

print(station_name_set)