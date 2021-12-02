import os
import re
import csv
from sqlalchemy import create_engine, insert, table, column, update
from sqlalchemy.schema import CreateTable
import datetime

# helper function to find closest
def closest(lst, K):
  return lst[min(range(len(lst)), key = lambda i: abs(lst[i]-K))]

mtadb=create_engine("sqlite:///mta.db")
daily=create_engine("sqlite:///mta-daily.db")

r_set=daily.execute('select * from station_data ')

stations = []
fixing = {}
for row in r_set:
  stations.append(row['id'])

# iterating through all the stations
for id in stations:
  print(f'processing station id {id}')
  # check if there are any counts that are abnormal
  abnormal = daily.execute(f'''select * from count_{id} where 
    graveyard_entries >= 50000 or graveyard_entries < -1 or
    morning_entries >= 50000 or morning_entries < -1 or
    afternoon_entries >= 50000 or afternoon_entries < -1 or
    night_entries >= 50000 or night_entries < -1 or
    graveyard_exits >= 50000 or graveyard_exits < -1 or
    morning_exits >= 50000 or morning_exits < -1 or
    afternoon_exits >= 50000 or afternoon_exits < -1 or
    night_exits >= 50000 or night_exits < -1
  ''')
  for ar in abnormal:
    if id not in fixing:
      fixing[id] = []
    fixing[id].append(ar['date'])

# after finding all of the abnormal data
for id, dates in fixing.items():
  for date in dates:
    print(f'fixing data for station {id} on {date}')
    start = datetime.datetime.strptime(date, "%Y/%m/%d")
    end = start + datetime.timedelta(days=1)
    day_start_unix = int(start.timestamp()) - 7200
    day_end_unix = int(end.timestamp()) + 7200
    result = mtadb.execute(f'select * from data_{start.year} where unix_timestamp >= {day_start_unix} and unix_timestamp <= {day_end_unix} and station_id = {id}')
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
    
    # once data has been loaded
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
        am0 = start.timestamp()
        am6 = am0 + 21600
        noon = am6 + 21600
        pm6 = noon + 21600
        pm12 = pm6 + 21600

        am0 = closest(timestamps, am0)
        am6 = closest(timestamps, am6)
        noon = closest(timestamps, noon)
        pm6 = closest(timestamps, pm6)
        pm12 = closest(timestamps, pm12)

        graveyard_entries_temp = tdata[td][am6]['entries'] - tdata[td][am0]['entries']
        morning_entries_temp = tdata[td][noon]['entries'] - tdata[td][am6]['entries']
        afternoon_entries_temp = tdata[td][pm6]['entries'] - tdata[td][noon]['entries']
        night_entries_temp = tdata[td][pm12]['entries'] - tdata[td][pm6]['entries']

        graveyard_exits_temp = tdata[td][am6]['exits'] - tdata[td][am0]['exits']
        morning_exits_temp = tdata[td][noon]['exits'] - tdata[td][am6]['exits']
        afternoon_exits_temp = tdata[td][pm6]['exits'] - tdata[td][noon]['exits']
        night_exits_temp = tdata[td][pm12]['exits'] - tdata[td][pm6]['exits']

        graveyard_entries = graveyard_entries + (graveyard_entries_temp if graveyard_entries_temp > 0 and graveyard_entries_temp < 50000 else 0)
        graveyard_exits = graveyard_exits + (graveyard_exits_temp if graveyard_exits_temp > 0 and graveyard_exits_temp < 50000 else 0)
        morning_entries = morning_entries + (morning_entries_temp if morning_entries_temp > 0 and morning_entries_temp < 50000 else 0)
        morning_exits = morning_exits + (morning_exits_temp if morning_exits_temp > 0 and morning_exits_temp < 50000 else 0)
        afternoon_entries = afternoon_entries + (afternoon_entries_temp if afternoon_entries_temp > 0 and afternoon_entries_temp < 50000 else 0)
        afternoon_exits = afternoon_exits + (afternoon_exits_temp if afternoon_exits_temp > 0 and afternoon_exits_temp < 50000 else 0)
        night_entries = night_entries + (night_entries_temp if night_entries_temp > 0 and night_entries_temp < 50000 else 0)
        night_exits = night_exits + (night_exits_temp if night_exits_temp > 0 and night_exits_temp < 50000 else 0)
    else:
      graveyard_entries = -1
      morning_entries = -1
      afternoon_entries = -1
      night_entries = -1
      graveyard_exits = -1
      morning_exits = -1
      afternoon_exits = -1
      night_exits = -1

    # prepare to update table
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
      update(data_table).
      where(data_table.c.date == date).
      values(
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
    print(f'Fixed entry for station {id} with g+: {graveyard_entries}, m+: {morning_entries}, an+: {afternoon_entries}, n+: {night_entries}, g-: {graveyard_exits}, m-: {morning_exits}, an-: {afternoon_exits}, n-: {night_exits}')

  