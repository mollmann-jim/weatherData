#!/usr/bin/env python3
import datetime as dt
import sqlite3
from dateutil.tz import tz
from sys import path
path.append('/home/jim/tools/')
from shared import getTimeInterval

DBname = '/home/jim/tools/weatherData/weather.sql'
table = 'weather'

def fmtLine(tag, row):
    line = tag + ': (none)'
    if row['minT']:
        period =  '{:>10s}'.format(tag)
        minT   = ' {:>5d}'.format(row['minT'])
        maxT   = ' {:>5d}'.format(row['maxT'])
        avgT   = ' {:>5.1f}'.format(row['avgT'])
        minD   = ' {:>5d}'.format(row['minD'])
        maxD   = ' {:>5d}'.format(row['maxD'])
        avgD   = ' {:>5.1f}'.format(row['avgD'])
        wind   = ' {:>5.1f}'.format(row['avgW'])
        rain   = ' {:>5.2f}'.format(row['rain']).replace(' 0.00','    0')
        line = period + minT + maxT + avgT + minD + maxD + avgD + wind + rain
    return line
                                

def printHeader():
    # period min/max/avg(temp) min/max/avg(dew) avg(wind) sum(precip)
    #      2020/07/02 mmmmm MMMMM aaaaa mmmmm MMMMM aaaaa wwwww rrrrr
    print('')
    print('             Min   Max   Avg   Min   Max   Avg   Avg Total')
    print('            Temp  Temp  Temp DewPt DewPt DewPt  Wind  Rain')

def getYears(c, site):
    select_min_yr = 'SELECT min(timestamp) AS min FROM ' + site + ';'
    c.execute(select_min_yr)
    min = c.fetchone()
    first = dt.datetime.strptime(min['min'], '%Y-%m-%d %H:%M:%S%z')
    select_max_yr = 'SELECT max(timestamp) AS max FROM ' + site + ';'
    c.execute(select_max_yr)
    max = c.fetchone()
    last = dt.datetime.strptime(max['max'], '%Y-%m-%d %H:%M:%S%z')
    return first, last

def makeSection(c, site, title, byDay = False, byMonth = False, year = None):
    start, end, name = getTimeInterval.getPeriod(title, year = year)
    selectFields = 'SELECT substr(timestamp,1,10) as date, ' +\
        'MIN(temperature) AS minT, MAX(temperature) AS maxT, AVG(temperature) AS avgT, ' +\
        'MIN(dewpoint) AS minD, MAX(dewpoint) AS maxD, AVG(dewpoint) AS avgD, ' +\
        'AVG(wind) AS avgW, TOTAL(precipitation1hr) AS rain ' +\
        'FROM ' + site + ' WHERE timestamp >= ? AND timestamp <= ?'
    select = selectFields + ' ;'
    # sqlite date(timestamp) returns the UTC date
    #selectByDay = selectFields + ' GROUP BY date(timestamp) ORDER BY date(timestamp) DESC;'
    selectByDay   = selectFields + ' GROUP BY substr(timestamp,1,10) ORDER BY timestamp DESC;'
    selectByMonth = selectFields + ' GROUP BY substr(timestamp,1, 7) ORDER BY timestamp DESC;'
    #print(title, start, end)
    if byDay:
        c.execute(selectByDay, (start, end))
    elif byMonth:
        c.execute(selectByMonth, (start, end))
    else:
        c.execute(select, (start, end))
    result = c.fetchall()
    #printHeader()
    for record in result:
        if byDay:
            print(fmtLine(record['date'], record))
        elif byMonth:
            print(fmtLine(record['date'][0:7], record))
        else:
            print(fmtLine(name, record))

def makeReport(c, site):
    first, last = getYears(c, site)
    print('---------------------------', site, '----------------------------')
    printHeader()
    makeSection(c, site, 'Today')
    makeSection(c, site, 'Prev7days', byDay = True)

    printHeader()
    for period in ['This Week', 'Last Week', 'This Month', 'Last Month']:
        makeSection(c, site,  period)
    for period in ['This Month', 'Last Month']:
        printHeader()
        for year in range(last.year, first.year - 1, -1):
            makeSection(c, site , period, year = year)
    printHeader()
    makeSection(c, site, 'YearByMonth', byMonth = True)
    makeSection(c, site, 'LastYear')
    printHeader()
    for year in range(last.year, first.year - 1, -1):
        makeSection(c, site, 'Year', year = year)
    print('')
    makeSection(c, site,  'All')

def main():
    db = sqlite3.connect(DBname)
    db.row_factory = sqlite3.Row
    c = db.cursor()
    #db.set_trace_callback(print)

    makeReport(c, 'RDU')
    makeReport(c, 'MYR')

if __name__ == '__main__':
  main()
