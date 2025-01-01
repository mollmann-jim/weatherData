#!/usr/bin/env python3
import datetime as dt
import sqlite3
from dateutil.tz import tz
from sys import path
path.append('/home/jim/tools/')
from shared import getTimeInterval

DBname = '/home/jim/tools/weatherData/weather.sql'
table = 'weather'

def adapt_datetime(dt):
    return dt.isoformat(sep=' ')

def convert_datetime(val):
    return dt.datetime.fromisoformat(val).replace('T', ' ')

def fmtLine(tag, row):
    fmts = [' {:>5d}',  ' {:>5d}',  ' {:>5.1f}',  ' {:>5d}',  ' {:>5d}',
            ' {:>5.1f}',  ' {:>5.1f}',  ' {:>6.2f}']
    fmts = [' {:>5.1f}',  ' {:>5.1f}',  ' {:>5.1f}',  ' {:>5.1f}',  ' {:>5.1f}',
            ' {:>5.1f}',  ' {:>5.1f}',  ' {:>6.2f}']
    keys = ['minT', 'maxT', 'avgT', 'minD', 'maxD', 'avgD', 'avgW', 'rain']
    line = '{:>10s}'.format(tag)
    if row['minT']:
        for (fmt, key) in zip(fmts, keys):
            if row[key] is not None:
                line += fmt.format(row[key]).replace(' 0.00','    0')
            else:
                fw = ''.join([i for i in fmt if i.isdigit() or i == '.']).split('.')[0]
                line += ' {s:>{w}s}'.format(s = 'na', w = fw)
    else:
        line += ' (none)'
    return line

def printHeader():
    # period min/max/avg(temp) min/max/avg(dew) avg(wind) sum(precip)
    #      2020/07/02 mmmmm MMMMM aaaaa mmmmm MMMMM aaaaa wwwww rrrrr
    print('')
    print('             Min   Max   Avg   Min   Max   Avg   Avg  Total')
    print('            Temp  Temp  Temp DewPt DewPt DewPt  Wind   Rain')

def getYears(c, site):
    select_min_yr = 'SELECT min(timestamp) AS min FROM ' + site + ';'
    c.execute(select_min_yr)
    min = c.fetchone()
    #first = dt.datetime.strptime(min['min'].replace('T', ' '), '%Y-%m-%d %H:%M:%S%z')
    first = dt.datetime.fromisoformat(min['min'])
    select_max_yr = 'SELECT max(timestamp) AS max FROM ' + site + ';'
    c.execute(select_max_yr)
    max = c.fetchone()
    #last = dt.datetime.strptime(max['max'].replace('T', ' '), '%Y-%m-%d %H:%M:%S%z')
    last = dt.datetime.fromisoformat(max['max'])
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

def printTitle(c, site):
    lineLen = 59
    lineLenSpc = lineLen - 2 # spaces around name
    select = 'SELECT name from loc2name WHERE id = ?;'
    c.execute(select, (site,))
    result = c.fetchall()
    for record in result:
        siteName = record['name']
        dashCnt = int((lineLenSpc - len(siteName)) / 2)
    print('')
    print('-'*lineLen)
    print(' '.join(['-'*dashCnt, siteName, '-'*dashCnt]))
    print('-'*lineLen)


            
def makeReport(c, site):
    first, last = getYears(c, site)
    printTitle(c, site)
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
    sqlite3.register_adapter(dt.datetime, adapt_datetime)
    sqlite3.register_converter("DATETIME", convert_datetime)
    db = sqlite3.connect(DBname, detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = sqlite3.Row
    c = db.cursor()
    #db.set_trace_callback(print)
    
    locations = ('RDU', 'CRE', 'MYR', 'HXD', 'LUK', 'CVG', 'JHW', 'HOG')
    for loc in locations:
        makeReport(c, loc)

if __name__ == '__main__':
  main()
