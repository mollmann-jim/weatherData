#!/usr/bin/env python3
from html.parser import HTMLParser
import requests
import datetime as dt
import sqlite3
from dateutil.tz import tz


RDU='https://w1.weather.gov/data/obhistory/KRDU.html'
CRE='https://w1.weather.gov/data/obhistory/KCRE.html'
MYR='https://w1.weather.gov/data/obhistory/KMYR.html'
HXD='https://w1.weather.gov/data/obhistory/KHXD.html'
LUK='https://w1.weather.gov/data/obhistory/KLUK.html'
CVG='https://w1.weather.gov/data/obhistory/KCVG.html'
JHW='https://w1.weather.gov/data/obhistory/KJHW.html'
HOG='https://w1.weather.gov/data/obhistory/PHOG.html'
locations = ('RDU', 'CRE', 'MYR', 'HXD', 'LUK', 'CVG', 'JHW', 'HOG')
URLs      = ( RDU,   CRE ,  MYR ,  HXD ,  LUK ,  CVG ,  JHW ,  HOG )
names     = ('Raleigh-Durham International Airport',
             'North Myrtle Beach Grand Strand Airport',
             'Myrtle Beach International Airport',
             'Hilton Head Island, Hilton Head Airport',
             'Cincinnati, Cincinnati Municipal Airport Lunken Field',
             'Cincinnati/Northern Kentucky International Airport',
             'Jamestown, Chautauqua County/Jamestown Airport',
             'Kahului, Kahului Airport')
DBname = '/home/jim/tools/weatherData/weather.sql'

class DB:
    def __init__(self, site):
        self.db = sqlite3.connect(DBname)
        self.db.row_factory = sqlite3.Row
        self.c = self.db.cursor()
        self.table = site
        
        ##self.c.execute('DROP TABLE IF EXISTS ' + self.table)

        create = 'CREATE TABLE IF NOT EXISTS ' + self.table + '(\n' +\
            ' timestamp        DATETIME DEFAULT CURRENT_TIMESTAMP PRIMARY KEY,\n' +\
            ' wind             INTEGER, \n' +\
            ' winddirection    TEXT, \n' +\
            ' gust             INTEGER, \n' +\
            ' visibility       REAL, \n' +\
            ' sky              TEXT, \n' +\
            ' clouds           TEXT, \n' +\
            ' temperature      INTEGER, \n' +\
            ' dewpoint         INTEGER, \n' +\
            ' temp6hrMax       INTEGER DEFAULT NULL, \n' +\
            ' temp6hrMin       INTEGER DEFAULT NULL, \n' +\
            ' humidity         INTEGER, \n' +\
            ' windchill        INTEGER DEFAULT NULL, \n' +\
            ' heatindex        INTEGER DEFAULT NULL, \n' +\
            ' altimiterIn      REAL, \n' +\
            ' altimiterMb      REAL, \n' +\
            ' precipitation1hr REAL DEFAULT NULL, \n' +\
            ' precipitation3hr REAL DEFAULT NULL, \n' +\
            ' precipitation6hr REAL DEFAULT NULL  \n' +\
            ' )'
        self.c.execute(create)
        self.db.commit()

    def Insert(self, data):
        DBdata = {}
        insert = 'INSERT OR REPLACE INTO ' + self.table + ' (timestamp, wind, winddirection, ' +\
            'gust, visibility, sky, clouds, temperature, dewpoint, temp6hrMax, ' +\
            'temp6hrMin, humidity, windchill, heatindex, altimiterIn, altimiterMb, ' +\
            'precipitation1hr, precipitation3hr, precipitation6hr )' +\
            'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'
        columns = ['timestamp', 'wind', 'winddirection',  \
            'gust', 'visibility', 'sky', 'clouds', 'temperature', 'dewpoint', 'temp6hrMax',  \
            'temp6hrMin', 'humidity', 'windchill', 'heatindex', 'altimiterIn', 'altimiterMb', \
            'precipitation1hr', 'precipitation3hr', 'precipitation6hr']
        for key in data:
            DBdata[key] = data[key]
            if DBdata[key] == 'NA':
                DBdata[key] = None
        winds = data['wind'].split(' ')
        DBdata['gust'] = None
        DBdata['winddirection'] = winds[0]
        if winds[0] == 'Calm':
            DBdata['wind'] = 0
        else:
            if len(winds) < 2:
                print('len(winds)', len(winds))
                print(data['wind'])
                print(winds)
                print(data)
                DBdata['wind'] = None
            else:
                DBdata['wind'] = winds[1]
        if winds[0] == 'G':
            DBdata['gust'] = winds[3]
        if DBdata['humidity'] is not None:
            DBdata['humidity'] = DBdata['humidity'].replace('%', '')
        now = dt.datetime.now()
        hh, mm = DBdata['time'].split(':')
        year = now.year
        month = now.month
        day = int(DBdata['day'])
        if now.day < day:
            month = month - 1
            if month < 1:
                month = 12
                year -= 1
        timestamp = dt.datetime(year, month, day, int(hh), int(mm), \
                                          tzinfo = tz.gettz('America/New_York'))
        DBdata['timestamp'] = timestamp
        values = []
        for col in columns:
            if DBdata[col] == '' : DBdata[col] = None
            values.append(DBdata[col])
        self.c.execute(insert, values)
        self.db.commit()
        

class MyHTMLParser(HTMLParser, DB):
    def __init__(self, site):
        self.i = 0
        HTMLParser.__init__(self)
        DB.__init__(self, site)
        self.line = []
        self.item = ''
        self.myInteresting = False
        self.tdOpen = False
        self.Cols = ['day', 'time', 'wind', 'visibility', 'sky', 'clouds', 'temperature', 'dewpoint', \
                     'temp6hrMax', 'temp6hrMin', 'humidity', 'windchill', 'heatindex', 'altimiterIn', \
                     'altimiterMb', 'precipitation1hr', 'precipitation3hr', 'precipitation6hr']
        
    def handle_starttag(self, tag, attrs):
        if tag == 'tr':
            attr = {}
            for name, value in attrs:
                attr[name] = value
            if attr.get('valign', 'no') == 'top' and attr.get('align', 'no') == 'center':
                self.i += 1
                self.myInteresting = True
                self.line = []
        elif tag == 'td':
            self.item = ''
            self.tdOpen = True

    def handle_endtag(self, tag):
        if tag == 'tr':
            if self.myInteresting:
                #print('*** DATA ***', self.line)
                myInfo = dict(zip(self.Cols, self.line))
                #print(myInfo)
                self.Insert(myInfo)
            self.myInteresting = False
        elif tag == 'td':
            self.tdOpen = False
            if self.myInteresting:
                self.line.append(self.item)

    def handle_data(self, data):
        if self.myInteresting:
            if self.tdOpen:
                self.item = data

class LocationName:
    def __init__(self, locID, name):
        self.db = sqlite3.connect(DBname)
        self.db.row_factory = sqlite3.Row
        self.c = self.db.cursor()
        create = 'CREATE TABLE IF NOT EXISTS loc2name (\n' +\
            ' id        TEXT PRIMARY KEY, \n' +\
            ' name      TEXT )'
        self.c.execute(create)
        self.db.commit()
        insert = 'INSERT OR REPLACE INTO loc2name ( id, name) VALUES(?, ?);'
        self.c.execute(insert,(locID, name) )
        self.db.commit()
        
for (loc, url, name) in zip(locations, URLs, names):
    print(loc)
    thisParser = MyHTMLParser(loc)

    r = requests.get(url)
    data = r.text
    lines = data.split('\r')
    lines.pop(0)
    data = ''.join(lines)
    data = data.replace('\n','')
    #print(data)
    thisParser.feed(data)
    thisLocName = LocationName(loc, name)
    
