#!/usr/bin/env python3
from html.parser import HTMLParser
import requests
import datetime as dt
import sqlite3
from dateutil.tz import tz


RDU='https://w1.weather.gov/data/obhistory/KRDU.html'
MYR='https://w1.weather.gov/data/obhistory/KMYR.html'
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
        winds = data['wind'].split(' ')
        DBdata['gust'] = None
        DBdata['winddirection'] = winds[0]
        if winds[0] == 'Calm':
            DBdata['wind'] = 0
        elif winds[0] == 'NA':
            DBdata['wind'] = 0
        else:
            if len(winds) < 2:
                print(winds)
                print(data)
            DBdata['wind'] = winds[1]
        if winds[0] == 'G':
            DBdata['gust'] = winds[3]
        DBdata['humidity'] = DBdata['humidity'].replace('%', '')
        if DBdata['windchill'] == 'NA' : DBdata['windchill'] = None
        if DBdata['heatindex'] == 'NA' : DBdata['heatindex'] = None
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
                pass
            self.myInteresting = False
        elif tag == 'td':
            self.tdOpen = False
            if self.myInteresting:
                self.line.append(self.item)
        pass

    def handle_data(self, data):
        if self.myInteresting:
            if self.tdOpen:
                self.item = data
        pass

RDUparser = MyHTMLParser('RDU')

r = requests.get(RDU)
data = r.text
lines = data.split('\r')
lines.pop(0)
data = ''.join(lines)
data = data.replace('\n','')
#print(data)
RDUparser.feed(data)

MYRparser = MyHTMLParser('MYR')

r = requests.get(MYR)
data = r.text
lines = data.split('\r')
lines.pop(0)
data = ''.join(lines)
data = data.replace('\n','')
#print(data)
MYRparser.feed(data)
