#!/bin/bash
#set -x
logDir="/home/jim/tools/weatherData/logs/"
log=$logDir/report.$(/bin/date +%F-%T | /bin/tr : .);
/home/jim/tools/weatherData/reportWeather.py > $log 2>&1
cat $log
# keep only the newest
REMOVE=$(ls -t $logDir/report* | sed 1,20d)
if [ -n "$REMOVE" ]; then
    /bin/rm $REMOVE
fi
me='jim.mollmann@gmail.com'
(echo -e "Subject: Weather Summary: $(date)\n"; cat $log) | sendmail -F $me $me

