#!/bin/bash
#set -x
logDir="$HOME/tools/weatherData/logs/"
log=$logDir/report.$(/bin/date +%F-%T | /usr/bin/tr : .);
reportDir="$HOME/SynologyDrive/Reports.Daily/"
if [[ "$HOSTNAME" != "jim4" ]]; then
    newAge=77
    updated=$(find $HOME/SynologyDrive/Reports.Daily/ -name Weather.txt -mmin -$newAge | wc -l)
    if [[ $(($updated + 0 ))  > 0 ]]; then
	#echo already run
	exit 0
    fi
fi
echo -e "--------- $HOSTNAME --------- $(date) ----------\n" > $log
$HOME/tools/weatherData/reportWeather.py >> $log 2>&1
cp -p $log $reportDir/Weather.txt
cp -p $log $reportDir/All/Weather.$(basename -- "$log").txt
#cat $log
# keep only the newest
REMOVE=$(ls -t $logDir/report* | sed 1,20d)
if [ -n "$REMOVE" ]; then
    /bin/rm $REMOVE
fi
me='jim.mollmann@gmail.com'
####(echo -e "Subject: Weather Summary: $(date)\n"; cat $log) | sendmail -F $me $me

