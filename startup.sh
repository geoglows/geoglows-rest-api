#!/bin/bash
python /app/azcopy/file_mounter.py

service cron start
crontab -l > dailyforecastcron
echo "0 12 * * * /bin/bash /app/azcopy/forecast-workflow.sh" >> dailyforecastcron
crontab dailyforecastcron

/usr/bin/supervisord
