#!/bin/bash
# python /app/azcopy/file_mounter.py

# crontab -l > dailyforecastcron
# echo "0 12 * * * /bin/bash /app/azcopy/forecast-workflow.sh" >> dailyforecastcron
# crontab dailyforecastcron
# rm dailyforecastcron

/usr/bin/supervisord
