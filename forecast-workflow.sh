#!/bin/bash

# create date variables
TODAY=$(date +"%Y%m%d")
DATE_LIMIT=$(date -d "$TODAY - 3 days" +"%Y%m%d")

# delete old forecasts
while read watershed
do
  while read rawdate
  do
    # delete forecasts older than date limit
    if [[ "${rawdate:0:8}" < "$DATE_LIMIT" ]]
    then
      rm -r /mnt/output/ecmwf/$watershed/$rawdate
    fi
  done < <(ls -d /mnt/output/ecmwf/$watershed/*/ | xargs -n 1 basename)
  /app/azcopy/azcopy cp "https://globalfloodsdiag360.file.core.windows.net/output/$watershed/*?sv=2018-03-28&ss=f&srt=sco&sp=rl&se=2024-10-05T07:43:26Z&st=2019-10-04T23:43:26Z&spr=https&sig=o8u%2BoutumDqKQL4A1I2Yft9T0S5M%2FJRBlMPuptfVehc%3D" "/mnt/output/ecmwf/$watershed/" --recursive
done < <(ls -d /mnt/output/ecmwf/*/ | xargs -n 1 basename)
