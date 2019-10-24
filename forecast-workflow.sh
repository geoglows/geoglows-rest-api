#!/bin/bash

# variables
TODAY=$(date +"%Y%m%d")
DATE_LIMIT=$(date -d "$TODAY - 2 days" +"%Y%m%d")

ACCOUNT=$(grep "accountName" file_mount.json -m 1 | awk -F':' '{print substr($2, 1, length($2)-1)}' | tr -d \")
OUTPUT_DIRECTORY="/mnt/output/ecmwf"
SIG=$(grep "sasSig" file_mount.json -m 1 | awk -F':' '{print substr($2, 1, length($2)-1)}' | tr -d \")

while read watershed
do
  while read rawdate
  do
    # delete forecasts older than date limit
    if [[ "${rawdate:0:8}" < "$DATE_LIMIT" ]]
    then
      rm -r $OUTPUT_DIRECTORY/$watershed/$rawdate
    fi
  done < <(ls -d $OUTPUT_DIRECTORY/$watershed/*/ | xargs -n 1 basename)

  # copy new forecasts
  /app/azcopy/azcopy cp "https://$ACCOUNT.file.core.windows.net/output/$watershed/*?sv=2018-03-28&ss=f&srt=sco&sp=rl&se=2024-10-05T07:43:26Z&st=2019-10-04T23:43:26Z&spr=https&sig=$SIG" "$OUTPUT_DIRECTORY/$watershed/" --recursive

  while read job
  do
    # resume any failed job until completed
    while true
    do
      if [[ -n "$(/app/azcopy/azcopy jobs show ${job} --with-status=Failed | grep Failed)" ]]
      then
        /app/azcopy/azcopy jobs resume ${job} --source-sas="sv=2018-03-28&ss=f&srt=sco&sp=rl&se=2024-10-05T07:43:26Z&st=2019-10-04T23:43:26Z&spr=https&sig=$SIG"
      else
        break
      fi
    done

  done < <(/app/azcopy/azcopy jobs list | grep JobId | awk '{print $2}')

done < <(ls -d $OUTPUT_DIRECTORY/*/ | xargs -n 1 basename)
