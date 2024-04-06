#!/bin/bash

uwsgi --master --virtualenv="/opt/conda/envs/app-env" --http 0.0.0.0:80 -b 32768 --die-on-term --enable-threads --log-date="%Y-%m-%d %H:%M:%S" --logformat-strftime --logto="/var/log/uwsgi/api.log" --processes=8 --wsgi-file="/app/app.py" --callable app 
