#!/bin/bash

uwsgi --master --uid nobody --gid nogroup --max-requests 1000 --max-worker-lifetime 1800 --worker-reload-mercy 30 --virtualenv="/opt/conda/envs/app-env" --http 0.0.0.0:80 -b 32768 --die-on-term --enable-threads --log-date="%Y-%m-%d %H:%M:%S" --logformat-strftime --processes=8 --wsgi-file="/geoglows/app/app.py" --callable app 
