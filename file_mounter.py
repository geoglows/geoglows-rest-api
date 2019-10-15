# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
import json
import subprocess
import os
import sys


def execute_bash(bash_command):
    print("Executing command: " + str(bash_command))
    process = subprocess.Popen(bash_command, shell=True)
    output, error = process.communicate()
    print("output: " + str(output))
    print("error: " + str(error))


with open('/app/azcopy/file_mount.json') as f:
    try:
        file_config = json.load(f)

        for cfg in file_config:
            resource_dir = os.path.basename(cfg["mappedDirectory"])

            copy_cmd = f'/app/azcopy/azcopy cp "https://{cfg["accountName"]}.file.core.windows.net/' \
                f'{cfg["volumeName"]}/*?sv=2018-03-28&ss=f&srt=sco&sp=rl&se=2024-10-05T07:43:26Z&' \
                f'st=2019-10-04T23:43:26Z&spr=https&sig={cfg["sasSig"]}" "{cfg["mappedDirectory"]}" --recursive'

            execute_bash(copy_cmd)

        resume_failed = 'while read job; do  while true; ' \
                        'do if [[ -n "$(/app/azcopy/azcopy jobs show ${job} --with-status=Failed | grep Failed)" ]]; ' \
                        'then /app/azcopy/azcopy jobs resume ${job} --source-sas="sv=2018-03-28&ss=f&srt=sco&sp=rl&' \
                        f'se=2024-10-05T07:43:26Z&st=2019-10-04T23:43:26Z&spr=https&sig={file_config[0]["sasSig"]}"; ' \
                        'else break; fi; done; done < <(/app/azcopy/azcopy jobs list | grep JobId | awk "{print $2}")'

        execute_bash(resume_failed)

    except Exception:
        print("Unexpected error during file mounting:", str(sys.exc_info()[0]))
        raise
