# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
import json
import subprocess
import os, shutil
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

            os.makedirs(resource_dir + "/" + cfg["region"] + "/" + cfg["timestamp"])
            shutil.chown(resource_dir, "root")
            os.makedirs(cfg["mappedDirectory"] + "/" + cfg["region"] + "/" + cfg["timestamp"][0:-1])

            copy_cmd = "azcopy --quiet --source https://" + cfg["accountName"] + ".file.core.windows.net/" + resource_dir + "/" + cfg["region"] + "/" + cfg["timestamp"] + " --destination " + cfg["mappedDirectory"] + "/" + cfg["region"] + "/" + cfg["timestamp"][0:-1] + " --source-key " + cfg["accountKey"] + " --recursive --exclude-older --exclude-newer"

            execute_bash(copy_cmd)

    except:
        print("Unexpected error during blob mounting:", str(sys.exc_info()[0]))
        raise