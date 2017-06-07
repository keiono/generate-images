import subprocess
import glob
import os
import logging
import json
import requests

# Script to run Luigi task.


# Cleanup all of the old SVG files in the system
if not os.path.exists('results'):
    os.makedirs('results')
img_files = glob.glob('results/*.svg')
report_files = glob.glob('results/report*.txt')


to_be_removed = img_files + report_files

for fl in to_be_removed:
    logging.info('* Removing old file: ' + str(fl.title()))
    os.remove(fl)


# subprocess.call(['/app/api/run_luigi.sh'])
#
# os.remove('id_list.json')
#
# try:
#     with open('report.txt') as report:
#         data = json.load(report)
# except FileNotFoundError:
#     return {"message": "Failed to run the workflow."}, 500
#
# logging.warn("Final result================")
# logging.warn(data)
#
# result_msg = {"image_urls": data}
#
# try:
#     os.remove('report.txt')
# except FileNotFoundError:
#     pass
#
# return result_msg
