from azure.devops.connection import Connection
from azure.devops.v7_1.test.models import RunUpdateModel
from msrest.authentication import BasicAuthentication

import sys
from datetime import timedelta, datetime
import json
import re
import requests
import getopt
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_organization_connection(organization_url, personal_access_token):
  credentials = BasicAuthentication('', personal_access_token)
  connection = Connection(base_url=organization_url, creds=credentials)
  return connection

def main():
  
  organization_url = ''
  pat = ''
  project_name = ''
  build_uri = ''
  md_path = ''

  try:
    opts, args = getopt.getopt(sys.argv[1:], 'o:a:p:b:m:',
      ['organization=', 'pat=' , 'project=', 'build='])
  except getopt.GetoptError:
    print(
      'adoclient.py -o <organization_url> -a <pat> -p <project_name> -b <build_uri>) -m <unravel_md>')
    sys.exit(2)

  for opt, arg in opts:
    if opt == '-h':
      print(
        'adoclient.py -o <organization_url> -a <pat> -p <project_name> -b <build_uri>) -m <unravel_md>')
      sys.exit()
    elif opt in ('-o', '--organization'):
        organization_url = arg
    elif opt in ('-a', '--pat'):
        pat = arg
    elif opt in ('-p', '--project'):
        project_name = arg
    elif opt in ('-b', '--build'):
        build_uri = arg
    elif opt in ('-m', '--markdown'):
        md_path = arg

  print('-o : ' + organization_url)
  print('-a : ' + pat)
  print('-p : ' + project_name)
  print('-b : ' + build_uri)
  print('-m : ' + md_path)

  
  # azure devops connection and test client
  connection = get_organization_connection(organization_url, pat)
  test_client =connection.clients.get_test_client()
  
  # get the list of test run based on the build
  test_runs = test_client.get_test_runs(project = project_name, build_uri = build_uri)
  run_id_list = []
  for run in test_runs:
    run_id_list.append(run.id)
  # get the latest test run
  run_id_list.sort()
  run_id = run_id_list[-1]
  
  
  isExist = os.path.exists(md_path)
  if isExist:
    comment_md = open(md_path, 'r').read()
    comment_rum = RunUpdateModel( comment = comment_md )
    test_client.update_test_run(run_update_model = comment_rum, project = project_name, run_id = run_id)
  else:
    print("No update as markdown path not found: " + md_path)

if __name__ == "__main__":
    main()