# executenotebook2.py
#!/usr/bin/python3
import json
import requests
import os
import sys
import getopt
import time

def main():
  shard = ''
  token = ''
  sparkversion = ''
  nodetypeid = ''
  numworkers = ''
  localpath = ''
  workspacepath = ''
  outfilepath = ''

  try:
    opts, args = getopt.getopt(sys.argv[1:], 'hs:t:v:n:m:l:w:o',
      ['shard=', 'token=', 'sparkversion=', 'nodetypeid=', 'numworkers=', 'localpath=', 'workspacepath=', 'outfilepath='])
  except getopt.GetoptError:
    print(
      'executenotebook2.py -s <shard> -t <token>  -v <sparkversion> -n <nodetypeid> -m <numworkers> -l <localpath> -w <workspacepath> -o <outfilepath>)')
    sys.exit(2)

  for opt, arg in opts:
    if opt == '-h':
      print(
        'executenotebook.py -s <shard> -t <token> -v <sparkversion> -n <nodetypeid> -m <numworkers> -l <localpath> -w <workspacepath> -o <outfilepath>')
      sys.exit()
    elif opt in ('-s', '--shard'):
        shard = arg
    elif opt in ('-t', '--token'):
        token = arg
    elif opt in ('-v', '--sparkversion'):
        sparkversion = arg
    elif opt in ('-n', '--nodetypeid'):
        nodetypeid = arg
    elif opt in ('-m', '--numworkers'):
        numworkers = arg
    elif opt in ('-l', '--localpath'):
        localpath = arg
    elif opt in ('-w', '--workspacepath'):
        workspacepath = arg
    elif opt in ('-o', '--outfilepath'):
        outfilepath = arg

  print('-s is ' + shard)
  print('-t is ' + token)
  print('-v is ' + sparkversion)
  print('-n is ' + nodetypeid)
  print('-m is ' + numworkers)
  print('-l is ' + localpath)
  print('-w is ' + workspacepath)
  print('-o is ' + outfilepath)

  # Generate the list of notebooks from walking the local path.
  notebooks = []
  for path, subdirs, files in os.walk(localpath):
    for name in files:
      fullpath = path + '/' + name
      # Remove the localpath to the repo but keep the workspace path.
      fullworkspacepath = workspacepath + path.replace(localpath, '')

      name, file_extension = os.path.splitext(fullpath)
      if file_extension.lower() in ['.scala', '.sql', '.r', '.py']:
        row = [fullpath, fullworkspacepath, 1]
        notebooks.append(row)


  # Run each notebook in the list.
  for notebook in notebooks:
    nameonly = os.path.basename(notebook[0])
    workspacepath = notebook[1]

    name, file_extension = os.path.splitext(nameonly)

    # workspacepath removes the extension, so now add it back.
    fullworkspacepath = workspacepath + '/' + name + file_extension

    print('Running job for: ' + fullworkspacepath)
    print('name:' + name)


    # delete existing job by name
    resp = requests.get(shard + '/api/2.0/jobs/list', auth=("token", token))
    listjson = resp.text
    
    d = json.loads(listjson)
    print(d)
    if 'jobs' in d:
      for job in d['jobs']:
        print(job['job_id'])
        print(job['settings']['name'])
        if name == job['settings']['name']:
            values = {"job_id": job['job_id']}
            print("delete ...")
            resp = requests.post(shard + '/api/2.0/jobs/delete',
                data=json.dumps(values), auth=("token", token))
            deljson = resp.text
            print("deljson: " + deljson)
    else:
       print("No jobs found.")

    
    # create a new job
    values = {'name': name,  
              "new_cluster": {
                "spark_version": sparkversion,
                "node_type_id": nodetypeid,
                "num_workers":  numworkers},
              'timeout_seconds': 3600, 
              'notebook_task': {'notebook_path': fullworkspacepath}}

    resp = requests.post(shard + '/api/2.0/jobs/create',
      data=json.dumps(values), auth=("token", token))
    createjson = resp.text
    print("createjson: " + createjson)
    d = json.loads(createjson)
  
    # print(d['job_id'])

    # run now
    values = {"job_id": d['job_id']}
    resp = requests.post(shard + '/api/2.0/jobs/run-now',
      data=json.dumps(values), auth=("token", token))
    runjson = resp.text
    print("runjson: " + runjson)
    d = json.loads(runjson)
    runid = d['run_id']
    
    
    
    i=0
    waiting = True
    while waiting:
      # sleep for 30 sec and 15 min in total for timeout
      time.sleep(30)
      jobresp = requests.get(shard + '/api/2.0/jobs/runs/get?run_id='+str(runid),
          auth=("token", token))
      
      jobjson = jobresp.text
      #print("jobjson: " + jobjson)
      j = json.loads(jobjson)
      #print(j)
      current_state = j['state']['life_cycle_state']
      print("i: %d" % i)
      print("state:" + current_state)
      runid = j['run_id']
      if current_state in ['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED'] or i >= 30:
        break
      i=i+1

    if outfilepath != '':
      file = open(outfilepath + '/' +  str(runid) + '.json', 'w')
      file.write(json.dumps(j))
      file.close()
    
if __name__ == '__main__':
  main()