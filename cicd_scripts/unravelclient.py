import sys
import os
import glob
import requests
import json
import getopt
import time
import re
import markdown
from datetime import timedelta, datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_URL_KEY='com.unraveldata.api.url'
API_TOKEN_KEY='com.unraveldata.api.token'
pattern_as_text = r"https://adb-([0-9]+).([0-9]+).azuredatabricks.net/\?o=([0-9]+)#job/([0-9]+)/run/([0-9]+)"  


def search_apps(base_url, api_token, start_time, end_time, cluster_id):
    api_url = base_url + \
              '/api/v1/apps/unifiedsearch'
    kv_pairs = {"from":0,
                "appTypes":["spark"],
                "appStatus":["K","F","R","S","P","U","W"],
                "size":"1",
                "start_time":start_time,
                "end_time":end_time,
                "clusters": [ cluster_id ],
                }
    print("URL: " + api_url)
    print(kv_pairs)
    json_val  = post_api(api_url, api_token, kv_pairs)
    check_response_on_post(json_val)
    #print(json_val)
    return json_val

def search_analysis(base_url, api_token, clusterUId, id):
    api_url = base_url + '/api/v1/spark/' + clusterUId + '/' + id + '/analysis'
    print(api_url)
    json_val  = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val

def check_response_on_get(json_val):
    if 'message' in json_val :
        if json_val['message'] == 'INVALID_TOKEN' :
           raise ValueError('INVALID_TOKEN')

def check_response_on_post(json_val):
    if 'message' in json_val :
        if json_val['message'] == 'INVALID_TOKEN' :
           raise ValueError('INVALID_TOKEN')
    elif len(json_val) == 0:
        print(json_val)
        raise ValueError('Response is empty') 
    elif 'results' not in json_val:
        print(json_val)
        raise ValueError('KEY results NOT FOUND')


def post_api(api_url, api_token, kv_dict):
    response = requests.post(api_url,
                             data = json.dumps(kv_dict),
                             verify=False,
                             headers={'Authorization': api_token,
                                      'accept': 'application/json',
                                      'Content-Type': 'application/json'})
    json_obj = json.loads(response.content)
    return json_obj

def get_api(api_url, api_token):
    response = requests.get(api_url,
                            verify=False,
                            headers={'Authorization': api_token})
    json_obj = json.loads(response.content)
    return json_obj


def get_job_runs_from_run_page_url(url):
  job_run = None
  matches = re.findall(pattern_as_text, url)
  if matches:
    for match in matches:
      workspace_id = match[2]
      job_id = match[3]
      run_id = match[4]
      job_run = {'workspace_id': workspace_id, 'job_id': job_id, 'run_id': run_id}
  else:
    print("no match")
  return job_run


def search_summary_by_globalsearchpattern(base_url, api_token, gsp):
    api_url = base_url + \
              '/api/v1/ds/api/v1/databricks/runs/' + gsp + '/tasks/summary'
    print("URL: " + api_url)
    json_val  = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val

def search_summary(base_url, api_token, clusterUId, id):
    api_url = base_url + "/api/v1/spark/" + clusterUId + "/" + id + "/appsummary"
    print("URL: " + api_url)
    json_val = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val

def fetch_app_summary(unravel_url, unravel_token, clusterUId, appId):
    app_summary_map = {}
    autoscale_dict = {}
    summary_dict = search_summary(unravel_url, unravel_token, clusterUId, appId)
    summary_dict = summary_dict["annotation"]
    url = '{}/#/app/application/spark?execId={}&clusterUid={}'.format(unravel_url,appId,clusterUId)
    app_summary_map["Spark App"] = '[{}]({})'.format(appId, url)
    app_summary_map["Cluster"] = clusterUId
    app_summary_map["Total cost"] = '${}'.format(summary_dict["cents"] + summary_dict["dbuCost"])
    runinfo = json.loads(summary_dict["runInfo"])
    app_summary_map["Executor Node Type"] = runinfo["node_type_id"]
    app_summary_map["Driver Node Type"] = runinfo["driver_node_type_id"]
    app_summary_map["Tags"] = runinfo["default_tags"]
    if 'custom_tags' in runinfo.keys():
        app_summary_map["Tags"] = {**app_summary_map["Tags"], **runinfo["default_tags"]}
    if "autoscale" in runinfo.keys():
        autoscale_dict["autoscale_min_workers"] = runinfo["autoscale"]["min_workers"]
        autoscale_dict["autoscale_max_workers"] = runinfo["autoscale"]["max_workers"]
        autoscale_dict["autoscale_target_workers"] = runinfo["autoscale"][
            "target_workers"
        ]
        app_summary_map['Autoscale'] = autoscale_dict
    else:
        app_summary_map['Autoscale'] = 'Autoscale is not enabled.'
    return app_summary_map

def create_comments_with_markdown(job_run_result_list):
    comments = ""
    if job_run_result_list:
        for r in job_run_result_list:
            #comments += "========\n"
            #comments += "<details>\n"
            # comments += "<img src='https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg' alt='Logo'>\n\n"
            #comments += "<summary> <img src='https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg' alt='Logo'> Job Id: {}, Run Id: {}</summary>\n\n".format(
            #    r["job_id"], r["run_id"]
            #)
            comments += "#### Workspace Id:" + r["workspace_id"] + "\n"
            comments += "#### Job Id:" + r["job_id"] + "\n"
            comments += "#### Run Id:" + r["run_id"] + "\n"
            #comments += "========\n"
            comments += "#### [{}]({})\n".format('Unravel url', r["unravel_url"])
            if r['app_summary']:
                # Get all unique keys from the dictionaries while preserving the order
                headers = []
                for key in r['app_summary'].keys():
                    if key not in headers:
                        headers.append(key)

                # Generate the header row
                header_row = "| " + " | ".join(headers) + " |"

                # Generate the separator row
                separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"

                # Generate the data rows
                data_rows = "\n".join(
                    [
                        "| " + " | ".join(str(r['app_summary'].get(h, "")) for h in headers)
                    ]
                )

                # Combine the header, separator, and data rows
                #comments += "========\n"
                comments += "# App Summary\n"
                #comments += "========\n"
                comments += header_row + "\n" + separator_row + "\n" + data_rows + "\n"
            if r["unravel_insights"]:
                #comments += "========\n"
                comments += "## Unravel Insights\n"
                for insight in r["unravel_insights"]:
                    categories = insight["categories"]
                    if categories:
                        for k in categories.keys():
                            instances = categories[k]["instances"]
                            if instances:
                                for i in instances:
                                    if i["key"].upper() != "SPARKAPPTIMEREPORT":
                                        comments += (
                                            "#### "
                                            + i["key"].upper()
                                            + ": "
                                            + i["title"]
                                            + "\n"
                                        )
                                        comments += "##### EVENT: " + i["events"] + "\n"
                                        comments += (
                                            "##### ACTIONS: " + i["actions"] + "\n"
                                        )
            #comments += "</details>\n\n"

    return comments

def main():
  unravel = ''
  token = ''
  localpath = ''
  infilepath = ''
  outfilepath = ''
  try:
    opts, args = getopt.getopt(sys.argv[1:], 'hu:t:l:i:o:',
      ['unravel=', 'token=', 'localpath=', 'infilepath=', 'outfilepath='])
  except getopt.GetoptError:
    print(
      'unravelnotebookruns.py -u <unravel> -t <token>  -l <localpath> -i <infilepath> -o <outfilepath>)')
    sys.exit(2)

  for opt, arg in opts:
    if opt == '-h':
      print(
        'unravelnotebookruns.py -u <unravel> -t <token>  -l <localpath>  -i <infilepath> -o <outfilepath>')
      sys.exit()
    elif opt in ('-u', '--unravel'):
        unravel = arg
    elif opt in ('-t', '--token'):
        token = arg
    elif opt in ('-l', '--localpath'):
        localpath = arg
    elif opt in ('-i', '--infilepath'):
        infilepath = arg
    elif opt in ('-o', '--outfilepath'):
        outfilepath = arg

  
  print('-u is ' + unravel)
  print('-t is ' + token)
  print('-l is ' + localpath)
  print('-i is ' + infilepath)
  print('-o is ' + outfilepath)

  list_of_jsons = glob.glob(os.path.join(infilepath, '*.json'))
  print(list_of_jsons)
  
  job_run_list = []
  if list_of_jsons:
    latest_file = max(list_of_jsons, key=os.path.getctime)
    d = json.load(open(latest_file))
    start_time = d['start_time']
    end_time = d['end_time']
    cluster_id = d['cluster_instance']['cluster_id']
    run_page_url = d["run_page_url"]
    job_run = get_job_runs_from_run_page_url(run_page_url)
    job_run_list.append(job_run)

    i=0
    waiting = True
    result = None
    while waiting:
      json_dict = search_apps(unravel,
                            token,
                            start_time,
                            end_time,
                            cluster_id)
    
      results = json_dict['results']
      if results != None:
        print("Results found: " + str(len(results)))
        result = results[0]
        if result['status'] in ["K","F","S", "U"] or i >= 15:
            break
      else:
        print("Not found in Unravel yet.")

      # sleep for 60 sec and 15 min in total for timeout
      time.sleep(60)
      i=i+1

  else:
    print("No JSON found in " + outfilepath)

  '''
  if outfilepath != '':
       file = open(outfilepath + '/job_run_list.json', 'w')
       file.write(json.dumps(job_run_list))
       file.close()
  '''

  job_run_result_list = []
  for run in job_run_list:
    gsp = run['workspace_id'] + '_' + run['job_id'] + '_' + run['run_id']
    job_runs_json = search_summary_by_globalsearchpattern(unravel, token,  gsp)
    
    if job_runs_json:
      
      clusterUId = job_runs_json[0]['clusterUid']
      appId      = job_runs_json[0]['sparkAppId']
      print("clusterUid: " + clusterUId)
      print("sparkAppId: " + appId)

      result_json = search_analysis(unravel, token, clusterUId, appId)
      if result_json:
        
        insights_json = result_json['insightsV2']
        recommendation_json = result_json['recommendation']
        insights2_json = []
        for item in insights_json:
           insights2_json.append(item)
        
        run['unravel_url'] = unravel + '/#/jobs/runs'
        run['unravel_keyword'] = gsp
        run['unravel_insights'] = insights2_json
        run['unravel_recommendation'] = recommendation_json
        run["app_summary"] = fetch_app_summary(unravel, token, clusterUId, appId)
        
        # add to the list
        job_run_result_list.append(run)
    else:
       print("job_run not found: " + gsp)

  if job_run_result_list:
    
    unravel_comments = create_comments_with_markdown(job_run_result_list)
    unravel_html = markdown.markdown(unravel_comments)
    if outfilepath != '':
       file = open(outfilepath + '/unravel.md', 'w')
       file.write(unravel_comments)
       file.close()  
       file = open(outfilepath + '/unravel.html', 'w')
       file.write(unravel_html)
       file.close()  

    unravel_json = {}
    unravel_json['insights'] = True
    if outfilepath != '':
      file = open(outfilepath + '/unravel.json', 'w')
      file.write(json.dumps(unravel_json))
      file.close()

  else:
    print("Nothing to do without Unravel integration")
    unravel_json = {}
    unravel_json['insights'] = True
    if outfilepath != '':
      file = open(outfilepath + '/unravel.json', 'w')
      file.write(json.dumps(unravel_json))
      file.close()
    sys.exit(0)

if __name__ == "__main__":
    main()
