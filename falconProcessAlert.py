#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
# Alert on Falcon process status
#
# Alex Bush <abush@hortonworks.com>
#

import time
import urllib2
import ambari_simplejson as json # simplejson is much faster comparing to Python 2.6 json module and has the same functions set.
import logging
import traceback
import subprocess
import xml.etree.ElementTree as ET
import re
import datetime

from resource_management.libraries.functions.curl_krb_request import curl_krb_request
from resource_management.core.environment import Environment


COORD_STATUS_ALLOW = ['RUNNING']

PROCESS_PATTERN_KEY = 'process_pattern'
MONITOR_WINDOW_KEY = 'monitor_window'
FAILURE_TOLERANCE_KEY = 'failure_tolerance'

FALCON_PORT = '{{falcon-env/falcon_port}}'
FALCON_TLS = '{{falcon-startup.properties/*.falcon.enableTLS}}'

KERBEROS_KEYTAB = '{{cluster-env/smokeuser_keytab}}'
KERBEROS_PRINCIPAL = '{{cluster-env/smokeuser_principal_name}}'
SECURITY_ENABLED_KEY = '{{cluster-env/security_enabled}}'
SMOKEUSER_KEY = "{{cluster-env/smokeuser}}"
EXECUTABLE_SEARCH_PATHS = '{{kerberos-env/executable_search_paths}}'

logger = logging.getLogger('ambari_alerts')

def get_tokens():
  """
  Returns a tuple of tokens in the format {{site/property}} that will be used
  to build the dictionary passed into execute
  """
  return ( EXECUTABLE_SEARCH_PATHS, KERBEROS_KEYTAB, KERBEROS_PRINCIPAL, SECURITY_ENABLED_KEY, SMOKEUSER_KEY, FALCON_PORT, FALCON_TLS)

def execute(configurations={}, parameters={}, host_name=None):
  """
  Returns a tuple containing the result code and a pre-formatted result label
  Keyword arguments:
  configurations (dictionary): a mapping of configuration key to value
  parameters (dictionary): a mapping of script parameter key to value
  host_name (string): the name of this host where the alert is running
  """

  if configurations is None:
    return (('UNKNOWN', ['There were no configurations supplied to the script.']))

  # Set configuration settings

  if SMOKEUSER_KEY in configurations:
    smokeuser = configurations[SMOKEUSER_KEY]

  executable_paths = None
  if EXECUTABLE_SEARCH_PATHS in configurations:
    executable_paths = configurations[EXECUTABLE_SEARCH_PATHS]

  security_enabled = False
  if SECURITY_ENABLED_KEY in configurations:
    security_enabled = str(configurations[SECURITY_ENABLED_KEY]).upper() == 'TRUE'

  kerberos_keytab = None
  if KERBEROS_KEYTAB in configurations:
    kerberos_keytab = configurations[KERBEROS_KEYTAB]

  kerberos_principal = None
  if KERBEROS_PRINCIPAL in configurations:
    kerberos_principal = configurations[KERBEROS_PRINCIPAL]
    kerberos_principal = kerberos_principal.replace('_HOST', host_name)

  falcon_port = configurations[FALCON_PORT]

  if str(configurations[FALCON_TLS]).lower() == 'true':
    protocol = 'https'
  else:
    protocol = 'http'

  if PROCESS_PATTERN_KEY in parameters:
    process_pattern = parameters[PROCESS_PATTERN_KEY]

  if MONITOR_WINDOW_KEY in parameters:
    try:
      monitor_window = float(parameters[MONITOR_WINDOW_KEY])
    except:
      return (('CRITICAL', [traceback.format_exc()]))

  if FAILURE_TOLERANCE_KEY in parameters:
    try:
      failure_tolerance = int(parameters[FAILURE_TOLERANCE_KEY])
    except:
      return (('CRITICAL', [traceback.format_exc()]))

  try:
    #Get kerberos ticket
    if security_enabled:
      run_command('kinit -kt '+kerberos_keytab+' '+kerberos_principal)
    #Get list of valid process names
    query_output = run_command('falcon entity -type process -list -url '+protocol+'://'+host_name+':'+falcon_port)
    process_names = [x.replace('(PROCESS) ','') for x in query_output.split('\n') if process_pattern in x and x.startswith('(PROCESS)')]
  except:
    return (('CRITICAL', [traceback.format_exc()]))

  #Case of no processes
  if not process_names:
    return (('OK', ['No Falcon processes found with pattern: '+process_pattern+'; Found processes were: '+query_output]))

  #List of return statuses
  return_status = list()

  #Try to get the status of each process
  for process_name in process_names:
    try:
      #Need to find the oozie server for this process
      #First get cluster name
      query_output = run_command('falcon entity -type process -definition -name '+process_name+' -url '+protocol+'://'+host_name+':'+falcon_port)
      root = ET.fromstring(query_output)
      clusters = [ cluster for clusters in root if 'clusters' in clusters.tag for cluster in clusters if 'cluster' in cluster.tag ]
      if not clusters or len(clusters) != 1 or 'name' not in clusters[0].attrib.keys():
        raise Exception('Could not find Falcon cluster name in: '+query_output)
      falcon_cluster_name = clusters[0].attrib['name']

      #Now get oozie address for cluster
      query_output = run_command('falcon entity -type cluster -definition -name '+falcon_cluster_name+' -url '+protocol+'://'+host_name+':'+falcon_port)
      root = ET.fromstring(query_output)
      execute_ep = [ interface for interfaces in root if 'interfaces' in interfaces.tag for interface in interfaces if 'interface' in interface.tag and 'type' in interface.attrib.keys() and interface.attrib['type'] == 'workflow' ]
      if not execute_ep or len(execute_ep) != 1:
        raise Exception('Could not find Oozie endpoint in cluster definition in: '+query_output)
      oozie_address = execute_ep[0].attrib['endpoint']

      #Now get latest coordinator job and its status
      query_output = run_command('oozie jobs -localtime -jobtype coordinator -len 1000 -oozie '+oozie_address)
      coordinator_lines = [ line for line in query_output.split('\n') if process_name in line ]
      if not coordinator_lines:
        raise Exception('Could not find Oozie coordinator for Falcon process: '+process_name)
      line_match = re.match('^([^C]+C).*'+process_name+'([A-Z]+).*',coordinator_lines[0])
      if not line_match:
        raise Exception('Could not determine coordinator id and status in: '+coordinator_lines[0])
      coordinator_id,coordinator_status = line_match.groups()

      #Check status and report
      if coordinator_status not in COORD_STATUS_ALLOW:
        raise Exception('Coordinator status is: '+coordinator_status+'; Allowed statuses are: '+COORD_STATUS_ALLOW.join(','))

      #Get status of recent workflows
      query_output = run_command('oozie job -localtime -order desc -len 50 -oozie '+oozie_address+' -info '+coordinator_id)
      regex = '^([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+ +[^ ]+ +[^ ]+) +([^ ]+ +[^ ]+ +[^ ]+).*'
      #This gives [(ID,Status,Ext ID,Err Code,Created,Nominal Time)]
      workflows = [ x.groups() for x in [ re.match(regex,line) for line in query_output.split('\n') ] if x ]
      #Keep only workflows executed in the past x minutes
      def date_comp(old_st,new_dt,diff_min):
        old_dt = datetime.datetime.strptime(old_st, '%Y-%m-%d %H:%M %Z')
        time_diff = (time.mktime(new_dt.timetuple()) - time.mktime(old_dt.timetuple()))/60
        return True if time_diff < diff_min and time_diff >= 0 else False
      datenow = datetime.datetime.now()
      filtered_workflows = [ wf for wf in workflows if date_comp(wf[5],datenow,monitor_window)]
      #Error if number of failed workflows is greater than threshold
      successful_workflows = len([ wf for wf in filtered_workflows if wf[1] == 'SUCCEEDED'])
      failed_workflows = [ wf for wf in filtered_workflows if wf[1] == 'KILLED']
      if failure_tolerance >= 0 and len(failed_workflows) > failure_tolerance:
        raise Exception('Coordinator '+coordinator_id+' has '+str(len(failed_workflows))+' failed workflows. Tolerance is: '+str(failure_tolerance)+' in '+str(monitor_window)+' minutes. There have been '+str(successful_workflows)+' successful workflows in this period')
      elif successful_workflows == 0:
        raise Exception('Coordinator '+coordinator_id+' has 0 successful workflows in '+str(monitor_window)+' minutes. There have been '+str(len(failed_workflows))+' killed workflows in this period')
      else:
        return_status.append({ 'label': str(successful_workflows)+' successful workflows and '+str(len(failed_workflows))+' failed workflows in the past '+str(monitor_window)+' minutes.', 'result_code': 'OK', 'process': process_name })

    #Catch any exceptions during the curls
    except:
      return_status.append({ 'label': traceback.format_exc(), 'result_code': 'CRITICAL', 'process': process_name })

  #Filter into ok and critical/warning and report
  ok_status = [ stat for stat in return_status if stat['result_code'] == 'OK' ]
  nok_status = [ stat for stat in return_status if stat['result_code'] != 'OK' ]
  if len(nok_status) != 0:
    result_code = 'CRITICAL'
  else:
    result_code = 'OK'
  label = ''
  for status in nok_status+ok_status:
    label+='Process: '+status['process']+'; Status: '+status['result_code']+'; Message:\n'+status['label']+'\n'
  return ((result_code, [label]))

#Utility function for calling commands on the CMD
def run_command(command):
  p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
  (output, err) = p.communicate()
  if p.returncode:
    raise Exception('Command: '+command+' returned with non-zero code: '+str(p.returncode)+' stderr: '+err+' stdout: '+output)
  else:
    return output
