#!/usr/bin/env python

# Copyright 2010 University of Chicago
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Demonstrate API calls.

Example run using standard globus toolkit certificate locations:

python example.py USERNAME -k ~/.globus/userkey.pem -c ~/.globus/usercert.pem
"""
import time
from datetime import datetime, timedelta
import traceback
import json, os
import re, sys

from fts3.rest.client.submitter import Submitter as FTSSubmitter
from fts3.rest.client.context import Context as FTSContext
from fts3.rest.client.delegator import Delegator as FTSDelegator
from fts3.rest.client.inquirer import Inquirer as FTSInquirer


def fts_mover(fts_endpoint, cert_path, src_site, dst_site, 
              dst_dir, waiting, timeout, overwrite):
    """
    Do a bunch of API calls and display the results. Does a small transfer
    between tutorial endpoints, but otherwise does not modify user data.

    Uses module global API client instance.
    """

    print "=== Activate endpoints ==="
    dest_directory= dst_dir
    site_ep1 = src_site
    site_ep2 = dst_site

    print "=== Prepare transfer ==="

    ####### invoke fts3 transfer #######

    fts_context = FTSContext(fts_endpoint, cert_path, cert_path)

    # create src/dst pairs
    src_paths = []
    with open('json_file') as src_path_file:
      src_paths = json.load(src_path_file)

    src_list = []
    dst_list = []

    for path in src_paths:
      src_list.append(site_ep1+path)
      dst_list.append(site_ep2+dest_directory+os.path.basename(path))

    fts_delegator = FTSDelegator(fts_context)
    delegationId = fts_delegator.delegate(timedelta(minutes = 420))
    fts_submitter = FTSSubmitter(fts_context)
    
    kwargs = {
        'checksum'          : None,
        'bring_online'      : None,
        'verify_checksum'   : False,
        'spacetoken'        : None,
        'source_spacetoken' : None,
        'fail_nearline'     : False,
        'file_metadata'     : None, 
        'filesize'          : None, 
        'gridftp'           : None, 
        'job_metadata'      : None, 
        'overwrite'         : overwrite,
        'copy_pin_lifetime' : -1,
        'reuse'             : False
        }

    job = {}
    job['files'] = [{'sources': src_list, 'destinations': dst_list}]
    job['params'] = kwargs

    print "=== Submit transfer ==="
    print delegationId
    print src_list
    print
    print dst_list
    jobId = json.loads(fts_context.post_json('/jobs', json.dumps(job)))['job_id']
    print jobId

    ####### end fts3 invokation ########

    print "=== Checking completion ==="

    ####### wait for results ###########

    if jobId and waiting:
      poll_interval = 10
      fts_inquirer = FTSInquirer(fts_context)
      while timeout:
        timeout -= 1
        time.sleep(poll_interval)
        job = fts_inquirer.getJobStatus(jobId)
        if job['job_state'] not in ['SUBMITTED', 'READY', 'STAGING', 'ACTIVE']:
          print "Job finished with state %s" % job['job_state']
          break
        print "Job in state %s" % job['job_state']

      if not timeout:
        print "we stopped looking after the timeout expired"
      if job['reason']:
        print "Reason: %s" % job['reason']


def lookforurl(username, task_id):
    """
    Uses module global API client instance.
    """
    activer=[username,"-c",os.getcwd()+"/credential.pem"]
    global api
    api, _ = create_client_from_args(activer)
    #print " Here: ",api.task_list()
    # See what is in the account before we make any submissions.
    #print "=== Before transfer ==="
    #display_tasksummary(); print
    #display_task_list(); print
    #display_endpoint_list(); print
    #status, reason, result = api.task(task_id)

    oldstdout=sys.stdout
    sys.stdout = open(os.devnull,'w')
    status, reason, result = api.task(task_id)
    sys.stdout = oldstdout # enable output
    if result["status"] != "SUCCEEDED":
        print "The process is not finished yet."
        print "Its status is "+result["status"]; print
        sys.exit(0)

    status, reason, result = api.subtask_list(task_id)
    #print "Transfer status is: "+result["status"]
    #print "Transfer command was: "+result["command"]
    #print result; print
    #print result["subtask_link"]; print
    #print result["subtask_link"]["resource"],type(result["subtask_link"]["resource"])
    inurllist    = []
    outurllist   = []
    destendpoint = []
    for subtask in result["DATA"]:
        #print subtask
        #print subtask["source_path"], subtask["destination_path"], subtask["destination_endpoint"]
        inurllist.append(subtask["source_path"])
        outurllist.append(subtask["destination_path"])
        destendpoint.append(re.split("#",subtask["destination_endpoint"])[1])
    return inurllist, outurllist, destendpoint

def display_tasksummary():
    print " Here: ",api.task_list()
    code, reason, data = api.tasksummary()
    print "Task Summary for %s:" % api.username
    for k, v in data.iteritems():
        if k == "DATA_TYPE":
            continue
        print "%3d %s" % (int(v), k.upper().ljust(9))


def display_task_list(max_age=None):
    """
    @param max_age: only show tasks requested at or after now - max_age.
    @type max_age: timedelta
    """
    kwargs = {}
    if max_age:
        min_request_time = datetime.utcnow() - max_age
        # filter on request_time starting at min_request_time, with no
        # upper limit on request_time.
        kwargs["request_time"] = "%s," % min_request_time

    code, reason, task_list = api.task_list(**kwargs)
    print "task_list for %s:" % api.username
    for task in task_list["DATA"]:
        print "Task %s:" % task["task_id"]
        _print_task(task)

def _print_task(data, indent_level=0):
    """
    Works for tasks and subtasks, since both have a task_id key
    and other key/values are printed by iterating through the items.
    """
    indent = " " * indent_level
    indent += " " * 2
    for k, v in data.iteritems():
        if k in ("DATA_TYPE", "LINKS"):
            continue
        print indent + "%s: %s" % (k, v)

def display_task(task_id, show_subtasks=True):
    code, reason, data = api.task(task_id)
    print "Task %s:" % task_id
    _print_task(data, 0)

    if show_subtasks:
        code, reason, data = api.subtask_list(task_id)
        subtask_list = data["DATA"]
        for t in subtask_list:
            print "  subtask %s:" % t["task_id"]
            _print_task(t, 4)

def wait_for_task(task_id, timeout=-1):
    status = "ACTIVE"
    while timeout and status == "ACTIVE":
        code, reason, data = api.task(task_id, fields="status")
        status = data["status"]
        time.sleep(1)
        timeout -= 1

    if status != "ACTIVE":
        print "Task %s complete!" % task_id
        return True
    else:
        print "Task still not complete after %d seconds" % timeout
        return False

def display_endpoint_list():
    code, reason, endpoint_list = api.endpoint_list(limit=100)
    print "Found %d endpoints for user %s:" \
          % (endpoint_list["length"], api.username)
    for ep in endpoint_list["DATA"]:
        _print_endpoint(ep)

def display_endpoint(endpoint_name):
    code, reason, data = api.endpoint(endpoint_name)
    _print_endpoint(data)

def _print_endpoint(ep):
    name = ep["canonical_name"]
    print name
    if ep["activated"]:
        print "  activated (expires: %s)" % ep["expire_time"]
    else:
        print "  not activated"
    if ep["public"]:
        print "  public"
    else:
        print "  not public"
    if ep["myproxy_server"]:
        print "  default myproxy server: %s" % ep["myproxy_server"]
    else:
        print "  no default myproxy server"
    servers = ep.get("DATA", ())
    print "  servers:"
    for s in servers:
        uri = s["uri"]
        if not uri:
            uri = "GC endpoint, no uri available"
        print "    " + uri,
        if s["subject"]:
            print " (%s)" % s["subject"]
        else:
            print

def unicode_(data):
    """
    Coerce any type to unicode, assuming utf-8 encoding for strings.
    """
    if isinstance(data, unicode):
        return data
    if isinstance(data, str):
        return unicode(data, "utf-8")
    else:
        return unicode(data)

def display_ls(endpoint_name, path=""):
    code, reason, data = api.endpoint_ls(endpoint_name, path)
    # Server returns canonical path; "" maps to the users default path,
    # which is typically their home directory "/~/".
    path = data["path"]
    print "Contents of %s on %s:" % (path, endpoint_name)
    headers = "name, type, permissions, size, user, group, last_modified"
    headers_list = headers.split(", ")
    print headers
    for f in data["DATA"]:
        print ", ".join([unicode_(f[k]) for k in headers_list])


if __name__ == '__main__':
    api, _ = create_client_from_args()
    #print " Here: ",api.task_list()
    #sys.exit(0)
    tutorial()
