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

from globusonline.transfer.api_client import Transfer, create_client_from_args
from globusonline.transfer.api_client import ActivationRequirementList
from globusonline.transfer.api_client import TransferAPIClient
from globusonline.transfer.api_client import x509_proxy


# TransferAPIClient instance.
api = None

def mover(username, src_site, dst_site, dst_dir):
    """
    Do a bunch of API calls and display the results. Does a small transfer
    between tutorial endpoints, but otherwise does not modify user data.

    Uses module global API client instance.
    """
    global api
    #activer=[username,"-c",os.getcwd()+"/credential.pem"]
    #api, _ = create_client_from_args(activer)
    user_credential_path=os.getcwd()+"/credential-"+username+".pem"
    #print "user_credential_path=",user_credential_path
    api = TransferAPIClient(username, cert_file=user_credential_path)
    api.set_debug_print(False, False)
    #print " Here (mover): ",api.task_list()
    # See what is in the account before we make any submissions.
    print "=== Before transfer ==="
    #display_tasksummary(); print
    #display_task_list(); print
    #display_endpoint_list(); print

    print "=== Activate endpoints ==="
    dest_directory= dst_dir
    site_ep1 = src_site
    site_ep2 = dst_site

    print "Please enter your myproxy username (\'none\' if you prefer to use your local credentials)."
    myproxy_username = sys.stdin.readline().rstrip()

    preferred_activation(username, site_ep1, myproxy_username)
    preferred_activation(username, site_ep2, myproxy_username)

    print "=== Prepare transfer ==="
    #raw_input("Press Enter to continue...")
    # submit a transfer
    oldstdout=sys.stdout
    sys.stdout = open(os.devnull,'w')
    code, message, data = api.transfer_submission_id()
    sys.stdout = oldstdout # enable output
    #code, message, data = api.transfer_submission_id()
    submission_id = data["value"]
    deadline = datetime.utcnow() + timedelta(minutes=10)
    t = Transfer(submission_id, site_ep1, site_ep2)#, deadline)

    f=open('json_file','r')
    json_results=f.read()
    f.close
    #print json_results,type(json_results)
    results=json.loads(json_results)
    #print results,type(results)
    #print results[0],type(results[0])
    for result in results:
        #print "Result: ",result
        if result[-1]=="/":
            #print "Result: it is a directory"
            t.add_item(result, dest_directory, recursive=True)
        else:
            #print "Result: it is a file"
            file_name=re.split("/",result)[-1]
            #print "Result: "+file_name
            t.add_item(result, dest_directory+"/"+file_name)

    print "=== Submit transfer ==="
    oldstdout=sys.stdout
    sys.stdout = open(os.devnull,'w')
    code, reason, data = api.transfer(t)
    sys.stdout = oldstdout # enable output
    #code, reason, data = api.transfer(t)
    task_id = data["task_id"]
    #print " Task ID is %s " % (task_id)

    # see the new transfer show up
    #print "=== After submit ==="
    #display_tasksummary(); print
    #display_task(task_id); print
    #raw_input("Press Enter to continue...")

    # wait for the task to complete, and see the summary and lists
    # update
    print "=== Checking completion ==="
    # To save the task_id for further check could be useful.
    #wait_for_task(task_id)
    max_wait = 10*1
    if wait_for_task(task_id,max_wait):
        print " Task %s is still under process " % (task_id)
        oldstdout=sys.stdout
        sys.stdout = open(os.devnull,'w')
        display_tasksummary(); print
        sys.stdout = oldstdout # enable output
        #display_task(task_id); print
        #display_ls("cin0641a#GSI-PLX"); print
    print "=== Exiting ==="
    #display_tasksummary(); print
    print "The task ID for this operation is: "+task_id; print
    oldstdout=sys.stdout
    sys.stdout = open(os.devnull,'w')
    status, reason, result = api.task(task_id)
    sys.stdout = oldstdout # enable output
    print "Its status is "+result["status"]; print

def canceltask(username, task_id):
    """
    Uses module global API client instance.
    """
    activer=[username,"-c",os.getcwd()+"/credential-"+username+".pem"]
    global api
    api, _ = create_client_from_args(activer)

    oldstdout=sys.stdout
    sys.stdout = open(os.devnull,'w')
    status, reason, result = api.task(task_id)
    sys.stdout = oldstdout # enable output
    if result["status"] != "SUCCEEDED":
        print "The process is not finished yet: its status is "+result["status"]; print
        print "It is going to be cancelled."; print
        status, reason, result = api.task_cancel(task_id)
        print "The cancel operation exited with the following message from GO:"
        print result["message"]; print
        sys.exit(0)
    else:
        print "The task already succeeded"
        sys.exit(0)

# Given the username and the task_id detailsoftask prints at video some details
# about the transfer, if it is still running. 
def detailsoftask(username, task_id):
    """
    Uses module global API client instance.
    """
    activer=[username,"-c",os.getcwd()+"/credential-"+username+".pem"]
    global api
    api, _ = create_client_from_args(activer)

    oldstdout=sys.stdout
    sys.stdout = open(os.devnull,'w')
    status, reason, result = api.task(task_id)
    sys.stdout = oldstdout # enable output
    if result["status"] != "SUCCEEDED":
        print "The process is not finished yet: its status is "+result["status"]; print
        status, reason, result = api.task_event_list(task_id)

        #print "====== File info ======"
        #print result
        #for data in result["DATA"]:
            #matchObj=re.search( r'(.*)Command(.*) .*',data["details"])
            #if matchObj:
                #containingfilename=matchObj.group(0)
                #break
        #filename=containingfilename.split()[2]    
        #print filename
        #api.file_list(filename) 
        #print "====== File info end ======"; print

        print "The operation has the following details:"; print
        try:
            data=result["DATA"][0]
        except:
            print "The data are not available yet. Try in a few seconds again."
            sys.exit(0)
        for key, value in data.iteritems():    
            print key, value; print
        print    
        sys.exit(0)
    else:
        print "The task already succeeded"
        sys.exit(0)

# This function query GO in order to return the urlendpoint dictionary
def defineurlendpoint(username):
    """
    Uses module global API client instance.
    """
    activer=[username,"-c",os.getcwd()+"/credential-"+username+".pem"]
    global api
    api, _ = create_client_from_args(activer)
     
    urlendpoint={} 

    #print; print "============= Retrieving  endpoint-list =============="
    status, message, data = api.endpoint_list(filter="username:"+username,limit="99")
    for ep_data in data["DATA"]:
        value = ep_data["canonical_name"]
        key   = ep_data["DATA"][0]["hostname"]
        if key is None:
            key="localhost"
        #print str(value)+" -> "+str(key)
        urlendpoint[key]=value
    #print "============= endpoint-list retrivied =============="

    return urlendpoint


def lookforurl(username, task_id):
    """
    Uses module global API client instance.
    """
    activer=[username,"-c",os.getcwd()+"/credential-"+username+".pem"]
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
    else:
        print "The task succeeded"

    #status, reason, result = api.subtask_list(task_id)
    destendpoint = []
    status, reason, result = api.get("/task/%s" % task_id)
    destendpoint.append(re.split("#",result["destination_endpoint"])[1])

    inurllist    = []
    outurllist   = []
    
    status, reason, result = api.task_successful_transfers(task_id)
    for subtask in result["DATA"]:
        #print subtask
        #print subtask["source_path"], subtask["destination_path"], subtask["destination_endpoint"]
        inurllist.append(subtask["source_path"])
        outurllist.append(subtask["destination_path"])
    
    while result["next_marker"] != [] and result["next_marker"] != None:
        #print result["next_marker"]
        #print "There are more pid..."
        status, reason, result = api.task_successful_transfers(task_id, marker=result["next_marker"])
        for subtask in result["DATA"]:
            #print subtask
            #print subtask["source_path"], subtask["destination_path"], subtask["destination_endpoint"]
            inurllist.append(subtask["source_path"])
            outurllist.append(subtask["destination_path"])

    return inurllist, outurllist, destendpoint

def preferred_activation(username, endpoint_name, myproxy_username):
    user_credential_path=os.getcwd()+"/credential-"+username+".pem"
    print "==Activating %s ==" % endpoint_name
    api = TransferAPIClient(username, cert_file=user_credential_path)
    api.set_debug_print(False, False)
    try:
        code, message, data = api.endpoint(endpoint_name)
        if not data["activated"]:
            try:
                print "==Try autoactivation=="
                code, message, data = api.endpoint_autoactivate(endpoint_name)
            except:
                print "Cannot autoactivate"
    except:
        pass
    
    try:
        code, message, data = api.endpoint(endpoint_name)
    except:
        data={'activated': "Unavailable"}

    if not data["activated"]: # and data["activated"] == "Unavailable":
        try:
            if myproxy_username != "none":
                print "==Try myproxy for %s ==" % myproxy_username
                status, message, data = api.endpoint_autoactivate(endpoint_name)
                data.set_requirement_value("myproxy", "username", myproxy_username)
                from getpass import getpass
                passphrase = getpass()
                data.set_requirement_value("myproxy", "passphrase", passphrase)
                api.endpoint_activate(endpoint_name, data)
                #activer=[username,"-c",os.getcwd()+"/credential.pem"]
                #api, _ = create_client_from_args(activer)
                #conditional_activation(endpoint_name,myproxy_username)
                code, message, data = api.endpoint(endpoint_name)
            else:
                raise 
        except:
            print "==Local proxy activation=="
            _, _, reqs = api.endpoint_activation_requirements(endpoint_name, type="delegate_proxy")
            #print "endpoint_name",endpoint_name
            #print reqs
            public_key = reqs.get_requirement_value("delegate_proxy", "public_key")
            #print public_key
            proxy = x509_proxy.create_proxy_from_file(user_credential_path, public_key)
            #print "proxy"
            #print proxy
            reqs.set_requirement_value("delegate_proxy", "proxy_chain", proxy)
            #print reqs
            result = api.endpoint_activate(endpoint_name, reqs)
            #print result
            #status, message, data = api.endpoint_autoactivate(endpoint_name)
            #print data["code"]




def conditional_activation(endpoint_name,site_username):
    api.endpoint_autoactivate(endpoint_name)
    code, reason, endpoint_list = api.endpoint_list(limit=100)
    #print "Found %d endpoints for user %s:" % (endpoint_list["length"], api.username)
    for ep in endpoint_list["DATA"]:
        if ep["name"] == endpoint_name:
                if ep["activated"]:
                        print " %s is already active. " % (endpoint_name)
                else:
                        print " Activating %s " % (endpoint_name)
                        status, message, data = api.endpoint_autoactivate(endpoint_name)
                        data.set_requirement_value("myproxy", "username", site_username)
                        from getpass import getpass
                        passphrase = getpass()
                        data.set_requirement_value("myproxy", "passphrase", passphrase)
                        api.endpoint_activate(endpoint_name, data)

def display_activation(endpoint_name):
    print "=== Endpoint pre-activation ==="
    display_endpoint(endpoint_name)
    print
    code, reason, result = api.endpoint_autoactivate(endpoint_name,
                                                     if_expires_in=600)
    if result["code"].startswith("AutoActivationFailed"):
        print "Auto activation failed, ls and transfers will likely fail!"
    print "result: %s (%s)" % (result["code"], result["message"])
    print "=== Endpoint post-activation ==="
    display_endpoint(endpoint_name)
    print


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
