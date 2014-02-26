#!/usr/bin/env python
import sys, os, subprocess
import argparse
from argparse import RawTextHelpFormatter

import datetime
import json
import re
from threading import Thread

import datamover

# To configure the following, please edit datastagerconfig.py
#iPATH='/home/jack/CINECA/GridTools/iRODS/iRODS/clients/icommands/bin/'

# Load configurations from datastagerconfig.py
execfile(os.getcwd()+"/datastagerconfig.py")

# Write y as an argument for iRODS in wich a is the variable and b the value
def formatter(a,x):
    y='"*'+a+'=\\"'+x+'\\"" '
    return y

# Chech if the arguments of an array differ
def all_same(items):
    return all(x == items[0] for x in items)

# Get the url given the PID
def pidfromurl(argument):
    os.system(iPATH+'/irule -F URLselecter.r '+argument+' >> json_file')

# Get the PID given the url
def urlfrompid(argument):
    os.system(iPATH+'/irule -F PIDselecter.r '+argument+' > json_file')

# Write the PID to pid.file
def pidtofile(argument):
    os.system(iPATH+'/irule -F PIDselecter.r '+argument+' | awk \'{print $2}\' >> pid.file')

# Write to json_file the json list of file to be transferred. 
def jsonformatter():
# Read the string from the file 
    fo = open("json_file", "r")
    strglist = fo.readlines();
    fo.close()
# Empty the file    
    open("json_file", 'w').close()
# Fromat the string    
    path=[]
    endpoint=[]
    strglistlength=len(strglist)
    elementnumber=0
    for strg in strglist:
        elementnumber=elementnumber+1
        if elementnumber%25:
            print "Element "+str(elementnumber)+" of "+str(strglistlength)
        lista = re.split(r'\s* \s*', strg.rstrip())
        if lista[1] == "None":
            print "An argument(pid, url...) does not exist. Continuing anyway!" 
            continue
        sublista = re.split(r'\s*\:\s*', lista[1])
        #print "lista : ", lista
        #print "sublista : ", sublista
        url=re.split("//",sublista[1])[1]
        prepath=re.split("^\d\d\d\d",sublista[2])[1].rstrip()
        path.append(prepath)
        urlendpoint = datamover.defineurlendpoint(str(arguments.user))
        try:
            endpoint.append(urlendpoint[url])
        except:
            print "The server "+url+" is not mapped to a GO enpoint in datastagerconfig" 
            sys.exit(0)
        #print url
        #print path
        #print endpoint
# Write path to the file    
    json_results=json.dumps(path)
    fo = open("json_file", "w")
    fo.write(json_results);
    fo.close()
    #print json_results
    return endpoint

def irodssource(arguments):
    argument=''
    if arguments.path:
        argument = arguments.path
# Empty the file    
        open("json_file", 'w').close()
# Populate it    
        jsonlist = open("json_file", 'w')
        path=[]
        path.append(argument)
        json_results=json.dumps(path)
        jsonlist.write(json_results)
        jsonlist.close
    elif arguments.pathfile:
        fo = open(arguments.pathfile, "r")
        irodslist = fo.readlines();
        fo.close()
# Empty the file    
        open("json_file", 'w').close()
# Populate it    
        jsonlist = open("json_file", 'w')
        path=[]
        for ifile in irodslist:
            path.append(ifile.rstrip())
        json_results=json.dumps(path)
        jsonlist.write(json_results)
        jsonlist.close
    else: 
        print "You selected irods so one between path and pathFile is required"
        sys.exit(1)
    if arguments.user is None:
        print " The username is mandatory! "
        sys.exit(1)

# Write to json_file (via jsonformatter) the list of url for the given dest endpoint. 
def pidsource(arguments):
    if arguments.pid:
        argument=formatter("pid",arguments.pid.rstrip())
        pidfromurl(argument)
        sslist=jsonformatter()
        return sslist[0]
    elif arguments.pidfile:
        fo = open(arguments.pidfile, "r")
        pidlist = fo.readlines();
        fo.close()
# Empty the file    
        open("json_file", 'w').close()
# Create and start the thread list to call pidfromurl in parallel
        threadlist=[]
        for pid in pidlist:
            argument=formatter("pid",pid.rstrip())
            #print argument
            T=Thread(target=pidfromurl,args=([argument]))
            T.start()
            threadlist.append(T)
        for t in threadlist:
            t.join()
        print "All pid(s) resolved to an url."
        sslist = jsonformatter()
        if not all_same(sslist):
            print "All the pids should be mapped to the same GO endpoint!"
            sys.exit(1)
        if sslist == []:
            print "None of the url correspond to an exixting file!"
            sys.exit()
        return sslist[0]
    else:
        print "You selected pid so the pid is required!"
        sys.exit(1)

def urlsource(arguments):
    if arguments.url:
        argument=formatter("url",arguments.url)
        #print "URL: "+argument
        urlfrompid(argument)
        fo = open("json_file", "r")
        strg = fo.readlines();
        fo.close()
        open("json_file", 'w').close()
        #print "iii "+strg
        arguments.pid=re.split("Output: ", strg[0])[1].rstrip()
        #print "pid "+arguments.pid
        src_site=pidsource(arguments)
        return src_site
    elif arguments.urlfile:
        fo = open(arguments.urlfile, "r")
        urllist = fo.readlines();
        fo.close()
# Empty the file    
        open("json_file", 'w').close()
# Create and start the thread list to call urlfrompid in parallel
        threadlist=[]
        for url in urllist:
            argument=formatter("url",url.rstrip())
            #print argument
            T=Thread(target=urlfrompid,args=([argument]))
            T.start()
            threadlist.append(T)
        for t in threadlist:
            t.join()
        print "All url(s) resolved to a pid."
        arguments.pidfile="pidfile"
        src_site=pidsource(arguments)
        return src_site
    else:
        print "You selected url so the url is required!"
        sys.exit(1)

def details():
    print """
Invoke as follow for stage out: 
----------------------------------------------------------------------------------    
./datastager.py out irods -p path  
                          -u GO-user 
                          --ss source-end-point --ds dest-end-point --dd dest-dir
./datastager.py out pid --pid prefix/pid 
                        -m PID-retrieving-mode
                        -u GO-user 
                        --ds dest-end-point --dd dest-dir
./datastager.py out url --url full-url
                        -u GO-user 
                        --ds dest-end-point --dd dest-dir

----------------------------------------------------------------------------------    
or as follow for stage in:
./datastager.py in path -u GO-user 
                        --ss source-end-point --sd source-dir -p path
                        --ds dest-end-point --dd dest-dir
./datastager.py in pid --taskid the-taskID-of-the-processr-you-want-thepid 
                       -m PID-retrieving-mode
                       -u GO-user 

----------------------------------------------------------------------------------    
or as follow for cancel the stage (in or out) operation or get details about it:
./datastager.py in details --taskid the-taskID-of-the-process-you-want-details 
                       -u GO-user 
./datastager.py out cancel --taskid the-taskID-of-the-process-you-want-to-cancel 
                       -u GO-user 
    """
    sys.exit(0)

parser = argparse.ArgumentParser(description=" Data stager: move a bounce of data inside or outside iRODS via GridFTP. \n The -d options requires both positional arguments.", 
        formatter_class=RawTextHelpFormatter)
taskgroup = parser.add_argument_group('taskid', 'Options specific to "stage {in,out} {pid(in only),details,cancel} --taskid"')
urlgroup = parser.add_argument_group('url', 'Options specific to url (mutually exclusive)')
pidgroup = parser.add_argument_group('pid', 'Options specific to pid') # (mutually exclusive)')
# Examples
parser.add_argument("-d", "--details", help="a longer description and some usage examples (invoke with \"datastager.py in pid -d\")", action="store_true")
# Stage in or stage out
parser.add_argument("direction",choices=['in','out'],default="NULL",help=" the direction of the stage: in or out")
# Kind of source: url or PID
parser.add_argument("kind",choices=['irods','pid','url','path','details','cancel'],default="NULL",help=" the description of your data")
# General informations
parser.add_argument("-p", "--path", help="the path of your file (iRODS collection or local file system depending on the circumstances)", 
        action="store", dest="path")
parser.add_argument("-pF", "--pathFile", help="the file listing your files (alternative to -p)",
        action="store", dest="pathfile")
parser.add_argument("-u", "--username", help="your username on globusonline.org", action="store",
        dest="user")
parser.add_argument("-cert", "--certificate", help="your x509 certificate (pem file)", action="store",
        dest="cert")
parser.add_argument("-key", "--secretekey", help="the key of your certificate", action="store",
        dest="key")
parser.add_argument("-certdir", "--trustedca", help="your trusted CA", action="store",
        dest="certdir")
# iRODS informations

# PID
pidgroup.add_argument("-P", "--pid", help="the PID of your data",
        action="store", dest="pid")
pidgroup.add_argument("-PF", "--pid-file", help="the file listing the PID(s) of your data",
        action="store", dest="pidfile")
pidgroup.add_argument("-m", "--pid-mode", help="the way you \"translate\" the PID(s) of your data (DSSfile or icommans)",
        action="store", dest="pidmode")
# URL
urlgroup.add_argument("-U", "--url", help="the URL of your data",
        action="store", dest="url")
urlgroup.add_argument("-UF", "--urlfile", help="the file listing the URL(s) of your data",
        action="store", dest="urlfile")
# TaskID
taskgroup.add_argument("-t", "--taskid", help="the taskID of your transfer",
        action="store", dest="taskid")
# Servers infromations
parser.add_argument("--ss", help="the GridFTP src server as GO endpoint", action="store",
        dest="src_site", default="irods-dev")
parser.add_argument("--ds", help="the GridFTP dst server as GO endpoint", action="store",
        dest="dst_site", default="GSI-PLX")
parser.add_argument("--sd", help="the GridFTP src directory", action="store",
        dest="src_dir", default="/~/")
parser.add_argument("--dd", help="the GridFTP dst directory", action="store",
        dest="dst_dir", default="/~/")
arguments = parser.parse_args()

# Invoke the detailed help if required
if arguments.details:
   details()

##################################################################################
# Start the execution
##################################################################################
os.system('clear')
print "Hello, welcome to data staging!" 

# Check for a local x509 proxy
grid_proxy_init_options=' -out credential-'+arguments.user+'.pem '
if arguments.cert:
    grid_proxy_init_options=grid_proxy_init_options+' -cert '+arguments.cert
if arguments.key:
    grid_proxy_init_options=grid_proxy_init_options+' -key '+arguments.key
if arguments.certdir:
    grid_proxy_init_options=grid_proxy_init_options+' -certdir '+arguments.certdir

#print "grid_proxy_init_options: "+grid_proxy_init_options

print ""
if os.path.exists('credential-'+arguments.user+'.pem'):
    print "credential-"+arguments.user+".pem exist" 
    try:
        retvalue = os.system('grid-proxy-info -exists -f credential-'+arguments.user+'.pem') 
        if retvalue == 0:
            print "Proxy found!"
        else:
            print "Proxy expired. New one, please!"
            os.system('grid-proxy-init'+grid_proxy_init_options)
    except:
        print "Proxy invalid. New one, please!"
        os.system('grid-proxy-init'+grid_proxy_init_options)
else:
    print "credential-"+arguments.user+".pem does not exist. Create it, please!"
    os.system('grid-proxy-init'+grid_proxy_init_options)
    try:
        retvalue = os.system('grid-proxy-info -exists -f credential-'+arguments.user+'.pem') 
        if retvalue == 0:
            print "Proxy found!"
        else:
            print "Proxy expired. New one, please!"
            os.system('grid-proxy-init'+grid_proxy_init_options)
    except:
        print "Proxy invalid. New one, please!"
        os.system('grid-proxy-init'+grid_proxy_init_options)
print ""

# Cencel or Details
if arguments.kind == "cancel":
    print "The transfer activity corresponding to the given task is going to be cancelled."
    if arguments.taskid:
        api = None
        inurllist, outurllist, destendpoint = datamover.canceltask(str(arguments.user), str(arguments.taskid))
        sys.exit(0)
    else:
        "You did not provide the taskid!"
        sys.exit(1)
elif arguments.kind == "details":
    print "The transfer activity corresponding to the given task follows."
    if arguments.taskid:
        api = None
        #urlendpoint = datamover.defineurlendpoint(str(arguments.user))
        #print urlendpoint
        datamover.detailsoftask(str(arguments.user), str(arguments.taskid))
        sys.exit(0)


# Stage out
if arguments.direction == "out":
    if arguments.kind == "irods":
        if arguments.path and arguments.pathfile:
            print "Only one between -p and -pF is allowed!"
            sys.exit(1)
        irodssource(arguments)
        print "Source end-point: "+arguments.src_site
        #sys.exit(1) 
    elif arguments.kind == "url":
        if arguments.url and arguments.urlfile:
            print "Only one between -U and -UF is allowed!"
            sys.exit(1)
        arguments.src_site=urlsource(arguments)
        print "Source end-point: "+arguments.src_site
        #sys.exit(1) 
    elif arguments.kind == "pid":
        if arguments.pid and arguments.pidfile:
            print "Only one between -P and -PF is allowed!"
            sys.exit(1)
        if not arguments.pidmode:
            print "The pidmode (-m) is mandatory!"
            sys.exit(1)
        arguments.src_site=pidsource(arguments)
        print "Source end-point: "+arguments.src_site
        #sys.exit(1) 
    else:
        print "You are staging out so you can only specify iRODS or PID or URL!"
        sys.exit(1)
        

# Stage in 
if arguments.direction == "in":
    if arguments.kind == "path":
        if arguments.path:
            if arguments.pathfile:
                print "Only one between -p and -pF is allowed!"
                sys.exit(1)
            print "You are staging in so save the taskID in order to know the PID(s)."
            file_list=[]
            file_list.append(arguments.src_dir+"/"+arguments.path)
            json_results=json.dumps(file_list)
            fo = open("json_file", "w")
            fo.write(json_results);
            fo.close()
        elif arguments.pathfile:
            strglist=[]
            fo = open(arguments.pathfile, "r")
            strglist = fo.readlines();
            fo.close()
            file_list=[]
            for filename in strglist:
                file_list.append(arguments.src_dir+"/"+filename.rstrip())
            json_results=json.dumps(file_list)
            fo = open("json_file", "w")
            fo.write(json_results);
            fo.close()
        else:
            print "One between -p and -pF is mandatory."
            sys.exit(1)
    elif arguments.kind == "pid":
        if not arguments.pidmode:
            print "The pidmode (-m) is mandatory!"
            sys.exit(1)
        print "The list of the corresponding PID is going to be saved in pid.file."
        if arguments.taskid:
            api = None
            inurllist, outurllist, destendpoint = datamover.lookforurl(str(arguments.user), str(arguments.taskid))
            #print inurllist
            #print outurllist
            #print destendpoint
            if not all_same(destendpoint):
                print "All the pid should be mapped to the same GO endpoint."
                sys.exit(1)
            urlendpoint = datamover.defineurlendpoint(str(arguments.user))
            for url, ep in urlendpoint.items():
                if ep == arguments.user+"#"+destendpoint[0]:
                    endpoint=url
            if endpoint=="":
                print "The server "+destendpoint[0]+" is not mapped to a GO enpoint in datastagerconfig." 
                sys.exit(0)
            #print endpoint
            fo = open("pid.file", "w").close
# Create and start the thread list to call urlfrompid in parallel
            if arguments.pidmode == "DSSfile":
                print "Retrieving the DSSfile via GridFTP"
                sys.exit(0)
            elif arguments.pidmode == "icommands":
                threadlist=[]
                for url in outurllist:
                    plainurl = url.replace("//","/")
                    #argument = formatter("url","irods://"+endpoint+":1247"+plainurl)
                    argument = formatter("url","\*"+plainurl)
                    #print argument
                    T=Thread(target=pidtofile,args=([argument]))
                    T.start()
                    threadlist.append(T)
                for t in threadlist:
                    t.join()
            print "All (available) pid(s) wrote in pid.file."
            sys.exit(0)
        else:
            print "You did not provide the taskid!"
            sys.exit(1)
    else:
        print "You are staging in so you can only specify taskID or PID!"
        sys.exit(1)


#sys.exit(1) 

##################################################################################
# Actually move the data
##################################################################################
api = None
datamover.mover(str(arguments.user), str(arguments.src_site), str(arguments.dst_site), str(arguments.dst_dir))
