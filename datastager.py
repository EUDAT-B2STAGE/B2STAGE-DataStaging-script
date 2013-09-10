#!/usr/bin/env python
import sys, os, subprocess
import argparse
from argparse import RawTextHelpFormatter

import datetime
import json
import re

import datamover

#iPATH='/home/jack/CINECA/GridTools/iRODS/iRODS/clients/icommands/bin/'

#urlendpoint={
    #'data.repo.cineca.it': "cinecaRepoSingl",
    #'irods-dev.cineca.it': "irods-dev",
    #'dtn01.hector.ac.uk': "dtn01",
    #'eudat-irodsdev.epcc.ed.ac.uk': "eudat-irodsdev"
#}
execfile(os.getcwd()+"/datastagerconfig.py")

def formatter(a,x):
    y='"*'+a+'=\\"'+x+'\\"" '
    return y

def all_same(items):
    return all(x == items[0] for x in items)

#def mapserver(url):
    #urlendpoint={'data.repo.cineca.it': "cinecaRepoSingl", 'irods-dev.cineca.it': "irods-dev"}
    #endpoint=urlendpoint[url]
    #return endpoint

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
    for strg in strglist:
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

def seedsource(arguments):
    argument=''
    if arguments.path:
        argument = argument+formatter("path",arguments.path)
    else:
        print "You selected seed so the path is required"
        sys.exit(1)
    if arguments.year:
        argument = argument+formatter("year",arguments.year)
    else:
        print "You selected seed so the year is required"
        sys.exit(1)
    if arguments.network:
        argument = argument+formatter("network",arguments.network)
    else:
        print "You selected seed so the network is required"
        sys.exit(1)
    if arguments.channel:
        argument = argument+formatter("channel",arguments.channel)
    else:
        print "You selected seed so the channel is required"
        sys.exit(1)
    if arguments.station:
        argument = argument+formatter("station",arguments.station)
    else:
        print "You selected seed so the station is required"
        sys.exit(1)
    if arguments.user is None:
        print " The username is mandatory! "
        sys.exit(1)

    #print "SEED: "+argument
    os.system(iPATH+'/irule -F seedselecter.r '+argument+' > json_file')

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

def pidsource(arguments):
    if arguments.pid:
        argument=formatter("pid",arguments.pid.rstrip())
        #print "PID: "+argument
        os.system(iPATH+'/irule -F URLselecter.r '+argument+' > json_file')
        sslist=jsonformatter()
        return sslist[0]
    elif arguments.pidfile:
        fo = open(arguments.pidfile, "r")
        pidlist = fo.readlines();
        fo.close()
# Empty the file    
        open("json_file", 'w').close()
# Populate it    
        for pid in pidlist:
            #print "pid: "+pid
            argument=formatter("pid",pid.rstrip())
            #print "pid: "+argument
            os.system(iPATH+'/irule -F URLselecter.r '+argument+' >> json_file')
        sslist = jsonformatter()
        if not all_same(sslist):
            print "All the pid should be mapped to the same GO endpoint"
            sys.exit(1)
        return sslist[0]
    else:
        print "You selected pid so the pid is required"
        sys.exit(1)

def urlsource(arguments):
    if arguments.url:
        argument=formatter("url",arguments.url)
        #print "URL: "+argument
        os.system(iPATH+'/irule -F PIDselecter.r '+argument+' > json_file')
        fo = open("json_file", "r")
        strg = fo.readlines();
        fo.close()
        open("json_file", 'w').close()
        #print "iii "+strg
        arguments.pid=re.split("Output: ", strg[0])[1].rstrip()
        #print "pid "+arguments.pid
        src_site=pidsource(arguments)
        #ss="iiidd"
        #ss=jsonformatter()
        return src_site
    elif arguments.urlfile:
        fo = open(arguments.urlfile, "r")
        urllist = fo.readlines();
        fo.close()
# Empty the file    
        open("json_file", 'w').close()
# Populate it    
        for url in urllist:
            argument=formatter("url",url.rstrip())
            os.system(iPATH+'/irule -F PIDselecter.r '+argument+' | tr -d "Output: " >> pidfile')
        arguments.pidfile="pidfile"
        src_site=pidsource(arguments)
        return src_site
    else:
        print "You selected url so the url is required"
        sys.exit(1)

def details():
    print """
Invoke as follow for stage out: 
./datastager.py out irods -p path  
                          -u GO-user 
                          --ss source-end-point --ds dest-end-point --dd dest-dir
or
./datastager.py out seed -p path -y year -n network -s station -c channel 
                         -u GO-user 
                         --ss source-end-point --ds dest-end-point --dd dest-dir
or
./datastager.py out pid --pid prefix/pid 
                        -u GO-user 
                        --ds dest-end-point --dd dest-dir
or
./datastager.py out url --url full-url
                        -u GO-user 
                        --ds dest-end-point --dd dest-dir

or as follow for stage in:
./datastager.py in taskid -u GO-user 
                          --ss source-end-point --sd source-dir -p path
                          --ds dest-end-point --dd dest-dir
./datastager.py in pid --taskid the-taskID-of-the-process you-want-thepid 
                       -u GO-user 

For example: 
./datastager.py -p /home/irods/data/archive -y 2004 -n MN -s AQU -c BHE -u cin0641a --ss ingv --ds vzSARA --dd vzSARA/home/rods#CINECA/
    """
    sys.exit(0)

parser = argparse.ArgumentParser(description=" Data stager: move a bounce of data inside or outside iRODS via GridFTP. \n The -d options requires both positional arguments.", 
        formatter_class=RawTextHelpFormatter)
taskgroup = parser.add_argument_group('taskid', 'Options specific to stage in taskid')
seedgroup = parser.add_argument_group('seed', 'Options specific to seed')
irodsgroup = parser.add_argument_group('irods', 'Options specific to irods')
urlgroup = parser.add_argument_group('url', 'Options specific to url (mutually exclusive)')
pidgroup = parser.add_argument_group('pid', 'Options specific to pid (mutually exclusive)')
# Examples
parser.add_argument("-d", "--details", help="a longer description and some usage examples", action="store_true")
# Stage in or stage out
parser.add_argument("direction",choices=['in','out'],default="NULL",help=" the direction of the stage: in or out")
# Kind of source: seed, url or PID
parser.add_argument("kind",choices=['seed','irods','pid','url','taskid'],default="NULL",help=" the description of your data")
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
# SEED informations
seedgroup.add_argument("-y", "--year", help="the year of interest", action="store",
        dest="year")
seedgroup.add_argument("-n", "--network", help="the network of interest",
        action="store", dest="network")
seedgroup.add_argument("-c", "--channel", help="the channel of interest",
        action="store", dest="channel")
seedgroup.add_argument("-s", "--station", help="the station of interest", action="store",
        dest="station")
# iRODS informations

# PID
pidgroup.add_argument("-P", "--pid", help="the PID of your data",
        action="store", dest="pid")
pidgroup.add_argument("-PF", "--pid-file", help="the file listing the PID(s) of your data",
        action="store", dest="pidfile")
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


# Stage out
if arguments.direction == "out":
    if arguments.kind == "seed":
        seedsource(arguments)
        print "Source end-point: "+arguments.src_site
        #sys.exit(1) 
    elif arguments.kind == "irods":
        if arguments.path and arguments.pathfile:
            print "Only one between -p and -pF is allowed"
            sys.exit(1)
        irodssource(arguments)
        print "Source end-point: "+arguments.src_site
        #sys.exit(1) 
    elif arguments.kind == "url":
        if arguments.url and arguments.urlfile:
            print "Only one between -U and -UF is allowed"
            sys.exit(1)
        arguments.src_site=urlsource(arguments)
        print "Source end-point: "+arguments.src_site
        #sys.exit(1) 
    elif arguments.kind == "pid":
        if arguments.pid and arguments.pidfile:
            print "Only one between -P and -PF is allowed"
            sys.exit(1)
        arguments.src_site=pidsource(arguments)
        print "Source end-point: "+arguments.src_site
        #sys.exit(1) 
    else:
        print "You are staging out so you can only specify seed or PID or URL!"
        sys.exit(1)
        

# Stage in 
if arguments.direction == "in":
    if arguments.kind == "taskid":
        if arguments.path:
            if arguments.pathfile:
                print "Only one between -p and -pF is allowed"
                sys.exit(1)
            print "You are staging in so save the taskID in order to know the PID(s)"
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
            print "One between -p and -pF is mandatory"
            sys.exit(1)
    elif arguments.kind == "pid":
        print "The list of the corresponding PID is going to be saved in pid.file"
        if arguments.taskid:
            api = None
            inurllist, outurllist, destendpoint = datamover.lookforurl(str(arguments.user), str(arguments.taskid))
            #print inurllist
            #print outurllist
            #print destendpoint
            if not all_same(destendpoint):
                print "All the pid should be mapped to the same GO endpoint"
                sys.exit(1)
            #endpoint = urlendpoint[destendpoint[0]]
            #print destendpoint[0]
            for url, ep in urlendpoint.items():
                if ep == destendpoint[0]:
                    endpoint=url
            if endpoint=="":
                print "The server "+destendpoint[0]+" is not mapped to a GO enpoint in datastagerconfig" 
                sys.exit(0)

            #print endpoint
            fo = open("pid.file", "w").close
            for url in outurllist:
                plainurl = url.replace("//","/")
                argument = formatter("url","irods://"+endpoint+":1247"+plainurl)
                argument = formatter("url","\*"+plainurl)
                #print argument
                os.system(iPATH+'/irule -F PIDselecter.r '+argument+' | awk \'{print $2}\' >> pid.file')
            sys.exit(0)
        else:
            "You did not provide the taskid!"
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
