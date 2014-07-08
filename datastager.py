#!/usr/bin/env python
import sys, os, subprocess
import argparse
from argparse import RawTextHelpFormatter
import ConfigParser

import datetime
import json
import string,re,csv
from threading import Thread

import datamover

# Write y as an argument for iRODS in wich a is the variable and b the value
def formatter(a,x):
    y='"*'+a+'=\\"'+x+'\\"" '
    return y

# Chech if the arguments of an array differ
def all_same(items):
    return all(x == items[0] for x in items)

# Get the url given the PID using DSSfile
def DSSfileURLfromPID(argument):
    #print "DSSfileURLfromPID -> argument: "+argument
    f = open('.DSSfile', 'r')
    strings = re.findall(r'.+,%s' % argument , f.read())
    f.close()
    if not strings:
        print "The PID "+argument+" is not in .DSSfile"
        sys.exit(1)
    #print strings[0]    
    url = strings[0].split(',')
    #print url[0]
    f = open('json_file', 'a')
    f.write("Output "+url[0]+"\n")
    f.close()

# Get the url given the PID using icommands
def iURLfromPID(argument):
    #print "iURLfromPID -> argument: "+argument
    os.system(iPATH+'/irule -F URLselecter.r '+argument+' >> json_file')

# Get the PID given the URL using DSSfile
def DSSfilePIDfromURL(argument):
    print "DSSfilePIDfromURL -> argument: "+argument
    f = open('.DSSfile', 'r')
    real_url=argument.split('=')
    print "DSSfilePIDfromURL -> argument: "+real_url[1]
    strings = re.findall(r'(.+)%s(.+)' % real_url[1] , f.read())
    argument=string.strip(real_url[1],"i") 
    strings = re.findall(r'(.+)%s(.+)' % argument , f.read())
    f.close()
    if not strings:
        print "The URL "+argument+" is not in .DSSfile"
        sys.exit(1)
    #print strings[0]    
    pid = strings[0].split(',')
    #print url[0]
    f = open('json_file', 'a')
    f.write("Output "+pid[1]+"\n")
    f.close()

# Get the PID given the URL using icommands
def iPIDfromURL(argument):
    #print "iPIDfromURL -> argument: "+argument
    os.system(iPATH+'/irule -F PIDselecter.r '+argument+' >> json_file')

# Write the PID to pid.file using icommands
def iPIDtoPIDFILE(argument):
    #print "iPIDtoPIDFILE -> argument: "+argument
    os.system(iPATH+'/irule -F PIDselecter.r '+argument+' | awk \'{print $2}\' >> pid.file')

# Read json_file, extract and write to json_file the json list of file to be 
# transferred and return the source endpoint. 
def jsonformatter():
# Read the string from the file 
    fo = open("json_file", "r")
    strglist = fo.readlines();
    fo.close()
# Empty the file    
    open("json_file", 'w').close()
# Format the string    
# Each element should of the form:
# 
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

# Write to json_file (via jsonformatter) the list of url for the given dest endpoint. 
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
        iURLfromPID(argument)
        sslist=jsonformatter()
        return sslist[0]
    elif arguments.pidfile:
        try: 
            fo = open(arguments.pidfile, "r")
            pidlist = fo.readlines();
            fo.close()
        except:
            fo = open(arguments.pidfile, "rw")
            pidlist = fo.readlines();
            fo.close()
# Empty the file    
        open("json_file", 'w').close()
# Create and start the thread list to call iURLfromPID in parallel
        threadlist=[]
        for pid in pidlist:
            argument=formatter("pid",pid.rstrip())
            #print argument
            T=Thread(target=iURLfromPID,args=([argument]))
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

# Write to json_file (via jsonformatter) the list of url for the given dest
# endpoint looking in DSSfile. 
def DSSfile_pidsource(arguments):
# Empty the file    
    open("json_file", 'w').close()
    if arguments.pid:
        argument=arguments.pid.rstrip()
        DSSfileURLfromPID(argument)
        print "The URL is in json_file"  
        sslist=jsonformatter()
        return sslist[0]
    elif arguments.pidfile:
        fo = open(arguments.pidfile, "r")
        pidlist = fo.readlines();
        fo.close()
# Empty the file    
        open("json_file", 'w').close()
# Create and start the thread list to call iURLfromPID in parallel
        threadlist=[]
        for pid in pidlist:
            argument=pid.rstrip()
            T=Thread(target=DSSfileURLfromPID,args=([argument]))
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


# Write to json_file (via jsonformatter) the list of url for the given dest
# endpoint using pidsource. 
def urlsource(arguments):
    if arguments.url:
        argument=formatter("url",arguments.url)
        print "URL: "+argument
        iPIDfromURL(argument)
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
# Create and start the thread list to call iPIDfromURL in parallel
        threadlist=[]
        for url in urllist:
            argument=formatter("url",url.rstrip())
            #print argument
            T=Thread(target=iPIDfromURL,args=([argument]))
            T.start()
            threadlist.append(T)
        for t in threadlist:
            t.join()
        print "All url(s) resolved to a pid."
        arguments.pidfile="pid.file"
        src_site=pidsource(arguments)
        return src_site
    else:
        print "You selected url so the url is required!"
        sys.exit(1)

# Write to json_file (via jsonformatter) the list of url for the given dest
# endpoint looking in DSSfile.
def DSSfile_urlsource(arguments):
    if arguments.url:
        argument=formatter("url",arguments.url)
        #print "URL: "+argument
        DSSfilePIDfromURL(argument)
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
# Create and start the thread list to call iPIDfromURL in parallel
        threadlist=[]
        for url in urllist:
            argument=formatter("url",url.rstrip())
            #print argument
            T=Thread(target=DSSfilePIDfromURL,args=([argument]))
            T.start()
            threadlist.append(T)
        for t in threadlist:
            t.join()
        print "All url(s) resolved to a pid."
        arguments.pidfile="pid.file"
        src_site=pidsource(arguments)
        return src_site
    else:
        print "You selected url so the url is required!"
        sys.exit(1)

def example():
    print """
Invoke as follow for stage out: 
----------------------------------------------------------------------------------    
./datastager.py out issue irods -p path  
                          -u GO-user 
                          --ss source-end-point --ds dest-end-point --dd dest-dir
./datastager.py out issue pid --pid prefix/pid 
                        -u GO-user 
                        --ds dest-end-point --dd dest-dir
./datastager.py out issue url --url full-url
                        -u GO-user 
                        --ds dest-end-point --dd dest-dir

----------------------------------------------------------------------------------    
or as follow for stage in:
./datastager.py in issue -u GO-user 
                        --ss source-end-point --sd source-dir -p path
                        --ds dest-end-point --dd dest-dir
./datastager.py in pid --taskid the-taskID-of-the-processr-you-want-thepid 
                       -u GO-user 

----------------------------------------------------------------------------------    
or as follow for cancel the stage (in or out) operation or get details about it:
./datastager.py in details --taskid the-taskID-of-the-process-you-want-details 
                       -u GO-user 
./datastager.py out cancel --taskid the-taskID-of-the-process-you-want-to-cancel 
                       -u GO-user 
    """
    sys.exit(0)

# Main program
def main(arguments=None):
    if arguments is None:
        arguments = sys.argv
# Top-level parser
    parser = argparse.ArgumentParser(description=" Data stager: move a bounce of data inside or outside iRODS via GridFTP. \n The -d options requires both positional arguments.", formatter_class=RawTextHelpFormatter,add_help=True)
    parser.add_argument("-e", "--example", 
            help="a longer description and some usage examples (invoke with \"datastager.py in pid -d\")", 
            action="store_true") # Examples
    parser.add_argument("--verbose", help="more informations at run time", 
            action="store_true")
#config file
    # Turn off help, so we print all options in response to -h
    parser_cfg_file = argparse.ArgumentParser( add_help=False)
    parser_cfg_file.add_argument("-c", "--cfg_file",
                    help="Specify config file", metavar="FILE", default="datastager.cfg")
    arguments, from_file_args = parser_cfg_file.parse_known_args()    
    if arguments.cfg_file:
        print "Reading config file..."
        config = ConfigParser.SafeConfigParser()
        config.read([arguments.cfg_file])
        defaults = dict(config.items("Defaults"))
        #print defaults
    else:
        defaults = { "option":"default" }
#
# Top-level credentials    
#
    parser.set_defaults(**defaults)
    parser.add_argument("-u", "--username", 
            help="your username on globusonline.org", action="store", dest="user")
    parser.add_argument("-cert", "--certificate", 
            help="your x509 certificate (pem file)", action="store", dest="cert")
    parser.add_argument("-key", "--secretekey", 
            help="the key of your certificate", action="store", dest="key")
    parser.add_argument("-certdir", "--trustedca", 
            help="your trusted CA", action="store", dest="certdir")
# Create the subparser    
    subparsers = parser.add_subparsers(help='Directional sub-command help')
#
# Parser for the "in" direction
#
    subparser_in = subparsers.add_parser('in', help='Stage _in_ is used when moving data into EUDAT.')
    subparser_in.set_defaults(direction='in')
    subparsers_in = subparser_in.add_subparsers(help='Stage _in_ sub-command help')
    subparser_in_issue   = subparsers_in.add_parser('issue',   help='To issue a transfer.')
    subparser_in_issue.set_defaults(action='issue')
    subparser_in_pid     = subparsers_in.add_parser('pid',     help='To retrieve the PIDs associated to the files you transfereed.')
    subparser_in_pid.set_defaults(action='pid')
    subparser_in_details = subparsers_in.add_parser('details', help='To know the status of a transfer.')
    subparser_in_details.set_defaults(action='details')
    subparser_in_cancel  = subparsers_in.add_parser('cancel',  help='To cancel a transfer.')
    subparser_in_cancel.set_defaults(action='cancel')
#issue       
    subparser_in_issue.add_argument("-p", "--path", 
            help="the path of your file (iRODS collection or local file system depending on the circumstances)", action="store", dest="path")
    subparser_in_issue.add_argument("-pF", "--pathFile", 
            help="a file containing the path of your file (iRODS collection or local file system depending on the circumstances)", 
            action="store", dest="pathfile")
    subparser_in_issue.add_argument("--ss", 
            help="the GridFTP src server as GO endpoint", action="store", dest="src_site", default="irods-dev")
    subparser_in_issue.add_argument("--sd", 
            help="the GridFTP src directory", action="store", dest="src_dir", default="/~/")
    subparser_in_issue.add_argument("--ds", 
            help="the GridFTP dst server as GO endpoint", action="store", dest="dst_site", default="GSI-PLX")
    subparser_in_issue.add_argument("--dd", 
            help="the GridFTP dst directory", action="store", dest="dst_dir", default="/~/")
#pid     
    subparser_in_pid.add_argument("-t", "--taskid", 
            help="the taskID of your transfer", action="store", dest="taskid")
    subparser_in_pid.add_argument("-RM", "--resolve-mode", 
            help="the way you resolve for source file: iRODS or DSSfile", action="store", dest="rmode")
#details 
    subparser_in_details.add_argument("-t", "--taskid", 
            help="the taskID of your transfer", action="store", 
            dest="taskid", required="true")
#cancel  
    subparser_in_cancel.add_argument("-t", "--taskid", 
            help="the taskID of your transfer", action="store", 
            dest="taskid", required="true")
#
# Parser for the "out" direction
#
    subparser_out = subparsers.add_parser('out', help='Stage _out_ is used when moving data outside EUDAT.')
    subparser_out.set_defaults(direction='out')
    subparsers_out = subparser_out.add_subparsers(help='Stage _out_ sub-command help')
    subparser_out_issue   = subparsers_out.add_parser('issue',   help='To issue a transfer.')
    subparser_out_issue.set_defaults(action='issue')
    subparser_out_details = subparsers_out.add_parser('details', help='To know the status of a transfer.')
    subparser_out_details.set_defaults(action='details')
    subparser_out_cancel  = subparsers_out.add_parser('cancel',  help='To cancel a transfer.')
    subparser_out_cancel.set_defaults(action='cancel')
#issue       
    subparsers_out_issues = subparser_out_issue.add_subparsers(help='Stage _out issue_ sub-command help')
    subparser_out_issue_pid   = subparsers_out_issues.add_parser('pid', help='Select data by PIDs')
    subparser_out_issue_pid.set_defaults(sub_action='pid')
    subparser_out_issue_url   = subparsers_out_issues.add_parser('url', help='Select data by URLs')
    subparser_out_issue_url.set_defaults(sub_action='url')
    subparser_out_issue_irods = subparsers_out_issues.add_parser('irods', help='Select data by iRODS URLs')
    subparser_out_issue_irods.set_defaults(sub_action='irods')
#issue -> pid      
    subparser_out_issue_pid.add_argument("-P", "--pid", 
            help="the PID of your data", action="store", dest="pid")
    subparser_out_issue_pid.add_argument("-PF", "--pid-file", 
            help="the file listing the PID(s) of your data", action="store", dest="pidfile")
    subparser_out_issue_pid.add_argument("-RM", "--resolve-mode", 
            help="the way you resolve for source file: iRODS or DSSfile", action="store", dest="rmode")
#issue -> url      
    subparser_out_issue_url.add_argument("-U", "--url", 
            help="the URL of your data", action="store", dest="url")
    subparser_out_issue_url.add_argument("-UF", "--urlfile", 
            help="the file listing the URL(s) of your data", action="store", dest="urlfile")
    subparser_out_issue_url.add_argument("-RM", "--resolve-mode", 
            help="the way you resolve for source file: iRODS or DSSfile", action="store", dest="rmode")
#issue -> irods
    subparser_out_issue_irods.add_argument("-p", "--path", 
            help="the path of your file (iRODS collection)", action="store", dest="path")
    subparser_out_issue_irods.add_argument("-RM", "--resolve-mode", 
            help="the way you resolve for source file: iRODS or DSSfile", action="store", dest="rmode")
    subparser_out_issue_irods.add_argument("--ss", 
            help="the GridFTP src server as GO endpoint", action="store", dest="src_site")
    subparser_out_issue_irods.add_argument("--sd", 
            help="the GridFTP src directory", action="store", dest="src_dir", default="/~/")
#issue -> destination
    subparser_out_issue.add_argument("--ds", 
            help="the GridFTP dst server as GO endpoint", action="store", dest="dst_site", required="true")
    subparser_out_issue.add_argument("--dd", 
            help="the GridFTP dst directory", action="store", dest="dst_dir", default="/~/", required="true")
#details 
    subparser_out_details.add_argument("-t", "--taskid", 
            help="the taskID of your transfer", action="store",
            dest="taskid", required="true")
#cancel  
    subparser_out_cancel.add_argument("-t", "--taskid", 
            help="the taskID of your transfer", action="store", 
            dest="taskid", required="true")
#
# get everything     
#

    arguments = parser.parse_args(from_file_args)
    if arguments.verbose: print arguments

# Invoke the detailed help if required
    if arguments.example:
        example()

##################################################################################
# Start the execution
##################################################################################
    #os.system('clear')
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
    if arguments.action=="cancel":
        print "The transfer activity corresponding to task %s is going to be cancelled." % arguments.taskid
        api = None
        inurllist, outurllist, destendpoint = datamover.canceltask(str(arguments.user), str(arguments.taskid))
        sys.exit(0)
    if arguments.action=="details":
        print "The transfer activity corresponding to task %s follows." % arguments.taskid 
        api = None
        #urlendpoint = datamover.defineurlendpoint(str(arguments.user))
        #print urlendpoint
        datamover.detailsoftask(str(arguments.user), str(arguments.taskid))
        sys.exit(0)
    
    
# Stage out
    if arguments.direction=="out":
        print "----------------HERE---------------"
        if arguments.sub_action == "irods":
            if arguments.path and arguments.pathfile:
                print "Only one between -p and -pF is allowed!"
                sys.exit(1)
            irodssource(arguments)
            print "Source end-point: "+arguments.src_site
            #sys.exit(1) 
        elif arguments.sub_action == "url":
            if arguments.url and arguments.urlfile:
                print "Only one between -U and -UF is allowed!"
            sys.exit(1)
            if arguments.pidmode == "DSSfile":
                print "Using .DSSfile"
                arguments.src_site=DSSfile_urlsource(arguments)
            elif arguments.pidmode == "icommands":
                arguments.src_site=urlsource(arguments)
            print "Source end-point: "+arguments.src_site
            #sys.exit(1) 
        elif arguments.sub_action == "pid":
            if arguments.pid and arguments.pidfile:
                print "Only one between -P and -PF is allowed!"
                sys.exit(1)
            if not arguments.pidmode:
                print "The pidmode (-m) is mandatory!"
                sys.exit(1)
            if arguments.pidmode == "DSSfile":
                print "Using .DSSfile"
                arguments.src_site=DSSfile_pidsource(arguments)
            elif arguments.pidmode == "icommands":
                arguments.src_site=pidsource(arguments)
                print "Source end-point: "+arguments.src_site
                #sys.exit(1) 
            else:
                print "You are staging out so you can only specify iRODS or PID or URL!"
                sys.exit(1)
        

# Stage in 
    if arguments.direction == "in":
        if arguments.action == "issue":
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
        elif arguments.action == "pid":
            if not arguments.pidmode:
                print "The pidmode (-m) is mandatory!"
                sys.exit(1)
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
                    #print url,ep,arguments.user+"#"+destendpoint[0]
                    if ep == arguments.user+"#"+destendpoint[0]:
                        endpoint=url
                if endpoint=="":
                    print "The server "+destendpoint[0]+" is not mapped to a GO enpoint in datastagerconfig." 
                    sys.exit(0)
                #print endpoint
                fo = open("pid.file", "w").close
# Create and start the thread list to call iPIDfromURL in parallel
                if arguments.pidmode == "DSSfile":
                    print "The list of the corresponding PID is going to be saved in DSSfile."
                    print "Retrieving the DSSfile via GridFTP from "+str(endpoint)+" i.e. "+str(destendpoint[0])
                    file_list=[]
                    file_list.append(DSSfilePath)
                    json_results=json.dumps(file_list)
                    fo = open("json_file", "w")
                    fo.write(json_results)
                    fo.close()
                    datamover.mover(str(arguments.user), str(destendpoint[0]), str(GClocalhost), str(os.getcwd()))
                    with open('.DSSfile', mode='r') as infile:
                        reader = csv.reader(infile,)
                        DSSlist = {rows[0]:rows[1] for rows in reader if len(rows) == 2}
                        #print DSSlist
                    sys.exit(0)
                elif arguments.pidmode == "icommands":
                    print "The list of the corresponding PID is going to be saved in pid.file."
                    threadlist=[]
                    for url in outurllist:
                        plainurl = url.replace("//","/")
                        #argument = formatter("url","irods://"+endpoint+":1247"+plainurl)
                        argument = formatter("url","\*"+plainurl)
                        #print argument
                        T=Thread(target=iPIDtoPIDFILE,args=([argument]))
                        T.start()
                        threadlist.append(T)
                    for t in threadlist:
                        t.join()
                    print "All (available) pid(s) wrote in pid.file."
                    sys.exit(0)
            else:
                "You did not provide the taskid!"
                sys.exit(1)
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



##################################################################################
# If called directly
##################################################################################
if __name__ == '__main__':
    main()
