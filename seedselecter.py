#!/repo/home/userprod/proirod1/.pythonbrew/pythons/Python-2.7.2/bin/python

import sys, os
import argparse

import datetime
import getopt
import json
   
class Finder:
    def __init__(self):
        """ Proviam """
        #myEnv, status = getRodsEnv()
 #       self.conn, errMsg = rcConnect(myEnv.getRodsHost(), myEnv.getRodsPort(), myEnv.getRodsUserName(), myEnv.getRodsZone())
 #       status = clientLogin(self.conn)
        #self.path = myEnv.getRodsHome()
        
    def setPath(self,  newPath):
        self.path = newPath
        
    def getPath(self):
        return self.path

    def sdsPathBuilder(self,  location, time={}):
        # based on scicomp3 data structure (SDS)
        # year, network, station, channel, day of the year
        results = []       
        location_paths = []
        for net in location['network']:
            if len(location['network'][net]) > 0:
                for station in location['network'][net]:
                    #if len(location['network'][net][station]) > 0:
                    if location['network'][net][station] != "null" and len(location['network'][net][station]) > 0:
                        for ch in location['network'][net][station]:
                            location_paths.append(net+'/'+station+'/'+ch+'.D')
                    else:
                        location_paths.append(net+'/'+station)
            else:
                location_paths.append(net)
                
        if len(time) == 0:
            time = {str(datetime.datetime.now().year):[]}
        for year in time:
            if len(location_paths) > 0:
                for location_path in location_paths:
                    results.append(self.path + '/' + year + '/' + location_path)
            else:
                results.append(self.path + '/' + year)
            
        return results
        
    def close(self):
        self.conn.disconnect()

def seedselecter(p, y, net, st, ch, output):
    
    path = pat
    year = yea
    network = net
    station = sta
    channel = cha
    #path = p.parseForStr()
    #year = y.parseForStr()
    #network = net.parseForStr()
    #station = st.parseForStr()
    #channel = ch.parseForStr()
    
    finder = Finder()
    #print "original path: " + finder.getPath()
    if path: 
        finder.setPath(path)
    #    print "new path: " + finder.getPath()
    if not network or network == "null":
        print "missing network"
        sys.exit(2)
    location = {'network': {network:{}}}
    if station:
    #if station != "null":
        location['network'][network] = {station:[]}
    if channel:
        location['network'][network][station].append(channel) 
    
    if year:
        time = {year:[]}
        results = finder.sdsPathBuilder(location, time)
    else:
        results = finder.sdsPathBuilder(location)

    #json_results=json.dumps(str(results))
    new_results=[]
    for p in results:
        p=p+"/"
        new_results.append(p)
    json_results=json.dumps(new_results)
    print json_results
    #print results
    #fillStrInMsParam(output, "** " + json_results + " **")
    
    #json_results = '["/CINECA/home/rods/archive/2012/AC/PUK/BHE.D\"]'

    #myEnv, status = getRodsEnv()
    #conn, errMsg = rcConnect(myEnv.getRodsHost(), myEnv.getRodsPort(), myEnv.getRodsUserName(), myEnv.getRodsZone())
    #status = clientLogin(conn)   
    #path = myEnv.getRodsHome() + '/json_file'
     
# Open a file for writing
    #f = iRodsOpen(conn, path, 'w')  
    #f=open('/devel/products/irods/3rdparty/DataStaging/tmp_json_file','w')
    #f=open('/home/jack/test/tmp_json_file','w')
    #f.write(json_results)
    #f.write(json_results)

    #f.close()
    
    #f.delete()
     
    #conn.disconnect()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=" Seed selector: select a bounch of seeds ")
    parser.add_argument("-p", "--path", help="the path of your iRODS collection",
        action="store", dest="pat")
    parser.add_argument("-y", "--year", help="the year of interest",
        action="store", dest="yea")
    parser.add_argument("-n", "--network", help="the network of interest",
        action="store", dest="net")
    parser.add_argument("-c", "--channel", help="the channel of interest",
        action="store", dest="cha")
    parser.add_argument("-s", "--station", help="the station of interest",
        action="store", dest="sta")
    arguments = parser.parse_args()
    # Example: print arguments.year
    output = ''
    if arguments.pat:
        pat = arguments.pat
    if arguments.yea:
        yea = arguments.yea
    if arguments.net:
        net = arguments.net
    if arguments.cha:
        cha = arguments.cha
    if arguments.sta:
        sta = arguments.sta
 
    #os.system('clear')

    seedselecter(pat, yea, net, sta, cha, output)
