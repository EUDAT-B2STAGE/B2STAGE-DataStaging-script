igetSEEDrule{
    getSEED(*path, *year, *network, *channel, *station, *response);
    #writeLine("stdout","Output: *response");
    writeLine("stdout", *response);
}

INPUT *path="CINECA01",*year="2005",*network="IV",*channel="DOI",*station="null"
OUTPUT ruleExecOut 
#INPUT *pat="/CINECA01/home/EUDAT_EPOS/EPOSReplica/archive/"%*year="2005"%*network="IV"%*channel="DOI"%*station="BHE.D"
#*pat=here%*year=200%*network=lan%*station=filo%*channel=rai
