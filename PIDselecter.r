igetPIDrule {
    getPID(*url, *response);
    writeLine("stdout","Output: *response");
}

INPUT *url="irods://irods-dev.cineca.it:1248/CINECA/home/szasada/testPID/foofile"
OUTPUT ruleExecOut 
