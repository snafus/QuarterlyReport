#!/usr/bin/env python 

# json is a list of entries of the form:
#  {u'BAD': 0,
#  u'DELETED': 0,
#  u'LOST': 0,
#  u'RECOVERED': 0,
#  u'SUSPICIOUS': u'<a href="/bad_replicas?rse=RAL-LCG2-ECHO_SCRATCHDISK&state=SUSPICIOUS">2</a>',
#  u'created_at': u'03 May 2017',
#  u'reason': u'Corrupted',
#  u'rse': u'RAL-LCG2-ECHO_SCRATCHDISK'},

import os,sys
inputfile  = sys.argv[1] if len(sys.argv) > 1 else "dump.json"
outputfile = ".".join(inputfile.strip().split("/")[-1].split(".")[:-1]) + ".tex"

import json, pprint, warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from bs4 import BeautifulSoup

import pandas as pd
from pandas import DataFrame,Series

sites = ["RAL-LCG2", "UKI-LT2-BRUNEL", "UKI-LT2-QMUL", "UKI-LT2-RHUL", "UKI-NORTHGRID-LANCS-HEP", "UKI-NORTHGRID-LIV-HEP", 
         "UKI-NORTHGRID-MAN-HEP", "UKI-NORTHGRID-SHEF-HEP", "UKI-SCOTGRID-DURHAM", "UKI-SCOTGRID-ECDF", "UKI-SCOTGRID-GLASGOW", 
         "UKI-SOUTHGRID-OX-HEP", "UKI-SOUTHGRID-RALPP"
        ]

with open(inputfile) as data_file:    
    data = json.load(data_file)

records = []
for site in sites:
    dataLoss = {"LOST" : 0, "RECOVERED" : 0}
    for entry in data:
        spacetoken = entry["rse"]
        if spacetoken.startswith(site):
             for k in dataLoss:
                 if isinstance(entry[k], int):
                     value = entry[k]
                 else:
                     soup = BeautifulSoup(entry[k],"html.parser")
                     soup.find_all('a')
                     value = int(soup.a.string)
#                 print(value)
                 dataLoss[k] = dataLoss[k] + value
    records.append( {'Site':site, 'LOST':dataLoss['LOST'], 'RECOVERED':dataLoss['RECOVERED']} )
    print (site)
    print (dataLoss)
    print ("**********************")


df = DataFrame.from_records(records,index='Site')
print(df.to_latex())
with open(outputfile,'w') as f:
    f.write(df.to_latex())

