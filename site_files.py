import requests, os, sys, argparse,glob
import bz2
import urllib.request
import uksites

bz2.BZ2Decompressor()

def parseit():
    pass
    req = requests.get('https://rucio-hadoop.cern.ch/consistency_datasets?rse=UKI-NORTHGRID-LANCS-HEP_LOCALGROUPDISK&date=01-10-2020',stream=True,verify=False)
    oldbuffer=""
    #for chunk in req.raw.read_chunked(decode_content=bz2.BZ2Decompressor()):
    #    for s in oldbuffer.split('\n')
    #    oldbuffer += chunk.decode('UTF-8')


def get_dumps(rse='UKI-NORTHGRID-LANCS-HEP_LOCALGROUPDISK',date='dd-mm-yyyy'):
    params = {'rse':rse, date:date}
    req = requests.get('https://rucio-hadoop.cern.ch/consistency_datasets?',params=params,
            verify=False,)
    print(req.status_code)
    #l = bz2.BZ2Decompressor().decompress(req.raw).split('\n')
    print("len",len(req.text.split('\n')))
    return req.text.split('\n')


def get_uk_datadisksizes(date='01-10-2020'):
    res = {}
    for site in uksites.sites:
        print(site)
        try:
            rse = rse="{}_{}".format(site,'DATADISK')
            l   = get_dumps(rse=rse,date=date)
            res[rse] = len[l]
        except Exception as e:
            print(e)
            continue
    print(res)
    return res
