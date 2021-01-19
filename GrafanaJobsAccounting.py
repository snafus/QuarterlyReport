import pandas as pd
from datetime import datetime
from json import loads

from requests import post

import configparser
config = configparser.ConfigParser()
config.read('config.cfg')

bearer_token = config['monit_9559']['Bearer']

headers = {}

headers['Authorization'] = 'Bearer {}'.format(bearer_token)
headers['Content-Type'] = 'application/json'
headers['Accept'] = 'application/json'

base = "https://monit-grafana.cern.ch"
url = "api/datasources/proxy/9559/_msearch"


date_from = datetime.strptime('01.07.2020 00:00:00', '%d.%m.%Y %H:%M:%S')
date_to =  datetime.strptime('01.10.2020 00:00:00', '%d.%m.%Y %H:%M:%S')

path = "./"
filename = "uk_report"

date_from_ms = str(int(date_from.timestamp() * 1000))
date_to_ms   = str(int(date_to.timestamp() * 1000))

time_range = """"gte":{0},"lte":{1}""".format(date_from_ms, date_to_ms)

#query_errors = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_atlasjm_agg_completed*"],"max_concurrent_shard_requests":256}\n{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"""+time_range+""","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"(data.error_category:* AND NOT data.error_category:\\\"No Error\\\") AND data.dst_experiment_site:* AND data.dst_cloud:(\\\"UK\\\") AND data.dst_country:* AND data.dst_federation:* AND data.adcactivity:* AND data.resourcesreporting:* AND data.actualcorecount:* AND data.resource_type:* AND data.workinggroup:* AND data.inputfiletype:* AND data.eventservice:* AND data.inputfileproject:* AND data.outputproject:* AND data.jobstatus:(\\\"failed\\\" OR \\\"finished\\\" OR \\\"cancelled\\\") AND data.computingsite:* AND data.gshare:* AND data.dst_tier:* AND data.processingtype:* AND ((NOT _exists_:data.nucleus) OR (data.nucleus:*)) AND ((NOT _exists_:data.prodsourcelabel) OR (data.prodsourcelabel:*))"}}]}},"aggs":{"sites":{"terms":{"field":"data.dst_experiment_site","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"errors":{"terms":{"field":"data.resourcesreporting","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"nerrors":{"sum":{"field":"data.sum_count","missing":0}}}}}}}}\n"""

#query_noerrors = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_atlasjm_agg_completed*"],"max_concurrent_shard_requests":256}\n{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"""+time_range+""","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"( data.jobstatus:(\\\"finished\\\") ) AND data.dst_experiment_site:* AND data.dst_cloud:(\\\"UK\\\") AND data.dst_country:* AND data.dst_federation:* AND data.adcactivity:* AND data.resourcesreporting:* AND data.actualcorecount:* AND data.resource_type:* AND data.workinggroup:* AND data.inputfiletype:* AND data.eventservice:* AND data.inputfileproject:* AND data.outputproject:* AND data.jobstatus:(\\\"finished\\\") AND data.computingsite:* AND data.gshare:* AND data.dst_tier:* AND data.processingtype:* AND ((NOT _exists_:data.nucleus) OR (data.nucleus:*)) AND ((NOT _exists_:data.prodsourcelabel) OR (data.prodsourcelabel:*))"}}]}},"aggs":{"sites":{"terms":{"field":"data.dst_experiment_site","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"errors":{"terms":{"field":"data.resourcesreporting","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"nerrors":{"sum":{"field":"data.sum_count","missing":0}}}}}}}}\n"""

#q = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_atlasjm_agg_completed*"],"max_concurrent_shard_requests":256}\n{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"""+time_range+""","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"(data.error_category:*) AND data.dst_experiment_site:* AND data.dst_cloud:(\\\"UK\\\") AND data.dst_country:* AND data.dst_federation:* AND data.adcactivity:* AND data.resourcesreporting:(\\\"Analysis\\\") AND data.actualcorecount:* AND data.resource_type:* AND data.workinggroup:* AND data.inputfiletype:* AND data.eventservice:* AND data.inputfileproject:* AND data.outputproject:* AND data.jobstatus:(\\\"failed\\\" OR \\"finished\\\" OR \\\"cancelled\\\") AND data.computingsite:* AND data.gshare:* AND data.dst_tier:* AND data.processingtype:* AND ((NOT _exists_:data.nucleus) OR (data.nucleus:*)) AND ((NOT _exists_:data.prodsourcelabel) OR (data.prodsourcelabel:*))"}}]}},"aggs":{"sites":{"terms":{"field":"data.dst_experiment_site","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"sites":{"terms":{"field":"data.jobstatus","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"nerrors":{"sum":{"field":"data.sum_count","missing":0}}}}}}}}\n"""

#q = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_atlasjm_agg_completed*"],"max_concurrent_shard_requests":256}\n{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"""+time_range+""","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"data.dst_cloud:(\\\"UK\\\")"}}]}},"aggs":{"sites":{"terms":{"field":"data.dst_experiment_site","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"sites":{"terms":{"field":"data.jobstatus","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"nerrors":{"sum":{"field":"data.sum_count","missing":0}}}}}}}}\n"""




query_analysis = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_atlasjm_agg_completed*"],"max_concurrent_shard_requests":256}\n{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"""+time_range+""","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"(data.error_category:*) AND data.dst_experiment_site:* AND data.dst_cloud:(\\\"UK\\\") AND data.dst_country:* AND data.dst_federation:* AND data.adcactivity:* AND data.resourcesreporting:(\\\"Analysis\\\") AND data.actualcorecount:* AND data.resource_type:* AND data.workinggroup:* AND data.inputfiletype:* AND data.eventservice:* AND data.inputfileproject:* AND data.outputproject:* AND data.jobstatus:(\\\"failed\\\" OR \\"finished\\\" OR \\\"cancelled\\\") AND data.computingsite:* AND data.gshare:* AND data.dst_tier:* AND data.processingtype:* AND ((NOT _exists_:data.nucleus) OR (data.nucleus:*)) AND ((NOT _exists_:data.prodsourcelabel) OR (data.prodsourcelabel:*))"}}]}},"aggs":{"sites":{"terms":{"field":"data.dst_experiment_site","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"errors":{"terms":{"field":"data.error_category","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"nerrors":{"sum":{"field":"data.sum_count","missing":0}}}}}}}}\n"""


query_production = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_atlasjm_agg_completed*"],"max_concurrent_shard_requests":256}\n{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"""+time_range+""","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"(data.error_category:*) AND data.dst_experiment_site:* AND data.dst_cloud:(\\\"UK\\\") AND data.dst_country:* AND data.dst_federation:* AND data.adcactivity:* AND data.resourcesreporting:(\\\"Group Production\\\" OR \\\"Data Processing\\\" OR \\\"MC Reconstruction\\\" OR \\\"MC Merge\\\" OR \\\"MC Simulation\\\") AND data.actualcorecount:* AND data.resource_type:* AND data.workinggroup:* AND data.inputfiletype:* AND data.eventservice:* AND data.inputfileproject:* AND data.outputproject:* AND data.jobstatus:(\\\"failed\\\" OR \\"finished\\\" OR \\\"cancelled\\\") AND data.computingsite:* AND data.gshare:* AND data.dst_tier:* AND data.processingtype:* AND ((NOT _exists_:data.nucleus) OR (data.nucleus:*)) AND ((NOT _exists_:data.prodsourcelabel) OR (data.prodsourcelabel:*))"}}]}},"aggs":{"sites":{"terms":{"field":"data.dst_experiment_site","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"errors":{"terms":{"field":"data.error_category","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"nerrors":{"sum":{"field":"data.sum_count","missing":0}}}}}}}}\n"""



def run_query(query):
    report = []

    request_url = "%s/%s" % (base, url)
    r = post(request_url, headers=headers, data=query)

    if r.ok:
        sites = loads(r.text)['responses'][0]['aggregations']['sites']['buckets']
        #print(sites)
        for site in sites:
            errors = site['errors']['buckets']
            for error in errors:
                report.append({'site': site['key'], 'error': error['key'], 'value': int(error['nerrors']['value'])})
    else:
        print(r.status_code)
        print(r.text)
        r.raise_for_status()

    df = pd.DataFrame(report)
    return df , sites


#df_errors  ,s1 = run_query(query_errors)
#df_noerrors,s2 = run_query(query_noerrors) 

#print(df_errors)
#print(df_noerrors)

#df = pd.merge(df_errors.set_index(['site','error']),df_noerrors.set_index(['site','error']),
#            left_index=True,right_index=True)
#df.columns = ['failed','completed']
#df.sort_index(inplace=True)

#df.to_csv('{0}/{1}_.csv'.format(path, filename))


df_analysis, s1 = run_query(query_analysis)

df_production, s2 = run_query(query_production)

df_analysis  .to_csv('{0}/{1}_analysis_.csv'  .format(path, filename))
df_production.to_csv('{0}/{1}_production_.csv'.format(path, filename))