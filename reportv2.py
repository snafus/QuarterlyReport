import pandas as pd
from datetime import datetime
from json import loads
import requests
from requests import post

headers = {}
path = "./"
filename = "report"
date_from =  '01.02.2021'  #datetime.strptime('01.07.2020 00:00:00', '%d.%m.%Y %H:%M:%S')
date_to   =  '08.02.2021'  #datetime.strptime('01.10.2020 00:00:00', '%d.%m.%Y %H:%M:%S')

#url_production = 'https://bigpanda.cern.ch/api/grafana?table=completed&groupby=dst_experiment_site,error_category&field=sum_count&jobstatus=finished%7Cfailed%7Ccancelled&resourcesreporting=Data%20Calib%5C%2FMonit%7CData%20Processing%7CEvent%20Index%7CGroup%20Production%7CMC%20Reconstruction%7CMC%20Simulation%7COthers&date_from={}%0000:00:01&date_to={}%0000:00:01&export=csv'.format(date_from, date_to)

url_analysis = 'https://bigpanda.cern.ch/api/grafana?table=completed&groupby=dst_experiment_site,error_category&field=sum_count&jobstatus=finished%7Cfailed%7Ccancelled&resourcesreporting=Analysis&date_from={}%0000:00:01&date_to={}%0000:00:01&export=csv'.format(date_from, date_to)

def run_query(query):
    report = []

    r = requests.get(query)
    
    if r.ok:
        q = r.json()
        sites = [x['key'] for x in q['dst_experiment_site']['buckets']]

        for site_data in [x for x in q['dst_experiment_site']['buckets']]:
            site_name = site_data['key']
            errors = site_data['error_category']['buckets']
            for error in errors:
                error_name  = error['key']
                error_count =  float(error['sum_count']['value'])
                report.append({'site': site_name, 'error': error_name, 'value': int(error_count)})

        # sites = loads(r.text)['responses'][0]['aggregations']['sites']['buckets']
        # for site in sites:
        #     errors = site['errors']['buckets']
        #     for error in errors:
        #         report.append({'site': site['key'], 'error': error['key'], 'value': int(error['nerrors']['value'])})
    else:
        print(r.status_code)
        print(r.text)
        r.raise_for_status()

    df = pd.DataFrame(report)
    return df, sites

df_analysis, s1 = run_query(url_analysis)
#df_production, s2 = run_query(url_production)

df_analysis  .to_csv('{0}/{1}_analysis_.csv'  .format(path, filename))
#df_production.to_csv('{0}/{1}_production_.csv'.format(path, filename))



