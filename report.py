import urllib.request
import ssl

start_date = "01.04.2019"
end_date = "01.07.2019"

ssl._create_default_https_context = ssl._create_unverified_context
urllib.request.urlretrieve('https://bigpanda.cern.ch/api/grafana?table=completed&groupby=dst_experiment_site&field=sum_count&dst_cloud=UK&resourcesreporting=Analysis&date_from=' + start_date + '%2000:00:00&date_to=' + end_date + '%2000:00:00&export=csv','analysis.csv')
urllib.request.urlretrieve('https://bigpanda.cern.ch/api/grafana?table=completed&groupby=dst_experiment_site&field=sum_count&dst_cloud=UK&resourcesreporting!=Analysis&date_from=' + start_date + '%2000:00:00&date_to=' + end_date + '%2000:00:00&export=csv','production.csv')
#print(url)
#urllib.request.urlretrieve(url,'pilot.csv')
