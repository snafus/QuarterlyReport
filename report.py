import urllib.request
import ssl
import csv
import datetime
import re

#now = datetime.datetime.now().date()
#start_date_str = start_date.strftime('%d.%m.%Y')
#end_date_str = end_date.strftime('%d.%m.%Y')
ssl._create_default_https_context = ssl._create_unverified_context

def retrieve_csv(resourcesreporting, start_date, end_date, filename = "output"):
    url = 'https://bigpanda.cern.ch/api/grafana?table=completed&groupby=dst_experiment_site,error_category&field=sum_count&dst_cloud=UK&resourcesreporting' + resourcesreporting + '&date_from=' + start_date + '%2000:00:00&date_to=' + end_date + '%2000:00:00&export=csv'
    print("Reading from " + url)
#    urllib.request.urlretrieve(url,filename+ '.csv')


#url = 'https://bigpanda.cern.ch/api/grafana?table=completed&groupby=dst_experiment_site,error_category&field=sum_count&dst_cloud=UK&resourcesreporting=' + jobtype + '&date_from=' + start_date + '%2000:00:00&date_to=' + end_date + '%2000:00:00&export=csv'
#print(url)
##
#url = 'https://bigpanda.cern.ch/api/grafana?table=completed&groupby=dst_experiment_site,error_category&field=sum_count&dst_cloud=UK&resourcesreporting!=Analysis&date_from=' + start_date + '%2000:00:00&date_to=' + end_date + '%2000:00:00&export=csv'
#print(url)
#urllib.request.urlretrieve(url,'production_and_errors.csv')


federations = ['UKI-LT2', 'UKI-NORTHGRID', 'UKI-SCOTGRID', 'UKI-SOUTHGRID']
sites = ['RAL-LCG2',
#         'RAL-LCG2-ECHO',
         'UKI-LT2-Brunel',
         'UKI-LT2-IC-HEP',
         'UKI-LT2-QMUL',
         'UKI-LT2-RHUL',
         'UKI-LT2-UCL-HEP',
         'UKI-NORTHGRID-LANCS-HEP',
         'UKI-NORTHGRID-LIV-HEP',
         'UKI-NORTHGRID-MAN-HEP',
         'UKI-NORTHGRID-SHEF-HEP',
         'UKI-SCOTGRID-DURHAM',
         'UKI-SCOTGRID-ECDF',
#         'UKI-SCOTGRID-ECDF-RDF',
         'UKI-SCOTGRID-GLASGOW',
         'UKI-SOUTHGRID-BHAM-HEP',
         'UKI-SOUTHGRID-CAM-HEP',
         'UKI-SOUTHGRID-OX-HEP',
         'UKI-SOUTHGRID-RALPP',
         'UKI-SOUTHGRID-SUSX',]

def site_name_special_case(site):
    if site == 'UKI-SCOTGRID-ECDF-RDF':
        return 'UKI-SCOTGRID-ECDF'
    elif site == 'RAL-LCG2-ECHO':
        return 'RAL-LCG2'
    else: 
        return site



def analyse_csv(filename):
    pilot_regex = re.compile('Pilot|pilot')
    get_put_regex = re.compile('DDM')
    
    
    total_completed = {}
    pilot_errors = {}
    get_put_errors = {}
    with open(filename + '.csv', 'r') as fp:
        reader = csv.reader(fp, delimiter=';', quotechar='"')
        rows = [row for row in reader if row[0] != 'dst_experiment_site']
        #sites =set([row[0] for row in rows])
        errors =set([row[1] for row in rows if row[1] != 'No Error'])
        
    
        pilot_error_types = [error for error in errors if pilot_regex.match(error)]
        get_put_error_types = [error for error in errors if get_put_regex.match(error)]
        
        #sites first
        for row in rows:
            site = site_name_special_case(row[0])
            total_completed[site] = total_completed.get(site, 0) + int(row[2])
            if row[1] in pilot_error_types:
                pilot_errors[site] = pilot_errors.get(site, 0) + int(row[2])
            if row[1] in get_put_error_types:
                get_put_errors[site] = get_put_errors.get(site, 0) + int(row[2])
            for fed in federations:
                if fed in site:
                    total_completed[fed] = total_completed.get(fed, 0) + int(row[2])
                    if row[1] in pilot_error_types:
                        pilot_errors[fed] = pilot_errors.get(fed, 0) + int(row[2])
                    if row[1] in get_put_error_types:
                        get_put_errors[fed] = get_put_errors.get(fed, 0) + int(row[2])

            
                
    print(filename)        
    print("site, total, pilot errors, get/put errors")
    print("UK-T1-RAL" + ", " + str(total_completed.get('RAL-LCG2',0)) + ", " + str(pilot_errors.get('RAL-LCG2',0)) + ", " + str(get_put_errors.get('RAL-LCG2',0)))
    for fed in sorted(federations):
        for site in sites:
            if fed in site: 
                print(site + ", " + str(total_completed.get(site,0)) + ", " + str(pilot_errors.get(site,0)) + ", " + str(get_put_errors.get(site,0)))
        print(fed + ", " + str(total_completed.get(fed,0)) + ", " + str(pilot_errors.get(fed,0)) + ", " + str(get_put_errors.get(fed,0)))
    

start_date = "01.04.2019"
end_date = "01.07.2019"
retrieve_csv("=Analysis", start_date, end_date, "analysis")
retrieve_csv("!=Analysis", start_date, end_date, "production")
#retrieve_csv("=Data%20Calib%5C%2FMonit%7CData%20Processing%7CEvent%20Index%7CGroup%20Production%7CMC%20Reconstruction%7CMC%20Simulation%7COthers", start_date, end_date, "production")

analyse_csv("analysis")
analyse_csv("production")
