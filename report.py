import urllib.parse, urllib.request
import ssl
import csv
import datetime
import re
import argparse

ssl._create_default_https_context = ssl._create_unverified_context

def retrieve_csv(resourcesreporting, start_date, end_date, filename = "output"):
    url = 'https://bigpanda.cern.ch/api/grafana?table=completed&groupby=dst_experiment_site,error_category&field=sum_count&dst_cloud=UK&jobstatus=&jobstatus=finished%7Cfailed%7Ccancelled&resourcesreporting='
    url += resourcesreporting + '&date_from=' + start_date + '%2000:00:00&date_to=' + end_date + '%2000:00:00&export=csv'
    print("Reading from " + url)
    urllib.request.urlretrieve(url,filename+ '.csv')

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



def analyse_csv(filename, start_date, end_date):
    pilot_regex = re.compile('^Pilot')
    #get_put_regex = re.compile('.*Get error|.*get error|.*Put error|.*put error')
    get_put_regex = re.compile('Get error|get error|Put error|put error')
    
    
    total_completed = {}
    pilot_errors = {}
    get_put_errors = {}
    with open(filename + '.csv', 'r') as filein:
        reader = csv.reader(filein, delimiter=';', quotechar='"')
        rows = [row for row in reader if row[0] != 'dst_experiment_site']
        errors =set([row[1] for row in rows if row[1] != 'No Error'])
        
    
        pilot_error_types = [error for error in errors if pilot_regex.search(error)]
        get_put_error_types = [error for error in errors if get_put_regex.search(error)]
        
        #sites first
        for row in rows:
            site = site_name_special_case(row[0])
            assert site in sites, "%r not in UK cloud site list" % site
            total_completed[site] = total_completed.get(site, 0) + int(row[2])
            if row[1] == "":
                print("Unclassified error state: " + site + " " + row[2])
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

            
                
    with open(filename + "_summed.csv", "w") as output:
        output.write(filename + " " + start_date + " - " + end_date + "\n")        
        output.write("site, total, pilot errors, get/put errors\n")
        output.write("UK-T1-RAL" + ", " + str(total_completed.get('RAL-LCG2',0)) + ", " + str(pilot_errors.get('RAL-LCG2',0)) + ", " + str(get_put_errors.get('RAL-LCG2',0)))
        output.write("\n")
        for fed in sorted(federations):
            for site in sites:
                if fed in site: 
                    output.write(site + ", " + str(total_completed.get(site,0)) + ", " + str(pilot_errors.get(site,0)) + ", " + str(get_put_errors.get(site,0)))
                    output.write("\n")
            output.write(fed + ", " + str(total_completed.get(fed,0)) + ", " + str(pilot_errors.get(fed,0)) + ", " + str(get_put_errors.get(fed,0)))
            output.write("\n")
 
    print(open(filename +"_summed.csv").read())
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start',
    type = str,
    help ='start date',
    default = "03.05.2019"
    )
    parser.add_argument('--end',
    type = str,
    help ='start date',
    default = "01.07.2019"
    )
    args = parser.parse_args()
    start_date = datetime.datetime.strptime(args.start, '%d.%m.%Y')
    end_date = datetime.datetime.strptime(args.end, '%d.%m.%Y')
    assert end_date > start_date, "End date must be after start date"
    print("Reading data from " + args.start + " to " + args.end)

    retrieve_csv("Analysis", args.start, args.end, "analysis")
    retrieve_csv("Data%20Calib%5C%2FMonit%7CData%20Processing%7CEvent%20Index%7CGroup%20Production%7CMC%20Reconstruction%7CMC%20Simulation%7COthers", args.start, args.end, "production")
    
    analyse_csv("analysis", args.start, args.end)
    analyse_csv("production", args.start, args.end)

if __name__ == "__main__":
    main()
