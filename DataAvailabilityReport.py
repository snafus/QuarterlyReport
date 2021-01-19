import sys
import uksites
import pandas as pd
from pandas import DataFrame,Series

inputfile_siteavail= sys.argv[1] if len(sys.argv)> 2 else 'WLCGAvailability.csv'

inputfile_totals = sys.argv[2] if len(sys.argv)> 2 else 'totals.csv' # total files held in each site

df_avail  = pd.read_csv(inputfile_siteavail).set_index('data.dst_experiment_site')
df_totals = pd.read_csv(inputfile_totals).set_index('Time')
df_totals = DataFrame(df_totals.iloc[-1])
df_totals.columns = ['Total']

sumw_incT1 = 0.
sumxw_incT1 = 0.
sumw = 0.
sumxw= 0.
for site in uksites.accountable_sites:
    print("Try", site)
    if site == 'UKI-NORTHGRID-SHEF-HEP':
        print("Ignoring UKI-NORTHGRID-SHEF-HEP")
        continue
    try:                
        w = df_totals.loc[site].values[0]
        x = df_avail.loc[site]['Average data.availability']
    except Exception as e:
        print(e)
        continue
    if site != "RAL-LCG2":
        sumxw+= w*x
        sumw += w
    sumxw_incT1 += w*x
    sumw_incT1  += w
print("Weighted average T2: {}\n\n".format(sumxw / sumw) )

print("Weighted average All: {}\n\n".format(sumxw_incT1 / sumw_incT1) )

ral = df_avail.loc['RAL-LCG2']['Average data.availability']
print('RAL avail: {}'.format(ral))

