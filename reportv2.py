# Generate Jobs accounting reports using Aleksendr script for ElasticSearch storage
# Updates Stewart's script from original reporting

# supply as input the two csv files:
# python reportv2.py analysis.csv production.csv

import sys,glob,re


import uksites
import pandas as pd
from pandas import DataFrame,Series

def csv_to_df(csvfname):
    df = pd.read_csv(csvfname)

    sites = list(df.site.unique())

    re_getput = re.compile('Get error|get error|Put error|put error')
    re_pilot  = re.compile('^Pilot')
    noerror = 'No Error'
    getput_errors = [x for x in df['error'].unique() if re_getput.search(x)]
    pilot_errors  = [x for x in df['error'].unique() if re_pilot .search(x)]
    other_errors = [x for x in df['error'].unique() if ((x not in getput_errors) and (x not in pilot_errors)  and  (x != noerror)) ]

    raw = {}
    for site in sites:
        df_site = df[df['site'] == site]
        completed = float(df_site[df_site['error'] == 'No Error'].value)
        get_put   = float(df_site[df_site.error.isin(getput_errors)].value.sum())
        pilot     = float(df_site[df_site.error.isin(pilot_errors) ].value.sum())
        other     = float(df_site[df_site.error.isin(other_errors) ].value.sum())
        #raw[site] = {'Completed':completed, 'Pilot Error':pilot, 'Get/Put Error':get_put,'Other Errors':other}
        raw[site] = {'Completed':completed, 'Pilot Error':pilot, 'Get/Put Error':get_put}

    df2 = DataFrame.from_dict(raw).transpose().sort_index()
    df2['Success Rate']      = df2['Completed'] / (df2['Completed'] + df2['Get/Put Error'] + df2['Pilot Error'])
    df2['Data availability'] = df2['Completed'] / (df2['Completed'] + df2['Get/Put Error'])
    return df2


dfa = None
dfp = None

def build_summary_df(df):
    dftmp = df.copy().transpose()
    df2   =  dftmp.get('RAL-LCG2',0)
    df_t2 = None
    for site in uksites.accountable_sites_t2:
        if df_t2 is None:
            df_t2 = dftmp.get(site,0)
        else:
            df_t2 += dftmp.get(site,0)
    df_all = df2 + df_t2
    df_out = pd.concat([df2,df_t2,df_all],axis=1)
    df_out.columns = ['RAL-LCG2','Tier-2s','Total']
    df_out = df_out.transpose()
    df_out['Success Rate']      = df_out['Completed'] / (df_out['Completed'] + df_out['Get/Put Error'] + df_out['Pilot Error'])
    df_out['Data availability'] = df_out['Completed'] / (df_out['Completed'] + df_out['Get/Put Error'])
    return df_out

def build_combined_summary(df_ana,df_prod):
    df2 = df_ana + df_prod
    df2['Success Rate']      = df2['Completed'] / (df2['Completed'] + df2['Get/Put Error'] + df2['Pilot Error'])
    df2['Data availability'] = df2['Completed'] / (df2['Completed'] + df2['Get/Put Error'])
    return df2


def main():
    analysis   = sys.argv[1]
    production = sys.argv[2]

    df_analysis = csv_to_df(analysis)
    df_prod     = csv_to_df(production)
    df_analysis.index.name = "Analysis"
    df_prod    .index.name = "Production"
    print(df_analysis)
    print(df_prod)

    df_analysis_summary = build_summary_df(df_analysis)
    df_prod_summary     = build_summary_df(df_prod)
    df_analysis_summary.index.name = "Analysis"
    df_prod_summary    .index.name = "Production"
    print(df_analysis_summary)
    print(df_prod_summary)

    df_analysis.to_csv("analysis.csv")
    df_analysis_summary.to_csv("analysis_summary.csv")
    df_prod.to_csv("production.csv")
    df_prod_summary.to_csv("production_summed.csv")
    
    df_comb =  build_combined_summary(df_analysis_summary, df_prod_summary )
    df_comb.index.name = "Combined"
    print(df_comb)
    df_comb.to_csv("combined.csv")


if __name__ == "__main__":
    main()



