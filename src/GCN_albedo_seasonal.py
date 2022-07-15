#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 07:08:54 2022

@author: jason
"""


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from glob import glob
from datetime import datetime
import os
base_dir = '/Users/jason/Dropbox/AWS/GCNET/GC-Net-level-1-data-processing/L1/'
files = sorted(glob(base_dir+'*'))
from os import path
import nead

n_stations=len(files)

sites_original_convention=[
# "gits",
"HUM",
"PET",
"TUNU-N",
# "swisscamp_10m_tower",
# "swisscamp",
# "crawfordpoint",
"NAU",
"Summit",
"DYE2",
# "jar1",
# "saddle",
# "southdome",
"NAE",
# "nasa_southeast",
"NEEM",
"E-GRIP"
]

th=1 ; fs=16
# plt.rcParams['font.sans-serif'] = ['Georgia']
plt.rcParams["font.size"] = fs
plt.rcParams['axes.facecolor'] = 'w'
plt.rcParams['axes.edgecolor'] = 'k'
plt.rcParams['axes.grid'] = True
plt.rcParams['grid.alpha'] = 1
plt.rcParams['grid.color'] = "#cccccc"
plt.rcParams["legend.facecolor"] ='w'
plt.rcParams["mathtext.default"]='regular'
plt.rcParams['grid.linewidth'] = th
plt.rcParams['axes.linewidth'] = th #set the value globally
plt.rcParams['figure.figsize'] = 17, 10
fg='k' ; bg='w'

name_alias = {'DY2': 'DYE2', 'CP1':'Crawford Point 1'}

site_list = pd.read_csv('/Users/jason/Dropbox/AWS/GCNET/GC-Net-level-1-data-processing/metadata/GC-Net_location.csv',header=0)

fn='/Users/jason/Dropbox/AWS/GCNET/ancillary/varnames_all.txt'
info=pd.read_csv(fn, header=None, delim_whitespace=True)
info.columns=['id','varnam']
# print(info.columns)
# print(info.varnam)

varsx=info.varnam[3:47]
varsx=info.varnam[3:29]
n_vars=len(varsx)

# -------------------------------- chdir
if os.getlogin() == 'jason':
    base_path = '/Users/jason/Dropbox/AWS/GCNET/GCN_climate_stats'

os.chdir(base_path)

# --------------------------------


do_plot=1
ly='x'
wo=1
wo_seasonal=1


sites=site_list.Name.values
# sites=info_all.name.values

# sites=['CP1']

# for st,site in enumerate(sites):
for site, ID in zip(site_list.Name,site_list.ID):
    site=site.replace(' ','')

    i_year=2000
    i_year=1996
    f_year=2021
    n_years=f_year-i_year+1
    
    
    May_to_September_albedo=np.zeros(n_years)
    May_to_September_albedo_stdev=np.zeros(n_years)
    May_to_September_albedo_count_valid_days=np.zeros(n_years)
    
    # -------------------------------- loop years
    if ID==6:
        for yy in range(n_years+1):
            yearx=yy+i_year
            print(site,yearx)
    
            if yearx<2022:
            # if yearx==2020:
                gc_file='/Users/jason/Dropbox/AWS/GCNET/GCN_climate_stats/output/albedo/'+site+'_'+str(yearx)+'.csv'
                df=pd.read_csv(gc_file)
                df["date"]=pd.to_datetime(df[['year', 'month', 'day']])
                df.index = pd.to_datetime(df.date)
    
                if do_plot:
                    plt.close()
                
                    # # gc_file='/Users/jason/Dropbox/AWS/GCNET/GCN_climate_stats/output/albedo/SwissCamp_'+str(yearx)+'.csv'
                    # gc_file='/Users/jason/Dropbox/AWS/GCNET/GCN_climate_stats/output/albedo/DYE2_'+str(yearx)+'.csv'
                    # df_GCN=pd.read_csv(gc_file)
                    # df_GCN["date"]=pd.to_datetime(df_GCN[['year', 'month', 'day']])
                    # df_GCN.index = pd.to_datetime(df_GCN.date)
        
                    fig, ax = plt.subplots(figsize=(6,5))
                    
                    # plt.plot(df.doy,df.alb,'.',c='k')
                    # plt.errorbar(df.doy,df.alb,yerr=df.stdev, fmt='none',label='albedo')
                    ax.plot(df["alb"],'.',c='b',label='GC-Net')
        
                
                    # plt.xlabel('day of year')
                    ax.set_title(site+' albedo '+str(yearx))
                    plt.legend()
                    # plt.plot(df.doy,df.stdev,'.',c='k')
                    # ax[cc].set_xlim(t0,t1)
                    # ax[cc].legend()
                    ax.set_xlim(datetime((yearx),1,1),datetime((yearx),12,31))
                    plt.setp(ax.xaxis.get_majorticklabels(), rotation=90,ha='center' )
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y %b'))
        
                    ofile='./metadata/Figs/'+site+'/'
                    os.system('mkdir -p '+ofile)
                    if ly=='p':
                        DPI=120
                        plt.savefig(ofile+site+'_'+str(yearx)+'_albedo.png', bbox_inches='tight', dpi=DPI, facecolor=bg, edgecolor=fg)
                        # os.system('open '+fig_path+figname+'.png')
                    
                t0=datetime((yearx),5,1)
                t1=datetime((yearx),9,30)
                May_to_September_albedo[yy]=np.nanmean(df.alb[((df.date>=t0)&(df.date<=t1))])
                May_to_September_albedo_stdev[yy]=np.nanstd(df.alb[((df.date>=t0)&(df.date<=t1))])
                May_to_September_albedo_count_valid_days[yy]=sum(np.isfinite(df.alb[((df.date>=t0)&(df.date<=t1))]))
                # ofile='/Users/jason/Dropbox/AWS/GCNET/GCN_climate_stats/output/albedo/'+site+'_'+str(yearx)
                # df['day'] = df['day'].map(lambda x: '%.0f' % x)
                # df['year'] = df['year'].map(lambda x: '%.0f' % x)
                # df['month'] = df['month'].map(lambda x: '%.0f' % x)
                # df['doy'] = df['doy'].map(lambda x: '%.0f' % x)
                # df['alb'] = df['alb'].map(lambda x: '%.3f' % x)
                # df['stdev'] = df['stdev'].map(lambda x: '%.2f' % x)
            
                # df.to_excel(ofile+'.xlsx')
        # 
                    # gc_file='/Users/jason/Dropbox/AWS/GCNET/GCN_climate_stats/output/albedo/'+site+'_'+str(yearx)
                    # df['alb'] = df['alb'].map(lambda x: '%.3f' % x)
                    # df['day'] = df['day'].map(lambda x: '%.0f' % x)
                    # df['year'] = df['year'].map(lambda x: '%.0f' % x)
                    # df['month'] = df['month'].map(lambda x: '%.0f' % x)
                    # df['doy'] = df['doy'].map(lambda x: '%.0f' % x)
                    # df = df.drop('date', axis=1)
                    # df.to_csv(gc_file+'.csv')
        
            if wo_seasonal:
                df_seasonal = pd.DataFrame(columns = ['year','May_to_September_albedo','May_to_September_albedo_stdev','May_to_September_albedo_count_valid_days']) 
                df_seasonal.index.name = 'index'
                df_seasonal["year"]=pd.Series(np.arange(i_year,f_year+1))
                df_seasonal["May_to_September_albedo"]=pd.Series(May_to_September_albedo)
                df_seasonal["May_to_September_albedo_stdev"]=pd.Series(May_to_September_albedo_stdev)
                df_seasonal["May_to_September_albedo_count_valid_days"]=pd.Series(May_to_September_albedo_count_valid_days)
                df_seasonal['May_to_September_albedo'] = df_seasonal['May_to_September_albedo'].map(lambda x: '%.3f' % x)
                df_seasonal['May_to_September_albedo_stdev'] = df_seasonal['May_to_September_albedo_stdev'].map(lambda x: '%.3f' % x)
                df_seasonal['May_to_September_albedo_count_valid_days'] = df_seasonal['May_to_September_albedo_count_valid_days'].map(lambda x: '%.0f' % x)
                
                ofile='/Users/jason/Dropbox/AWS/GCNET/GCN_climate_stats/output/albedo/'+site+'_'+str(i_year)+'-'+str(f_year)+'_May_to_September_albedo'
                df_seasonal.to_csv(ofile+'.csv',index=None)
                df_seasonal.to_excel(ofile+'.xlsx',index=None)
