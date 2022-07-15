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
    base_path = '/Users/jason/Dropbox/AWS/GCNET/GCN_albedo/'

os.chdir(base_path)

# --------------------------------

# ----------------------------------------------------------- adjuster routine
# jason box
# procedure with different filter functions
# counts cases rejected by filters
# outputs filter to a format that is compatible for GC-Net-level-1-data-processing/GC-Net-level-1-data-processing.py
def adjuster(site,df,var_list,y0,m0,d0,func,y1,m1,d1,comment,val):
    
    tstring='%Y-%m-%dT%H:%M:%S'+'+00:00'

    df_out = df.copy()
    
    t0=datetime(y0,m0,d0)
    t1=datetime(y1,m1,d1)

    # two variables required for abs_diff
    if func == 'abs_diff': 
        tmp0=df_out.loc[t0:t1,var_list[0]].values
        tmp1=df_out.loc[t0:t1,var_list[1]].values
        tmp = df_out.loc[t0:t1,var_list[1]].values-df_out.loc[t0:t1,var_list[0]].values
        count=sum(abs(tmp)>val)
        tmp0[abs(tmp)>val] = np.nan
        tmp1[abs(tmp)>val] = np.nan
        df_out.loc[t0:t1,var_list[0]] = tmp0
        df_out.loc[t0:t1,var_list[1]] = tmp1

    if func == 'abs_diff_del_instrument_2': 
        tmp0=df_out.loc[t0:t1,var_list[0]].values
        tmp1=df_out.loc[t0:t1,var_list[1]].values
        tmp = df_out.loc[t0:t1,var_list[1]].values-df_out.loc[t0:t1,var_list[0]].values
        count=sum(abs(tmp)>val)
        # tmp0[abs(tmp)>val] = np.nan
        tmp1[abs(tmp)>val] = np.nan
        # df_out.loc[t0:t1,var_list[0]] = tmp0
        df_out.loc[t0:t1,var_list[1]] = tmp1
 
    if func == 'swap':
            val_var = df_out.loc[t0:t1,var_list[0]].values.copy()
            val_var2 = df_out.loc[t0:t1,var_list[1]].values.copy()
            df_out.loc[t0:t1,var_list[1]] = val_var
            df_out.loc[t0:t1,var_list[0]] = val_var2
            count=len(df_out.loc[t0:t1,var_list[1]])

    for var in var_list:
        # set to nan stuck values
        if func == 'nan_constant': 
            tmp = df_out.loc[t0:t1,var]
            count=sum(tmp.diff()==0)
            tmp[tmp.diff()==0]=np.nan
            df_out.loc[t0:t1,var] = tmp

        if func == 'min_filter': 
            tmp = df_out.loc[t0:t1,var].values
            count=sum(tmp<val)
            tmp[tmp<val] = np.nan
            df_out.loc[t0:t1,var] = tmp

        if func == 'nan_var': 
            tmp = df_out.loc[t0:t1,var].values
            count=len(tmp)
            tmp[:] = np.nan
            df_out.loc[t0:t1,var] = tmp
            
        if func == 'max_filter': 
            tmp = df_out.loc[t0:t1,var].values
            count=sum(tmp>val)
            tmp[tmp>val] = np.nan
            df_out.loc[t0:t1,var] = tmp

        # if 'swap_with_' in func: 
        #     var2 = func[10:]
        #     val_var = df_out.loc[t0:t1,var].values.copy()
        #     val_var2 = df_out.loc[t0:t1,var2].values.copy()
        #     df_out.loc[t0:t1,var2] = val_var
        #     df_out.loc[t0:t1,var] = val_var2

        msg=datetime(y0,m0,d0).strftime(tstring)+\
        ','+datetime(y1,m1,d1).strftime(tstring)+\
        ','+var+','+func+','+str(val)+','+comment+','+str(count)
        # print(msg)

        # dfx=pd.read_csv('/Users/jason/Dropbox/AWS/GCNET/GC-Net-level-1-data-processing/metadata/adjustments/'+site+'.csv')
        # print(dfx)

        wo=1
        if wo and count>0:
            opath_adjustments='./metadata/adjustments/'+site+'/'
            os.system('mkdir -p '+opath_adjustments)
            out_fn=opath_adjustments+var+'_'+func+'_'+datetime(y0,m0,d0).strftime('%Y-%m-%d')+'_'+datetime(y1,m1,d1).strftime('%Y-%m-%d')+'.csv'
            out_concept=open(out_fn,'w')
            out_concept.write('t0,t1,variable,adjust_function,adjust_value,comment,count\n')
            out_concept.write(msg)
            out_concept.close()

    return(df_out)
    # ----------------------------------------------------------- end adjuster routine

do_plot=1
ly='p'
wo=1
wo_seasonal=0


sites=site_list.Name.values
# sites=info_all.name.values

# sites=['CP1']

# for st,site in enumerate(sites):
for site, ID in zip(site_list.Name,site_list.ID):
    site=site.replace(' ','')
    # if site!='null':
    # if site=='NASA-E':
    if site=='Summit':
    # if site=='SwissCamp':
    # if site=='DYE2':
    #     print(ID)
    # if ID>=0:
    # if ID==12:
        # site='swisscamp'
        # df=pd.read_csv('./output/swc_air_t_1990-2021.csv')
        # print(site)
        # fn=base_dir+str(info_all['Station Number'][st].astype(int)).zfill(2)+'-'+site+'.csv'
        # print(fn)
        # print('# '+str(ID)+ ' ' + site)
        filename = base_dir+str(ID).zfill(2)+'-'+site+'.csv'
        if not path.exists(filename):
            print('Warning: No file for station '+str(ID)+' '+site)
            continue
        ds = nead.read(filename)
        df = ds.to_dataframe()
        df=df.reset_index(drop=True)
        df[df == -999] = np.nan
        df['time'] = pd.to_datetime(df.timestamp)
        df = df.set_index('time')

        # print(df.columns)
        df[df==999]=np.nan
        
        df['year'] = df.index.year
        df['month'] = df.index.month
        
        df.columns
        df['doy'] = df.index.dayofyear
        
        import calendar

        time = pd.to_datetime(df.timestamp)

        i_year=2000
        i_year=1996
        f_year=2021
        n_years=f_year-i_year+1
        
        
        JJA_albedo=np.zeros(n_years)
        JJA_albedo_stdev=np.zeros(n_years)
        
        # -------------------------------- loop years

        for yy in range(n_years+1):
            yearx=yy+i_year
            print(site,yearx)
            n_days=365
            if calendar.isleap(yearx):
                n_days=366

            # if yearx==2022:
            if yearx>=209:
            
                alb=np.zeros(n_days)*np.nan
                year=np.zeros(n_days)*np.nan
                month=np.zeros(n_days)*np.nan
                day=np.zeros(n_days)*np.nan
                doy=np.zeros(n_days)*np.nan
                for dd in range(n_days):
                    time=datetime.strptime(str(yearx)+' '+str(dd+1), '%Y %j')
                    v=((df.year==yearx)&(df.doy==dd+1))
                    if sum(v)==24:
                        denom=np.nansum(df.ISWR[v])
                        numerator=np.nansum(df.OSWR[v])
                        if denom>numerator:alb[dd]=numerator/denom
                        # print(dd,sum(v),alb[dd])
                    year[dd]=time.strftime('%Y')
                    month[dd]=time.strftime('%m')
                    day[dd]=time.strftime('%d')
                    doy[dd]=time.strftime('%j')
                    
                df_daily = pd.DataFrame(columns = ['year','month','day','doy','alb']) 
                df_daily.index.name = 'index'
                df_daily["year"]=pd.Series(year)
                df_daily["month"]=pd.Series(month)
                df_daily["day"]=pd.Series(day)
                df_daily["doy"]=pd.Series(doy)
                df_daily["alb"]=pd.Series(alb)                
                df_daily["date"]=pd.to_datetime(df_daily[['year', 'month', 'day']])
            
                drop=0
                if drop:
                    t0=datetime((yearx),4,1) ; t1=datetime((yearx),10,31)
                    # if meta.alt_name[i]!='JAR':
                    df_daily.drop(df_daily[df_daily.date<t0].index, inplace=True)
                    df_daily.reset_index(drop=True, inplace=True)
                    df_daily.drop(df_daily[df_daily.date>=t1].index, inplace=True)
                    df_daily.reset_index(drop=True, inplace=True)
            
                df_daily.index = pd.to_datetime(df_daily.date)
##%%

                
                if site=='Summit':
                    df_daily=adjuster(site,df_daily,['alb'],1996,1,1,'min_filter',1996,12,31,'outlier?',0.775)
                    df_daily=adjuster(site,df_daily,['alb'],1996,1,1,'max_filter',1996,12,31,'outlier?',0.89)
                    df_daily=adjuster(site,df_daily,['alb'],1997,1,1,'min_filter',1997,12,31,'outlier?',0.785)
                    df_daily=adjuster(site,df_daily,['alb'],1998,1,1,'min_filter',1998,12,31,'outlier?',0.775)
                    df_daily=adjuster(site,df_daily,['alb'],1999,1,1,'min_filter',1999,12,31,'outlier?',0.799)
                    df_daily=adjuster(site,df_daily,['alb'],2000,1,1,'min_filter',2000,12,31,'outlier?',0.765)
                    df_daily=adjuster(site,df_daily,['alb'],2001,1,1,'min_filter',2001,12,31,'outlier?',0.845)
                    df_daily=adjuster(site,df_daily,['alb'],2002,1,1,'min_filter',2002,12,31,'outlier?',0.765)
                    df_daily=adjuster(site,df_daily,['alb'],2003,1,1,'min_filter',2003,12,31,'outlier?',0.788)
                    df_daily=adjuster(site,df_daily,['alb'],2004,1,1,'min_filter',2004,12,31,'outlier?',0.78)
                    df_daily=adjuster(site,df_daily,['alb'],2005,1,1,'min_filter',2005,12,31,'outlier?',0.76)
                    df_daily=adjuster(site,df_daily,['alb'],2006,1,1,'min_filter',2006,12,31,'outlier?',0.74)          
                    df_daily=adjuster(site,df_daily,['alb'],2007,1,1,'min_filter',2007,12,31,'outlier?',0.76)
                    df_daily=adjuster(site,df_daily,['alb'],2007,1,1,'min_filter',2007,4,3,'outlier?',0.84)
                    df_daily=adjuster(site,df_daily,['alb'],2007,9,1,'min_filter',2007,12,31,'outlier?',0.81)
                    df_daily=adjuster(site,df_daily,['alb'],2008,1,1,'min_filter',2008,12,31,'outlier?',0.78)
                    df_daily=adjuster(site,df_daily,['alb'],2009,1,1,'min_filter',2009,12,31,'outlier?',0.865)
                    df_daily=adjuster(site,df_daily,['alb'],2010,1,1,'min_filter',2010,12,31,'outlier?',0.865)
                    df_daily=adjuster(site,df_daily,['alb'],2011,1,1,'min_filter',2011,12,31,'outlier?',0.76)
                    df_daily=adjuster(site,df_daily,['alb'],2012,1,1,'min_filter',2012,12,31,'outlier?',0.835)
                    df_daily=adjuster(site,df_daily,['alb'],2013,1,1,'min_filter',2013,12,31,'outlier?',0.87)
                    df_daily=adjuster(site,df_daily,['alb'],2014,1,1,'min_filter',2014,12,31,'outlier?',0.863)
                    df_daily=adjuster(site,df_daily,['alb'],2015,1,1,'min_filter',2015,12,31,'outlier?',0.845)
                    df_daily=adjuster(site,df_daily,['alb'],2016,1,1,'min_filter',2016,12,31,'outlier?',0.85)
                    df_daily=adjuster(site,df_daily,['alb'],2017,1,1,'min_filter',2017,12,31,'outlier?',0.84)
                    df_daily=adjuster(site,df_daily,['alb'],2018,1,1,'min_filter',2018,12,31,'outlier?',0.82)
                    df_daily=adjuster(site,df_daily,['alb'],2019,1,1,'min_filter',2019,12,31,'outlier?',0.81)
                    df_daily=adjuster(site,df_daily,['alb'],2020,1,1,'min_filter',2020,12,31,'outlier?',0.84)
                    df_daily=adjuster(site,df_daily,['alb'],2021,1,1,'min_filter',2021,12,31,'outlier?',0.78)
                    df_daily=adjuster(site,df_daily,['alb'],2022,1,1,'min_filter',2022,12,31,'outlier?',0.81)
                    
                    df_daily=adjuster(site,df_daily,['alb'],yearx,6,1,'max_filter',yearx,8,31,'outlier?',0.94)
                    
                # if aws_nam(st)=='PTE':

                    #   alb(st,yy,0:99)=-999.
                #   alb(st,yy,280:364)=-999.
                #   if year(yy)=='2014' then alb(st,yy,0:191)=-999.
                # endif
                
                # if aws_nam(st)=='JR2':
                #   if yearx==2002:
                #     v=where(reform(alb(st,yy,*)) lt 0.65 and days_365 lt 125) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2010' then alb(st,yy,170:364)=-999.
                #     if yearx le 2012:
                #     v=where(alb(st,yy,*) gt 0.85) & alb(st,yy,v)=-999
                #   endif
                # endif
                
                # if aws_nam(st)=='NSE':
                #   if yearx==2000:
                #     v=where(reform(alb(st,yy,*)) gt 0.88 and days_365 lt 425) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2001:
                #     v=where(reform(alb(st,yy,*)) lt 0.77 and days_365 lt 425) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2002:
                #     v=where(reform(alb(st,yy,*)) lt 0.79 and days_365 lt 425) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2003:
                #     v=where(reform(alb(st,yy,*)) lt 0.79 and days_365 lt 425) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2004:
                #     v=where(reform(alb(st,yy,*)) lt 0.7 and days_365 lt 425) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2005 then alb(st,yy,200:364)=-999.
                #   if year(yy)=='2006' then alb(st,yy,0:100)=-999.
                #   if yearx==2010:
                #     v=where(reform(alb(st,yy,*)) lt 0.8 and days_365 gt 270) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2007:
                #     v=where(reform(alb(st,yy,*)) lt 0.75 and days_365 lt 425) & alb(st,yy,v)=-999
                #     v=where(reform(alb(st,yy,*)) gt 0.88 and days_365 lt 425) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2010:
                #     v=where(reform(alb(st,yy,*)) lt 0.75 and days_365 lt 425) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2012:
                #     v=where(reform(alb(st,yy,*)) lt 0.7 and days_365 lt 425) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2013' then alb(st,yy,248:364)=-999.
                #   if yearx==2014:
                #     v=where(reform(alb(st,yy,*)) lt 0.78 and days_365 gt 270) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2016' then alb(st,yy,0:75)=-999.
                # endif
                
                # if aws_nam(st)=='NGP':
                #   if year(yy)=='2002' then alb(st,yy,250:364)=-999.
                #   if year(yy)=='2003' then alb(st,yy,0:100)=-999. & if year(yy)=='2003' then alb(st,yy,245:364)=-999.
                #   if year(yy)=='2004' then alb(st,yy,0:120)=-999.
                #   if year(yy)=='2005' then alb(st,yy,250:364)=-999.
                #   if year(yy)=='2006' then alb(st,yy,250:364)=-999.
                #   if year(yy)=='2007' then alb(st,yy,0:130)=-999. & if year(yy)=='2007' then alb(st,yy,250:364)=-999.
                #   if year(yy)=='2008' then alb(st,yy,0:100)=-999. & if year(yy)=='2008' then alb(st,yy,260:364)=-999.
                #   if year(yy)=='2009' then alb(st,yy,0:105)=-999. & if year(yy)=='2009' then alb(st,yy,240:364)=-999.
                #   if year(yy)=='2010' then alb(st,yy,0:130)=-999.
                # endif
                
                # if aws_nam(st)=='CP2':
                #   if year(yy)=='2000':
                #     v=where(alb(st,yy,*) lt 0.65)
                #     alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2014' then alb(st,yy,*)=-999.
                #   if year(yy)=='2015' then alb(st,yy,*)=-999.
                # endif
                
                # if aws_nam(st)=='NAE':
                #   if yearx==2000:
                #     v=where(reform(alb(st,yy,*)) lt 0.78) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2000' then alb(st,yy,0:105)=-999. & if year(yy)=='2000' then alb(st,yy,255:364)=-999.
                #   if year(yy)=='2001' then alb(st,yy,0:123)=-999. & if year(yy)=='2001' then alb(st,yy,264:364)=-999.
                #   if yearx==2001:
                #     v=where(reform(alb(st,yy,*)) lt 0.78) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2002' then alb(st,yy,0:110)=-999. & if year(yy)=='2002' then alb(st,yy,250:364)=-999.
                #   if year(yy)=='2003' then alb(st,yy,0:136)=-999. & if year(yy)=='2003' then alb(st,yy,255:364)=-999.
                #   if year(yy)=='2004' then alb(st,yy,0:110)=-999. & if year(yy)=='2004' then alb(st,yy,244:364)=-999.
                #   if year(yy)=='2005' then alb(st,yy,0:88)=-999. & if year(yy)=='2005' then alb(st,yy,255:364)=-999.
                #   if year(yy)=='2006' then alb(st,yy,0:112)=-999. & if year(yy)=='2006' then alb(st,yy,244:364)=-999.
                #   if year(yy)=='2007' then alb(st,yy,0:115)=-999. & if year(yy)=='2007' then alb(st,yy,268:364)=-999.
                #   if year(yy)=='2008' then alb(st,yy,0:115)=-999.
                #   if year(yy)=='2010' then alb(st,yy,0:115)=-999. & if year(yy)=='2010' then alb(st,yy,250:364)=-999.
                #   if year(yy)=='2011' then alb(st,yy,0:116)=-999. & if year(yy)=='2011' then alb(st,yy,255:364)=-999.
                #   if year(yy)=='2012' then alb(st,yy,0:142)=-999. & if year(yy)=='2012' then alb(st,yy,255:364)=-999.  
                #   if year(yy)=='2013' then alb(st,yy,0:136)=-999. & if year(yy)=='2013' then alb(st,yy,253:364)=-999.
                #   if year(yy)=='2014' then alb(st,yy,0:127)=-999. & if year(yy)=='2014' then alb(st,yy,253:364)=-999.
                #   if year(yy)=='2015' then alb(st,yy,0:112)=-999. & if year(yy)=='2015' then alb(st,yy,260:364)=-999.
                # endif
                
                # if aws_nam(st)=='SDM':
                #   if yearx==2000:
                #     v=where(reform(alb(st,yy,*)) gt 0.85) & alb(st,yy,v)=0.85
                #   endif
                #   if yearx==2001:
                #     v=where(reform(alb(st,yy,*)) lt 0.8 and days_365 lt 140) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2003 then alb(st,yy,*)=-999
                #   if yearx==2004 then alb(st,yy,*)=-999
                #   if yearx==2005 then alb(st,yy,*)=-999
                
                #   if yearx==2008:
                #     v=where(reform(alb(st,yy,*)) lt 0.75 and days_365 lt 110) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2011:
                #     v=where(reform(alb(st,yy,*)) lt 0.7 and days_365 gt 260) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2013' then alb(st,yy,280:364)=-999.
                #   if yearx==2015 or yearx==2014:
                #     v=where(reform(alb(st,yy,*)) gt 0.88) & alb(st,yy,v)=0.88
                #   endif
                
                # endif
                
                # if aws_nam(st)=='SDL':
                #   if yearx==2000:
                #     v=where(reform(alb(st,yy,*)) lt 0.8 and days_365 lt 100) & alb(st,yy,v)=-999
                #   endif
                #   if yearx==2001 or yearx==2006 or yearx==2007 or yearx==2012 or yearx==2013 or yearx==2016:
                #     v=where(reform(alb(st,yy,*)) lt 0.8 and days_365 lt 110)  & alb(st,yy,v)=-999
                #     v=where(reform(alb(st,yy,*)) lt 0.8 and days_365 gt 260)  & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2009' then alb(st,yy,0:80)=-999.
                #   if year(yy)=='2005' or year(yy)=='2006' or year(yy)=='2007' or year(yy)=='2011' or year(yy)=='2013' then alb(st,yy,255:364)=-999.
                #     if yearx==2008 or yearx==2009:
                #     v=where(reform(alb(st,yy,*)) lt 0.8 and days_365 lt 140)  & alb(st,yy,v)=-999
                #   endif
                # endif
                
                # if aws_nam(st)=='JR1':
                #   if year(yy)=='2004' then alb(st,yy,250:364)=-999.
                #   if year(yy)=='2005' then alb(st,yy,0:127)=-999.
                #   if year(yy)=='2007' then alb(st,yy,240:364)=-999.
                #   if year(yy)=='2008' then alb(st,yy,0:129)=-999.
                #   if year(yy)=='2010' then alb(st,yy,207:364)=-999.
                #   if year(yy)=='2011' then alb(st,yy,0:159)=-999.
                #   if year(yy)=='2012' then alb(st,yy,0:130)=-999.
                # endif
                
                # if aws_nam(st)=='DY2':
                #   if year(yy)=='2004' then alb(st,yy,190:364)=-999.
                #   if year(yy)=='2005' then alb(st,yy,0:140)=-999. 
                #   if year(yy)=='2003' then alb(st,yy,200:364)=-999.
                #   if year(yy)=='2009' then alb(st,yy,0:90)=-999. 
                #   if year(yy)=='2011' then alb(st,yy,0:75)=-999. 
                #   if yearx==2012:
                #     v=where(reform(alb(st,yy,*)) lt 0.8 and days_365 lt 110) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2013' then alb(st,yy,265:364)=-999.
                # endif
                
                # if aws_nam(st)=='TUN':
                #   if yearx==2001:
                #     v=where(alb(st,yy,*) lt 0.7) & alb(st,yy,v)=-999
                #     alb(st,yy,0:95)=-999.
                #   endif
                #   if yearx==2010:
                #     v=where(alb(st,yy,*) lt 0.8) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2005' then alb(st,yy,0:95)=-999.
                #   if year(yy)=='2007' then alb(st,yy,0:95)=-999.
                #   if year(yy)=='2008' then alb(st,yy,0:95)=-999.
                #   if year(yy)=='2009' then alb(st,yy,0:95)=-999.
                #   if year(yy)=='2011' then alb(st,yy,260:364)=-999.
                #   if year(yy)=='2013' then alb(st,yy,280:364)=-999.
                #   if year(yy)=='2013' then alb(st,yy,0:95)=-999.
                #   if year(yy)=='2014' then alb(st,yy,0:116)=-999. &if year(yy)=='2014' then alb(st,yy,290:364)=-999.
                # endif


                #     df_daily.alb[]v=where(alb(st,yy,*) lt 0.7) & alb(st,yy,v)=-999
                #     alb(st,yy,247:364)=-999.
                #     v=where(reform(alb(st,yy,*)) gt 0.85 and days_365 gt 150 and days_365 lt 180) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2003' then alb(st,yy,240:364)=-999.
                #   if year(yy)=='2004' then alb(st,yy,245:364)=-999.
                #   if year(yy)=='2005' then alb(st,yy,238:364)=-999.
                #   if year(yy)=='2006' then alb(st,yy,238:364)=-999.
                #   if year(yy)=='2010' then alb(st,yy,0:130)=-999. & if year(yy)=='2010' then alb(st,yy,260:364)=-999.
                #   if year(yy)=='2011':
                #     alb(st,yy,250:364)=-999.
                #     v=where(alb(st,yy,*) lt 0.76) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2012' then alb(st,yy,258:364)=-999.
                #   if year(yy)=='2013' then alb(st,yy,0:110)=-999.
                #   if year(yy)=='2014':
                #     v=where(alb(st,yy,*) lt 0.885) & alb(st,yy,v)=-999
                #   endif
                # endif
                
                
                # if aws_nam(st)=='HUM':
                #   if year(yy)=='2002':
                #     v=where(alb(st,yy,*) gt 0.9) & alb(st,yy,v)=-999
                #     alb(st,yy,v)+=0.02
                #   endif
                #   if year(yy)=='2004' then alb(st,yy,0:110)=-999.
                #   if year(yy)=='2005' then alb(st,yy,0:90)=-999. & if year(yy)=='2005' then alb(st,yy,250:364)=-999.
                #   if year(yy)=='2006' then alb(st,yy,0:84)=-999.
                #   alb(st,yy,0:90)=-999. & alb(st,yy,255:364)=-999.
                
                # endif
                
                # if aws_nam(st)=='GIT':
                #   if year(yy)=='2001' then alb(st,yy,0:115)=-999. & if year(yy)=='2001' then alb(st,yy,250:364)=-999.
                #   if year(yy)=='2002' then alb(st,yy,0:115)=-999.
                #   if year(yy)=='2004':
                #     v=where(alb(st,yy,*) lt 0.73) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2006' then alb(st,yy,0:120)=-999. & if year(yy)=='2006' then alb(st,yy,250:364)=-999.
                #   if year(yy)=='2007' then alb(st,yy,0:105)=-999.
                #   if year(yy)=='2014' then alb(st,yy,200:364)=-999.
                #   if year(yy)=='2015' then alb(st,yy,*)=-999.
                #   if year(yy)=='2016' then alb(st,yy,*)=-999.
                # endif
                
                # if aws_nam(st)=='NAU':
                #   a1_temp=[2.003,2.054,2.658]
                #   a0_temp=[0.224,0.215,0.037]
                #   a1=mean(a1_temp) & a0=mean(a0_temp)
                #   if yearx le 2014:
                #     v=where(alb(st,yy,*) gt -999) & alb(st,yy,v)=alb(st,yy,v)*a1+a0
                #   endif
                #   if yearx le 2015:
                #     alb(st,yy,*)+=0.05
                #     alb(st,yy,245:364)=-999
                #   endif
                #   if yearx==2015:
                #     alb(st,yy,*)+=0.035
                #   endif
                # endif
                
                # if aws_nam(st)=='CP1':
                #   if year(yy)=='2003':
                #     v=where(alb(st,yy,*) lt 0.78) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2005':
                #     v=where(alb(st,yy,*) lt 0.7) & alb(st,yy,v)=-999
                #     alb(st,yy,*)-=0.093
                #   endif
                #   if year(yy)=='2006' then alb(st,yy,*)-=0.086
                #   if year(yy)=='2007' then alb(st,yy,*)-=0.073
                #   if year(yy)=='2008' then alb(st,yy,*)-=0.08  
                #   if year(yy)=='2009' then alb(st,yy,*)-=0.071
                #   if yearx==2009:
                #     v=where(reform(alb(st,yy,*)) lt 0.8 and days_365 gt 150 and days_365 lt 160) & alb(st,yy,v)=-999
                #   endif
                #   if year(yy)=='2010' then alb(st,yy,*)-=0.068 
                #   if year(yy)=='2014' then alb(st,yy,*)=-999.
                #   if year(yy)=='2015' then alb(st,yy,*)=-999.
                # endif
                
                # if aws_nam(st)=='SWC':
                #   if year(yy)=='2007' then alb(st,yy,*)=-999.
                #   if year(yy)=='2011' then alb(st,yy,0:139)=-999.
                #   if year(yy)=='2012' then alb(st,yy,*)=-999.
                #   if year(yy)=='2013' then alb(st,yy,*)=-999.
                #   if year(yy)=='2014' then alb(st,yy,0:88)=-999.
                #   if year(yy)=='2015':
                #     alb(st,yy,*)-=0.18
                #     v=where(reform(alb(st,yy,*)) gt 0.9) & alb(st,yy,v)=-999
                #     alb(st,yy,206:364)=-999.
                #   endif
                #   if year(yy)=='2016' then alb(st,yy,90:110)=-999.
                #   if year(yy)=='2003':
                #     v=where(alb(st,yy,*) lt 0.45) & alb(st,yy,v)=-999
                #     alb(st,yy,200:364)=-999.
                #   endif
                # endif


                if do_plot:
                    plt.close()
                
                    # df_GCN=pd.read_csv(gc_file)
                    # df_GCN["date"]=pd.to_datetime(df_GCN[['year', 'month', 'day']])
                    # df_GCN.index = pd.to_datetime(df_GCN.date)
        
                    fig, ax = plt.subplots(figsize=(6,5))
                    
                    # plt.plot(df.doy,df.alb,'.',c='k')
                    # plt.errorbar(df.doy,df.alb,yerr=df.stdev, fmt='none',label='albedo')
                    ax.plot(df_daily["alb"],'.',c='b',label='GC-Net')

                
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
                
                # if wo:        
                #     t0=datetime((yearx),6,1)
                #     t1=datetime((yearx),8,31)
                #     JJA_albedo[yy]=np.nanmean(df_daily.alb[((df_daily.date>=t0)&(df_daily.date<=t1))])
                #     JJA_albedo_stdev[yy]=np.nanstd(df_daily.alb[((df_daily.date>=t0)&(df_daily.date<=t1))])
                #     ofile='/Users/jason/Dropbox/AWS/GCNET/GCN_climate_stats/output/albedo/'+site+'_'+str(yearx)
                #     df_daily['day'] = df_daily['day'].map(lambda x: '%.0f' % x)
                #     df_daily['year'] = df_daily['year'].map(lambda x: '%.0f' % x)
                #     df_daily['month'] = df_daily['month'].map(lambda x: '%.0f' % x)
                #     df_daily['doy'] = df_daily['doy'].map(lambda x: '%.0f' % x)
                #     df_daily['alb'] = df_daily['alb'].map(lambda x: '%.3f' % x)
                #     # df_daily['stdev'] = df_daily['stdev'].map(lambda x: '%.2f' % x)
                
                #     df_daily = df_daily.drop('date', axis=1)
                #     df_daily.to_csv(ofile+'.csv')
                #     # df_daily.to_excel(ofile+'.xlsx')

                gc_file='./data_daily/'+site+'_'+str(yearx)
                df_daily['alb'] = df_daily['alb'].map(lambda x: '%.3f' % x)
                df_daily['day'] = df_daily['day'].map(lambda x: '%.0f' % x)
                df_daily['year'] = df_daily['year'].map(lambda x: '%.0f' % x)
                df_daily['month'] = df_daily['month'].map(lambda x: '%.0f' % x)
                df_daily['doy'] = df_daily['doy'].map(lambda x: '%.0f' % x)
                df_daily = df_daily.drop('date', axis=1)
                df_daily.to_csv(gc_file+'.csv')

        # if wo_seasonal:
        #     df_JJA = pd.DataFrame(columns = ['year','JJA_albedo','JJA_albedo_stdev']) 
        #     df_JJA.index.name = 'index'
        #     df_JJA["year"]=pd.Series(np.arange(i_year,f_year+1))
        #     df_JJA["JJA_albedo"]=pd.Series(JJA_albedo)
        #     df_JJA["JJA_albedo_stdev"]=pd.Series(JJA_albedo_stdev)
        #     df_JJA['JJA_albedo'] = df_JJA['JJA_albedo'].map(lambda x: '%.3f' % x)
        #     df_JJA['JJA_albedo_stdev'] = df_JJA['JJA_albedo_stdev'].map(lambda x: '%.3f' % x)
            
        #     ofile='/Users/jason/Dropbox/AWS/GCNET/GCN_climate_stats/output/albedo/'+site+'_'+str(i_year)+'-'+str(f_year)+'_JJA_albedo'
        #     df_JJA.to_csv(ofile+'.csv')
