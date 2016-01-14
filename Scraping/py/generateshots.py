'''
generate advanced shots data 

'''

import pandas as pd
import tables
import sys
import csv

DATAPATH = '/media/sf_GitHub/Database/Modeling/processor/data'

f = open('xefg_by_date.csv', 'w')
writer = csv.writer(f,delimiter=',',quoting=csv.QUOTE_MINIMAL)
f.write('date,season,player_size,atb3_pps,c3_pps,mid_pps,ra_pps,paint_pps'+'\n')
#load all the files first

pxefg_s = pd.read_csv('xefg_by_player.csv')
size = ['Big','Small','All']

for seasons in range(2000,2015):
  pxefg = pxefg_s[pxefg_s['season']==seasons]

  for date in pd.unique(pxefg['date'].values):

    pxefg_size = pxefg[pxefg['date']<date]
    for sizes in size:
      if sizes == 'All':
        pxefg_date = pxefg_size
      else:
        pxefg_date = pxefg_size[pxefg_size['player_size']==sizes]  

      try: 
        atb3_pps = pxefg_date['atb3_pts'].sum()/pxefg_date['atb3_attempt'].sum()    
        c3_pps = pxefg_date['c3_pts'].sum()/pxefg_date['c3_attempt'].sum()
        mid_pps = pxefg_date['mid_pts'].sum()/pxefg_date['mid_attempt'].sum()
        ra_pps = pxefg_date['ra_pts'].sum()/pxefg_date['ra_attempt'].sum()
        paint_pps = pxefg_date['paint_pts'].sum()/pxefg_date['paint_attempt'].sum()
        writer.writerow([Date,Seasons,sizes,atb3_pps,c3_pps,mid_pps,ra_pps,paint_pps])
      except ZeroDivisionError:
        print ('first day of season')
    print(date)


