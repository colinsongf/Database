'''
generate advanced shots data 

'''

import pandas as pd
import tables
import sys
import csv

DATAPATH = '/media/sf_GitHub/Database/Modeling/refactored/data'

f = open('xefg_by_date.csv', 'w')
writer = csv.writer(f,delimiter=',',quoting=csv.QUOTE_MINIMAL)
f.write('date,atb3_pps,c3_pps,mid_pps,ra_pps,paint_pps'+'\n')
#load all the files first

for seasons in range(2000,2015):

  df_shots= pd.read_hdf('%s/shots/shots%s.h5' % (DATAPATH,str(seasons)[2:4]))

   
  for date in pd.unique(df_shots['Date'].values):
    df_shots_game = df_shots[df_shots['Date']<date]

    #generate XeFG% by area 
    #get all shot attempts by area from game 1 to game x
    try:

      fgm_by_area = df_shots_game.SHOT_MADE_FLAG.groupby(df_shots_game.SHOT_ZONE_BASIC).sum()
      fga_by_area = df_shots_game.SHOT_ATTEMPTED_FLAG.groupby(df_shots_game.SHOT_ZONE_BASIC).sum()

      atb3_pps = (fgm_by_area['Above the Break 3']*3+fga_by_area['Above the Break 3']*0.0125*3*0.8)/fga_by_area['Above the Break 3']
      c3_pps = ((fgm_by_area['Left Corner 3']+fgm_by_area['Right Corner 3'])*3+(fga_by_area['Left Corner 3']+fga_by_area['Right Corner 3'])*0.01*3*0.8)/(fga_by_area['Right Corner 3']+fga_by_area['Left Corner 3'])
      midrange_pps = (fgm_by_area['Mid-Range']*2+fga_by_area['Mid-Range']*0.052*2*0.8)/fga_by_area['Mid-Range']
      ra_pps = (fgm_by_area['Restricted Area']*2+fga_by_area['Restricted Area']*0.22*2*0.8)/fga_by_area['Restricted Area']
      paint_pps = (fgm_by_area['In The Paint (Non-RA)']*2+fga_by_area['In The Paint (Non-RA)']*0.1855*2*0.8)/fga_by_area['In The Paint (Non-RA)']
      
      writer.writerow([date,atb3_pps,c3_pps,midrange_pps,ra_pps,paint_pps])
      print(date)
      
    except KeyError:
      print ("first day of season, lol")

