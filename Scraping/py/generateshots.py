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

def write_global_xefg_by_date(datenum,season):

#load the shots dataframe file

  df_shots = pd.read_hdf('%s/shots/shots%s.h5' % (DATAPATH,season), 'df_shots')
  datelimit = int(datenum)
  df_shots_game = df_shots[df_shots['Date']<datelimit]

  #generate XeFG% by area 
  #get all shot attempts by area from game 1 to game x
  

  fgm_by_area = df_shots_game.SHOT_MADE_FLAG.groupby(df_shots_game.SHOT_ZONE_BASIC).sum()
  fga_by_area = df_shots_game.SHOT_ATTEMPTED_FLAG.groupby(df_shots_game.SHOT_ZONE_BASIC).sum()

  atb3_pps = (fgm_by_area['Above the Break 3']*3+fga_by_area['Above the Break 3']*0.0125*3*0.8)/fga_by_area['Above the Break 3']
  c3_pps = ((fgm_by_area['Left Corner 3']+fgm_by_area['Right Corner 3'])*3+(fga_by_area['Left Corner 3']+fga_by_area['Right Corner 3'])*0.01*3*0.8)/(fga_by_area['Right Corner 3']+fga_by_area['Left Corner 3'])
  midrange_pps = (fgm_by_area['Mid-Range']*2+fga_by_area['Mid-Range']*0.052*2*0.8)/fga_by_area['Mid-Range']
  ra_pps = (fgm_by_area['Restricted Area']*2+fga_by_area['Restricted Area']*0.22*2*0.8)/fga_by_area['Restricted Area']
  paint_pps = (fgm_by_area['In The Paint (Non-RA)']*2+fga_by_area['In The Paint (Non-RA)']*0.1855*2*0.8)/fga_by_area['In The Paint (Non-RA)']
  
  writer.writerow([datelimit,atb3_pps,c3_pps,midrange_pps,ra_pps,paint_pps])
  print(datelimit)


#get all shot made by area from game 1 to game x


def read_gamelist(datelist):
  f = open(datelist, 'r')
  print('opening')
  f.readline() # drop headers    
  for r in f.readlines():
    datenum = r.split(',')[0]
    seasonfull = r.split(',')[1]
    season = seasonfull[2:4]
    write_global_xefg_by_date(datenum,season)

def main():
   read_gamelist(sys.argv[1]) 

if __name__ == '__main__': 
  main()