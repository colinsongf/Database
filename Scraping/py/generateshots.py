'''
generate advanced shots data 

'''

import pandas as pd
import tables

#load the shots dataframe file
df_shots = pd.read_hdf('shots03.h5', 'df_shots')
game_id = 20300009
df_shots_game = df_shots[df_shots['GAME_ID']<game_id]



#generate XeFG% by area 
#get all shot attempts by area from game 1 to game x
totalfgr = df_shots_game['SHOT_MADE_FLAG'].mean(axis=0)
totalfgr = df_shots_game['SHOT_ATTEMPTED_FLAG'].sum(axis=0)

fgr_by_area = df_shots_game.SHOT_MADE_FLAG.groupby(df_shots_game.SHOT_ZONE_BASIC).mean()

print (totalfgr)
print (fgr_by_area)
print (totalfg)



#get all shot made by area from game 1 to game x











def read_gamelist(gamelist):
  f = open(gamelist, 'r')
  print('opening')
  f.readline() # drop headers    
  for r in f.readlines():
    gameid = r.split(',')[0]
    hteamabvr = r.split(',')[3]
    hteam = teamNames[hteamabvr]
    dateid = r.split(',')[2]
    dates = dateid[0:8]
    write_lines_by_date(dates,hteam,gameid)

def main():
   read_gamelist(sys.argv[1]) 

if __name__ == '__main__': 
  main()