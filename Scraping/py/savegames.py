#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 21 23:28:55 2015

savegames.py
Get lists of games from NBA.com -> save as comma-separated-values (csv)

@author: gmf
"""

import requests
import datetime

games_url_base = 'http://data.nba.com/5s/json/cms/noseason/scoreboard/%s/games.json'
f = open('gamelist.csv','w')
f.write('gameid_num,seasonid,gameid,home,away'+ '\n')

teamNames = {'ATL': '1610612737', 'BOS':'1610612738', 'BKN':'1610612751', 'CHA':'1610612766','CHI':'1610612741','CLE':'1610612739','DAL':'1610612742','DEN':'1610612743','DET':'1610612765','GSW':'1610612744','HOU':'1610612745','IND':'1610612754','LAC':'1610612746','LAL':'1610612747','MEM':'1610612763','MIA':'1610612748','MIL':'1610612749','MIN':'1610612750','NOP':'1610612740','NYK':'1610612752','OKC':'1610612760','ORL':'1610612753','PHI':'1610612755','PHX':'1610612756','POR':'1610612757','SAC':'1610612758','SAS':'1610612759','TOR':'1610612761','UTA':'1610612762','WAS':'1610612764'}  


def get_gamelist_by_date(date_iso):
  games_url = games_url_base % date_iso
  G = requests.get(games_url).json()['sports_content']['games']
  if G is not None and len(G)>0:
    return G['game']
  else:
    return []

def write_gamelist_by_date(seasonid,startday,stopday):
  numdays = (stopday-startday).days
  datelist = [startday + datetime.timedelta(days=x) for x in range(0, numdays+1)]
  count=1
  season_year = str(startday.year - (startday.month<8))
  
  
  for d in datelist:
    diso = str.replace(d.isoformat(),'-','')
    gamelist = get_gamelist_by_date(diso)
    # do something with gamelist
    ngames = len(gamelist)
    for n in range(ngames):
      seasonid0 = gamelist[n]['season_id']
      seasonid1 = '00%s%s%s' % (seasonid0[0], seasonid0[3], seasonid0[4])
      if seasonid1 == seasonid:
        gameid,homeabbr,awayabbr = gamelist[n]['id'],gamelist[n]['home']['abbreviation'],gamelist[n]['visitor']['abbreviation']    
        gameid0 = diso + awayabbr + homeabbr
        print d,gameid,seasonid,homeabbr,awayabbr
        f.write(str(gameid) +','+ str(seasonid1) +','+ str(gameid0) +','+ teamNames[homeabbr] +','+ teamNames[awayabbr] + '\n')
  f.close()

def main():
  seasonid = '00215'
  startday = datetime.date(2015,10,27)
  stopday = datetime.date(2016,1,19)
  write_gamelist_by_date(seasonid, startday, stopday)

if __name__ == '__main__': 
  main()
