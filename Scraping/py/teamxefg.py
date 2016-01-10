'''
generate team offense and defensive xefg and extra pts/pps from player and global xefg files 

'''

import pandas as pd
import tables
import sys
import csv
import numpy as np

f = open('xefg_by_team.csv', 'w')
gxefg_s = pd.read_csv('xefg_by_date.csv')
pxefg = pd.read_csv('xefg_by_player.csv')
writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
f.write('game_id,team_id,team_name,date,atb3_pts,atb3_attempt,atb3_pps,c3_pts,c3_attempt,c3_pps,mid_pts,mid_attempt,mid_pps,ra_pts,ra_attempt,ra_pps,paint_pts,paint_attempt,paint_pps,total_pps,location,size,oord,atb3_extrapps,c3_extrapps,mid_extrapps,ra_extrapps,paint_extrapps,atb3_extrapts,c3_extrapts,mid_extrapts,ra_extrapts,paint_extrapts,total_extrapts'+'\n')
locations = ['home','away']
p_sizes = ['All','Big','Small']


#for each game_id, generate 12 rows of data 
for gameid in pd.unique(pxefg['game_id'].values):
	print gameid
	#generate home offense/defense stats for all, big, small
	shots_game = pxefg[pxefg['game_id']==gameid]
	for loc in locations:
		for sizes in p_sizes:

			gxefg = gxefg_s[gxefg_s['player_size']==sizes]
			shots_game_size = shots_game[shots_game['location']==loc]
			if sizes == 'All':
				shots_game_loc = shots_game_size

			else: 
				shots_game_loc = shots_game_size[shots_game_size['player_size']==sizes]

			try:	
				team_id = int(shots_game_loc['team_id'].mean())
			except ValueError:
				team_id = 'nan'
			team_name_s = shots_game_loc['team_name'].values
			try: 
				team_name= team_name_s[0]
			except IndexError:
				team_name = 'nan'	
			date = shots_game_loc['date'].mean()
			if loc == 'home':
				oppoloc = 'away'
			else:
				oppoloc = 'home'	
			shots_game_oppoloc = shots_game[shots_game['location']==oppoloc]
			try:
				oppo_id = int(shots_game_oppoloc['team_id'].mean())
			except ValueError:
				oppo_id = 'nan'
			oppo_name_s = shots_game_oppoloc['team_name'].values
			try:
				oppo_name = oppo_name_s[0]
			except IndexError:
				oppo_name = 'nan'			

			#stats for all
			atb3_pts = shots_game_loc['atb3_pts'].sum()
			atb3_attempt = shots_game_loc['atb3_attempt'].sum()		
			try:
				atb3_pps = atb3_pts/atb3_attempt
			except ZeroDivisionError:
				atb3_pps = 0
			team_atb3_pps_extra = atb3_pps - gxefg[gxefg['date']==date]['atb3_pps'].sum()
			team_atb3_pts_extra = team_atb3_pps_extra*atb3_attempt
			if team_atb3_pps_extra == atb3_pps:
						team_atb3_pps_extra = 0
						team_atb3_pts_extra = 0

			c3_pts = shots_game_loc['c3_pts'].sum()
			c3_attempt = shots_game_loc['c3_attempt'].sum()
			try:
				c3_pps = c3_pts/c3_attempt
			except ZeroDivisionError:
				c3_pps = 0	
			team_c3_pps_extra = c3_pps - gxefg[gxefg['date']==date]['c3_pps'].sum()
			team_c3_pts_extra = team_c3_pps_extra*c3_attempt
			if team_c3_pps_extra == c3_pps:
						team_c3_pps_extra = 0
						team_c3_pts_extra = 0

			mid_pts = shots_game_loc['mid_pts'].sum()
			mid_attempt = shots_game_loc['mid_attempt'].sum()
			try:
				mid_pps = mid_pts/mid_attempt
			except ZeroDivisionError:
				mid_pps = 0	
			team_mid_pps_extra = mid_pps - gxefg[gxefg['date']==date]['mid_pps'].sum()
			team_mid_pts_extra = team_mid_pps_extra*mid_attempt
			if team_mid_pps_extra == mid_pps:
						team_mid_pps_extra = 0
						team_mid_pts_extra = 0

			ra_pts = shots_game_loc['ra_pts'].sum()
			ra_attempt = shots_game_loc['ra_attempt'].sum()
			try:
				ra_pps = ra_pts/ra_attempt
			except ZeroDivisionError:
				ra_pps = 0	
			team_ra_pps_extra = ra_pps - gxefg[gxefg['date']==date]['ra_pps'].sum()
			team_ra_pts_extra = team_ra_pps_extra*ra_attempt
			if team_ra_pps_extra == ra_pps:
						team_ra_pps_extra = 0
						team_ra_pts_extra = 0

			paint_pts = shots_game_loc['paint_pts'].sum()
			paint_attempt = shots_game_loc['paint_attempt'].sum()
			try:
				paint_pps = paint_pts/paint_attempt
			except ZeroDivisionError:
				paint_pps = 0
			team_paint_pps_extra = paint_pps - gxefg[gxefg['date']==date]['paint_pps'].sum()
			team_paint_pts_extra = team_paint_pps_extra*paint_attempt
			if team_paint_pps_extra == paint_pps:
						team_paint_pps_extra = 0
						team_paint_pts_extra = 0
			try:
				total_pps = np.nansum([atb3_pts,c3_pts,mid_pts,ra_pts,paint_pts])/np.nansum([atb3_attempt,c3_attempt,mid_attempt,ra_attempt,paint_attempt])
			except ZeroDivisionError:
				total_pps = np.nan
			total_extrapts = np.nansum([team_atb3_pts_extra,team_c3_pts_extra,team_mid_pts_extra,team_ra_pts_extra,team_paint_pts_extra])


			writer.writerow([gameid,team_id,team_name,date,atb3_pts,atb3_attempt,atb3_pps,c3_pts,c3_attempt,c3_pps,mid_pts,mid_attempt,mid_pps,ra_pts,ra_attempt,ra_pps,paint_pts,paint_attempt,paint_pps,total_pps,loc,sizes,'offense',team_atb3_pps_extra,team_c3_pps_extra,team_mid_pps_extra,team_ra_pps_extra,team_paint_pps_extra,team_atb3_pts_extra,team_c3_pts_extra,team_mid_pts_extra,team_ra_pts_extra,team_paint_pts_extra,total_extrapts])
			writer.writerow([gameid,oppo_id,oppo_name,date,atb3_pts,atb3_attempt,atb3_pps,c3_pts,c3_attempt,c3_pps,mid_pts,mid_attempt,mid_pps,ra_pts,ra_attempt,ra_pps,paint_pts,paint_attempt,paint_pps,total_pps,oppoloc,sizes,'defense',team_atb3_pps_extra,team_c3_pps_extra,team_mid_pps_extra,team_ra_pps_extra,team_paint_pps_extra,team_atb3_pts_extra,team_c3_pts_extra,team_mid_pts_extra,team_ra_pts_extra,team_paint_pts_extra,total_extrapts])




