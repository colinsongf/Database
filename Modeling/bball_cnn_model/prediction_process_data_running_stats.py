from psycopg2.extras import DictCursor
from random import shuffle
from pprint import pprint
import psycopg2
import numpy as np
import cPickle
from collections import defaultdict
import sys, re
import pandas as pd
import json
import csv

def build_data_cv(good_stats_list, cv=10, clean_string=True):
	"""
	Loads data and split into 10 folds.
	"""

	# inital variable params
	min_year = 14
	max_year = 14
	history_window = 5

	# loading line data
	print 'pulling line data ...'
	line_counter = -1
	line_header = []
	line_data = {}
	player_translations = {}
	f = open('/Users/terryjames/Dropbox/Public/NBA data/NBA_OU_data (since 2000).csv','r')
	for line in f:
		rows = line.split('\r')
		for row in rows:
			line_counter = line_counter + 1
			entries = row.split(',')
			if line_counter == 0:
				for entry in entries:
					line_header.append(entry)
				game_id_index = line_header.index('game_id')
				line_index = line_header.index('Line')
				atsr_index = line_header.index('ATSr')
			else:
				game_index = entries[game_id_index]
				line_data[game_index] = {}
				line_data[game_index]['line'] = entries[line_index]
				line_data[game_index]['atsr'] = entries[atsr_index]
	f.close()

	# loading game data
	print 'pulling game data ...'
	revs = []
	revs_header = []
	k = 40 # image width
	W = []
	W.append([0.0]*k)
	word_idx_map = {}
	word_idx_map['-1'] = 0
	file_counter = -1
	player_histories = {}
	team_histories = {}
	history_steps = history_window + 1
	player_game_counter = 0
	for c in range(min_year,max_year+1):
		year_indicator = "%02d" % (c,)
		for m in range(1,10000):

			if file_counter == -1:
				print 'year range: ' + year_indicator + ' - ' + ("%02d" % (max_year,))
				print 'history window: ' + str(history_window)
				file_counter = file_counter + 1
			game_indicator = "%05d" % (m,)
			try:
				file_suffix = 'bs_002' + year_indicator + game_indicator
				file_name = '/Users/terryjames/Dropbox/Public/NBA data/json/' + file_suffix + '.json'
				f = open(file_name,'r')
			except:
				continue
			for line in f:
				data = json.loads(line)
				game_summary_header = data[0]["headers"]
				game_summary = data[0]["rowSet"][0]
				line_score_header = data[1]["headers"]
				line_score = data[1]["rowSet"]
				player_stats_header = data[4]['headers']
				player_stats = data[4]["rowSet"]

			# determining teams
			home_team_index = game_summary_header.index('HOME_TEAM_ID')
			away_team_index = game_summary_header.index('VISITOR_TEAM_ID')
			home_team = game_summary[home_team_index]
			away_team = game_summary[away_team_index]
			team_dict = {}
			team_dict[home_team] = {}
			team_dict[away_team] = {}

			# determining winner with respect to line
			pts_index = line_score_header.index('PTS')
			team_index = line_score_header.index('TEAM_ID')
			game_index = line_score_header.index('GAME_ID')
			for team in line_score:
				game_id = team[game_index]
				if team[team_index] == home_team:
					home_score = team[pts_index]
				if team[team_index] == away_team:
					away_score = team[pts_index]
			if game_id in line_data:
				line_value = float(line_data[game_id]['line'])
				if line_data[game_id]['atsr'] == 'W':
					y = 1 # means home covers
				else:
					y = 0 # means home did not cover
			else:
				continue

			# gathering player stats
			sort_key = 'PTS'
			home_player_list = []
			home_player_sort_list = []
			away_player_list = []
			away_player_sort_list = []
			game_index = player_stats_header.index('GAME_ID')
			team_index = player_stats_header.index('TEAM_ID')
			comment_index = player_stats_header.index('COMMENT')
			point_index = player_stats_header.index('PTS')
			min_index = player_stats_header.index('MIN')
			history_met = 1
			team_history_met = 1
			for player in player_stats:

				player_dict = {}
				player_index = player_stats_header.index('PLAYER_ID')
				player_name_index = player_stats_header.index('PLAYER_NAME')
				player_key = player[player_index]
				player_name_key = player[player_name_index].lower()
				player_translations[player_key] = player_name_key
				player_game_key = str(player[player_index]) + '_' + str(player[game_index])

				# skip player if did not play any time
				if player[min_index] == None:
					continue

				# iterating through stats
				for i in range(0,len(player)):
					if player_stats_header[i] in good_stats_list:
						if player_stats_header[i] == 'MIN':
							min_parts = player[i].split(':')
							min_var = float(min_parts[0]) + (float(min_parts[1]) / 60.0)
							player_dict[player_stats_header[i]] = min_var
						else:
							player_dict[player_stats_header[i]] = float(player[i])

				# adding team specific stats
				team = player[team_index]
				player_dict['TEAM_ID'] = team
				if team == home_team:
					player_dict['TEAM'] = 1 # 1 means home team
					home_player_sort_list.append(player_dict[sort_key])
					home_player_list.append(player_game_key)
				if team == away_team:
					player_dict['TEAM'] = 0 # 0 means away team
					away_player_sort_list.append(player_dict[sort_key])
					away_player_list.append(player_game_key)

				# preparing aggregate team stats
				for stat in player_dict:
					if stat not in team_dict[team]:
						team_dict[team][stat] = 0.0
					team_dict[team][stat] = team_dict[team][stat] + player_dict[stat]
				team_dict[team]['GAME_ID'] = player[game_index]

				# recording player history
				if player_key not in player_histories:
					player_histories[player_key] = {}
					for b in range(0,history_steps):
						player_histories[player_key][str(b)] = {}
					player_histories[player_key]['sum'] = {}
					player_histories[player_key]['avg'] = {}

				for b in range(history_steps-1,0,-1):
					player_histories[player_key][str(b)] = dict(player_histories[player_key][str(b-1)])
				player_histories[player_key]['0'] = dict(player_dict)
				if player_histories[player_key][str(history_steps-1)] == {}:
					history_met = 0
				else:
					for key in player_histories[player_key]['0']:
						if key not in ['TEAM_ID','TEAM']:
							cur_sum = 0.0
							for b in range(1,history_steps):
								cur_sum = cur_sum + player_histories[player_key][str(b)][key]
							cur_avg = cur_sum / float(history_steps - 1)
							player_histories[player_key]['sum'][key] = cur_sum
							player_histories[player_key]['avg'][key] = cur_avg

			# recording team and opponent history
			team_list = [home_team,away_team]
			for team in team_list:
				if team not in team_histories:
					team_histories[team] = {}
					for b in range(0,history_steps):
						team_histories[team][str(b)] = {}
						team_histories[team]['oppo_' + str(b)] = {}
					team_histories[team]['sum'] = {}
					team_histories[team]['avg'] = {}
					team_histories[team]['oppo_' + 'sum'] = {}
					team_histories[team]['oppo_' + 'avg'] = {}
			team_counter = 0
			for team in team_list:
				oppo_team = team_list[team_counter * (-1) + 1]
				for b in range(history_steps-1,0,-1):
					team_histories[team][str(b)] = dict(team_histories[team][str(b-1)])
					team_histories[team]['oppo_' + str(b)] = dict(team_histories[team]['oppo_' + str(b-1)])
				team_histories[team]['0'] = dict(team_dict[team])
				team_histories[team]['oppo_' + '0'] = dict(team_dict[oppo_team])
				if (team_histories[team][str(history_steps-1)] == {}) or (team_histories[team]['oppo_' + str(history_steps-1)] == {}):
					team_history_met = 0
				else:
					for key in team_histories[team]['0']:
						if key not in ['GAME_ID','TEAM_ID','TEAM','FG_PCT','FG3_PCT','FT_PCT','PLUS_MINUS']:
							cur_sum = 0.0
							cur_oppo_sum = 0.0
							for b in range(1,history_steps):
								cur_sum = cur_sum + team_histories[team][str(b)][key]
								cur_oppo_sum = cur_oppo_sum + team_histories[team]['oppo_' + str(b)][key]
							cur_avg = cur_sum / float(history_steps - 1)
							cur_oppo_avg = cur_oppo_sum / float(history_steps - 1)
							team_histories[team]['sum'][key] = cur_sum
							team_histories[team]['avg'][key] = cur_avg
							team_histories[team]['oppo_' + 'sum'][key] = cur_oppo_sum
							team_histories[team]['oppo_' + 'avg'][key] = cur_oppo_avg
				team_counter = team_counter + 1

			# pass over game if players do not have history required
			if (history_met == 0) or (team_history_met == 0):
				continue

			# team aggregate stats continued
			for team in team_list:
				team_histories[team]['sum']['FG_PCT'] = team_histories[team]['sum']['FGM'] / team_histories[team]['sum']['FGA']
				team_histories[team]['sum']['FG3_PCT'] = team_histories[team]['sum']['FG3M'] / team_histories[team]['sum']['FG3A']
				team_histories[team]['sum']['FT_PCT'] = team_histories[team]['sum']['FTM'] / team_histories[team]['sum']['FTA']

				TM_MP = team_histories[team]['sum']['MIN']
				TM_PTS = team_histories[team]['sum']['PTS']
				TM_FGA = team_histories[team]['sum']['FGA']
				TM_FTA = team_histories[team]['sum']['FTA']
				TM_ORB = team_histories[team]['sum']['OREB']
				TM_DRB = team_histories[team]['sum']['DREB']
				TM_FGM = team_histories[team]['sum']['FGM']
				TM_TOV = team_histories[team]['sum']['TO']

				OP_MP = team_histories[team]['oppo_' + 'sum']['MIN']
				OP_PTS = team_histories[team]['oppo_' + 'sum']['PTS']
				OP_FGA = team_histories[team]['oppo_' + 'sum']['FGA']
				OP_FTA = team_histories[team]['oppo_' + 'sum']['FTA']
				OP_ORB = team_histories[team]['oppo_' + 'sum']['OREB']
				OP_DRB = team_histories[team]['oppo_' + 'sum']['DREB']
				OP_FGM = team_histories[team]['oppo_' + 'sum']['FGM']
				OP_TOV = team_histories[team]['oppo_' + 'sum']['TO']

				TM_TRB = TM_ORB + TM_DRB
				OP_TRB = OP_ORB + OP_DRB
				team_histories[team]['sum']['TRB'] = TM_TRB
				team_histories[team]['oppo_' + 'sum']['TRB'] = OP_TRB
				TM_POS = 0.5*((TM_FGA+0.4*TM_FTA-1.07*(TM_ORB/(TM_ORB+OP_DRB))*(TM_FGA-TM_FGM)+TM_TOV)+(OP_FGA+0.4*OP_FTA-1.07*(OP_ORB/(OP_ORB+TM_DRB))*(OP_FGA-OP_FGM)+OP_TOV))
				OP_POS = TM_POS
				team_histories[team]['sum']['POS'] = TM_POS
				team_histories[team]['oppo_' + 'sum']['POS'] = OP_POS
				TM_PACE = 48.0*((TM_POS+OP_POS)/(2.0*(TM_MP/5.0)))
				OP_PACE = 48.0*((OP_POS+TM_POS)/(2.0*(OP_MP/5.0)))
				team_histories[team]['sum']['PACE'] = TM_PACE
				team_histories[team]['oppo_' + 'sum']['PACE'] = OP_PACE

			# league stats
			lg_games = {}
			temp_team = team
			league_stats = {}
			for key in team_histories[temp_team]['sum']:
				if key not in ['GAME_ID','TEAM_ID','TEAM','FG_PCT','FG3_PCT','FT_PCT','PLUS_MINUS']:
					league_stats[key] = 0.0
			for team in team_histories:
				if 'POS' in team_histories[team]['sum']: # ensuring teams have team stat history
					for key in league_stats:
						league_stats[key] = league_stats[key] + team_histories[team]['sum'][key]
					for b in range(1,history_steps):
						cur_game_id = team_histories[team][str(b)]['GAME_ID']
						lg_games[cur_game_id] = 1
			league_stats['FG_PCT'] = league_stats['FGM'] / league_stats['FGA']
			league_stats['FG3_PCT'] = league_stats['FG3M'] / league_stats['FG3A']
			league_stats['FT_PCT'] = league_stats['FTM'] / league_stats['FTA']
			league_stats['PACE'] = league_stats['PACE'] / float(len(team_histories))
			league_stats['PPG'] = league_stats['PTS'] / float(len(lg_games))

			# advanced stats and line
			game_player_list = home_player_list + away_player_list
			for player_game_key in game_player_list:

				player_key = int(player_game_key.split('_')[0])
				plyr_sum = player_histories[player_key]['sum']
				plyr_avg = player_histories[player_key]['avg']
				player_list = []


				# adv stat: team indicators and team stats
				plyr_avg['TEAM'] = player_histories[player_key]['0']['TEAM']
				team = player_histories[player_key]['0']['TEAM_ID']

				TM_MP = team_histories[team]['sum']['MIN']
				TM_AST = team_histories[team]['sum']['AST']
				TM_BLK = team_histories[team]['sum']['BLK']
				TM_STL = team_histories[team]['sum']['STL']
				TM_PF = team_histories[team]['sum']['PF']
				TM_PTS = team_histories[team]['sum']['PTS']
				TM_FGA = team_histories[team]['sum']['FGA']
				TM_FTA = team_histories[team]['sum']['FTA']
				TM_FTM = team_histories[team]['sum']['FTM']
				TM_3PA = team_histories[team]['sum']['FG3A']
				TM_3PM = team_histories[team]['sum']['FG3M']
				TM_ORB = team_histories[team]['sum']['OREB']
				TM_FGM = team_histories[team]['sum']['FGM']
				TM_TOV = team_histories[team]['sum']['TO']
				TM_DRB = team_histories[team]['sum']['DREB']

				OP_MP = team_histories[team]['oppo_' + 'sum']['MIN']
				OP_AST = team_histories[team]['oppo_' + 'sum']['AST']
				OP_BLK = team_histories[team]['oppo_' + 'sum']['BLK']
				OP_STL = team_histories[team]['oppo_' + 'sum']['STL']
				OP_PF = team_histories[team]['oppo_' + 'sum']['PF']
				OP_PTS = team_histories[team]['oppo_' + 'sum']['PTS']
				OP_FGA = team_histories[team]['oppo_' + 'sum']['FGA']
				OP_FTA = team_histories[team]['oppo_' + 'sum']['FTA']
				OP_FTM = team_histories[team]['oppo_' + 'sum']['FTM']
				OP_3PA = team_histories[team]['oppo_' + 'sum']['FG3A']
				OP_3PM = team_histories[team]['oppo_' + 'sum']['FG3M']
				OP_ORB = team_histories[team]['oppo_' + 'sum']['OREB']
				OP_FGM = team_histories[team]['oppo_' + 'sum']['FGM']
				OP_TOV = team_histories[team]['oppo_' + 'sum']['TO']
				OP_DRB = team_histories[team]['oppo_' + 'sum']['DREB']

				TM_TRB = team_histories[team]['sum']['TRB']
				OP_TRB = team_histories[team]['oppo_' + 'sum']['TRB']
				TM_POS = team_histories[team]['sum']['POS']
				OP_POS = team_histories[team]['oppo_' + 'sum']['POS']
				TM_PACE = team_histories[team]['sum']['PACE']
				OP_PACE = team_histories[team]['oppo_' + 'sum']['PACE']


				# adv stat: average pace
				plyr_avg['PACE'] = TM_PACE / float(history_steps - 1)

				# adv stat: true shot percentage
				if plyr_sum['FGA'] + plyr_sum['FTA'] == 0.0:
					plyr_avg['TS'] = 0.0
				else:
					plyr_avg['TS'] = plyr_sum['PTS'] / (2*(plyr_sum['FGA'] + 0.44*plyr_sum['FTA']))

				# adv stat: 3 pointers attempt rate
				if plyr_sum['FGA'] == 0.0:
					plyr_avg['3PAr'] = 0.0
				else:
					plyr_avg['3PAr'] = plyr_sum['FG3A'] / plyr_sum['FGA']

				# adv stat: free throw attempt rate
				if plyr_sum['FGA'] == 0.0:
					plyr_avg['FTr'] = 0.0
				else:
					plyr_avg['FTr'] = plyr_sum['FTA'] / plyr_sum['FGA']

				# adv stat: offensive rebounding rate
				plyr_avg['ORBr'] = (plyr_sum['OREB']*(TM_MP/5))/(plyr_sum['MIN']*(TM_ORB+OP_ORB))

				# adv stat: defensive rebounding rate
				plyr_avg['DRBr'] = (plyr_sum['DREB']*(TM_MP/5))/(plyr_sum['MIN']*(TM_DRB+OP_ORB))

				# adv stat: total rebounding rate
				TRB = plyr_sum['OREB'] + plyr_sum['DREB']
				plyr_avg['TRBr'] = (TRB*(TM_MP/5))/(plyr_sum['MIN']*(TM_TRB+OP_ORB))

				# adv stat: assist rate
				plyr_avg['ASTr'] = plyr_sum['AST']/(((plyr_sum['MIN']/(TM_MP/5))*TM_FGM)-plyr_sum['FGM'])

				# adv stat: steal rate
				plyr_avg['STLr'] = (plyr_sum['STL']*(TM_MP/5))/(plyr_sum['MIN']*OP_POS)

				# adv stat: blocking rate
				plyr_avg['BLKr'] = (plyr_sum['BLK']*(TM_MP/5))/(plyr_sum['MIN']*(OP_FGA - OP_3PA))

				# adv stat: turnover rate
				if plyr_sum['FGA'] + plyr_sum['FTA'] + plyr_sum['TO'] == 0.0:
					plyr_avg['TOVr'] = 0.0
				else:
					plyr_avg['TOVr'] = plyr_sum['TO']/(plyr_sum['FGA']+0.44*plyr_sum['FTA']+plyr_sum['TO'])

				# adv stat: usage rate
				plyr_avg['USGr'] = ((plyr_sum['FGA']+0.44*plyr_sum['FTA']+plyr_sum['TO'])*(TM_MP/5))/(plyr_sum['MIN']*(TM_FGA+0.44*TM_FTA+TM_TOV))


				# adv stat: offensive win share and ortg
				qAST = ((plyr_sum['MIN']/(TM_MP/5))*(1.14*((TM_AST-plyr_sum['AST'])/TM_FGM)))+((((TM_AST/TM_MP)*plyr_sum['MIN']*5-plyr_sum['AST'])/((TM_FGM/TM_MP)*plyr_sum['MIN']*5-plyr_sum['FGM']))*(1-(plyr_sum['MIN']/(TM_MP/5))))
				if plyr_sum['FGA'] == 0.0:
					FG_part = plyr_sum['FGM']*(1-0.5*(0.0)*qAST)
				else:
					FG_part = plyr_sum['FGM']*(1-0.5*((plyr_sum['PTS']-plyr_sum['FTM'])/(2*plyr_sum['FGA']))*qAST)
				AST_part = 0.5*(((TM_PTS-TM_FTM)-(plyr_sum['PTS']-plyr_sum['FTM']))/(2*(TM_FGA-plyr_sum['FGA'])))*plyr_sum['AST']
				if plyr_sum['FTA'] == 0.0:
					FT_part = (1-(1-(0.0))**2)*0.4*plyr_sum['FTA']
				else:
					FT_part = (1-(1-(plyr_sum['FTM']/plyr_sum['FTA']))**2)*0.4*plyr_sum['FTA']

				TM_scoring_poss = TM_FGM+(1-(1-(TM_FTM/TM_FTA))**2)*TM_FTA*0.4
				TM_plays = TM_scoring_poss/(TM_FGA+TM_FTA*0.4+TM_TOV)
				TM_ORBr = TM_ORB/(TM_ORB+(OP_TRB-OP_ORB))
				TM_ORB_weight = ((1-TM_ORBr)*TM_plays)/((1-TM_ORBr)*TM_plays+TM_ORBr*(1-TM_plays))

				ORB_part = plyr_sum['OREB']*TM_ORB_weight*TM_plays
				missed_FG_pos = (plyr_sum['FGA']-plyr_sum['FGM'])*(1-1.07*TM_ORBr)
				if plyr_sum['FTA'] == 0.0:
					missed_FT_pos = ((1-(0.0))**2)*0.4*plyr_sum['FTA']
				else:
					missed_FT_pos = ((1-(plyr_sum['FTM']/plyr_sum['FTA']))**2)*0.4*plyr_sum['FTA']

				if plyr_sum['FGA'] == 0.0:
					PProd_FG_part = 2*(plyr_sum['FGM']+0.5*plyr_sum['FG3M'])*(1-0.5*(0.0)*qAST)
				else:
					PProd_FG_part = 2*(plyr_sum['FGM']+0.5*plyr_sum['FG3M'])*(1-0.5*((plyr_sum['PTS']-plyr_sum['FTM'])/(2*plyr_sum['FGA']))*qAST)
				PProd_AST_part = 2*((TM_FGM - plyr_sum['FGM'] +0.5*(TM_3PM-plyr_sum['FG3M']))/(TM_FGM-plyr_sum['FGM']))*0.5*(((TM_PTS-TM_FTM)-(plyr_sum['PTS']-plyr_sum['FTM']))/(2*(TM_FGA-plyr_sum['FGA'])))*plyr_sum['AST']
				PProd_ORB_part = plyr_sum['OREB']*TM_ORB_weight*TM_plays*(TM_PTS/(TM_FGM+(1-(1-(TM_FTM/TM_FTA))**2)*0.4*TM_FTA))
				points_produced = (PProd_FG_part+PProd_AST_part+plyr_sum['FTM'])*(1-(TM_ORB/TM_scoring_poss)*TM_ORB_weight*TM_plays)+PProd_ORB_part

				scoring_posessions = (FG_part+AST_part+FT_part)*(1-(TM_ORB/TM_scoring_poss)*TM_ORB_weight*TM_plays)+ORB_part
				total_off_possessions = scoring_posessions+missed_FG_pos+missed_FT_pos+plyr_sum['TO']
				if total_off_possessions == 0:
					plyr_avg['ORtg'] = 0.0
				else:
					plyr_avg['ORtg'] = 100*points_produced/total_off_possessions
				LG_points_per_poss = league_stats['PTS'] / league_stats['POS']
				marginal_offense = points_produced-0.92*(LG_points_per_poss)*total_off_possessions
				marginal_pts_per_win = 0.32*(league_stats['PPG'])*((TM_PACE)/(league_stats['PACE']))
				plyr_avg['OWS'] = marginal_offense/marginal_pts_per_win


				# adv stat: defensive win share and drtg
				DORr = OP_ORB/(OP_ORB+TM_DRB)
				DFGr = OP_FGM/OP_FGA
				TM_DRTG = 100*(OP_PTS/TM_POS)
				FMwt = (DFGr*(1-DORr))/(DFGr*(1-DORr)+(1-DFGr)*DORr)

				stops1 = plyr_sum['STL']+plyr_sum['BLK']*FMwt*(1-1.07*DORr)+plyr_sum['DREB']*(1-FMwt)
				stops2 = (((OP_FGA-OP_FGM-TM_BLK)/TM_MP)*FMwt*(1-1.07*DORr)+((OP_TOV-TM_STL)/TM_MP))*plyr_sum['MIN']+(plyr_sum['PF']/TM_PF)*0.4*OP_FTA*(1-(OP_FTM/OP_FTA))**2
				stops = stops1+stops2
				stopr = (stops*OP_MP)/(TM_POS*plyr_sum['MIN'])

				D_pts_per_ScPoss = OP_PTS/(OP_FGM+(1-(1-(OP_FTM/OP_FTA))**2)*OP_FTA*0.4)
				DRtg = TM_DRTG+0.2*(100*D_pts_per_ScPoss*(1-stopr)-TM_DRTG)
				plyr_avg['DRtg'] = DRtg
				marginal_defense = (plyr_sum['MIN']/TM_MP)*(TM_POS)*(1.08*(LG_points_per_poss)-((DRtg)/100))
				plyr_avg['DWS'] = marginal_defense/marginal_pts_per_win


				# adv stat: total win share
				plyr_avg['WS'] = plyr_avg['DWS'] + plyr_avg['OWS']

				# adv stat: win share per 48 minutes
				plyr_avg['WS48'] = plyr_avg['WS'] / ((TM_MP/5)/plyr_sum['MIN'])

				# line stat: value given for home players, 0 for away players
				if plyr_avg['TEAM'] == 1:
					plyr_avg['LINE'] = line_value
				if plyr_avg['TEAM'] == 0:
					plyr_avg['LINE'] = 0.0

				# inserting into CNN dict while maintaining data order
				for key in sorted(plyr_avg):
					if player_game_counter == 0:
						revs_header.append(key)
					player_list.append(plyr_avg[key])
				W.append(player_list)
				player_game_counter = player_game_counter + 1
				word_idx_map[player_game_key] = player_game_counter

			# formatting data output
			padding = 15
			padding_2 = 5
			home_player_list = [x for (q,x) in sorted(zip(home_player_sort_list,home_player_list), reverse=True)]
			away_player_list = [x for (q,x) in sorted(zip(away_player_sort_list,away_player_list), reverse=True)]
			while len(home_player_list) < padding :
				home_player_list.append('-1')
			while len(away_player_list) < padding :
				away_player_list.append('-1')

			text = ' '.join(home_player_list + ['-1']*padding_2  + away_player_list)
			datum  = {"y": y,
					  "text": text,
					  "game_id": game_id,
					  "split": np.random.randint(0,cv)}
			revs.append(datum)
			file_counter = file_counter + 1
			print 'finished preping file: ' + str(file_counter) + ', game: ' + file_suffix
			f.close()

	# player translation output
	f_plyr = open('prediction_process_data_running_stats_players.txt','w')
	f_plyr.write('player_id,player_name\n')
	for plyr in player_translations:
		f_plyr.write(str(plyr) + ',' + player_translations[plyr] + '\n')
	f_plyr.close()

	# final output for model input
	print "rows pulled: " + str(file_counter)
	print "revs length: " + str(len(revs))
	print 'datum length: ' + str(len(datum))
	W2 = np.zeros(shape=(player_game_counter+1, k))
	for z in range(0,len(W)):
		W2[z] = W[z]
	return revs, W2, word_idx_map, revs_header

if __name__=="__main__":
	good_stats_list = ['MIN','FGM','FGA','FG_PCT','FG3M','FG3A','FG3_PCT','FTM','FTA',
						'FT_PCT','OREB','DREB','REB','AST','STL','BLK','TO','PF',
						'PTS','PLUS_MINUS']
	revs, W, word_idx_map, revs_header = build_data_cv(good_stats_list, cv=10, clean_string=True)
	cPickle.dump([revs, W, word_idx_map], open("mr_prediction.p", "wb"))
	print "dataset created!"

	# outputting full formatted file for visual checking
	full_output = {}
	full_output['games'] = {}
	full_output['header'] = ', '.join(map(str,revs_header))
	print "outputting checking data file ... "
	for rev in revs:
		game_id = str(int(rev['game_id']))
		full_output['games'][game_id] = {}
		full_output['games'][game_id]['y'] = str(int(rev['y']))
		full_output['games'][game_id]['image'] = {}
		map_key = rev['text'].split(' ')
		for j in range(0,len(map_key)):
			list_val = list(W[word_idx_map[map_key[j]]])
			full_output['games'][game_id]['image'][str(j)] = {}
			full_output['games'][game_id]['image'][str(j)]['data'] = ', '.join(map(str,list_val))
			full_output['games'][game_id]['image'][str(j)]['player'] = map_key[j].split('_')[0]
	f_out = open('prediction_process_data_running_stats_checking.txt','w')
	f_out.write(str(full_output))
	f_out.close()
	print "checked file created!"
