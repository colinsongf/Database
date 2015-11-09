# IMPORTS
from psycopg2.extras import DictCursor
from collections import defaultdict
from random import shuffle
from pprint import pprint
import scipy.stats as ss
import pandas as pd
import numpy as np
import psycopg2
import cPickle
import json
import copy
import csv
import sys
import re



# CREATING GAME IMAGES
def build_data_cv(hardcoded_stat_list, cv=10, clean_string=True):
	"""
	Loads data and split into 10 folds.
	"""

	# inital params
	min_year = 14
	max_year = 14
	history_window = 5
	good_stats_list = hardcoded_stat_list[0]
	action_types = hardcoded_stat_list[1]
	shot_zones_basic = hardcoded_stat_list[2]
	shot_zones_areas = hardcoded_stat_list[3]


	# initializing template for special stats
	player_action_initialize = {}
	for action in action_types:
		player_action_initialize[action] = 0.0
	player_action_initialize['avg_dist'] = 0.0
	player_action_initialize['avg_dist_GE10'] = 0.0
	player_action_initialize['avg_dist_LE2'] = 0.0
	periods = ['P1','P2','P3','P4','Pover']
	quarter_section = ['GE10','LE2','ALL']
	metric_type = ['FGMade','FGMiss','FGB']
	player_shots_timing_initialize = {}
	for period in periods:
		for section in quarter_section:
			for metric in metric_type:
				key = period + '_' + section + '_' + metric
				player_action_initialize[key] = 0.0
				player_shots_timing_initialize[key] = 0.0
	fgmade_suffix = '_FGMade'
	fgmiss_suffix = '_FGMiss'
	fgb_suffix = '_FGB'


	# loading line data
	print '\npulling line data ...'
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
				rest_index = line_header.index('Rest')
			else:
				game_index = entries[game_id_index]
				line_data[game_index] = {}
				line_data[game_index]['line'] = entries[line_index]
				line_data[game_index]['atsr'] = entries[atsr_index]
				line_data[game_index]['rest'] = entries[rest_index]
	f.close()


	# loading game data
	print 'pulling game data ...'
	revs = []
	revs_header = []

	W = []
	word_idx_map = {}
	word_idx_map['-1'] = 0
	player_game_counter = 0

	W2 = []
	word_idx_map2 = {}
	word_idx_map2['-1'] = 0
	player_game_counter2 = 0

	file_counter = -1
	player_histories = {}
	team_histories = {}
	history_steps = history_window + 1

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


			# determining winner with respect to line and rest stat
			pts_index = line_score_header.index('PTS')
			team_index = line_score_header.index('TEAM_ID')
			game_index = line_score_header.index('GAME_ID')
			for team in line_score:
				game_id = team[game_index]
			if game_id not in line_data:
				continue

			for team in line_score:
				rest_list = (line_data[game_id]['rest']).split('&')
				if team[team_index] == home_team:
					home_rest = rest_list[0]
					if home_rest == '':
						home_rest = 0.0
					else:
						home_rest = float(home_rest)
				if team[team_index] == away_team:
					away_rest = rest_list[1]
					if away_rest == '':
						away_rest = 0.0
					else:
						away_rest = float(away_rest)
			line_value = float(line_data[game_id]['line'])
			if line_data[game_id]['atsr'] == 'W':
				y = 1 # means home covers
			else:
				y = 0 # means home did not cover


			# loading team shot data
			shots_prefix = '/Users/terryjames/Dropbox/Public/NBA data/json/'
			shots_file = shots_prefix + 'shots_' + str(game_id) + '.json'
			f_shots = open(shots_file,'r')
			shot_action_zones = {}
			player_shots_dict = {}
			player_shots_timing_dict = {}
			player_shots_location_dict = {}

			for shots_line in f_shots:
				shots_dict = json.loads(shots_line)
				shots_header = shots_dict['headers']
				shots_rows = shots_dict['rowSet']

				action_type_index = shots_header.index('ACTION_TYPE')
				player_index = shots_header.index('PLAYER_ID')
				cur_team_index = shots_header.index('TEAM_ID')
				period_index = shots_header.index('PERIOD')
				min_remain_index = shots_header.index('MINUTES_REMAINING')
				shot_zone_basic_index = shots_header.index('SHOT_ZONE_BASIC')
				shot_zone_area_index = shots_header.index('SHOT_ZONE_AREA')
				shot_attempted_index = shots_header.index('SHOT_ATTEMPTED_FLAG')
				shot_made_index = shots_header.index('SHOT_MADE_FLAG')
				dist_index = shots_header.index('SHOT_DISTANCE')

				# iterating through each shot
				for row in shots_rows:

					# ignore shot if unrecognized action performed
					action_type = row[action_type_index]
					if action_type not in action_types:
						continue

					player = row[player_index]
					cur_team = row[cur_team_index]
					period = row[period_index]
					min_remain = row[min_remain_index]
					shot_zone_basic = row[shot_zone_basic_index]
					shot_zone_area = row[shot_zone_area_index]
					shot_attempted = row[shot_attempted_index]
					shot_made = row[shot_made_index]
					dist_loc = row[dist_index]

					if player not in player_shots_dict:
						player_shots_dict[player] = {}
						player_shots_timing_dict[player] = copy.deepcopy(player_shots_timing_initialize)
						player_shots_location_dict[player] = {}
						player_shots_location_dict[player]['avg_dist'] = 0.0
						player_shots_location_dict[player]['avg_dist_GE10'] = 0.0
						player_shots_location_dict[player]['avg_dist_GE10_c'] = 0.0
						player_shots_location_dict[player]['avg_dist_LE2'] = 0.0
						player_shots_location_dict[player]['avg_dist_LE2_c'] = 0.0
					if action_type not in player_shots_dict[player]:
						player_shots_dict[player][action_type] = {}
						player_shots_dict[player][action_type]['misses'] = 0
						player_shots_dict[player][action_type]['mades'] = 0
						player_shots_dict[player][action_type]['beta'] = 0.0

					# only check if shot attempted
					if shot_attempted == 1:

						# shot position related stats
						player_shots_location_dict[player]['avg_dist'] = dist_loc + player_shots_location_dict[player]['avg_dist']

						# shot time related stats
						if period >= 5:
							period = 'over'
						period_str = 'P' + str(period)
						if min_remain >= 10:
							timing = 'GE10'
							player_shots_location_dict[player]['avg_dist_GE10'] = dist_loc + player_shots_location_dict[player]['avg_dist_GE10']
							player_shots_location_dict[player]['avg_dist_GE10_c'] = 1.0 + player_shots_location_dict[player]['avg_dist_GE10_c']
						if min_remain <= 2:
							timing = 'LE2'
							player_shots_location_dict[player]['avg_dist_LE2'] = dist_loc + player_shots_location_dict[player]['avg_dist_LE2']
							player_shots_location_dict[player]['avg_dist_LE2_c'] = 1.0 + player_shots_location_dict[player]['avg_dist_LE2_c']
						timing_key = period_str + '_' + timing
						all_timing_key = period_str + '_ALL'

						# if the shot was missed
						if shot_made == 0:
							misses = player_shots_dict[player][action_type]['misses'] + 1
							mades = player_shots_dict[player][action_type]['mades']
							player_shots_dict[player][action_type]['misses'] = misses

							timing_misses = player_shots_timing_dict[player][timing_key + fgmiss_suffix] + 1
							timing_mades = player_shots_timing_dict[player][timing_key + fgmade_suffix]
							player_shots_timing_dict[player][timing_key + fgmiss_suffix] = timing_misses

							all_timing_misses = player_shots_timing_dict[player][all_timing_key + fgmiss_suffix] + 1
							all_timing_mades = player_shots_timing_dict[player][all_timing_key + fgmade_suffix]
							player_shots_timing_dict[player][all_timing_key + fgmiss_suffix] = all_timing_misses

						# if the shot was made
						if shot_made == 1:
							misses = player_shots_dict[player][action_type]['misses']
							mades = player_shots_dict[player][action_type]['mades'] + 1
							player_shots_dict[player][action_type]['mades'] = mades

							timing_misses = player_shots_timing_dict[player][timing_key + fgmiss_suffix]
							timing_mades = player_shots_timing_dict[player][timing_key + fgmade_suffix] + 1
							player_shots_timing_dict[player][timing_key + fgmade_suffix] = timing_mades

							all_timing_misses = player_shots_timing_dict[player][all_timing_key + fgmiss_suffix]
							all_timing_mades = player_shots_timing_dict[player][all_timing_key + fgmade_suffix] + 1
							player_shots_timing_dict[player][all_timing_key + fgmade_suffix] = all_timing_mades


			# gathering player stats
			sort_key = 'PLUS_MINUS'
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
				player_dict = copy.deepcopy(player_action_initialize)
				player_index = player_stats_header.index('PLAYER_ID')
				player_name_index = player_stats_header.index('PLAYER_NAME')
				player_key = player[player_index]
				player_name_key = player[player_name_index].lower()
				player_translations[player_key] = player_name_key
				player_game_key = str(player_key) + '_' + str(player[game_index])

				# skip player if did not play any time
				if player[min_index] == None:
					continue

				# iterating through main stats
				for i in range(0,len(player)):
					if player_stats_header[i] in good_stats_list:
						if player_stats_header[i] == 'MIN':
							min_parts = player[i].split(':')
							min_var = float(min_parts[0]) + (float(min_parts[1]) / 60.0)
							player_dict[player_stats_header[i]] = min_var
						else:
							player_dict[player_stats_header[i]] = float(player[i])

				# adding shot and special player stats
				if player_key in player_shots_timing_dict:
					for period in periods:
						for section in quarter_section:
							cur_key = period + '_' + section
							misses = player_shots_timing_dict[player_key][cur_key + fgmiss_suffix]
							mades = player_shots_timing_dict[player_key][cur_key + fgmade_suffix]
							player_shots_timing_dict[player_key][cur_key + fgb_suffix] = ss.beta.ppf(.1,1+mades,1+misses)
				if player_key in player_shots_dict:
					for action in player_shots_dict[player_key]:
						player_dict[action] = float(player_shots_dict[player_key][action]['mades'])
					for action in player_shots_timing_dict[player_key]:
						player_dict[action] = float(player_shots_timing_dict[player_key][action])
					player_dict['avg_dist'] = player_shots_location_dict[player_key]['avg_dist'] / player_dict['FGA']
					if player_shots_location_dict[player_key]['avg_dist_GE10_c'] > 0:
						player_dict['avg_dist_GE10'] = player_shots_location_dict[player_key]['avg_dist_GE10'] / player_shots_location_dict[player_key]['avg_dist_GE10_c']
					if player_shots_location_dict[player_key]['avg_dist_LE2_c'] > 0:
						player_dict['avg_dist_LE2'] = player_shots_location_dict[player_key]['avg_dist_LE2'] / player_shots_location_dict[player_key]['avg_dist_LE2_c']

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
					player_histories[player_key][str(b)] = copy.deepcopy(player_histories[player_key][str(b-1)])
				player_histories[player_key]['0'] = copy.deepcopy(player_dict)
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
					team_histories[team][str(b)] = copy.deepcopy(team_histories[team][str(b-1)])
					team_histories[team]['oppo_' + str(b)] = copy.deepcopy(team_histories[team]['oppo_' + str(b-1)])
				team_histories[team]['0'] = copy.deepcopy(team_dict[team])
				team_histories[team]['oppo_' + '0'] = copy.deepcopy(team_dict[oppo_team])
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
			lg_teams = {}
			temp_team = team
			league_stats = {}
			for key in team_histories[temp_team]['sum']:
				if key not in ['GAME_ID','TEAM_ID','TEAM','FG_PCT','FG3_PCT','FT_PCT','PLUS_MINUS']:
					league_stats[key] = 0.0
			for team in team_histories:
				if 'POS' in team_histories[team]['sum']: # ensuring teams have team stat history
					lg_teams[team] = 1
					for key in league_stats:
						league_stats[key] = league_stats[key] + team_histories[team]['sum'][key]
					for b in range(1,history_steps):
						cur_game_id = team_histories[team][str(b)]['GAME_ID']
						lg_games[cur_game_id] = 1
			league_stats['FG_PCT'] = league_stats['FGM'] / league_stats['FGA']
			league_stats['FG3_PCT'] = league_stats['FG3M'] / league_stats['FG3A']
			league_stats['FT_PCT'] = league_stats['FTM'] / league_stats['FTA']
			league_stats['PACE'] = league_stats['PACE'] / float(len(lg_teams))
			league_stats['PPG'] = league_stats['PTS'] / float(len(lg_games))


			# advanced stats and line
			game_player_list = home_player_list + away_player_list
			for player_game_key in game_player_list:

				player_key = int(player_game_key.split('_')[0])
				plyr_sum = player_histories[player_key]['sum']
				plyr_avg = player_histories[player_key]['avg']
				#plyr_avg = {}
				player_list = []

				# adv stat: team indicators and team stats
				plyr_avg['TEAM'] = player_histories[player_key]['0']['TEAM']
				team = player_histories[player_key]['0']['TEAM_ID']

				# team shot stats
				'''
				for shot_key in team_histories[team]['avg']:
					if 'SHOTSTATS' in shot_key:
						if shot_key[-1] == 'M':
							plyr_avg[shot_key + ' TM'] = 1.0 if (team_histories[team]['avg'][shot_key] - (league_stats[shot_key] / float(len(lg_teams)))) > 0.0 else 0.0
							plyr_avg[shot_key + ' OP'] = 1.0 if (team_histories[team]['oppo_' + 'avg'][shot_key] - (league_stats[shot_key] / float(len(lg_teams)))) > 0.0 else 0.0
						if shot_key[-1] == 'P':
							plyr_avg[shot_key + ' TM'] = team_histories[team]['avg'][shot_key]
							plyr_avg[shot_key + ' OP'] = team_histories[team]['oppo_' + 'avg'][shot_key]
				'''

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

				# adv stat: average pos
				plyr_avg['POS'] = TM_POS / float(history_steps - 1)

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

				# line stat and rest stat: value given for home players, 0 for away players
				if plyr_avg['TEAM'] == 1:
					plyr_avg['LINE'] = line_value
					plyr_avg['REST'] = home_rest
				if plyr_avg['TEAM'] == 0:
					plyr_avg['LINE'] = 0.0
					plyr_avg['REST'] = away_rest

				# inserting into CNN dict while maintaining data order
				for key in sorted(plyr_avg):
					if player_game_counter == 0:
						revs_header.append(key)
					player_list.append(plyr_avg[key])
				if player_game_counter == 0:
					k = len(player_list)
					k2 = k*2
					W.append([0.0]*k)
					W2.append([0.0]*k2)
					print 'number of attributes: ' + str(k)
					print 'width of images: ' + str(k2) + '\n'
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

			text_list = []
			for m in range(0,padding):
				player_game_player_game_key = home_player_list[m] + '_' + away_player_list[m]
				if player_game_player_game_key == '-1_-1':
					player_game_player_game_key = '-1'
				player_player_row = W[word_idx_map[home_player_list[m]]] + W[word_idx_map[away_player_list[m]]]
				W2.append(player_player_row)
				player_game_counter2 = player_game_counter2 + 1
				word_idx_map2[player_game_player_game_key] = player_game_counter2
				text_list.append(player_game_player_game_key)
			text = ' '.join(text_list)

			datum  = {"y": y,
					  "text": text,
					  "game_id": game_id,
					  "split": np.random.randint(0,cv)}
			revs.append(datum)
			file_counter = file_counter + 1
			print 'finished preping file: ' + str(file_counter) + ', game: ' + file_suffix
			f.close()

			if file_counter == 5:
				break
		break


	# player translation output
	f_plyr = open('5_prediction_process_data_running_stats_players.txt','w')
	f_plyr.write('player_id,player_name\n')
	for plyr in player_translations:
		f_plyr.write(str(plyr) + ',' + player_translations[plyr] + '\n')
	f_plyr.close()


	# final output for model input
	print "rows pulled: " + str(file_counter)
	print "revs length: " + str(len(revs))
	print 'datum length: ' + str(len(datum))
	W3 = np.zeros(shape=(player_game_counter2+1, k2))
	for z in range(0,len(W2)):
		W3[z] = W2[z]
	return revs, W3, word_idx_map2, revs_header



# HARDCODED STATS
def hardcode_stats():
	good_stats_list = ['MIN','FGM','FGA','FG_PCT','FG3M','FG3A','FG3_PCT','FTM','FTA',
						'FT_PCT','OREB','DREB','REB','AST','STL','BLK','TO','PF',
						'PTS','PLUS_MINUS']
	action_types = {
		'Alley Oop Dunk Shot': 1, 'Alley Oop Layup shot': 1,
		'Driving Bank Hook Shot': 1, 'Driving Bank shot': 1, 'Driving Dunk Shot': 1, 'Driving Finger Roll Layup Shot': 1, 'Driving Hook Shot': 1, 'Driving Jump shot': 1, 'Driving Layup Shot': 1, 'Driving Reverse Layup Shot': 1, 'Driving Slam Dunk Shot': 1,
		'Dunk Shot': 1,
		'Fadeaway Bank shot': 1,
		'Fadeaway Jump Shot': 1,
		'Finger Roll Layup Shot': 1,
		'Floating Jump shot': 1,
		'Hook Bank Shot': 1, 'Hook Shot': 1,
		'Jump Bank Hook Shot': 1, 'Jump Bank Shot': 1, 'Jump Hook Shot': 1, 'Jump Shot': 1,
		'Layup Shot': 1,
		'Pullup Bank shot': 1, 'Pullup Jump shot': 1,
		'Putback Dunk Shot': 1, 'Putback Layup Shot': 1, 'Putback Reverse Dunk Shot': 1, 'Putback Slam Dunk Shot': 1,
		'Reverse Dunk Shot': 1, 'Reverse Layup Shot': 1, 'Reverse Slam Dunk Shot': 1,
		'Running Bank Hook Shot': 1, 'Running Bank shot': 1, 'Running Dunk Shot': 1, 'Running Finger Roll Layup Shot': 1, 'Running Hook Shot': 1, 'Running Jump Shot': 1, 'Running Layup Shot': 1, 'Running Reverse Layup Shot': 1, 'Running Slam Dunk Shot': 1, 'Running Tip Shot': 1,
		'Slam Dunk Shot': 1,
		'Step Back Jump shot': 1,
		'Tip Shot': 1,
		'Turnaround Bank Hook Shot': 1, 'Turnaround Bank shot': 1, 'Turnaround Fadeaway shot': 1, 'Turnaround Hook Shot': 1, 'Turnaround Jump Shot': 1
	}
	shot_zones_basic = {
		'Above the Break 3': 1,
		'Backcourt': 1,
		'In The Paint (Non-RA)': 1,
		'Left Corner 3': 1,
		'Mid-Range': 1,
		'Restricted Area': 1,
		'Right Corner 3': 1
	}
	shot_zones_areas = {
		'Back Court(BC)': 1,
		'Center(C)': 1,
		'Left Side Center(LC)': 1, 'Left Side(L)': 1,
		'Right Side Center(RC)': 1,'Right Side(R)': 1
	}
	return good_stats_list, action_types, shot_zones_basic, shot_zones_areas



# MAIN
if __name__=="__main__":

	hardcoded_stat_list = hardcode_stats()
	revs, W, word_idx_map, revs_header = build_data_cv(hardcoded_stat_list, cv=10, clean_string=True)
	#cPickle.dump([revs, W, word_idx_map], open("mr_prediction.p", "wb"))
	#print "dataset created!"

	# outputting full formatted file for visual checking
	full_output = {}
	full_output['games'] = {}
	full_output['header'] = ', '.join(map(str,revs_header))
	head_len = len(revs_header)
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
			full_output['games'][game_id]['image'][str(j)]['player_game_player_game'] = map_key[j]
			full_output['games'][game_id]['image'][str(j)]['keyed_data'] = {}
			for q in range(0,len(list_val)):
				if q < head_len:
					cur_key = str('home_' + revs_header[q])
				else:
					cur_key = str('away_' + revs_header[q - head_len])
				full_output['games'][game_id]['image'][str(j)]['keyed_data'][cur_key] = list_val[q]
	f_out = open('5_prediction_process_data_running_stats_checking.txt','w')
	f_out.write(str(full_output))
	f_out.close()
	print "checked file created!"
