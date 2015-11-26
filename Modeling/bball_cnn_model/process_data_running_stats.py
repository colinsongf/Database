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



#########################
###  HARDCODED STATS  ###
#########################

good_stats_list = ['MIN','FGM','FGA','FG_PCT','FG3M','FG3A','FG3_PCT','FTM','FTA',
					'FT_PCT','OREB','DREB','REB','AST','STL','BLK','TO','PF',
					'PTS','PLUS_MINUS']
actions = {
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

periods = ['P1','P2','P3','P4','Pover']
quarter_timeframe = {'GE10':'12-10', 'LE2':'2-0', 'ALL':'12-0'}
metric_type = ['FGMade','FGMiss','FGB']

fgmade_suffix = '_FGMade'
fgmiss_suffix = '_FGMiss'
fgb_suffix = '_FGB'



############################################
###  FUNCTIONS FOR CREATING GAME IMAGES  ###
############################################

# Determining If Valid Arguments
def arguments_validation(arguments):

	# Specifying Running Mode
	if len(arguments) != 3:
		print "\nthis script needs the arguments {'model' or 'predict'} and {'cnn' or 'trees'} \n"
		sys.exit()
	else:
		if (arguments[1] not in ['model','predict']) and (arguments[2] not in ['cnn','trees']):
			print "\nthis script needs the arguments {'model' or 'predict'} and {'cnn' or 'trees'} \n"
			sys.exit()
		else:
			running_mode = arguments[1]
			model_mode = arguments[2]
	return running_mode, model_mode



# template initialized for player special stats
def initial_special_stats():

	# main template dictionaries
	plyr_act_init = {}
	plry_shot_time_init = {}

	# adding actions and distinct metrics to template dictionary
	for action in actions:
		plyr_act_init[action] = 0.0
	for timeframe in quarter_timeframe:
		plyr_act_init['avg_dist_' + timeframe] = 0.0

	# adding period / timeframe based shooting metrics to template dictionary
	for period in periods:
		for timeframe in quarter_timeframe:
			for metric in metric_type:
				key = period + '_' + timeframe + '_' + metric
				plyr_act_init[key] = 0.0
				plry_shot_time_init[key] = 0.0

	# returning template dictionaries
	return plyr_act_init, plry_shot_time_init



# grabbing line data for all games
def pull_line_data():

	# iterating over all games to pull line data
	print '\npulling line data'
	line_counter = -1; line_header = []; line_data = {}
	f = open('/Users/terryjames/Dropbox/Public/NBA_data/NBA OU data (since 2000).csv','r')
	for line in f:
		rows = line.split('\r')
		for row in rows:

			# storing header order on 0th row
			line_counter = line_counter + 1
			entries = row.split(',')
			if line_counter == 0:
				for entry in entries:
					line_header.append(entry)
				game_id_index = line_header.index('game_id')
				line_stats = {'Line':-100,'ATSr':-100,'Rest':-100}
				for key in line_stats:
					line_stats[key] = line_header.index(key)

			# storing line data on all other rows
			else:
				game_id = entries[game_id_index]
				line_data[game_id] = {}
				for key in line_stats:
					line_data[game_id][key] = entries[line_stats[key]]

	# closing file and returning all line data
	f.close(); return line_data



# loading data from current bs file
def loading_game_from_bs(c,m,file_counter,max_year,history_window):

	# loading bs game file
	year_indicator = "%02d" % (c,)
	if file_counter == -1:
		print 'year range: ' + year_indicator + ' - ' + ("%02d" % (max_year,))
		print 'history window: ' + str(history_window)
		file_counter = file_counter + 1
	game_indicator = "%05d" % (m,)
	try:
		file_suffix = 'bs_002' + year_indicator + game_indicator
		file_name = '/Users/terryjames/Dropbox/Public/NBA_data/json/' + file_suffix + '.json'
		f = open(file_name,'r')
	except:
		return -1, file_counter, 'bad_suffix'

	# storing game data
	game_dict = {}
	for line in f:
		data = json.loads(line)
		game_dict['game_summary_header'] = data[0]["headers"]; game_dict['game_summary'] = data[0]["rowSet"][0]
		game_dict['line_score_header'] = data[1]["headers"]; game_dict['line_score'] = data[1]["rowSet"]
		game_dict['player_stats_header'] = data[4]['headers']; game_dict['player_stats'] = data[4]["rowSet"]
	f.close()

	# returning dictionary with essential basic game data
	return game_dict, file_counter, file_suffix



# determining home and away teams
def determine_teams(game_summary_header,game_summary):

	# grabbing from game summary section of bs file
	home_team_index = game_summary_header.index('HOME_TEAM_ID')
	away_team_index = game_summary_header.index('VISITOR_TEAM_ID')
	home_team = game_summary[home_team_index]
	away_team = game_summary[away_team_index]
	team_dict = {}; team_dict[home_team] = {}; team_dict[away_team] = {}
	return team_dict, home_team, away_team



# determining whether the home team covered for each game (binary categorization)
def determine_cover(line_data,game_id):

	# calculating game outcome where y = 1 means home team covers
	if (line_data[game_id]['Line'] != '') and (line_data[game_id]['ATSr'] != ''):
		line_value = float(line_data[game_id]['Line'])
		y = 1 if line_data[game_id]['ATSr'] == 'W' else 0
		return y, line_value
	else:
		return '', line_data[game_id]['Line']



# determining the rest stat for each team
def determine_rest(line_score_header,line_score,game_id,home_team,away_team,line_data):

	# retrieving rest in terms of consecutive games played for both teams
	team_index = line_score_header.index('TEAM_ID')
	for team in line_score:
		rest_list = (line_data[game_id]['Rest']).split('&')
		if team[team_index] == home_team:
			home_rest = rest_list[0]
			home_rest = 0.0 if home_rest == '' else float(home_rest)
		if team[team_index] == away_team:
			away_rest = rest_list[1]
			away_rest = 0.0 if away_rest == '' else float(away_rest)
	return home_rest, away_rest



# recording player historical averages and determining current player teams
def record_player_history(player_stats,player_stats_header,player_histories,history_steps,home_team,away_team):

	# iterating over all players
	history_met = 1; home_players = []; away_players = []; player_teams = {}
	for player in player_stats:

		# skip player if player did not play at any time in current game
		if player[player_stats_header.index('MIN')] == None:
			continue

		# adding player to history dictionary if non-existant
		player_key = player[player_stats_header.index('PLAYER_ID')]
		if player_key not in player_histories:
			player_histories[player_key] = {}
			for b in range(0,history_steps):
				player_histories[player_key][str(b)] = {}
			player_histories[player_key]['sum'] = {}; player_histories[player_key]['avg'] = {}

		# recording player history
		for b in range(history_steps-1,0,-1):
			player_histories[player_key][str(b)] = copy.deepcopy(player_histories[player_key][str(b-1)])
		player_histories[player_key]['0'] = {}
		if player_histories[player_key][str(history_steps-1)] == {}:
			history_met = 0
		else:
			for key in player_histories[player_key]['1']:
				if key not in ['TEAM_ID','TEAM']:
					cur_sum = 0.0
					for b in range(1,history_steps):
						cur_sum = cur_sum + player_histories[player_key][str(b)][key]
					cur_avg = cur_sum / float(history_steps - 1)
					player_histories[player_key]['sum'][key] = cur_sum; player_histories[player_key]['avg'][key] = cur_avg

		# determining current player teams
		player_teams[player_key] = player[player_stats_header.index('TEAM_ID')]
		player_game_key = str(player_key) + '_' + str(player[player_stats_header.index('GAME_ID')])
		if player_teams[player_key] == home_team:
			home_players.append(player_game_key)
		if player_teams[player_key] == away_team:
			away_players.append(player_game_key)

	# returning player histories and indicator for sufficient history
	return player_histories, history_met, home_players, away_players, player_teams



# recording team and opponent historical averages
def record_team_history(team_histories,home_team,away_team,history_steps):

	# iterating over both teams
	team_history_met = 1
	team_list = [home_team,away_team]
	for team in team_list:

		# adding team to history dictionary if non-existant
		if team not in team_histories:
			team_histories[team] = {}
			for b in range(0,history_steps):
				team_histories[team][str(b)] = {}; team_histories[team]['oppo_' + str(b)] = {}
			team_histories[team]['sum'] = {}; team_histories[team]['avg'] = {}
			team_histories[team]['oppo_' + 'sum'] = {}; team_histories[team]['oppo_' + 'avg'] = {}

	# recording team history
	team_counter = 0
	for team in team_list:
		oppo_team = team_list[team_counter * (-1) + 1]
		for b in range(history_steps-1,0,-1):
			team_histories[team][str(b)] = copy.deepcopy(team_histories[team][str(b-1)])
			team_histories[team]['oppo_' + str(b)] = copy.deepcopy(team_histories[team]['oppo_' + str(b-1)])
		team_histories[team]['0'] = {}; team_histories[team]['oppo_' + '0'] = {}
		if (team_histories[team][str(history_steps-1)] == {}) or (team_histories[team]['oppo_' + str(history_steps-1)] == {}):
			team_history_met = 0
		else:
			for key in team_histories[team]['1']:
				if key not in ['GAME_ID','TEAM_ID','TEAM','FG_PCT','FG3_PCT','FT_PCT','PLUS_MINUS']:
					cur_sum = 0.0; cur_oppo_sum = 0.0
					for b in range(1,history_steps):
						cur_sum = cur_sum + team_histories[team][str(b)][key]
						cur_oppo_sum = cur_oppo_sum + team_histories[team]['oppo_' + str(b)][key]
					cur_avg = cur_sum / float(history_steps - 1)
					cur_oppo_avg = cur_oppo_sum / float(history_steps - 1)
					team_histories[team]['sum'][key] = cur_sum; team_histories[team]['avg'][key] = cur_avg
					team_histories[team]['oppo_' + 'sum'][key] = cur_oppo_sum; team_histories[team]['oppo_' + 'avg'][key] = cur_oppo_avg
		team_counter = team_counter + 1

	# returning team histories and indicator for sufficient history
	return team_histories, team_history_met



# calculate team and opponent stats from historical data
def calculate_team_stats(team_histories,home_team,away_team):

	# iterating over both teams
	team_list = [home_team,away_team]
	for team in team_list:

		# storing team and opponent stats temporarily for advanced stats calculations
		TM_MP = team_histories[team]['sum']['MIN']; OP_MP = team_histories[team]['oppo_' + 'sum']['MIN']
		TM_PTS = team_histories[team]['sum']['PTS']; OP_PTS = team_histories[team]['oppo_' + 'sum']['PTS']
		TM_FGA = team_histories[team]['sum']['FGA']; OP_FGA = team_histories[team]['oppo_' + 'sum']['FGA']
		TM_FTA = team_histories[team]['sum']['FTA']; OP_FTA = team_histories[team]['oppo_' + 'sum']['FTA']
		TM_ORB = team_histories[team]['sum']['OREB']; OP_ORB = team_histories[team]['oppo_' + 'sum']['OREB']
		TM_DRB = team_histories[team]['sum']['DREB']; OP_DRB = team_histories[team]['oppo_' + 'sum']['DREB']
		TM_FGM = team_histories[team]['sum']['FGM']; OP_FGM = team_histories[team]['oppo_' + 'sum']['FGM']
		TM_TOV = team_histories[team]['sum']['TO']; OP_TOV = team_histories[team]['oppo_' + 'sum']['TO']

		# advanced team stats
		team_histories[team]['sum']['TRB'] = TM_ORB + TM_DRB
		team_histories[team]['oppo_' + 'sum']['TRB'] = OP_ORB + OP_DRB
		TM_POS = 0.5*((TM_FGA+0.4*TM_FTA-1.07*(TM_ORB/(TM_ORB+OP_DRB))*(TM_FGA-TM_FGM)+TM_TOV)+(OP_FGA+0.4*OP_FTA-1.07*(OP_ORB/(OP_ORB+TM_DRB))*(OP_FGA-OP_FGM)+OP_TOV))
		OP_POS = TM_POS
		team_histories[team]['sum']['POS'] = TM_POS
		team_histories[team]['oppo_' + 'sum']['POS'] = OP_POS
		team_histories[team]['sum']['PACE'] = 48.0*((TM_POS+OP_POS)/(2.0*(TM_MP/5.0)))
		team_histories[team]['oppo_' + 'sum']['PACE'] = 48.0*((OP_POS+TM_POS)/(2.0*(OP_MP/5.0)))

	# returing team histories dictionary with newly added stats
	return team_histories



# calculate league stats from historical data
def calculate_league_stats(team_histories,home_team,history_steps):

	# forming league stats template dictionaries
	lg_games = {}; lg_teams = {}; league_stats = {}
	for key in team_histories[home_team]['sum']:
		if key not in ['GAME_ID','TEAM_ID','TEAM','FG_PCT','FG3_PCT','FT_PCT','PLUS_MINUS']:
			league_stats[key] = 0.0

	# calculating summations of teams stats (where team stat history exists: aka POS available) to form league stats
	for team in team_histories:
		if 'POS' in team_histories[team]['sum']:
			for key in league_stats:
				league_stats[key] = league_stats[key] + team_histories[team]['sum'][key]

			# tracking number of distinct teams and games played for league stats
			lg_teams[team] = 1
			for b in range(1,history_steps):
				lg_games[team_histories[team][str(b)]['GAME_ID']] = 1

	# advanced league stats
	league_stats['PACE'] = league_stats['PACE'] / float(len(lg_teams))
	league_stats['PPG'] = league_stats['PTS'] / float(len(lg_games))

	# return league stats dictionary
	return league_stats



# calculating advanced stats for a player
def calculate_advanced_stats(plyr_avg,plyr_sum,team_histories,team,league_stats,history_window):

	# storing team and opponent stats temporarily for advanced stats calculations
	TM_MP = team_histories[team]['sum']['MIN']; OP_MP = team_histories[team]['oppo_' + 'sum']['MIN']
	TM_AST = team_histories[team]['sum']['AST']; OP_AST = team_histories[team]['oppo_' + 'sum']['AST']
	TM_BLK = team_histories[team]['sum']['BLK']; OP_BLK = team_histories[team]['oppo_' + 'sum']['BLK']
	TM_STL = team_histories[team]['sum']['STL']; OP_STL = team_histories[team]['oppo_' + 'sum']['STL']
	TM_PF = team_histories[team]['sum']['PF']; OP_PF = team_histories[team]['oppo_' + 'sum']['PF']
	TM_PTS = team_histories[team]['sum']['PTS']; OP_PTS = team_histories[team]['oppo_' + 'sum']['PTS']
	TM_FGA = team_histories[team]['sum']['FGA']; OP_FGA = team_histories[team]['oppo_' + 'sum']['FGA']
	TM_FTA = team_histories[team]['sum']['FTA']; OP_FTA = team_histories[team]['oppo_' + 'sum']['FTA']
	TM_FTM = team_histories[team]['sum']['FTM']; OP_FTM = team_histories[team]['oppo_' + 'sum']['FTM']
	TM_3PA = team_histories[team]['sum']['FG3A']; OP_3PA = team_histories[team]['oppo_' + 'sum']['FG3A']
	TM_3PM = team_histories[team]['sum']['FG3M']; OP_3PM = team_histories[team]['oppo_' + 'sum']['FG3M']
	TM_ORB = team_histories[team]['sum']['OREB']; OP_ORB = team_histories[team]['oppo_' + 'sum']['OREB']
	TM_FGM = team_histories[team]['sum']['FGM']; OP_FGM = team_histories[team]['oppo_' + 'sum']['FGM']
	TM_TOV = team_histories[team]['sum']['TO']; OP_TOV = team_histories[team]['oppo_' + 'sum']['TO']
	TM_DRB = team_histories[team]['sum']['DREB']; OP_DRB = team_histories[team]['oppo_' + 'sum']['DREB']
	TM_TRB = team_histories[team]['sum']['TRB']; OP_TRB = team_histories[team]['oppo_' + 'sum']['TRB']
	TM_POS = team_histories[team]['sum']['POS']; OP_POS = team_histories[team]['oppo_' + 'sum']['POS']
	TM_PACE = team_histories[team]['sum']['PACE']; OP_PACE = team_histories[team]['oppo_' + 'sum']['PACE']

	# adv stat: average pos
	plyr_avg['POS'] = TM_POS / float(history_window)

	# adv stat: average pace
	plyr_avg['PACE'] = TM_PACE / float(history_window)

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

	# return player stats dictionary supplemented with advanced stats
	return plyr_avg



# creating CNN player row while maintaining data order
def forming_cnn_player_row(W,W2,word_idx_map,player_game_idx,plyr_avg,revs_header,player_game_key):

	# forming sorted player row
	player_list = []
	for key in sorted(plyr_avg):
		if player_game_idx == 0:
			revs_header.append(key)
		player_list.append(plyr_avg[key])

	# specifying image row parameters on first pass through
	k = len(player_list); k2 = k*2
	if player_game_idx == 0:
		W.append([0.0]*k); W2.append([0.0]*k2)
		print 'number of attributes: ' + str(k) + '\nwidth of images: ' + str(k2) + '\n'

	# adding player rows to master cnn dictionaries and returning updates
	W.append(player_list)
	player_game_idx = player_game_idx + 1
	word_idx_map[player_game_key] = player_game_idx
	return W, W2, k, k2, word_idx_map, player_game_idx, revs_header



# formatting and storing game cnn image
def storing_cnn_image(y,game_id,home_players,away_players,W,W2,word_idx_map,word_idx_map2,player_game_idx2,revs):

	# adding padding to make player lists the same length
	padding = 15; padding_2 = 5
	while len(home_players) < padding :
		home_players.append('-1')
	while len(away_players) < padding :
		away_players.append('-1')

	# concatenating team players (home + away) rows to make single rows for image
	text_list = []
	for m in range(0,padding):
		player_game_player_game_key = home_players[m] + '_' + away_players[m]
		if player_game_player_game_key == '-1_-1':
			player_game_player_game_key = '-1'
		player_player_row = W[word_idx_map[home_players[m]]] + W[word_idx_map[away_players[m]]]
		W2.append(player_player_row)
		player_game_idx2 = player_game_idx2 + 1
		word_idx_map2[player_game_player_game_key] = player_game_idx2
		text_list.append(player_game_player_game_key)

	# forming final game image
	text = ' '.join(text_list)
	datum  = {"y": y,
			  "text": text,
			  "game_id": game_id,
			  "split": np.random.randint(0,10)}
	revs.append(datum)

	# returning updated cnn dictionary mappings and images
	return W2, player_game_idx2, word_idx_map2, revs



# adding stats required from current game
def set_current_game_specific_stats(plyr_avg,team,line_value,home_team,home_rest,away_rest):

	# 1 means home team, 0 means away team. line only specified for home team
	plyr_avg['TEAM'] = 1 if team == home_team else 0
	plyr_avg['LINE'] = line_value if plyr_avg['TEAM'] == 1 else 0.0
	plyr_avg['REST'] = home_rest if plyr_avg['TEAM'] == 1 else away_rest
	return plyr_avg



# loading player shot data for current game
def pull_shots_data(game_id,plry_shot_time_init):

	# opening shots file
	shots_prefix = '/Users/terryjames/Dropbox/Public/NBA_data/json/'
	shots_file = shots_prefix + 'shots_' + str(game_id) + '.json'
	f_shots = open(shots_file,'r')
	shot_action_zones = {}
	plyr_shots = {}; plyr_shots_time = {}; plyr_shots_loc = {}

	# determining file indicies
	for shots_line in f_shots:
		shots_dict = json.loads(shots_line)
		shots_header = shots_dict['headers']
		shots_rows = shots_dict['rowSet']
		shot_indicies = {'ACTION_TYPE':-100, 'PLAYER_ID':-100, 'TEAM_ID':-100, 'PERIOD':-100, 'MINUTES_REMAINING':-100,
			'SHOT_ZONE_BASIC':-100, 'SHOT_ZONE_AREA':-100, 'SHOT_ATTEMPTED_FLAG':-100, 'SHOT_MADE_FLAG':-100, 'SHOT_DISTANCE':-100}
		for idx in shot_indicies:
			shot_indicies[idx] = shots_header.index(idx)

		# iterating through each shot
		for row in shots_rows:

			# only check if shot attempted
			if row[shot_indicies['SHOT_ATTEMPTED_FLAG']] == 1:

				# ignore shot if unrecognized action performed
				action_type = row[shot_indicies['ACTION_TYPE']]
				if action_type not in actions:
					continue

				# initializing shot action, location, and shot time (when in game) dictionaries for a new player
				player = row[shot_indicies['PLAYER_ID']]
				if player not in plyr_shots:
					plyr_shots[player] = {}
					plyr_shots_time[player] = copy.deepcopy(plry_shot_time_init)
					plyr_shots_loc[player] = {}
					for timeframe in quarter_timeframe:
						plyr_shots_loc[player]['avg_dist_' + timeframe] = 0.0
						plyr_shots_loc[player]['avg_dist_' + timeframe + '_c'] = 0.0

				# adding new shot types (actions) to a player's dictionary
				if action_type not in plyr_shots[player]:
					plyr_shots[player][action_type] = {}
					for metric in metric_type:
						plyr_shots[player][action_type][metric] = 0.0

				# shot location-timeframe related stats
				timing = 'ALL'; timing_dict = {}; timing_dict[timing] = 1
				for timeframe in quarter_timeframe:
					bounds = quarter_timeframe[timeframe].split('-')
					if (row[shot_indicies['MINUTES_REMAINING']] <= int(bounds[0])) and (row[shot_indicies['MINUTES_REMAINING']] >= int(bounds[1])):
						dist_loc = row[shot_indicies['SHOT_DISTANCE']]
						plyr_shots_loc[player]['avg_dist_' + timeframe] = dist_loc + plyr_shots_loc[player]['avg_dist_' + timeframe]
						plyr_shots_loc[player]['avg_dist_' + timeframe + '_c'] = 1.0 + plyr_shots_loc[player]['avg_dist_' + timeframe + '_c']
						timing = timeframe if timing == 'ALL' else timing
						timing_dict[timing] = 1

				# determining shot outcome (made vs miss)
				miss_change = 1.0 if row[shot_indicies['SHOT_MADE_FLAG']] == 0 else 0.0
				made_change = 1.0 if row[shot_indicies['SHOT_MADE_FLAG']] == 1 else 0.0

				# shot time related stats
				period_str = 'Pover' if row[shot_indicies['PERIOD']] >= 5 else 'P' + str(row[shot_indicies['PERIOD']])
				for key in timing_dict:
					timing_key = period_str + '_' + key
					plyr_shots_time[player][timing_key + fgmiss_suffix] = plyr_shots_time[player][timing_key + fgmiss_suffix] + miss_change
					plyr_shots_time[player][timing_key + fgmade_suffix] = plyr_shots_time[player][timing_key + fgmade_suffix] + made_change

				# shot type (action) stats
				plyr_shots[player][action_type]['FGMiss'] = plyr_shots[player][action_type]['FGMiss'] + miss_change
				plyr_shots[player][action_type]['FGMade'] = plyr_shots[player][action_type]['FGMade'] + made_change

	return plyr_shots, plyr_shots_time, plyr_shots_loc



# loading current game stats for current player
def pull_player_stats(player,plyr_act_init,player_stats_header,plyr_shots_time,plyr_shots,plyr_shots_loc,player_translations):

	# initialize player specific data structures
	player_dict = {}; player_dict = copy.deepcopy(plyr_act_init)
	player_key = player[player_stats_header.index('PLAYER_ID')]
	player_team = player[player_stats_header.index('TEAM_ID')]
	player_translations[player_key] = player[player_stats_header.index('PLAYER_NAME')].lower()

	# iterating through and pulling basic stats for the current player
	for i in range(0,len(player)):
		if player_stats_header[i] in good_stats_list:
			if player_stats_header[i] == 'MIN':
				min_parts = player[i].split(':')
				min_var = float(min_parts[0]) + (float(min_parts[1]) / 60.0)
				player_dict[player_stats_header[i]] = min_var
			else:
				player_dict[player_stats_header[i]] = float(player[i])

	# adding shot time related player stats
	if player_key in plyr_shots_time:
		for period in periods:
			for timeframe in quarter_timeframe:
				cur_key = period + '_' + timeframe
				player_dict[cur_key + fgmade_suffix] = plyr_shots_time[player_key][cur_key + fgmade_suffix]
				player_dict[cur_key + fgmiss_suffix] = plyr_shots_time[player_key][cur_key + fgmiss_suffix]
				player_dict[cur_key + fgb_suffix] = ss.beta.ppf(.1,1+player_dict[cur_key + fgmade_suffix],1+player_dict[cur_key + fgmiss_suffix])

	# adding shot type stats and shot location-timeframe stats
	if player_key in plyr_shots:
		for action in plyr_shots[player_key]:
			player_dict[action] = float(plyr_shots[player_key][action]['FGMade'])
		for timeframe in quarter_timeframe:
			if plyr_shots_loc[player_key]['avg_dist_' + timeframe + '_c'] > 0:
				player_dict['avg_dist_' + timeframe] = plyr_shots_loc[player_key]['avg_dist_' + timeframe] / plyr_shots_loc[player_key]['avg_dist_' + timeframe + '_c']

	# return current game player stats
	return player_dict, player_translations, player_team, player_key



# running summation of player stats for calculating current game team stats
def current_game_team_stats(player,player_dict,player_stats_header,team_dict,team):

	# iterating over all player stats to continue running summations
	for stat in player_dict:
		if stat not in team_dict[team]:
			team_dict[team][stat] = 0.0
		team_dict[team][stat] = team_dict[team][stat] + player_dict[stat]
	team_dict[team]['GAME_ID'] = player[player_stats_header.index('GAME_ID')]
	return team_dict



# outputting player translation file
def saving_all_players(player_translations):

	# players saved are only from games covered by current script run
	f_plyr = open('process_data_running_stats_players.txt','w')
	f_plyr.write('player_id,player_name\n')
	for plyr in player_translations:
		f_plyr.write(str(plyr) + ',' + player_translations[plyr] + '\n')
	f_plyr.close()



# final output for model input
def formatting_final_cnn_structure(file_counter,revs,player_game_idx2,k2,W2):

	# step needed for mandatory numpy datastructure
	print "rows pulled: " + str(file_counter) + "\nrevs length: " + str(len(revs))
	W3 = np.zeros(shape=(player_game_idx2+1, k2))
	for z in range(0,len(W2)):
		W3[z] = W2[z]
	return W3



# outputting full formatted file for visual checking (currently not finished because i was lazy ...)
def games_pretty_output():

	# outputting game data images in easy to read json file
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
	f_out = open('process_data_running_stats_checking.txt','w')
	f_out.write(str(full_output))
	f_out.close()
	print "checked file created!"



##########################################
###  MAIN METHOD CREATING GAME IMAGES  ###
##########################################

# main function for create cnn data images
def build_data(running_mode):

	# inital variable params
	min_year = 15; max_year = 15; history_window = 5

	# data structures for output cnn images
	revs = []; revs_header = []
	W = []; W2 = []
	word_idx_map = {}; word_idx_map2 = {}
	word_idx_map['-1'] = 0; word_idx_map2['-1'] = 0
	player_game_idx = 0; player_game_idx2 = 0

	# initializing template for special custom stats
	plyr_act_init, plry_shot_time_init = initial_special_stats()

	# loading line data for all games
	line_data = pull_line_data()

	# iterating over all games
	print 'running mode: ' + running_mode
	print 'pulling game data ...'
	file_counter = -1; player_histories = {}; player_translations = {}; team_histories = {}; history_steps = history_window + 1
	for c in range(min_year,max_year+1):
		for m in range(1,10000):

			# loading data from current bs file if it exists
			game_dict, file_counter, file_suffix = loading_game_from_bs(c,m,file_counter,max_year,history_window)
			if game_dict == -1:
				continue

			# determining teams
			team_dict, home_team, away_team = determine_teams(game_dict['game_summary_header'],game_dict['game_summary'])

			# skip game if no existing line data for the current game
			for team in game_dict['line_score']:
				game_id = team[game_dict['line_score_header'].index('GAME_ID')]
			if game_id not in line_data:
				continue

			# determining winner with respect to line
			y, line_value = determine_cover(line_data,game_id)
			if (y == '') or (line_value == ''):
				continue

			# determining rest stat for each team
			home_rest, away_rest = determine_rest(game_dict['line_score_header'],game_dict['line_score'],game_id,home_team,away_team,line_data)

			# recoring player historical averages and determining current player teams
			player_histories, history_met, home_players, away_players, player_teams = record_player_history(game_dict['player_stats'],game_dict['player_stats_header'],player_histories,history_steps,home_team,away_team)

			# recording team and opponent historical averages
			team_histories, team_history_met = record_team_history(team_histories,home_team,away_team,history_steps)

			# If player and team history lengths satisfied, begin calculating stats (based on historical games) for current game image
			if (history_met == 1) and (team_history_met == 1):

				# calculate team and opponent stats from historical data
				team_histories = calculate_team_stats(team_histories,home_team,away_team)

				# calculate league stats from historical data
				league_stats = calculate_league_stats(team_histories,home_team,history_steps)

				# iterating over all current game players
				for player_game_key in (home_players + away_players):

					# calculating advanced stats
					player_key = int(player_game_key.split('_')[0])
					plyr_sum = player_histories[player_key]['sum']; plyr_avg = player_histories[player_key]['avg'] #plyr_avg = {}
					plyr_avg = calculate_advanced_stats(plyr_avg,plyr_sum,team_histories,player_teams[player_key],league_stats,history_window)

					# adding stats required from current game
					plyr_avg = set_current_game_specific_stats(plyr_avg,player_teams[player_key],line_value,home_team,home_rest,away_rest)

					# creating CNN player row while maintaining data order
					W, W2, k, k2, word_idx_map, player_game_idx, revs_header = forming_cnn_player_row(W,W2,word_idx_map,player_game_idx,plyr_avg,revs_header,player_game_key)

				# formatting and storing current game cnn image
				W2, player_game_idx2, word_idx_map2, revs = storing_cnn_image(y,game_id,home_players,away_players,W,W2,word_idx_map,word_idx_map2,player_game_idx2,revs)
				file_counter = file_counter + 1; print 'finished preping file: ' + str(file_counter) + ', game: ' + file_suffix

			# start loading current game stats. loading player shot data
			plyr_shots, plyr_shots_time, plyr_shots_loc = pull_shots_data(game_id,plry_shot_time_init)

			# iterating over all current game players
			for player in game_dict['player_stats']:

				# skip player if player did not play at any time in current game
				if player[game_dict['player_stats_header'].index('MIN')] == None:
					continue

				# loading current game stats for current player
				player_dict, player_translations, player_team, player_key = pull_player_stats(player,plyr_act_init,game_dict['player_stats_header'],plyr_shots_time,plyr_shots,plyr_shots_loc,player_translations)

				# running summation of player stats for calculating current game team stats
				team_dict = current_game_team_stats(player,player_dict,game_dict['player_stats_header'],team_dict,player_team)

				# storing current game player dictionary in player histories dictionary
				player_histories[player_key]['0'] = copy.deepcopy(player_dict)

			# storing current game team and opponent dictionaries in team histories dictionary
			team_counter = 0
			team_list = [home_team,away_team]
			for team in team_list:
				oppo_team = team_list[team_counter * (-1) + 1]
				team_histories[team]['0'] = copy.deepcopy(team_dict[team])
				team_histories[team]['oppo_' + '0'] = copy.deepcopy(team_dict[oppo_team])
				team_counter = team_counter + 1

	# outputting player translation file
	if running_mode == 'model':
		saving_all_players(player_translations)

	# final output for model input
	W3 = formatting_final_cnn_structure(file_counter,revs,player_game_idx2,k2,W2)

	# returning all data needed for full CNN
	return revs, W3, word_idx_map2, revs_header



##############
###  MAIN  ###
##############

if __name__=="__main__":

	# specifying running mode
	running_mode, model_mode = arguments_validation(sys.argv)

	# building cnn data images
	revs, W, word_idx_map, revs_header = build_data(running_mode)
	cPickle.dump([revs, W, word_idx_map, revs_header], open(model_mode + "_" + running_mode + "_data.p", "wb"))
	print "dataset created!"

	# outputting full formatted file for visual checking (currently not finished because i was lazy ...)
	# games_pretty_output()
