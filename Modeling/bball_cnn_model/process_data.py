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

def build_data_cv(good_stats_list, cv=10, clean_string=True):
	"""
	Loads data and split into 10 folds.
	"""

	revs = []
	k = len(good_stats_list) + 1
	W = []
	W.append([0.0]*k)
	word_idx_map = {}
	word_idx_map['-1'] = 0
	file_counter = 0
	player_game_counter = 0

	print 'pulling data ...'
	for c in range(1,15):
		year_indicator = "%02d" % (c,)
		for m in range(1,10000):

			# loading data
			print 'preping file: ' + str(file_counter+1)
			game_indicator = "%05d" % (m,)
			try:
				f = open('/Users/terryjames/Dropbox/NBA data/json/bs_002' + year_indicator + game_indicator + '.json','r')
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

			# determining winner
			pts_index = line_score_header.index('PTS')
			team_index = line_score_header.index('TEAM_ID')
			for team in line_score:
				if team[team_index] == home_team:
					home_score = team[pts_index]
				if team[team_index] == away_team:
					away_score = team[pts_index]
			if home_score > away_score:
				y = 1 # means home wins
			else:
				y = 0 # means home loses

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
			for player in player_stats:

				player_dict = {}
				player_list = []
				player_index = player_stats_header.index('PLAYER_ID')
				player_game_key = str(player[player_index]) + '_' + str(player[game_index])

				# skip player if not in game
				if player[point_index] == None:
					continue

				# iterating through stats and adding team stat
				for i in range(0,len(player)):
					if player_stats_header[i] in good_stats_list:
						player_dict[player_stats_header[i]] = float(player[i])
				if player[team_index] == home_team:
					player_dict['TEAM'] = 1 # 1 means home team
					home_player_sort_list.append(player_dict[sort_key])
					home_player_list.append(player_game_key)
				if player[team_index] == away_team:
					player_dict['TEAM'] = 0 # 0 means away team
					away_player_sort_list.append(player_dict[sort_key])
					away_player_list.append(player_game_key)

				# maintaining data order
				for key in sorted(player_dict):
					player_list.append(player_dict[key])
				W.append(player_list)
				player_game_counter = player_game_counter + 1
				word_idx_map[player_game_key] = player_game_counter

			# formatting data output
			padding = 15
			home_player_list = [x for (y,x) in sorted(zip(home_player_sort_list,home_player_list), reverse=True)]
			away_player_list = [x for (y,x) in sorted(zip(away_player_sort_list,away_player_list), reverse=True)]
			while len(home_player_list) < padding :
				home_player_list.append('-1')
			while len(away_player_list) < padding :
				away_player_list.append('-1')
			text = ' '.join(home_player_list + ['-1']*padding  + away_player_list)
			datum  = {"y": y,
					  "text": text,
					  "split": np.random.randint(0,cv)}
			revs.append(datum)
			file_counter = file_counter + 1
			f.close()

	print "rows pulled: " + str(file_counter)
	print 'datum length: ' + str(len(datum))
	W2 = np.zeros(shape=(player_game_counter+1, k))
	for z in range(0,len(W)):
		W2[z] = W[z]
	return revs, W2, word_idx_map

if __name__=="__main__":
	good_stats_list = ['FGM','FGA','FG_PCT','FG3M','FG3A','FG3_PCT','FTM','FTA',
						'FT_PCT','OREB','DREB','REB','AST','STL','BLK','TO','PF',
						'PTS','PLUS_MINUS']
	revs, W, word_idx_map = build_data_cv(good_stats_list, cv=10, clean_string=True)
	cPickle.dump([revs, W, word_idx_map], open("mr.p", "wb"))
	print "dataset created!"

	# ADD 20 ROWS OF ZERO PADDING
	# SHUFFLE NOT RANDOM (BY POINTS COUNT LATER)
