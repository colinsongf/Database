
    df_lines = df_lines.query('season >= ' + min_year + 'and season <= ' + max_year)
    select a season of df_lines and same season of df_bs
    # import df_lines
    for index, row in df_lines.iterrows():
        if (!index):
            continue

        determine_player_list(game_id)
            query previous team boxscore for homeplayerlist
            query previous away team boxscore for awayplayerlist
            playerlist = home+ away
            return playerlist

        for player in playerlist
            calc_averages_data(game_id, player_id)
                query X previous games for this player
                calc_sum_stats
                calc_average_stats
                calc_shots_stats
                return sums, averages, game_list of X previous games

                calculate_team_stats(game_list)
                return team_stats

                calculate_advanced_stats(sum_stats, average_stats, team_stats)
                return advanced_stats

                stats_list = sum_stats + average_stats + advanced_stats + shots_stats




def record_player_history(df_players,player_histories,history_steps,home_team,away_team,running_mode):

	# specifying required number of historical steps needed for a player
	required_steps = '1' if running_mode == 'predict' else str(history_steps-1)

	# iterating over all players
	num_players = 0; history_met = 1; home_players = []; away_players = []; player_teams = {}

    df_players

    # determining current player teams
    for index, row in df_players.iterrows():
        player_key = index
        player_teams[player_key] = row['TEAM_ID']
        player_game_key = str(player_key) + '_' + str(row['GAME_ID'])

		if player_teams[player_key] == home_team:
			home_players.append(player_game_key)
		if player_teams[player_key] == away_team:
			away_players.append(player_game_key)

		# skip player if player did not play at any time in current game
		if player[player_stats_header.index('MIN')] == None:
			continue

		# adding player to history dictionary if non-existant
		if player_key not in player_histories:
			player_histories[player_key] = {}
			for b in range(0,history_steps):
				player_histories[player_key][str(b)] = {}
			player_histories[player_key]['sum'] = {}; player_histories[player_key]['avg'] = {}

		# recording player history
		for b in range(history_steps-1,0,-1):
			player_histories[player_key][str(b)] = copy.deepcopy(player_histories[player_key][str(b-1)])
		player_histories[player_key]['0'] = {}
		if player_histories[player_key][required_steps] == {}:
			history_met = 0
		else:
			for key in player_histories[player_key]['1']:
				if key not in ['TEAM_ID','TEAM']:

					# iterating over each history step
					cur_sum = 0.0; cur_counter = 0.0
					for b in range(1,history_steps):

						# if predict mode, non-full history is acceptable
						if running_mode == 'predict':
							if key in player_histories[player_key][str(b)]:
								cur_sum = cur_sum + player_histories[player_key][str(b)][key]
								cur_counter = cur_counter + 1.0
						else:
							cur_sum = cur_sum + player_histories[player_key][str(b)][key]
					cur_avg = (cur_sum / cur_counter) if running_mode == 'predict' else (cur_sum / float(history_steps - 1))
					player_histories[player_key]['sum'][key] = cur_sum; player_histories[player_key]['avg'][key] = cur_avg

			# track number of games played for predict mode
			player_histories[player_key]['sum']['game_counter'] = cur_counter

	# ensure sufficient history even for players who did not have history updates
	num_home_players = 0; num_away_players = 0; temp_home_players = []; temp_away_players = []

    for index, row in df_players.iterrows():
        player_key = index
        player_teams[player_key] = row['TEAM_ID']
		if player_teams[player_key] == home_team:
			if num_home_players >= 9:
				continue
			temp_home_players.append(home_players[num_home_players])
			num_home_players = num_home_players + 1

		if player_teams[player_key] == away_team:
			if num_away_players >= 9:
				continue
			temp_away_players.append(away_players[num_away_players])
			num_away_players = num_away_players + 1

		if player_key in player_histories:
			if player_histories[player_key][required_steps] == {}:
				history_met = 0
		else:
			history_met = 0

	home_players = temp_home_players
	away_players = temp_away_players

	# returning player histories and indicator for sufficient history
	return player_histories, history_met, home_players, away_players, player_teams
